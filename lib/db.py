"""Supabase client and table management for the research pipeline.

DDL (CREATE TABLE / indexes / triggers) runs through a direct psycopg2
connection because the Supabase Python client cannot execute raw DDL. DML
(select / upsert / update) uses the Supabase client. Both target the same
research project; the service key bypasses RLS.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
from supabase import Client, create_client

from lib import schema

_client: Client | None = None


def get_client() -> Client:
    """Return a cached, module-level Supabase client (service-role)."""
    global _client
    if _client is None:
        url = os.environ.get("RESEARCH_SUPABASE_URL")
        key = os.environ.get("RESEARCH_SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise RuntimeError(
                "RESEARCH_SUPABASE_URL and RESEARCH_SUPABASE_SERVICE_KEY must be set "
                "(see .env.example)."
            )
        _client = create_client(url, key)
    return _client


@contextmanager
def _pg_cursor():
    """Yield a psycopg2 cursor on the research DB; commit on success, always close."""
    dsn = os.environ.get("RESEARCH_DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "RESEARCH_DATABASE_URL must be set for DDL operations (see .env.example)."
        )
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    finally:
        conn.close()


def ensure_domain_table(domain_key: str, config: dict) -> None:
    """Create the domain's table if needed and register it in research_domains.

    The DDL is idempotent, so this is safe to call on every run — it also
    propagates any newly added indexes.
    """
    table_name = config["table_name"]
    sql = schema.get_create_table_sql(domain_key, config)
    with _pg_cursor() as cur:
        cur.execute(sql)

    # Register / refresh the domain in the registry table.
    get_client().table("research_domains").upsert(
        {
            "domain_key": domain_key,
            "display_name": config["display_name"],
            "table_name": table_name,
        },
        on_conflict="domain_key",
    ).execute()


def update_domain_stats(domain_key: str, field: str) -> None:
    """Recount the domain table and stamp a last_*_at timestamp in the registry.

    `field` is one of last_ingested_at | last_enriched_at | last_connected_at.
    The table name is resolved from the registry — never from caller input.
    """
    valid = {"last_ingested_at", "last_enriched_at", "last_connected_at"}
    if field not in valid:
        raise ValueError(f"field must be one of {sorted(valid)}, got '{field}'")

    client = get_client()
    row = (
        client.table("research_domains")
        .select("table_name")
        .eq("domain_key", domain_key)
        .single()
        .execute()
    )
    table_name = row.data["table_name"]
    record_count = count_rows(table_name)

    now_iso = datetime.now(timezone.utc).isoformat()
    client.table("research_domains").update(
        {"record_count": record_count, field: now_iso}
    ).eq("domain_key", domain_key).execute()


def count_rows(table_name: str, **eq) -> int:
    """Exact row count for a table, with optional equality filters."""
    q = get_client().table(table_name).select("id", count="exact").limit(1)
    for col, val in eq.items():
        q = q.eq(col, val)
    return q.execute().count or 0


def fetch_all(table_name: str, columns: str = "*", page: int = 1000) -> list[dict]:
    """Fetch every row from a table, paging past the PostgREST row cap."""
    client = get_client()
    rows: list[dict] = []
    start = 0
    while True:
        chunk = (
            client.table(table_name)
            .select(columns)
            .range(start, start + page - 1)
            .execute()
            .data
            or []
        )
        rows.extend(chunk)
        if len(chunk) < page:
            return rows
        start += page
