"""Postgres connections to the research corpus, one per database role.

  admin       postgres, RESEARCH_DB_PASSWORD       migrations only (migrate.py)
  pipeline    PIPELINE_DB_PASSWORD                 ingest + enrichment
  mcp_reader  MCP_READER_DB_PASSWORD               the MCP server; SELECT on the mcp_* views only

Every role goes through the Supavisor pooler in RESEARCH_DATABASE_URL (the direct host is
IPv6-only); custom roles log in as <role>.<project-ref>. Passwords are passed separately
from the DSN so special characters survive.
"""
from __future__ import annotations

import os
import time

import psycopg2
from psycopg2.extensions import parse_dsn

_PASSWORD_ENV = {
    "admin": "RESEARCH_DB_PASSWORD",
    "pipeline": "PIPELINE_DB_PASSWORD",
    "mcp_reader": "MCP_READER_DB_PASSWORD",
}
RETRYABLE = (psycopg2.OperationalError, psycopg2.InterfaceError)


def connect(role: str = "pipeline"):
    """Open a psycopg2 connection as `role` (caller commits / closes)."""
    dsn = os.environ.get("RESEARCH_DATABASE_URL")
    password = os.environ.get(_PASSWORD_ENV[role])
    if not dsn or not password:
        raise RuntimeError(f"RESEARCH_DATABASE_URL and {_PASSWORD_ENV[role]} must be set (see .env.example).")
    params = {"password": password}
    if role != "admin":
        project_ref = parse_dsn(dsn)["user"].split(".", 1)[1]
        params["user"] = f"{role}.{project_ref}"
    return psycopg2.connect(dsn, **params)


def with_retry(fn, tries: int = 6):
    """Run fn() — which must open its own connection — retrying transient pooler drops."""
    for attempt in range(tries):
        try:
            return fn()
        except RETRYABLE:
            if attempt == tries - 1:
                raise
            time.sleep(min(2 ** attempt, 20))
