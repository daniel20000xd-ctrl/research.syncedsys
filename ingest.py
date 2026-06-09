#!/usr/bin/env python3
"""Phase 1 — Ingest: pull records from a source into a domain table.

Usage: python ingest.py --domain <domain_key> [--limit N] [--dry-run]

Idempotent. New records are inserted with pipeline-state defaults (pending,
empty tag arrays). Existing records (matched on external_id) have their source
fields refreshed but their enrichment columns left untouched, so re-running
ingest never wipes Phase 2/3 work.
"""
from __future__ import annotations

import argparse
import importlib

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()  # fall back to .env

from lib import db, registry  # noqa: E402 — after dotenv

BATCH_SIZE = 100

# Pipeline-state columns: defaulted on insert, never overwritten on update.
PIPELINE_STATE_KEYS = (
    "phase2_status",
    "phase2_ran_at",
    "phase2_error",
    "structural_tags",
    "derived_tags",
    "phase3_concept_ids",
    "ingested_at",
)


def _upsert_batches(client, table: str, rows: list[dict], label: str) -> None:
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        client.table(table).upsert(batch, on_conflict="external_id,domain").execute()
        print(f"  {label}: {min(start + BATCH_SIZE, len(rows))}/{len(rows)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1 — ingest records into a domain table")
    parser.add_argument("--domain", required=True, help="domain_key from domains.yaml")
    parser.add_argument("--limit", type=int, default=None, help="cap records after fetch")
    parser.add_argument("--dry-run", action="store_true", help="print records instead of writing")
    args = parser.parse_args()

    config = registry.get_domain(args.domain)
    table = config["table_name"]

    if not args.dry_run:
        db.ensure_domain_table(args.domain, config)

    adapter = importlib.import_module(f"adapters.{config['source_adapter']}")
    print(f"Fetching from adapter '{config['source_adapter']}'...")
    records = adapter.fetch(config["source_config"])
    print(f"Adapter returned {len(records)} records.")

    if args.limit is not None:
        records = records[: args.limit]
        print(f"Capped to {len(records)} records (--limit {args.limit}).")

    # Existing external_ids (the table is single-domain, so this is all of them).
    existing_ids: set[str] = set()
    if not args.dry_run:
        existing_ids = {r["external_id"] for r in db.fetch_all(table, "external_id")}

    new_rows: list[dict] = []
    upd_rows: list[dict] = []
    skipped = 0
    for rec in records:
        ext = rec.get("external_id")
        if not ext:
            skipped += 1  # no dedup key — cannot store
            continue
        row = dict(rec)
        row["external_id"] = str(ext)
        row["domain"] = args.domain
        row.setdefault("source_name", config["source_adapter"])
        if str(ext) in existing_ids:
            for key in PIPELINE_STATE_KEYS:
                row.pop(key, None)  # preserve enrichment on re-ingest
            upd_rows.append(row)
        else:
            row.setdefault("phase2_status", "pending")
            row.setdefault("structural_tags", [])
            row.setdefault("derived_tags", [])
            row.setdefault("phase3_concept_ids", [])
            new_rows.append(row)

    if args.dry_run:
        for row in new_rows + upd_rows:
            print(row)
        print(
            f"\n[dry-run] {len(new_rows)} new, {len(upd_rows)} updated, "
            f"{skipped} skipped (nothing written)."
        )
        return

    client = db.get_client()
    if new_rows:
        _upsert_batches(client, table, new_rows, "new")
    if upd_rows:
        _upsert_batches(client, table, upd_rows, "updated")

    db.update_domain_stats(args.domain, "last_ingested_at")
    total = db.count_rows(table)
    print(
        f"\nDone. {len(new_rows)} new, {len(upd_rows)} updated, "
        f"{skipped} skipped. {total} total in table."
    )


if __name__ == "__main__":
    main()
