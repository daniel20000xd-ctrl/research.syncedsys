#!/usr/bin/env python3
"""Phase 2 — Enrich: Haiku assigns structural tags to records.

Usage: python enrich.py --domain <domain_key> [--force] [--limit N] [--batch-size N]

Processes records with phase2_status = 'pending' (or all, with --force), oldest
first, one Haiku call per record. Resumable: each run handles up to --batch-size
records (default 50). Errors are recorded per-record and never abort the run.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from lib import db, haiku, registry  # noqa: E402 — after dotenv

DEFAULT_BATCH_SIZE = 50


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2 — enrich records with structural tags")
    parser.add_argument("--domain", required=True, help="domain_key from domains.yaml")
    parser.add_argument("--force", action="store_true", help="re-enrich all records, not just pending")
    parser.add_argument("--limit", type=int, default=None, help="cap records this run")
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"max records per run (default {DEFAULT_BATCH_SIZE})",
    )
    args = parser.parse_args()

    config = registry.get_domain(args.domain)
    table = config["table_name"]
    client = db.get_client()

    already_done = 0 if args.force else db.count_rows(table, phase2_status="done")

    query = client.table(table).select("*").order("ingested_at", desc=False)
    if not args.force:
        query = query.eq("phase2_status", "pending")
    query = query.limit(args.limit if args.limit is not None else args.batch_size)
    records = query.execute().data or []

    print(f"Found {len(records)} records to enrich.")
    total = len(records)
    done = errors = 0

    for i, rec in enumerate(records, start=1):
        title = (rec.get("title") or "")[:60]
        try:
            tags = haiku.classify_record(rec, config)
            client.table(table).update(
                {
                    "structural_tags": tags,
                    "phase2_status": "done",
                    "phase2_ran_at": _now_iso(),
                    "phase2_error": None,
                }
            ).eq("id", rec["id"]).execute()
            done += 1
            print(f"[{i}/{total}] {title} — {len(tags)} tags")
        except Exception as e:  # noqa: BLE001 — never abort the whole run
            errors += 1
            client.table(table).update(
                {
                    "phase2_status": "error",
                    "phase2_error": str(e),
                    "phase2_ran_at": _now_iso(),
                }
            ).eq("id", rec["id"]).execute()
            print(f"[{i}/{total}] {title} — ERROR: {e}")

    db.update_domain_stats(args.domain, "last_enriched_at")
    print(f"\nDone. {done} done, {errors} errors, {already_done} skipped (already enriched).")


if __name__ == "__main__":
    main()
