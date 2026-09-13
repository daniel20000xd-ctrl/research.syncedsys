#!/usr/bin/env python3
"""Pipeline status dashboard.

Usage: python status.py

Reads research_domains and concepts, then counts phase2 status per domain table
(table names resolved from the registry, never hardcoded).
"""
from __future__ import annotations

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from lib import db  # noqa: E402 — after dotenv

BAR = "═" * 50
SEP = "─" * 49


def main() -> None:
    client = db.get_client()
    domains = client.table("research_domains").select("*").order("domain_key").execute().data or []
    concepts = client.table("concepts").select("*").order("name").execute().data or []

    print(BAR)
    print("  SYNCEDSYS RESEARCH — PIPELINE STATUS")
    print(BAR)
    print()

    print(f"DOMAINS ({len(domains)})")
    print(SEP)
    if not domains:
        print("  (none yet — run ingest.py to create one)")
    for d in domains:
        key = d["domain_key"]
        table = d["table_name"]
        try:
            total = db.count_rows(table)
            done = db.count_rows(table, phase2_status="done")
            pending = db.count_rows(table, phase2_status="pending")
        except Exception as e:  # noqa: BLE001 — table may not exist yet
            print(f"  {key:<22}  (table unavailable: {e})")
            continue
        concepts_run = sum(1 for c in concepts if key in (c.get("domains_searched") or []))
        last_ingest = (d.get("last_ingested_at") or "")[:10] or "—"
        last_enrich = (d.get("last_enriched_at") or "")[:10] or "—"

        print(f"  {key:<22}  {total:>4} records    Phase 2: {done} done, {pending} pending")
        print(f"  ({d.get('display_name', '')})".ljust(24) + f"               Phase 3: {concepts_run} concepts run")
        print(f"  {'':<22}                         Last ingest:  {last_ingest}")
        print(f"  {'':<22}                         Last enrich:  {last_enrich}")
        print()

    print(f"CONCEPTS ({len(concepts)})")
    print(SEP)
    if not concepts:
        print("  (none yet — run connect.py to create one)")
    for c in concepts:
        searched = ", ".join(c.get("domains_searched") or []) or "—"
        print(f"  {c['name']:<26}  run {c.get('total_runs', 0)}×    domains: {searched}")
    print()
    print(BAR)


if __name__ == "__main__":
    main()
