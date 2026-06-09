#!/usr/bin/env python3
"""Phase 3 — Connect: Haiku searches for a concept latently across a domain.

Usage:
  python connect.py --domain <domain_key> --concept "concept name"
                    [--description "..."] [--limit N] [--threshold 0.7]

A concept is defined once (name + description) and stored in the `concepts`
table; it can then be re-run on any domain at any time. Records already searched
for this concept (concept_id present in phase3_concept_ids) are skipped, so runs
are resumable. Relevant hits are appended to the record's derived_tags.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from lib import db, haiku, registry  # noqa: E402 — after dotenv


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prompt_for_description() -> str:
    print("No description found for this concept.")
    print("Enter a description (what Haiku should search for).")
    print("End with a single '.' on its own line:")
    lines: list[str] = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line.strip() == ".":
            break
        lines.append(line)
    return "\n".join(lines).strip()


def _resolve_concept(client, name: str, description: str | None) -> tuple[str, str]:
    """Find-or-create the concept; return (concept_id, description)."""
    existing = client.table("concepts").select("*").eq("name", name).execute().data
    existing = existing[0] if existing else None

    if existing:
        if description:  # caller provided a new description -> update it
            client.table("concepts").update({"description": description}).eq(
                "id", existing["id"]
            ).execute()
            return existing["id"], description
        return existing["id"], existing["description"]

    if not description:
        description = _prompt_for_description()
        if not description:
            print("A description is required to create a new concept. Aborting.")
            sys.exit(1)
    row = (
        client.table("concepts")
        .insert({"name": name, "description": description})
        .execute()
        .data[0]
    )
    return row["id"], description


def _update_concept_after_run(client, concept_id: str, domain_key: str) -> None:
    row = (
        client.table("concepts")
        .select("total_runs,domains_searched")
        .eq("id", concept_id)
        .single()
        .execute()
        .data
    )
    domains = row.get("domains_searched") or []
    if domain_key not in domains:
        domains.append(domain_key)
    client.table("concepts").update(
        {
            "last_run_at": _now_iso(),
            "total_runs": (row.get("total_runs") or 0) + 1,
            "domains_searched": domains,
        }
    ).eq("id", concept_id).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 3 — search a concept latently across a domain")
    parser.add_argument("--domain", required=True, help="domain_key from domains.yaml")
    parser.add_argument("--concept", required=True, help="concept name")
    parser.add_argument("--description", default=None, help="concept description (created/updated if given)")
    parser.add_argument("--limit", type=int, default=None, help="cap records this run")
    parser.add_argument("--threshold", type=float, default=0.7, help="min confidence to tag (default 0.7)")
    args = parser.parse_args()

    config = registry.get_domain(args.domain)
    table = config["table_name"]
    client = db.get_client()

    concept_id, description = _resolve_concept(client, args.concept, args.description)
    cid = str(concept_id)

    # Records not yet searched for this concept. Filter in Python to stay robust
    # against jsonb-containment quirks; the table is single-domain.
    records = [
        r
        for r in db.fetch_all(table)
        if cid not in [str(c) for c in (r.get("phase3_concept_ids") or [])]
    ]
    if args.limit is not None:
        records = records[: args.limit]

    print(f'Concept: "{args.concept}"')
    print(f"Searching {len(records)} records in {args.domain}...")

    total = len(records)
    found = 0
    for i, rec in enumerate(records, start=1):
        title = (rec.get("title") or "")[:60]
        result = haiku.search_concept(rec, args.concept, description)
        relevant = bool(result["relevant"]) and result["confidence"] >= args.threshold

        if relevant:
            derived = rec.get("derived_tags") or []
            derived.append(
                {
                    "tag": args.concept,
                    "concept_id": cid,
                    "confidence": result["confidence"],
                    "reasoning": result["reasoning"],
                    "specific_passage": result["specific_passage"],
                    "added_at": _now_iso(),
                }
            )
            client.table(table).update({"derived_tags": derived}).eq("id", rec["id"]).execute()
            found += 1

        # Mark this concept as searched on the record regardless of relevance.
        concept_ids = [str(c) for c in (rec.get("phase3_concept_ids") or [])]
        if cid not in concept_ids:
            concept_ids.append(cid)
            client.table(table).update({"phase3_concept_ids": concept_ids}).eq(
                "id", rec["id"]
            ).execute()

        print(f"[{i}/{total}] {title} — {'✓ RELEVANT' if relevant else '·'}")

    _update_concept_after_run(client, concept_id, args.domain)
    db.update_domain_stats(args.domain, "last_connected_at")

    print("─────────────────────────────────")
    print(f'Concept:  "{args.concept}"')
    print(f"Domain:   {args.domain}")
    print(f"Searched: {total} records")
    print(f"Found:    {found} relevant (threshold: {args.threshold})")
    print("─────────────────────────────────")


if __name__ == "__main__":
    main()
