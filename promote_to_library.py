#!/usr/bin/env python3
"""Promote enriched precedents into the hub Library so the research MCP
(search_library) can find them, grouped by tag.

The Library (`library_items`) lives in the MAIN HUB Supabase project — a DIFFERENT
project from the research one where `precedents` lives. This reads enriched
precedents from research and upserts them as legal_case library items in the hub.

Each library item's `tags[]` = its `area` + the substantive structural tags
(principle / lagrum_area), which for IP includes the `immaterialrätt` umbrella —
so e.g. a patent case is findable by tag `patenträtt` AND `immaterialrätt`.

Idempotent: skips cases whose `beteckning` (citation / målnummer) is already a
legal_case in the library, so it won't duplicate the existing arv_testamente items.

Usage: python promote_to_library.py [--limit N] [--dry-run]
Run from the repo root.
"""
from __future__ import annotations

import argparse
import hashlib

from dotenv import dotenv_values, load_dotenv

load_dotenv(".env.local")  # research creds (read precedents)

from lib import db  # noqa: E402 — after dotenv
from supabase import create_client  # noqa: E402

HUB_ENV = "A:/Projects/syncedsys/.env.local"
TAG_CATEGORIES = {"principle", "lagrum_area"}  # substantive tags -> library groups


def _hub_client():
    cfg = dotenv_values(HUB_ENV)
    url, key = cfg.get("NEXT_PUBLIC_SUPABASE_URL"), cfg.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise SystemExit(f"hub Supabase creds not found in {HUB_ENV}")
    return create_client(url, key)


def _existing_beteckningar(hub) -> set[str]:
    seen: set[str] = set()
    start = 0
    while True:
        rows = (hub.table("library_items").select("metadata")
                .eq("type", "legal_case").eq("deleted", False)
                .range(start, start + 999).execute().data) or []
        for r in rows:
            b = (r.get("metadata") or {}).get("beteckning")
            if b:
                seen.add(b)
        if len(rows) < 1000:
            return seen
        start += 1000


def main() -> None:
    ap = argparse.ArgumentParser(description="Promote enriched precedents to the hub Library")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    hub = _hub_client()
    src = hub.table("library_items").select("user_id").limit(1).execute().data
    if not src:
        raise SystemExit("Library is empty — can't source the admin user_id.")
    user_id = src[0]["user_id"]
    seen = _existing_beteckningar(hub)
    print(f"library already has {len(seen)} legal_case beteckningar")

    conn = db.get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            """select canonical_id, court, court_raw, to_char(decision_date,'YYYY-MM-DD'),
                      malnummer_normalized, title, summary, source_url, area, structural_tags
               from precedents where area is not null order by decision_date desc nulls last"""
        )
        rows = cur.fetchall()
    conn.close()
    print(f"enriched precedents to consider: {len(rows)}")

    items, skipped = [], 0
    for (cid, court, court_raw, ddate, maln, title, summary, url, area, stags) in rows:
        bet = maln or cid
        if bet in seen:
            skipped += 1
            continue
        seen.add(bet)
        tags = [area] + [t.get("tag") for t in (stags or [])
                         if t.get("category") in TAG_CATEGORIES and t.get("tag")]
        tags = list(dict.fromkeys(tags))  # de-dupe, keep order (area first)
        ch = hashlib.sha1(f"{title}|{summary}|{','.join(tags)}".encode("utf-8")).hexdigest()
        items.append({
            "user_id": user_id, "type": "legal_case", "title": title or bet,
            "summary": summary, "tags": tags, "source_url": url, "verified": False,
            "content_hash": ch,
            "metadata": {"beteckning": bet, "canonical_id": cid, "court": court,
                         "court_raw": court_raw, "decision_date": ddate, "area": area},
        })
        if args.limit and len(items) >= args.limit:
            break

    print(f"new items to promote: {len(items)} (skipped {skipped} already in library)")
    if args.dry_run:
        for it in items[:5]:
            print("  ", it["metadata"]["beteckning"], "->", it["tags"])
        print("[dry-run] nothing written")
        return

    inserted = 0
    for i in range(0, len(items), 200):
        batch = items[i:i + 200]
        hub.table("library_items").insert(batch).execute()
        inserted += len(batch)
        print(f"  inserted {inserted}/{len(items)}")
    print(f"done: promoted {inserted} precedents into the Library.")


if __name__ == "__main__":
    main()
