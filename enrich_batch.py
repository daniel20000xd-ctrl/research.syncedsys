#!/usr/bin/env python3
"""Phase-2 enrichment helper for the higher-court precedents corpus.

The COGNITION — assigning `area` + structural tags by reading a case — is done by
Claude (a workflow sub-agent, or Claude Code), NOT here. This script only does the
I/O around that, so the subscription/API separation holds: no model is ever called
from this repo. An enrichment agent's loop is:

    python enrich_batch.py --claim 15           # -> JSON: instruction, areas, cases
    # (agent reads each case, decides area + tags)
    python enrich_batch.py --write results.json # applies them, marks done

Commands:
  --claim N [--court CODE]  Atomically claim N pending precedents (highest
                            classification_queue priority first; --court filters to
                            one court, e.g. PMOD). Marks them phase2_status
                            'in_progress' (FOR UPDATE SKIP LOCKED, so parallel agents
                            never collide) and prints {instruction, areas, ip_areas,
                            cases:[{canonical_id, court, malnummer, title, summary,
                            full_text}]} as JSON.
  --write PATH              Apply results: a JSON array of
                            {canonical_id, area, tags:[{tag,category}]}. Writes area +
                            structural_tags, marks phase2 'done' and the queue row
                            'classified'. Auto-adds the immaterialrätt umbrella tag for
                            IP sub-areas, and the source `typ` as a significance tag.
  --reset-stale [MIN]       Reset 'in_progress' rows older than MIN (default 30) back to
                            'pending' — recovers cases a crashed agent left claimed.
  --status                  Print phase2 + area counts.

Run from the repo root.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from lib import db, r2, registry  # noqa: E402 — after dotenv

DOMAIN = "higher_court_precedents"
_VALID_CATEGORIES = {"principle", "procedural", "outcome", "lagrum_area"}
_MAX_TEXT_CHARS = 12000  # cap full text handed to the agent (keeps its context lean)


def _spec() -> tuple[list[str], list[str], str]:
    cfg = registry.get_domain(DOMAIN)["source_config"]["classification"]
    return cfg["areas"], cfg.get("ip_areas", []), cfg["instruction"]


def claim(n: int, court: str | None) -> None:
    areas, ip_areas, instruction = _spec()
    conn = db.get_pg_connection()
    try:
        filt = "p.phase2_status = 'pending'"
        params: list = []
        if court:
            filt += " and p.court = %s"
            params.append(court)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                with picked as (
                  select p.id
                  from precedents p
                  left join classification_queue q on q.precedent_id = p.id
                  where {filt}
                  order by q.priority desc nulls last, p.id
                  limit %s
                  for update of p skip locked
                )
                update precedents p set phase2_status = 'in_progress'
                from picked where p.id = picked.id
                returning p.id
                """,
                (*params, n),
            )
            ids = [r[0] for r in cur.fetchall()]
        conn.commit()

        cases = []
        if ids:
            with conn.cursor() as cur:
                cur.execute(
                    """select canonical_id, court, court_raw, malnummer_normalized,
                              to_char(decision_date,'YYYY-MM-DD'), title, summary, full_text_path
                       from precedents where id = any(%s::uuid[])""",
                    ([str(i) for i in ids],),
                )
                for r in cur.fetchall():
                    body = ""
                    if r[7]:
                        try:
                            body = r2.download_text(r[7])[:_MAX_TEXT_CHARS]
                        except Exception:  # noqa: BLE001 — agent can still use summary
                            body = ""
                    cases.append({
                        "canonical_id": r[0], "court": r[1], "court_raw": r[2],
                        "malnummer": r[3], "decision_date": r[4], "title": r[5],
                        "summary": r[6], "full_text": body,
                    })
        print(json.dumps(
            {"instruction": instruction, "areas": areas, "ip_areas": ip_areas, "cases": cases},
            ensure_ascii=False,
        ))
    finally:
        conn.close()


def write(path: str) -> None:
    areas, ip_areas, _ = _spec()
    areaset, ipset = set(areas), set(ip_areas)
    with open(path, "r", encoding="utf-8") as f:
        results = json.load(f)
    now = datetime.now(timezone.utc).isoformat()

    conn = db.get_pg_connection()
    written, errors = 0, []
    try:
        with conn.cursor() as cur:
            for res in results:
                cid = res.get("canonical_id")
                area = res.get("area")
                if area not in areaset:
                    errors.append({"canonical_id": cid, "error": f"invalid area {area!r}"})
                    continue
                tags = [
                    {"tag": t["tag"], "category": t["category"], "added_at": now}
                    for t in (res.get("tags") or [])
                    if isinstance(t, dict) and t.get("tag") and t.get("category") in _VALID_CATEGORIES
                ]
                if area in ipset:
                    tags.append({"tag": "immaterialrätt", "category": "lagrum_area", "added_at": now})
                cur.execute("select id, metadata->>'typ' from precedents where canonical_id = %s", (cid,))
                row = cur.fetchone()
                if not row:
                    errors.append({"canonical_id": cid, "error": "not found"})
                    continue
                pid, typ = row
                if typ:
                    tags.append({"tag": typ, "category": "significance", "added_at": now})
                cur.execute(
                    """update precedents set area=%s, structural_tags=%s,
                       phase2_status='done', phase2_ran_at=%s where id=%s""",
                    (area, psycopg2.extras.Json(tags), now, pid),
                )
                cur.execute(
                    "update classification_queue set status='classified', last_attempted_at=%s where precedent_id=%s",
                    (now, pid),
                )
                written += 1
        conn.commit()
    finally:
        conn.close()
    print(json.dumps({"written": written, "errors": errors}, ensure_ascii=False))


def reset_stale(minutes: int) -> None:
    conn = db.get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """update precedents set phase2_status='pending'
                   where phase2_status='in_progress'
                     and (phase2_ran_at is null or phase2_ran_at < now() - (%s || ' minutes')::interval)""",
                (str(minutes),),
            )
            n = cur.rowcount
        conn.commit()
        print(json.dumps({"reset": n}))
    finally:
        conn.close()


def status() -> None:
    conn = db.get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("select phase2_status, count(*) from precedents group by 1 order by 2 desc")
            phases = dict(cur.fetchall())
            cur.execute("select area, count(*) from precedents where area is not null group by 1 order by 2 desc")
            byarea = dict(cur.fetchall())
        print(json.dumps({"phase2_status": phases, "by_area": byarea}, ensure_ascii=False, indent=2))
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Phase-2 enrichment I/O helper for precedents")
    ap.add_argument("--claim", type=int, metavar="N", help="claim N pending precedents")
    ap.add_argument("--court", default=None, help="restrict --claim to one court code (e.g. PMOD)")
    ap.add_argument("--write", metavar="PATH", help="apply results JSON")
    ap.add_argument("--reset-stale", nargs="?", type=int, const=30, metavar="MIN",
                    help="reset in_progress older than MIN minutes (default 30)")
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()

    if args.claim is not None:
        claim(args.claim, args.court)
    elif args.write:
        write(args.write)
    elif args.reset_stale is not None:
        reset_stale(args.reset_stale)
    elif args.status:
        status()
    else:
        ap.error("one of --claim / --write / --reset-stale / --status is required")


if __name__ == "__main__":
    main()
