#!/usr/bin/env python3
"""Ingest the Domstolsverket corpus into `cases`.

    python ingest.py backfill [--limit N] [--seed-from-precedents]
    python ingest.py embed [--limit N]
    python ingest.py verify

backfill  Full unfiltered pull, upserted on source_id; a re-run over unchanged source data
          writes nothing. Heavy content lives in R2 — extracted text (full_text_path), PDF
          originals (raw_pdf_path) and the untouched source record (raw.json) — and content
          already in R2 is reused, never re-downloaded. A case that disappears from the
          source is reported for review, never deleted.
embed     Local KBLab embeddings for every case without one (body read back from R2).
verify    Per-court totals, Domstolsverket vs the database; exits 1 on any mismatch.

--seed-from-precedents is the one-time v1 -> v2 move: newly inserted cases take area,
structural_tags and embedding from the matching `precedents` row. Remove the flag once
precedents is dropped.

Run from the repo root.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor

for _stream in (sys.stdout, sys.stderr):
    _stream.reconfigure(encoding="utf-8")

import psycopg2.extras  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv(".env.local")

from adapters import domstolsverket_precedents as source  # noqa: E402
from lib import db, pdf_text, r2  # noqa: E402
from lib.embeddings import Embedder, to_pgvector  # noqa: E402

R2_PREFIX = "precedents"
PAGE_SIZE = 100
EMBED_BATCH = 128

_UPSERT = """
insert into cases (source_id, court, case_number, title, summary, decision_date,
                   source_area_code, full_text_path, raw_pdf_path, raw_data{seed_columns})
select v.source_id, v.court, v.case_number, v.title, v.summary, v.decision_date,
       v.source_area_code, v.full_text_path, v.raw_pdf_path, v.raw_data{seed_values}
from (values %s) as v(source_id, court, case_number, title, summary, decision_date,
                      source_area_code, full_text_path, raw_pdf_path, raw_data)
{seed_join}
on conflict (source_id) do update set
  court = excluded.court,
  case_number = excluded.case_number,
  title = excluded.title,
  summary = excluded.summary,
  decision_date = excluded.decision_date,
  source_area_code = excluded.source_area_code,
  full_text_path = coalesce(cases.full_text_path, excluded.full_text_path),
  raw_pdf_path = coalesce(cases.raw_pdf_path, excluded.raw_pdf_path),
  raw_data = excluded.raw_data
where (cases.court, cases.case_number, cases.title, cases.summary, cases.decision_date,
       cases.source_area_code, cases.raw_data)
      is distinct from (excluded.court, excluded.case_number, excluded.title, excluded.summary,
                        excluded.decision_date, excluded.source_area_code, excluded.raw_data)
   or (cases.full_text_path is null and excluded.full_text_path is not null)
   or (cases.raw_pdf_path is null and excluded.raw_pdf_path is not null)
"""
_TEMPLATE = "(%s, %s, %s, %s, %s, %s::date, %s, %s, %s, %s::jsonb)"
_SEED = {
    True: {"seed_columns": ", area, structural_tags, embedding",
           "seed_values": ", p.area, coalesce(p.structural_tags, '[]'::jsonb), p.embedding",
           "seed_join": "left join precedents p on p.canonical_id = v.source_id"},
    False: {"seed_columns": "", "seed_values": "", "seed_join": ""},
}


# ──────────────────────────────────────────────────────────────────────────────
# backfill
# ──────────────────────────────────────────────────────────────────────────────
def _safe(part: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", part or "")[:120]


def _case_dir(rec: dict) -> str:
    return f"{R2_PREFIX}/{_safe(rec['court'])}/{_safe(rec['source_id'])}"


def _r2_index() -> dict[str, set[str]]:
    index: dict[str, set[str]] = {}
    for key in r2.list_keys(R2_PREFIX + "/"):
        directory, _, name = key.rpartition("/")
        index.setdefault(directory, set()).add(name)
    return index


def _attach_content(rec: dict, directory: str, names: set[str]) -> tuple[str | None, str | None]:
    """(full_text_path, raw_pdf_path): reuse what R2 already holds, upload only what is missing."""
    pdfs = sorted(n for n in names if n.lower().endswith(".pdf"))
    if "text.txt" in names:
        return f"{directory}/text.txt", f"{directory}/{pdfs[0]}" if pdfs else None

    text, pdf_keys = rec["_full_text"], []
    if not text:
        extracted = []
        for i, bilaga in enumerate(rec["_bilagor"]):
            data = source.download_bilaga(bilaga["fillagring_id"])
            name = f"{i}_{_safe(bilaga['filnamn'] or 'doc.pdf')}"
            pdf_keys.append(r2.upload_bytes(f"{directory}/{name}", data, "application/pdf"))
            extracted.append(pdf_text.extract_text(data) or "")
        text = "\n\n".join(t for t in extracted if t).strip() or None
    text = text or rec["summary"]
    text_key = r2.upload_text(f"{directory}/text.txt", text) if text else None
    return text_key, pdf_keys[0] if pdf_keys else None


def _process_page(pubs: list[dict], index: dict[str, set[str]], seed: bool,
                  pool: ThreadPoolExecutor) -> tuple[int, list[str]]:
    records = {}
    for raw in pubs:
        rec = source.normalize(raw)
        if rec["source_id"]:
            records[rec["source_id"]] = (raw, rec)
    if not records:
        return 0, []

    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select source_id, full_text_path, raw_data from cases where source_id = any(%s)",
                        (list(records),))
            existing = {sid: (path, raw_data) for sid, path, raw_data in cur.fetchall()}

        def prepare(item: tuple[dict, dict]) -> tuple:
            raw, rec = item
            directory = _case_dir(rec)
            names = index.get(directory, set())
            previous = existing.get(rec["source_id"])
            text_path = pdf_path = None
            try:
                if previous is None or previous[0] is None:
                    text_path, pdf_path = _attach_content(rec, directory, names)
                if previous is None or previous[1] != rec["raw_data"] or "raw.json" not in names:
                    r2.upload_text(f"{directory}/raw.json", json.dumps(raw, ensure_ascii=False))
            except Exception as e:  # noqa: BLE001 — pointers stay null and the next run retries this case
                print(f"    content failed for {rec['source_id']}: {str(e)[:160]}")
            return (rec["source_id"], rec["court"], rec["case_number"], rec["title"], rec["summary"],
                    rec["decision_date"], rec["source_area_code"], text_path, pdf_path,
                    json.dumps(rec["raw_data"], ensure_ascii=False))

        rows = list(pool.map(prepare, records.values()))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, _UPSERT.format(**_SEED[seed]), rows,
                                           template=_TEMPLATE, page_size=len(rows))
            written = cur.rowcount
        conn.commit()
        return written, list(records)
    finally:
        conn.close()


def backfill(limit: int | None, seed: bool) -> None:
    if not r2.is_configured():
        sys.exit("R2 is required: set CF_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY.")
    index = _r2_index()
    print(f"[backfill] {sum(len(n) for n in index.values())} R2 objects indexed across {len(index)} cases")

    seen: set[str] = set()
    written_total = 0
    with ThreadPoolExecutor(max_workers=16) as pool:
        for page, pubs, total in source.iter_pages(PAGE_SIZE):
            written, ids = db.with_retry(lambda: _process_page(pubs, index, seed, pool))
            seen.update(ids)
            written_total += written
            print(f"  page {page}: {len(ids)} records, {written} written ({len(seen)}/{total})")
            if limit and len(seen) >= limit:
                print(f"[backfill] stopped at --limit {limit}: {written_total} rows written")
                return

    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select source_id from cases")
            vanished = sorted(sid for (sid,) in cur.fetchall() if sid not in seen)
    finally:
        conn.close()
    print(f"[backfill] done: {len(seen)} source records, {written_total} rows written")
    if vanished:
        print(f"WARNING: {len(vanished)} cases are no longer in the source (flagged for review, not deleted):")
        for sid in vanished[:20]:
            print(f"    {sid}")


# ──────────────────────────────────────────────────────────────────────────────
# embed
# ──────────────────────────────────────────────────────────────────────────────
def _body(path: str | None) -> str:
    try:
        return r2.download_text(path) if path else ""
    except Exception:  # noqa: BLE001 — missing object: embed title + summary only
        return ""


def _embed_batch(embedder: Embedder, pool: ThreadPoolExecutor) -> int:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select id, title, summary, full_text_path from cases "
                        "where embedding is null order by id limit %s", (EMBED_BATCH,))
            rows = cur.fetchall()
        if not rows:
            return 0
        bodies = pool.map(_body, [r[3] for r in rows])
        # Summary first (densest signal), then title, then body; the Embedder truncates.
        texts = ["\n\n".join(p for p in (summary, title, body) if p)
                 for (_, title, summary, _), body in zip(rows, bodies)]
        vectors = embedder.embed_passages(texts)
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "update cases as c set embedding = d.embedding::vector "
                "from (values %s) as d(embedding, id) where c.id = d.id::uuid",
                [(to_pgvector(v), str(r[0])) for v, r in zip(vectors, rows)],
                page_size=len(rows),
            )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def embed(limit: int | None) -> None:
    embedder = Embedder()
    done = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        while not limit or done < limit:
            n = db.with_retry(lambda: _embed_batch(embedder, pool))
            if not n:
                break
            done += n
            print(f"  embedded {done}")
    print(f"[embed] done: {done} embeddings written")


# ──────────────────────────────────────────────────────────────────────────────
# verify
# ──────────────────────────────────────────────────────────────────────────────
def verify() -> None:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select court, count(*) from cases group by court")
            in_db = dict(cur.fetchall())
    finally:
        conn.close()

    mismatch = False
    print(f"{'court':<8}{'source':>9}{'db':>9}")
    for court in sorted(in_db):
        at_source = source.total(court)
        mismatch |= at_source != in_db[court]
        print(f"{court:<8}{at_source:>9}{in_db[court]:>9}{'  MISMATCH' if at_source != in_db[court] else ''}")
    source_total, db_total = source.total(), sum(in_db.values())
    mismatch |= source_total != db_total
    print(f"{'total':<8}{source_total:>9}{db_total:>9}{'  MISMATCH' if source_total != db_total else ''}")
    if mismatch:
        sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest the Domstolsverket corpus into cases")
    sub = ap.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--limit", type=int)
    b.add_argument("--seed-from-precedents", action="store_true")
    e = sub.add_parser("embed")
    e.add_argument("--limit", type=int)
    sub.add_parser("verify")
    args = ap.parse_args()

    if args.command == "backfill":
        backfill(args.limit, args.seed_from_precedents)
    elif args.command == "embed":
        embed(args.limit)
    else:
        verify()


if __name__ == "__main__":
    main()
