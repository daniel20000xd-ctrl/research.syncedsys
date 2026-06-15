"""Orchestration for the higher-court precedents corpus.

`precedents` is NOT a base-schema domain table, so it does not go through
ingest.py / schema.py / ensure_domain_table. This module owns its bespoke flow
against the tables created by migrations/003_precedents_corpus.sql:

  backfill()       probe -> resumable unfiltered pull -> PDF→R2→text ->
                   idempotent upsert on canonical_id -> cursor in ingestion_progress
  embed()          embed every precedent where embedding is null (LOCAL model)
  populate_queue() rank ALL precedents by max cosine similarity to seed phrases;
                   one classification_queue row per precedent (ranking, not gating)

Invariants (mirroring ingest.py's PIPELINE_STATE_KEYS discipline):
  * Upsert NEVER writes `area` or `embedding` — owned by classification / embed,
    and must survive re-ingest. (Enforced by _UPSERT_COLUMNS + the ON CONFLICT set.)
  * full_text / raw_pdf_path are COALESCE-preserved so a transient PDF failure on
    re-run can't blank good content.
  * Dedup/upsert key is canonical_id; re-runs never duplicate.
"""
from __future__ import annotations

import importlib

import psycopg2.extras

from lib import db, pdf_text, r2
from lib.embeddings import Embedder, to_pgvector

SOURCE = "domstolsverket_precedents"  # ingestion_progress.source key
R2_PREFIX = "precedents"

# Source/advisory columns written on backfill. `area` and `embedding` are
# deliberately absent — see module docstring.
_UPSERT_COLUMNS = (
    "canonical_id", "court", "court_raw", "doc_type", "malnummer_raw",
    "malnummer_normalized", "decision_date", "publication_date",
    "source_area_code", "title", "full_text", "source_url", "raw_pdf_path",
    "metadata",
)

_CONFLICT_SET = """
  court = excluded.court,
  court_raw = excluded.court_raw,
  doc_type = excluded.doc_type,
  malnummer_raw = excluded.malnummer_raw,
  malnummer_normalized = excluded.malnummer_normalized,
  decision_date = excluded.decision_date,
  publication_date = excluded.publication_date,
  source_area_code = excluded.source_area_code,
  title = excluded.title,
  full_text = coalesce(nullif(excluded.full_text, ''), precedents.full_text),
  source_url = excluded.source_url,
  raw_pdf_path = coalesce(excluded.raw_pdf_path, precedents.raw_pdf_path),
  metadata = excluded.metadata
"""


def _safe_key_part(s: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9._-]", "_", s or "")[:120]


# ──────────────────────────────────────────────────────────────────────────────
# ingestion_progress (resumable cursor)
# ──────────────────────────────────────────────────────────────────────────────
def _progress_get(conn) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            "select last_cursor, total_expected, total_ingested "
            "from ingestion_progress where source = %s",
            (SOURCE,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"last_cursor": row[0], "total_expected": row[1], "total_ingested": row[2]}


def _progress_set(conn, last_cursor: dict, total_expected, total_ingested: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingestion_progress
                (source, last_cursor, total_expected, total_ingested, last_run_at)
            values (%s, %s, %s, %s, now())
            on conflict (source) do update set
                last_cursor    = excluded.last_cursor,
                total_expected = excluded.total_expected,
                total_ingested = excluded.total_ingested,
                last_run_at    = now()
            """,
            (SOURCE, psycopg2.extras.Json(last_cursor), total_expected, total_ingested),
        )
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# probe
# ──────────────────────────────────────────────────────────────────────────────
def probe(config: dict) -> dict:
    adapter = importlib.import_module(f"adapters.{config['source_adapter']}")
    info = adapter.probe(config["source_config"])
    print("[probe] total hit count :", info["total"])
    print("[probe] page_size        :", info["page_size"])
    print("[probe] pages to fetch   :", info["pages_to_fetch"])
    print("[probe] pagination       :", info["pagination"])
    print("[probe] response keys    :", info["response_keys"])
    print("[probe] record keys      :", info["record_keys"])
    return info


# ──────────────────────────────────────────────────────────────────────────────
# backfill
# ──────────────────────────────────────────────────────────────────────────────
def _attach_pdf_text(adapter, rec: dict, base_url: str, r2_ready: bool) -> None:
    """Resolve full_text + raw_pdf_path for one record, handling PDF attachments."""
    bilagor = rec.pop("_bilagor", []) or []
    rec.setdefault("raw_pdf_path", None)
    summary = (rec.get("metadata") or {}).get("summary")

    if rec.get("full_text"):  # inline HTML body (referat) — no PDF needed
        return
    if not bilagor:
        rec["full_text"] = summary
        return

    texts: list[str] = []
    r2_keys: list[str] = []
    for i, b in enumerate(bilagor):
        fid = b.get("fillagring_id")
        if not fid:
            continue
        try:
            pdf_bytes = adapter.download_bilaga(base_url, fid)
        except Exception as e:  # noqa: BLE001 — one bad attachment shouldn't abort
            print(f"    pdf download failed ({rec['canonical_id']}): {str(e)[:120]}")
            continue
        if r2_ready:
            key = (
                f"{R2_PREFIX}/{_safe_key_part(rec.get('court') or 'NA')}/"
                f"{_safe_key_part(rec['canonical_id'])}/{i}_{_safe_key_part(b.get('filnamn') or 'doc.pdf')}"
            )
            try:
                if not r2.object_exists(key):
                    r2.upload_bytes(key, pdf_bytes, "application/pdf")
                r2_keys.append(key)
            except Exception as e:  # noqa: BLE001 — storage failure is non-fatal
                print(f"    r2 upload failed ({key}): {str(e)[:120]}")
        txt = pdf_text.extract_text(pdf_bytes)
        if txt:
            texts.append(txt)

    rec["full_text"] = ("\n\n".join(texts)).strip() or summary
    if r2_keys:
        rec["raw_pdf_path"] = r2_keys[0]  # primary original; all listed in metadata
        rec.setdefault("metadata", {})["r2_keys"] = r2_keys


def _upsert(conn, rows: list[dict]) -> None:
    if not rows:
        return
    values = [
        (
            r["canonical_id"], r.get("court"), r.get("court_raw"), r.get("doc_type"),
            r.get("malnummer_raw"), r.get("malnummer_normalized"),
            r.get("decision_date"), r.get("publication_date"), r.get("source_area_code"),
            r.get("title"), r.get("full_text"), r.get("source_url"), r.get("raw_pdf_path"),
            psycopg2.extras.Json(r.get("metadata") or {}),
        )
        for r in rows
    ]
    sql = (
        f"insert into precedents ({', '.join(_UPSERT_COLUMNS)}) values %s "
        f"on conflict (canonical_id) do update set {_CONFLICT_SET}"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=100)
    conn.commit()


def backfill(config: dict, limit: int | None = None, dry_run: bool = False) -> None:
    sc = config["source_config"]
    base_url = sc["base_url"].rstrip("/")
    adapter = importlib.import_module(f"adapters.{config['source_adapter']}")
    sort_order = getattr(adapter, "SORT_ORDER", "publiceringstid")

    info = probe(config)

    if dry_run:
        print("\n[dry-run] sample of normalized records (no DB, no R2, no PDF):")
        shown = 0
        for _page, pubs, _total in adapter.iter_pages(sc, 0):
            for raw in pubs:
                rec = adapter.normalize(raw, base_url)
                rec.pop("_bilagor", None)
                print("  ", {k: rec.get(k) for k in (
                    "canonical_id", "court", "doc_type", "malnummer_normalized",
                    "decision_date", "title")})
                shown += 1
                if shown >= (limit or 5):
                    break
            break
        print(f"[dry-run] showed {shown} records; nothing written.")
        return

    conn = db.get_pg_connection()
    try:
        prog = _progress_get(conn)
        start_page = 0
        if prog and prog.get("last_cursor"):
            start_page = int(prog["last_cursor"].get("sid_index", 0))
            print(f"[resume] continuing from page {start_page} "
                  f"(progress: {prog.get('total_ingested')}/{prog.get('total_expected')})")

        r2_ready = r2.is_configured()
        if not r2_ready:
            print("[warn] R2 not configured (CF_ACCOUNT_ID / R2_ACCESS_KEY_ID / "
                  "R2_SECRET_ACCESS_KEY) — PDF originals will NOT be archived; "
                  "text is still extracted in-memory into full_text.")

        page_size = int(info["page_size"])
        total_expected = info["total"] if isinstance(info["total"], int) else None
        processed = 0
        for page, pubs, total in adapter.iter_pages(sc, start_page):
            rows: list[dict] = []
            for raw in pubs:
                rec = adapter.normalize(raw, base_url)
                if not rec.get("canonical_id"):
                    continue  # no upsert key — cannot store
                _attach_pdf_text(adapter, rec, base_url, r2_ready)
                rows.append(rec)
                processed += 1
            _upsert(conn, rows)

            te = total if isinstance(total, int) else total_expected
            covered = min((page + 1) * page_size, te) if te else (page + 1) * page_size
            _progress_set(
                conn,
                {"sid_index": page + 1, "page_size": page_size,
                 "sortorder": sort_order, "asc": True},
                te, covered,
            )
            print(f"  page {page}: upserted {len(rows)} (run total {processed}, "
                  f"corpus covered ~{covered}/{te})")

            if limit and processed >= limit:
                print(f"[limit] stopping after {processed} records (--limit {limit}).")
                break

        with conn.cursor() as cur:
            cur.execute("select count(*) from precedents")
            db_total = cur.fetchone()[0]
        print(f"\n[backfill] done. {processed} records processed this run; "
              f"{db_total} total in precedents.")
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# embed
# ──────────────────────────────────────────────────────────────────────────────
def _embed_text(title, full_text, summary) -> str:
    # Summary first (densest signal), then title, then body; Embedder truncates.
    return "\n\n".join(p for p in (summary, title, full_text) if p)


def embed(config: dict, limit: int | None = None, dry_run: bool = False) -> None:
    sc = config["source_config"]
    emb_cfg = sc.get("embeddings") or {}
    embedder = Embedder(emb_cfg)

    conn = db.get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("select count(*) from precedents where embedding is null")
            remaining = cur.fetchone()[0]
        print(f"[embed] model={embedder.model_name} dim={embedder.dimension}; "
              f"{remaining} precedents need embedding.")
        if dry_run:
            print("[dry-run] would load the model and embed the rows above.")
            return
        if remaining == 0:
            return

        embedder.load()
        batch = int(emb_cfg.get("db_batch_size", 128))
        done = 0
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    "select id, title, full_text, metadata->>'summary' "
                    "from precedents where embedding is null order by id limit %s",
                    (batch,),
                )
                rows = cur.fetchall()
            if not rows:
                break
            texts = [_embed_text(t, ft, s) for (_id, t, ft, s) in rows]
            vecs = embedder.embed_passages(texts)
            updates = [(to_pgvector(v), str(rows[i][0])) for i, v in enumerate(vecs)]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "update precedents as p set embedding = d.emb::vector "
                    "from (values %s) as d(emb, id) where p.id = d.id::uuid",
                    updates, page_size=batch,
                )
            conn.commit()
            done += len(rows)
            print(f"  embedded {done}/{remaining}")
            if limit and done >= limit:
                print(f"[limit] stopping after {done} (--limit {limit}).")
                break
        print(f"[embed] done. {done} embeddings written.")
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# queue population (similarity ranking — never gates)
# ──────────────────────────────────────────────────────────────────────────────
def populate_queue(config: dict, seeds: list[str] | None = None,
                   dry_run: bool = False) -> None:
    sc = config["source_config"]
    emb_cfg = sc.get("embeddings") or {}
    seed_phrases = [s for s in (seeds or sc.get("seed_phrases") or []) if s and s.strip()]
    if not seed_phrases:
        raise ValueError(
            "Queue population needs seed phrases describing the target niche. "
            "Set source_config.seed_phrases in domains.yaml or pass --seed."
        )
    print(f"[queue] {len(seed_phrases)} seed phrase(s):")
    for s in seed_phrases:
        print("   •", s)

    if dry_run:
        print("[dry-run] would embed the seeds and rank every precedent by max "
              "cosine similarity, writing one classification_queue row each.")
        return

    embedder = Embedder(emb_cfg)
    seed_literals = [to_pgvector(v) for v in embedder.embed_queries(seed_phrases)]

    conn = db.get_pg_connection()
    try:
        with conn.cursor() as cur:
            # One row per precedent. priority = max cosine similarity to any seed
            # (1 - cosine distance). NULL embedding -> NULL priority (ranked last,
            # never dropped). Re-runs refresh priority but preserve status/attempts.
            cur.execute(
                """
                insert into classification_queue (precedent_id, status, priority)
                select p.id, 'pending',
                       (select max(1 - (p.embedding <=> s.vec))
                          from unnest(%s::vector[]) as s(vec))
                from precedents p
                on conflict (precedent_id) do update
                    set priority = excluded.priority
                """,
                (seed_literals,),
            )
            conn.commit()
            cur.execute("select count(*) from classification_queue")
            total_q = cur.fetchone()[0]
            cur.execute("select count(*) from classification_queue where priority is not null")
            ranked = cur.fetchone()[0]
            cur.execute(
                "select p.canonical_id, q.priority from classification_queue q "
                "join precedents p on p.id = q.precedent_id "
                "where q.priority is not null order by q.priority desc limit 5"
            )
            top = cur.fetchall()
        print(f"[queue] {total_q} queue rows total — {ranked} ranked, "
              f"{total_q - ranked} unranked (no embedding yet, ranked last).")
        if top:
            print("[queue] top matches to seeds:")
            for cid, pri in top:
                print(f"   {pri:.4f}  {cid}")
    finally:
        conn.close()
