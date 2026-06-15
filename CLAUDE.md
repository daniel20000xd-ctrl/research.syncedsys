# CLAUDE.md — syncedsys-pipeline

Phase 1 (ingest) of the Syncedsys research stack. A local Python CLI — no server, no
scheduler, no model client. It pulls raw records from sources into per-domain tables in
the dedicated `syncedsys-research` Supabase project and publishes per-domain tagging
guidance to the `research_domains` registry.

Phases 2 (structural tagging) and 3 (latent-concept connection) do **not** run here.
They run in claude.ai through the research MCP, backed by the i.syncedsys read/write
API. The `anthropic` dependency and the old `enrich.py` / `connect.py` / `lib/haiku.py`
were removed in commit 6f0baa8 — do not resurrect them.

## Commands

```bash
python ingest.py --domain <domain_key> [--limit N] [--dry-run]
python status.py

# Bespoke higher-court precedents corpus (NOT a base-schema domain — see below):
python backfill_precedents.py --phase probe|backfill|embed|queue|all [--limit N] [--seed "…"] [--dry-run]
```

Run from the repo root (imports of `lib/` and `adapters/` are cwd-relative).
Env lives in `.env.local` (template: `.env.example`):
- `RESEARCH_SUPABASE_URL` + `RESEARCH_SUPABASE_SERVICE_KEY` — DML via supabase client
- `RESEARCH_DATABASE_URL` + `RESEARCH_DB_PASSWORD` — DDL via psycopg2 (session pooler;
  password passed separately so special characters survive)

## Architecture

- `domains.yaml` — single source of truth. One entry per domain: table name, source
  adapter + config, `extra_columns`, and the Phase 2/3 guidance (`enrichment_context`,
  `structural_tag_categories`) that gets published to the DB for Claude.
- `lib/registry.py` — loads/validates domains.yaml. Required: display_name, table_name,
  source_adapter, source_config, structural_tag_categories (`enrichment_context` is
  optional in validation, though everything downstream expects it).
- `lib/schema.py` — generates idempotent DDL: base schema (external_id/domain unique
  key, raw_data, structural_tags, derived_tags, phase2_* state, phase3_concept_ids)
  plus extra_columns, six indexes, updated_at trigger.
- `lib/db.py` — supabase client (DML) + psycopg2 (DDL). `ensure_domain_table()` creates
  the table, `NOTIFY pgrst` to reload the PostgREST schema cache, waits until the table
  is visible, and upserts the domain row **including enrichment_context and
  structural_tag_categories** into `research_domains`.
- `adapters/<name>.py` — one per source type, each exposing
  `fetch(source_config) -> list[dict]` returning base-schema-shaped records.
  Current: `domstolsverket` (rättspraxis API), `manual` (local JSON array).
- `ingest.py` — orchestrates: registry → ensure table → adapter fetch → diff against
  existing external_ids → batch upserts (100/batch) → stamp `last_ingested_at`.
- `migrations/` — foundation SQL, run **by hand** in the Supabase SQL editor (001 then
  002). Creates `update_updated_at()`, `research_domains`, `concepts`; RLS enabled with
  zero policies (service-key only; auth lives in the i.syncedsys proxy). `003` adds the
  bespoke precedents corpus (below).

## Higher-court precedents — bespoke pipeline (NOT a base-schema domain)

One unfiltered domain (`higher_court_precedents`, marker `pipeline: precedents`) targeting
the single `precedents` table from migration `003` — *not* the base schema, so it bypasses
`ingest.py` / `schema.py` / `ensure_domain_table` entirely. `ingest.py` refuses it and points
to `backfill_precedents.py`. Legal-area classification happens AFTER ingest (no per-area
domains; no court/keyword/area filter at ingest).

- `adapters/domstolsverket_precedents.py` — Domstolsverket "Sök rättspraxis" open data.
  `probe()` (total + pagination), `iter_pages()` (resumable unfiltered pull),
  `download_bilaga()` (PDF), `normalize()` (source → precedents row; shared with a future
  RSS delta mode). UNFILTERED = empty `{"filter":{}}` on `POST /api/v1/sok`; `sidIndex`
  (0-based page) + `antalPerSida`; sorted `publiceringstid` ASC so the page cursor is stable.
- `lib/precedents.py` — orchestration: `backfill` (probe → page loop → PDF→R2→text →
  upsert on `canonical_id` → cursor in `ingestion_progress`), `embed` (local model, rows
  where `embedding is null`), `populate_queue` (rank ALL precedents by max cosine similarity
  to seeds → `classification_queue.priority`).
- `lib/embeddings.py` — local `sentence-transformers` (default `KBLab/sentence-bert-swedish-cased`,
  768d); model+dim configurable. `lib/pdf_text.py` — PyMuPDF. `lib/r2.py` — Cloudflare R2
  (boto3; same env/bucket as the app's `lib/r2.ts`). `lib/db.get_pg_connection()` — shared
  psycopg2 connection (embed/queue do pgvector writes + cosine ranking directly).

## Invariants — do not regress

- Re-ingest never touches Phase 2/3 work: on the update path, `PIPELINE_STATE_KEYS`
  (phase2_*, structural_tags, derived_tags, phase3_concept_ids, ingested_at) are popped
  before upsert.
- Dedup key is `UNIQUE (external_id, domain)`; upserts use
  `on_conflict="external_id,domain"`.
- Table names are always resolved from the `research_domains` registry, never from
  caller/user input.
- `raw_data` holds the complete original record and is never mutated.
- **Precedents**: upsert is on `canonical_id` (the source UUID) and **never** writes
  `area` (set by classification — the only trusted area field; `source_area_code` is
  advisory, never filter on it) or `embedding` (set by the embed phase). `full_text` /
  `raw_pdf_path` are COALESCE-preserved so a transient PDF failure on re-run can't blank
  good content. Every precedent gets a `classification_queue` row — ranking orders the
  work, it never drops a case (null-embedding rows get null priority, ranked last).

## Gotchas

- **Migrations are manual.** Nothing in this repo executes `migrations/`. The header
  comment in 002 claiming the pipeline applies it automatically is wrong — if 002 hasn't
  been run, the first ingest fails at the `research_domains` upsert (missing columns).
- **`CREATE TABLE IF NOT EXISTS` does not add columns.** Adding `extra_columns` to a
  domain whose table already exists requires a manual `ALTER TABLE`; only the indexes
  propagate on re-ingest.
- **Domstolsverket adapter is tightly coupled to the API shape**: hardcoded
  `POST /api/v1/sok`; request fields `filter.sokordLista`, `filter.domstolKodLista`,
  `sidIndex` (0-based), `antalPerSida`, `sortorder: "publiceringstid"`, `asc`; response
  `{ total, publiceringLista }`; record fields id/gruppKorrelationsnummer,
  domstol.domstolKod/domstolNamn, malNummerLista, referatNummerLista, benamning,
  sammanfattning (HTML, stripped), avgorandedatum, lagrumLista, nyckelordLista. Any
  rename upstream breaks it. No retry/backoff — one HTTP error aborts the run (safe to
  re-run; ingest is idempotent).
- **Recall is keyword-bound**: only cases tagged with one of the `search_queries`
  keywords are ever fetched (`sokordLista`, assumed OR logic). Court code is `HDO`, not
  `HD`. `source_url` is always NULL (the API has no stable public web URLs).
- `--limit` truncates **after** the full API fetch; `--dry-run` skips the DB entirely,
  so every record prints as "new" and the live API is still hit.
- `status.py` counts only done/pending; `phase2_status='error'` rows are invisible
  there. `last_enriched_at`/`last_connected_at` are displayed here but must be stamped
  by the Phase 2/3 side (i.syncedsys MCP) — this repo only writes `last_ingested_at`.
- **Precedents PDF endpoint**: `GET /api/v1/bilagor/{fillagringId}` with the id
  **URL-encoded** (raw slashes 404) and `Accept: application/pdf` (it's `bilagor` plural —
  `/bilaga/` 404s). Discovered by reverse-engineering the SPA bundle under `/sok/`, not
  documented. Detail web URL is `/sok/publicering/{id}`.
- **Precedents embed/queue need the direct DB connection** (`RESEARCH_DATABASE_URL` +
  `RESEARCH_DB_PASSWORD`) for pgvector writes and `<=>` cosine ranking — the service-key
  PostgREST path can't express them. The embedding model is **local** (no paid API);
  its dimension MUST equal the `precedents.embedding vector(N)` column (768) or `Embedder`
  raises — to change it, alter the column + rebuild the HNSW index (migration `003`).
- **Precedents R2 is optional but recommended**: without `CF_ACCOUNT_ID`/`R2_ACCESS_KEY_ID`/
  `R2_SECRET_ACCESS_KEY` the backfill still extracts PDF text into `full_text`, it just
  doesn't archive the originals (`raw_pdf_path` stays null).
- Stale pre-MCP comments still reference Haiku/enrich.py/connect.py:
  `domains.yaml:9`, `domains.yaml:78`, `status.py:59`, comment blocks in
  `migrations/001` (lines 69, 75, 109, 115), plus orphaned `.pyc` files in
  `__pycache__/`. Harmless but misleading — clean up opportunistically.
