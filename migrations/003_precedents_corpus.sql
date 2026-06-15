-- ═════════════════════════════════════════════════════════════════════════════
-- 003_precedents_corpus.sql
-- ═════════════════════════════════════════════════════════════════════════════
-- Higher-court legal precedent corpus for the `syncedsys-research` Supabase
-- project. A single raw landing table we backfill in bulk and classify
-- ourselves, plus the two tables that drive that work.
--
-- Run ONCE, by hand, in the SQL editor of the research Supabase project, AFTER
-- 001 and 002 (this migration reuses the update_updated_at() function defined in
-- 001).
--
-- Creates:
--   Section 1  vector ext            pgvector, for embedding similarity search
--   Section 2  precedents            single raw landing table (NOT split by area)
--   Section 3  indexes               fts (GIN), embedding (HNSW), court/date/area
--   Section 4  classification_queue  similarity-ranked work queue for our classifier
--   Section 5  ingestion_progress    resumable bulk-backfill cursor, per source
--   Section 6  RLS                   enabled, no policies (service-key only)
--   Section 7  trigger               updated_at on precedents (reuses 001's fn)
--
-- ─────────────────────────────────────────────────────────────────────────────
-- Why a NEW queue and progress table (vs. reusing existing infrastructure)
-- ─────────────────────────────────────────────────────────────────────────────
-- The existing pipeline already has an enrichment work-queue and an ingestion
-- tracker, but neither fits this corpus, so they are intentionally NOT reused:
--
--   * Enrichment queue: today it's an INLINE `phase2_status` column
--     (pending|done|error) on each DOMAIN table. It has no priority and no
--     attempt tracking, and precedents is not a domain table. Classification
--     here is similarity-RANKED (priority) and retried (attempts), which the
--     inline flag cannot express → dedicated `classification_queue`.
--
--   * Ingestion tracker: today it's the `research_domains` registry — per-domain
--     timestamps + record_count, keyed by domain_key, with no cursor. The
--     precedents corpus is a standalone bulk source outside the domain system
--     and needs a resumable cursor + expected/ingested counts → dedicated
--     `ingestion_progress`.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- Legal-area trust model (load-bearing — do not regress)
-- ─────────────────────────────────────────────────────────────────────────────
--   * `area` is the ONLY trustworthy legal-area field. It is NULL until our own
--     classification sets it, and is the only area field anything downstream may
--     filter on.
--   * `source_area_code` is the courthouse's own tag. It is STORED for reference
--     and ADVISORY ONLY — it must NEVER be used as a filter anywhere downstream.
-- ═════════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 1 — pgvector extension
-- ─────────────────────────────────────────────────────────────────────────────
-- On Supabase this installs into the `extensions` schema and the `vector` type
-- is then available unqualified.

create extension if not exists vector;


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 2 — precedents
-- ─────────────────────────────────────────────────────────────────────────────
-- Single raw landing table for the whole corpus. Do NOT split by legal area —
-- area is decided later by our classification, not at ingest time.
--
-- `embedding` is vector(768) — the default; change the dimension to match
-- whichever embedding model the backfill ultimately uses (and rebuild the HNSW
-- index in Section 3 to match).
--
-- `fts` is a STORED generated column. to_tsvector/2 (with the explicit 'swedish'
-- regconfig) is IMMUTABLE, which is what lets it be generated — to_tsvector/1
-- would not be.

create table if not exists precedents (
  id                    uuid        primary key default gen_random_uuid(),
  canonical_id          text        not null unique,   -- upsert key: normalized målnummer or stable source doc id
  court                 text,                           -- normalized court code (advisory)
  court_raw             text,                           -- source's raw court label
  doc_type              text,                           -- 'referat' | 'dom' | 'beslut' | … (advisory)
  malnummer_raw         text,
  malnummer_normalized  text,
  decision_date         date,
  publication_date      date,
  source_area_code      text,                           -- courthouse's own legal-area tag — STORED, ADVISORY ONLY, never filter on it
  area                  text,                           -- NULLABLE — set later by our classification. The trusted area.
  title                 text,
  full_text             text,
  source_url            text,
  raw_pdf_path          text,                           -- R2 path when the original was a PDF
  metadata              jsonb       not null default '{}',
  embedding             vector(768),
  fts                   tsvector    generated always as (
                          to_tsvector('swedish', coalesce(title, '') || ' ' || coalesce(full_text, ''))
                        ) stored,
  ingested_at           timestamptz not null default now(),
  updated_at            timestamptz not null default now()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 3 — precedents indexes
-- ─────────────────────────────────────────────────────────────────────────────
-- (The unique index on canonical_id is created by the column's UNIQUE above.)

-- Full-text search over the generated tsvector.
create index if not exists precedents_fts_idx
  on precedents using gin (fts);

-- Approximate nearest-neighbour over embeddings (cosine).
-- HNSW requires pgvector >= 0.5.0 (standard on Supabase). If your pgvector
-- predates it, comment this out and use the ivfflat fallback immediately below.
create index if not exists precedents_embedding_idx
  on precedents using hnsw (embedding vector_cosine_ops);

-- Fallback for pre-0.5.0 pgvector (ivfflat). NOTE: build this AFTER some rows
-- exist and tune `lists` (~rows/1000); an empty-table ivfflat index is useless.
--   create index if not exists precedents_embedding_idx
--     on precedents using ivfflat (embedding vector_cosine_ops) with (lists = 100);

-- Btree filters.
create index if not exists precedents_court_idx
  on precedents (court);
create index if not exists precedents_decision_date_idx
  on precedents (decision_date);
create index if not exists precedents_area_idx
  on precedents (area);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 4 — classification_queue
-- ─────────────────────────────────────────────────────────────────────────────
-- Drives our own classification of precedents. One row per precedent (unique).
-- `priority` is the similarity score from ranking (higher = classify sooner;
-- NULL = unranked, classified last). `status` values are advisory (documented,
-- not CHECK-constrained — matching how phase2_status is handled elsewhere):
--   'pending' | 'in_progress' | 'classified' | 'skipped' | 'error'

create table if not exists classification_queue (
  precedent_id      uuid             not null references precedents (id) on delete cascade,
  status            text             not null default 'pending',
  priority          double precision,                       -- similarity score from ranking
  attempts          int              not null default 0,
  last_attempted_at timestamptz,
  error             text,
  constraint classification_queue_precedent_id_key unique (precedent_id)
);

-- Claim query is "highest-priority pending first" — index status + priority so
-- it doesn't scan the whole queue each pass.
create index if not exists classification_queue_status_priority_idx
  on classification_queue (status, priority desc nulls last);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 5 — ingestion_progress
-- ─────────────────────────────────────────────────────────────────────────────
-- Resumable cursor for the bulk backfill, one row per source. `last_cursor` is
-- whatever opaque position the source adapter needs to resume (page token,
-- offset, last-seen id, …).

create table if not exists ingestion_progress (
  source         text        primary key,
  last_cursor    jsonb,
  total_expected int,
  total_ingested int         not null default 0,
  last_run_at    timestamptz
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 6 — Row-Level Security
-- ─────────────────────────────────────────────────────────────────────────────
-- Same posture as 001: RLS ENABLED, NO policies. The service role bypasses RLS,
-- so the pipeline/API still reach these tables; anon / authenticated keys get
-- nothing. Access control is enforced at the app layer, never in the DB.

alter table precedents           enable row level security;
alter table classification_queue enable row level security;
alter table ingestion_progress   enable row level security;


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 7 — updated_at trigger (precedents)
-- ─────────────────────────────────────────────────────────────────────────────
-- Reuses update_updated_at() from 001. CREATE TRIGGER has no IF NOT EXISTS
-- before PG14, so drop-then-create to stay idempotent.

drop trigger if exists precedents_set_updated_at on precedents;
create trigger precedents_set_updated_at
  before update on precedents
  for each row execute function update_updated_at();


-- Tell PostgREST to refresh its schema cache so the new tables are immediately
-- queryable through the Supabase client (matches lib/db.py's DDL path).
notify pgrst, 'reload schema';
