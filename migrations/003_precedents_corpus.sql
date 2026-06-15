-- ═════════════════════════════════════════════════════════════════════════════
-- 003_precedents_corpus.sql
-- ═════════════════════════════════════════════════════════════════════════════
-- Higher-court legal precedent corpus for the `syncedsys-research` Supabase
-- project. A LEAN landing table we backfill in bulk and classify ourselves,
-- plus the two tables that drive that work.
--
-- Run ONCE, by hand, in the SQL editor of the research Supabase project, AFTER
-- 001 and 002 (this migration reuses the update_updated_at() function from 001).
--
-- ─────────────────────────────────────────────────────────────────────────────
-- STORAGE ARCHITECTURE — heavy content lives in R2, NOT Postgres
-- ─────────────────────────────────────────────────────────────────────────────
-- Postgres holds ONLY what has to be queried/ranked/joined: the structured
-- fields, a short summary (headnote), the embedding (for pgvector cosine
-- ranking) and the classification queue. The bulk content is offloaded to
-- Cloudflare R2 (same bucket as the photo pipeline):
--
--   raw_pdf_path    R2 key of the ORIGINAL pdf  (only for PDF-served records)
--   full_text_path  R2 key of the EXTRACTED plain text (every record)
--
-- The full body is therefore NOT a Postgres column. This keeps the table small
-- (~embeddings + HNSW + short fields), well within the free-tier disk budget,
-- and matches the intended architecture: PG is a thin, searchable index over
-- content that lives in object storage. The embed phase reads the text back from
-- R2 by full_text_path; classification (Phase 2/3) does the same.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- Legal-area trust model (load-bearing — do not regress)
-- ─────────────────────────────────────────────────────────────────────────────
--   * `area` is the ONLY trustworthy legal-area field. NULL until OUR own
--     classification sets it; the only area field anything downstream may filter on.
--   * `source_area_code` is the courthouse's own tag — STORED, ADVISORY ONLY,
--     never used as a filter anywhere downstream.
-- ═════════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 1 — pgvector extension
-- ─────────────────────────────────────────────────────────────────────────────
create extension if not exists vector;


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 2 — precedents (LEAN: no body column; content offloaded to R2)
-- ─────────────────────────────────────────────────────────────────────────────
-- `embedding` is vector(768) — matches the default local model
-- (KBLab/sentence-bert-swedish-cased). To use a model of a different width,
-- change the dimension here AND rebuild the HNSW index in Section 3.
--
-- `fts` is a STORED generated column over title + summary (NOT the full body,
-- which is in R2). to_tsvector/2 with the explicit 'swedish' regconfig is
-- IMMUTABLE, which is what lets it be generated.

create table if not exists precedents (
  id                    uuid        primary key default gen_random_uuid(),
  canonical_id          text        not null unique,   -- upsert key: stable source doc id (UUID) or normalized målnummer
  court                 text,                           -- normalized court code (advisory)
  court_raw             text,                           -- source's raw court label
  doc_type              text,                           -- 'referat' | 'dom' | 'beslut' | 'notis' (advisory)
  malnummer_raw         text,
  malnummer_normalized  text,
  decision_date         date,
  publication_date      date,
  source_area_code      text,                           -- courthouse's own area tag — ADVISORY ONLY, never filter on it
  area                  text,                           -- NULLABLE — set later by OUR classification. The trusted area.
  title                 text,
  summary               text,                           -- short headnote (sammanfattning) — kept for search/snippet
  source_url            text,                           -- stable public web URL (/sok/publicering/{id})
  raw_pdf_path          text,                           -- R2 key: original PDF (PDF-served records only)
  full_text_path        text,                           -- R2 key: extracted plain text (all records)
  metadata              jsonb       not null default '{}',  -- small: keywords, lagrum, rättsområde, referat, typ, …
  embedding             vector(768),
  fts                   tsvector    generated always as (
                          to_tsvector('swedish', coalesce(title, '') || ' ' || coalesce(summary, ''))
                        ) stored,
  ingested_at           timestamptz not null default now(),
  updated_at            timestamptz not null default now()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 3 — precedents indexes
-- ─────────────────────────────────────────────────────────────────────────────
-- (The unique index on canonical_id is created by the column's UNIQUE above.)

-- Full-text search over title + summary.
create index if not exists precedents_fts_idx
  on precedents using gin (fts);

-- Approximate nearest-neighbour over embeddings (cosine). HNSW needs pgvector
-- >= 0.5.0 (standard on Supabase). Pre-0.5.0: use the ivfflat fallback below.
create index if not exists precedents_embedding_idx
  on precedents using hnsw (embedding vector_cosine_ops);
--   create index if not exists precedents_embedding_idx
--     on precedents using ivfflat (embedding vector_cosine_ops) with (lists = 100);

-- Btree filters.
create index if not exists precedents_court_idx         on precedents (court);
create index if not exists precedents_decision_date_idx on precedents (decision_date);
create index if not exists precedents_area_idx          on precedents (area);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 4 — classification_queue
-- ─────────────────────────────────────────────────────────────────────────────
-- One row per precedent (unique). `priority` is the similarity score from
-- ranking (higher = classify sooner; NULL = unranked, classified last). `status`
-- values are advisory (documented, not CHECK-constrained):
--   'pending' | 'in_progress' | 'classified' | 'skipped' | 'error'

create table if not exists classification_queue (
  precedent_id      uuid             not null references precedents (id) on delete cascade,
  status            text             not null default 'pending',
  priority          double precision,
  attempts          int              not null default 0,
  last_attempted_at timestamptz,
  error             text,
  constraint classification_queue_precedent_id_key unique (precedent_id)
);

create index if not exists classification_queue_status_priority_idx
  on classification_queue (status, priority desc nulls last);


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 5 — ingestion_progress
-- ─────────────────────────────────────────────────────────────────────────────
-- Resumable cursor for the bulk backfill, one row per source. `last_cursor` is
-- whatever opaque position the source adapter needs to resume (here: the page
-- index for POST /api/v1/sok).

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
-- Same posture as 001: RLS ENABLED, NO policies. The service role bypasses RLS;
-- anon / authenticated keys get nothing. Access control is at the app layer.

alter table precedents           enable row level security;
alter table classification_queue enable row level security;
alter table ingestion_progress   enable row level security;


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 7 — updated_at trigger (precedents)
-- ─────────────────────────────────────────────────────────────────────────────
drop trigger if exists precedents_set_updated_at on precedents;
create trigger precedents_set_updated_at
  before update on precedents
  for each row execute function update_updated_at();


-- Refresh the PostgREST schema cache so the new tables are immediately queryable.
notify pgrst, 'reload schema';
