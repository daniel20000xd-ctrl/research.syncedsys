-- ═════════════════════════════════════════════════════════════════════════════
-- 001_research_foundation.sql
-- ═════════════════════════════════════════════════════════════════════════════
-- Foundation migration for the `syncedsys-research` Supabase project — a
-- dedicated personal knowledge-infrastructure database, completely separate
-- from the main Syncedsys product DB.
--
-- Run ONCE, by hand, in the SQL editor of the new research Supabase project.
--
-- Creates only the shared foundation:
--   Section 1  update_updated_at()   shared trigger function for updated_at
--   Section 2  research_domains      registry of active domains
--   Section 3  concepts              registry of Phase 3 latent-concept searches
--   Section 4  (doc block)           base schema every domain table inherits
--   Section 5  RLS                   enabled, no policies (service-key only)
--
-- Domain-specific tables (arv_testamente, migration_cases, …) are NOT created
-- here. The Python pipeline creates them automatically on first ingest, applying
-- the base schema documented in Section 4.
-- ═════════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 1 — Shared utility function
-- ─────────────────────────────────────────────────────────────────────────────
-- Trigger function that stamps updated_at = now() on every UPDATE. It is defined
-- before any table because every domain table the pipeline creates later attaches
-- this same function — which is why it lives in the foundation migration.

create or replace function update_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 2 — research_domains
-- ─────────────────────────────────────────────────────────────────────────────
-- Registry of every active domain. Populated automatically by the pipeline when
-- a domain is first ingested. The API reads this to discover what domains exist
-- and to resolve table names SAFELY: never use user input as a table name
-- directly — always look up table_name from this registry first.

create table research_domains (
  domain_key        text        primary key,         -- matches the key in domains.yaml (e.g. 'arv_testamente')
  display_name      text        not null,            -- human-readable name (e.g. 'Arv & Testamente')
  table_name        text        not null unique,     -- actual Supabase table name (e.g. 'arv_testamente')
  record_count      int         not null default 0,  -- updated by pipeline after each ingest run
  last_ingested_at  timestamptz,                      -- when Phase 1 last ran
  last_enriched_at  timestamptz,                      -- when Phase 2 last ran
  last_connected_at timestamptz,                      -- when Phase 3 last ran
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

-- Keep updated_at fresh on every change.
create trigger research_domains_set_updated_at
  before update on research_domains
  for each row execute function update_updated_at();


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 3 — concepts
-- ─────────────────────────────────────────────────────────────────────────────
-- Registry of every latent-concept search ever run (Phase 3). Each concept has a
-- name and a full description (what Haiku searches for) plus run metadata. Once a
-- concept is defined it can be re-run on any domain at any time.

create table concepts (
  id               uuid        primary key default gen_random_uuid(),
  name             text        not null unique,        -- short name (e.g. 'retroaktivitetsprincipen')
  description      text        not null,               -- the full description passed to Haiku
  created_at       timestamptz not null default now(),
  last_run_at      timestamptz,                         -- when it was last applied to any domain
  total_runs       int         not null default 0,     -- total times run across all domains
  domains_searched jsonb       not null default '[]'    -- array of domain_keys it has been run on
);

create index concepts_name_idx on concepts (name);


-- ─────────────────────────────────────────────────────────────────────────────
-- BASE SCHEMA — applied to every domain table by the pipeline (not created here)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Core identity
--   id                    uuid primary key default gen_random_uuid()
--   external_id           text not null          (source's own ID — dedup key)
--   source_name           text not null          (e.g. 'domstolsverket', 'manual')
--   source_url            text                   (original URL)
--   domain                text not null          (matches domain_key in registry)
--
-- Display fields (denormalised from raw for query performance)
--   title                 text                   (rubrik / study title / headline)
--   summary               text                   (sammanfattning / abstract)
--   record_date           date                   (ruling date / publication date)
--
-- Raw content
--   raw_data              jsonb not null default '{}'
--                         (complete original record, untouched — never mutated)
--
-- TAG LAYERS — the critical separation
--   structural_tags       jsonb not null default '[]'
--     Shape: [{"tag": "laglott", "category": "principle",
--              "confidence": 0.95, "added_at": "<iso>"}]
--     Added by: Phase 2 (enrich.py) — what the record explicitly IS
--
--   derived_tags          jsonb not null default '[]'
--     Shape: [{"tag": "retroaktivitetsprincipen", "concept_id": "<uuid>",
--              "confidence": 0.87, "reasoning": "...",
--              "specific_passage": "...", "added_at": "<iso>"}]
--     Added by: Phase 3 (connect.py) — latent connections discovered later
--
-- Pipeline state
--   phase2_status         text not null default 'pending'   (pending|done|error)
--   phase2_ran_at         timestamptz
--   phase2_error          text                              (if status = error)
--   phase3_concept_ids    jsonb not null default '[]'
--                         (array of concept UUIDs already run on this record)
--
-- Housekeeping
--   ingested_at           timestamptz not null default now()
--   updated_at            timestamptz not null default now()
--
-- Unique constraint: (external_id, domain)
-- ─────────────────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────────────────
-- Section 5 — Row-Level Security
-- ─────────────────────────────────────────────────────────────────────────────
-- RLS is ENABLED on both tables, with NO policies defined. The service role
-- (used by both the pipeline and the API) bypasses RLS by default, so these
-- tables stay reachable with the service key alone. Enabling RLS with zero
-- policies means anon / authenticated keys get no access at all. Access control
-- is enforced at the API layer (i.syncedsys.com middleware), never in the DB.

alter table research_domains enable row level security;
alter table concepts         enable row level security;
