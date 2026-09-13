-- ═════════════════════════════════════════════════════════════════════════════
-- 005_concept_corpus.sql
-- ═════════════════════════════════════════════════════════════════════════════
-- The v2 corpus: `cases`, immutable `concepts`, the `concept_evaluations` coverage
-- ledger, the pipeline / mcp_reader roles and the read-only mcp_* views.
--
-- Additive. precedents, classification_queue, research_domains, arv_testamente and
-- ingestion_progress are untouched — dropping them needs explicit approval. The one
-- change to an existing object: the empty v1 `concepts` table is renamed to
-- legacy_v1_concepts so the new table can take the name.
--
-- Apply with `python migrate.py migrations/005_concept_corpus.sql` — one transaction as
-- postgres, then the role passwords are set from .env.local (no secrets in this file).
-- Idempotent.
-- ═════════════════════════════════════════════════════════════════════════════

do $$
begin
  if exists (select 1 from information_schema.columns
             where table_schema = 'public' and table_name = 'concepts' and column_name = 'domains_searched') then
    alter table public.concepts rename to legacy_v1_concepts;
    alter table public.legacy_v1_concepts rename constraint concepts_pkey to legacy_v1_concepts_pkey;
    alter table public.legacy_v1_concepts rename constraint concepts_name_key to legacy_v1_concepts_name_key;
    alter index public.concepts_name_idx rename to legacy_v1_concepts_name_idx;
  end if;
end $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- cases — a thin searchable index over R2
-- ─────────────────────────────────────────────────────────────────────────────
create table if not exists cases (
  id                uuid primary key default gen_random_uuid(),
  source_id         text unique not null,      -- Domstolsverket id; idempotency key for all ingest
  court             text not null,
  case_number       text,
  title             text,
  summary           text,                      -- the referat headnote (sammanfattning)
  decision_date     date,
  area              text,                      -- set ONLY by classification; the only filterable area field
  source_area_code  text,                      -- the court's own tag; advisory, NEVER filtered on
  full_text_path    text,                      -- R2 key
  raw_pdf_path      text,                      -- R2 key
  raw_data          jsonb not null,            -- source record minus the innehall body; untouched record is R2 raw.json
  structural_tags   jsonb default '[]',
  derived_tags      jsonb default '[]',        -- cache of matched concept ids, rebuilt from the ledger
  embedding         vector(768),
  fts               tsvector generated always as (
                      to_tsvector('swedish', coalesce(title, '') || ' ' || coalesce(summary, ''))
                    ) stored,
  created_at        timestamptz default now(),
  updated_at        timestamptz default now()
);

create index if not exists cases_embedding_idx           on cases using hnsw (embedding vector_cosine_ops);
create index if not exists cases_fts_idx                 on cases using gin (fts);
create index if not exists cases_court_decision_date_idx on cases (court, decision_date desc);
create index if not exists cases_area_idx                on cases (area) where area is not null;

drop trigger if exists cases_set_updated_at on cases;
create trigger cases_set_updated_at
  before update on cases
  for each row execute function update_updated_at();


-- ─────────────────────────────────────────────────────────────────────────────
-- concepts — immutable; revising a concept means a new row with a new key
-- ─────────────────────────────────────────────────────────────────────────────
create table if not exists concepts (
  id              uuid primary key default gen_random_uuid(),
  key             text unique not null,        -- display label only; never a foreign key
  definition      text not null,
  strategy        text not null check (strategy in ('lexical', 'vector_llm', 'sweep', 'manual')),
  patterns        jsonb,
  exemplars       jsonb,
  prompt_template text not null,
  candidate_depth int,
  created_at      timestamptz default now()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- concept_evaluations — the coverage ledger, one slot per (case, concept)
-- ─────────────────────────────────────────────────────────────────────────────
create table if not exists concept_evaluations (
  case_id          uuid not null references cases (id) on delete cascade,
  concept_id       uuid not null references concepts (id) on delete cascade,
  matched          boolean,
  confidence       real,
  evidence         text,
  method           text not null check (method in
                     ('lexical', 'llm_candidate', 'llm_sweep', 'out_of_candidate_window', 'abstain', 'error', 'manual')),
  escalation_level int default 0,
  model            text,
  error_message    text,
  evaluated_at     timestamptz default now(),
  primary key (case_id, concept_id),
  constraint concept_evaluations_matched_null check ((matched is null) = (method in ('error', 'abstain')))
);

create index if not exists concept_evaluations_matched_idx on concept_evaluations (concept_id) where matched = true;
create index if not exists concept_evaluations_method_idx  on concept_evaluations (concept_id, method);


-- RLS on, zero policies: only roles that bypass RLS reach rows directly.
alter table cases               enable row level security;
alter table concepts            enable row level security;
alter table concept_evaluations enable row level security;


-- ─────────────────────────────────────────────────────────────────────────────
-- Roles
-- ─────────────────────────────────────────────────────────────────────────────
do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'pipeline') then
    create role pipeline login bypassrls;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'mcp_reader') then
    create role mcp_reader login;
  end if;
end $$;

-- Supabase default privileges hand every new table to anon/authenticated; take them back.
revoke all on cases, concepts, concept_evaluations from anon, authenticated;

grant usage on schema public to pipeline, mcp_reader;
grant select, insert, update, delete on cases, concept_evaluations to pipeline;
grant select, insert, delete on concepts to pipeline;

-- Concept immutability. A column-level REVOKE cannot subtract from a table-level UPDATE
-- grant, so table-level UPDATE is withheld and only `key` (renaming) is granted.
revoke update on concepts from service_role;
grant update (key) on concepts to pipeline, service_role;

-- One-time seed of v1 classification + embeddings (ingest.py backfill --seed-from-precedents).
grant select on precedents to pipeline;


-- ─────────────────────────────────────────────────────────────────────────────
-- MCP views — mcp_reader can read nothing else. source_area_code, raw_data and the
-- ledger's non-matches are not reachable from any of them.
-- ─────────────────────────────────────────────────────────────────────────────
create or replace view mcp_cases as
  select id, source_id, court, case_number, title, summary, decision_date, area,
         structural_tags, derived_tags, full_text_path, raw_pdf_path, created_at, updated_at
  from cases;

-- Ranking columns only; never returned by a tool.
create or replace view mcp_case_search as
  select id, fts, embedding
  from cases;

create or replace view mcp_concept_matches as
  select case_id, concept_id, confidence, evidence, method
  from concept_evaluations
  where matched;

create or replace view mcp_concepts as
  select c.id, c.key, c.definition, c.strategy, c.candidate_depth, c.created_at,
         count(*) filter (where e.method in ('lexical', 'llm_candidate', 'llm_sweep', 'manual')) as evaluated_count,
         count(*) filter (where e.matched)                                                   as matched_count,
         count(*) filter (where e.method = 'out_of_candidate_window')                        as out_of_window_count,
         count(*) filter (where e.method = 'abstain' and e.escalation_level < 4)             as abstaining_count,
         count(*) filter (where e.method = 'abstain' and e.escalation_level >= 4)            as abstaining_at_max_count,
         count(*) filter (where e.method = 'error')                                          as errored_count,
         (select count(*) from cases)
           - count(*) filter (where e.method in ('lexical', 'llm_candidate', 'llm_sweep', 'manual')) as unevaluated_count,
         max(e.evaluated_at)                                                                  as last_run_at
  from concepts c
  left join concept_evaluations e on e.concept_id = c.id
  group by c.id;

revoke all on mcp_cases, mcp_case_search, mcp_concept_matches, mcp_concepts from anon, authenticated;
grant select on mcp_cases, mcp_case_search, mcp_concept_matches, mcp_concepts to mcp_reader;
