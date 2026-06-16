-- ═════════════════════════════════════════════════════════════════════════════
-- 004_precedents_domain_bridge.sql
-- ═════════════════════════════════════════════════════════════════════════════
-- Makes the bespoke `precedents` corpus usable through the EXISTING research
-- MCP / API / UI (i.syncedsys) — so Claude can enrich/tag/connect it and you can
-- browse it — WITHOUT any i.syncedsys code change or redeploy.
--
-- The research routes are domain-agnostic: they resolve the table from the
-- `research_domains` registry and read/write a fixed set of base-schema columns
-- (structural_tags, derived_tags, phase2_status, phase2_ran_at, phase3_concept_ids,
-- record_date; FTS over title+summary). `precedents` was created outside that
-- system, so it was invisible. This migration:
--   1. adds those base-schema tag/state columns to `precedents`
--   2. registers `precedents` in `research_domains`
--
-- The bespoke layer is UNCHANGED and complementary: `embedding` + the
-- similarity-ranked `classification_queue` give the work PRIORITY ordering, while
-- `structural_tags`/`derived_tags` hold the classification RESULT (the groupings).
-- Heavy content stays in R2 (full_text_path / raw_pdf_path).
--
-- Run ONCE in the research project's SQL editor (or apply via psycopg2), AFTER 003.
-- All column adds use CONSTANT defaults => metadata-only, no table rewrite.
-- ═════════════════════════════════════════════════════════════════════════════

alter table precedents
  add column if not exists record_date         date,
  add column if not exists structural_tags     jsonb       not null default '[]',
  add column if not exists derived_tags        jsonb       not null default '[]',
  add column if not exists phase2_status        text        not null default 'pending',
  add column if not exists phase2_ran_at        timestamptz,
  add column if not exists phase2_error         text,
  add column if not exists phase3_concept_ids   jsonb       not null default '[]';

-- record_date drives the API's default ordering and date filters. Use the
-- decision date, falling back to the publication date.
update precedents
   set record_date = coalesce(decision_date, publication_date)
 where record_date is null;

-- Indexes mirroring the base-schema domain tables (the routes filter on these).
create index if not exists precedents_phase2_status_idx      on precedents (phase2_status);
create index if not exists precedents_structural_tags_idx    on precedents using gin (structural_tags);
create index if not exists precedents_derived_tags_idx       on precedents using gin (derived_tags);
create index if not exists precedents_phase3_concept_ids_idx on precedents using gin (phase3_concept_ids);
create index if not exists precedents_record_date_idx        on precedents (record_date desc);

-- Register the domain so the MCP / API / UI can see it. (The pipeline also
-- upserts this from domains.yaml on every backfill; kept here so the existing
-- corpus is reachable immediately.)
insert into research_domains
  (domain_key, display_name, table_name, enrichment_context, structural_tag_categories)
values
  ('higher_court_precedents',
   'Higher-Court Precedents (Sök rättspraxis)',
   'precedents',
   'Swedish higher-court precedents (HD/HFD, the hovrätter and kammarrätter, ' ||
   'Arbetsdomstolen, Mark- och miljööverdomstolen, Patent- och marknadsöver' ||
   'domstolen, Migrationsöverdomstolen, etc.). Raw unfiltered corpus; the legal ' ||
   'area is assigned by our own classification (the `area` column / structural ' ||
   'tags), never taken from the source. The full text of each case lives in R2 ' ||
   '(full_text_path); summary holds the headnote.',
   '["principle","procedural","outcome","lagrum_area","significance"]'::jsonb)
on conflict (domain_key) do update set
  display_name              = excluded.display_name,
  table_name                = excluded.table_name,
  enrichment_context        = excluded.enrichment_context,
  structural_tag_categories = excluded.structural_tag_categories;

notify pgrst, 'reload schema';
