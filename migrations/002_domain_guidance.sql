-- ═════════════════════════════════════════════════════════════════════════════
-- 002_domain_guidance.sql
-- ═════════════════════════════════════════════════════════════════════════════
-- Adds per-domain classification guidance to research_domains. These are read by
-- the i.syncedsys research API (GET /api/research/domains) and the MCP so Claude
-- can tag a domain's records consistently (Phase 2/3). The pipeline populates
-- them from domains.yaml on every ingest (see lib/db.ensure_domain_table).
--
-- Idempotent. Run once in the research project's SQL editor (or it is applied
-- automatically by the pipeline's DDL path).
-- ═════════════════════════════════════════════════════════════════════════════

alter table research_domains
  add column if not exists enrichment_context text;

alter table research_domains
  add column if not exists structural_tag_categories jsonb not null default '[]';
