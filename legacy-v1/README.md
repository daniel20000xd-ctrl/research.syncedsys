# legacy-v1: superseded reference

Code from the v1 research pipeline, kept for reference only. It is not maintained and is not
expected to run against the v2 schema (`cases`, immutable `concepts`, the `concept_evaluations`
ledger) — see the root README.

| v1 | replaced by |
|---|---|
| `ingest.py`, `domains.yaml`, `lib/registry.py`, `lib/schema.py`, `adapters/domstolsverket.py`, `adapters/manual.py` — one table per legal domain behind the `research_domains` registry | one `cases` table; a legal domain is the `area` column |
| `backfill_precedents.py`, `lib/precedents.py` — `precedents` + `classification_queue` + `ingestion_progress` | root `ingest.py` (upsert on `source_id`; the ledger gap is the work queue) |
| `enrich_batch.py` — agent claim/write loop for `area` + structural tags | `enrich.py` concepts; area classification of new cases has no v2 runner yet |
| `promote_to_library.py` — copied enriched cases into the hub's `library_items` | nothing: `mcp_server.py` reads the corpus directly |
| `lib/db.py` — service-key Supabase client + DDL helpers | root `lib/db.py`, one psycopg2 connection per database role |

The v1 MCP server lived outside this repo: the hub's `app/api/mcp/route.ts` (88 tools; deleted in
daniel20000xd-ctrl/Syncedsys commit `50c28e1`, readable with `git show 50c28e1^:app/api/mcp/route.ts`),
which proxied the `research_*` tools to the i.syncedsys research API (`app/api/research/**`).
`README-v1.md` and `CLAUDE-v1.md` are this repo's v1 docs.

The v1 database objects still exist in the research project — `precedents`, `classification_queue`,
`research_domains`, `arv_testamente`, `ingestion_progress`, `legacy_v1_concepts` — until dropping
them is explicitly approved.
