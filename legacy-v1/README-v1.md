# syncedsys-pipeline

A domain-agnostic research data pipeline. A small local CLI (no server, no
scheduler, no GUI) that **ingests** research data across independent domains —
legal cases, academic studies, research projects — each in its own table in a
dedicated Supabase project (`syncedsys-research`).

Everything is driven by a single registry, **`domains.yaml`**. Adding a new
domain means adding one entry to that file — **zero code changes**.

## What the pipeline does (and what runs in chat)

This repo is **Phase 1 only**: pull raw records from a source into a domain's
table, and report status. The two tagging phases that used to run here now run
through **Claude + the research MCP** in claude.ai chat — there is no model or
API client in this repo.

| Phase | Where it runs | What it does |
|-------|---------------|--------------|
| **1 — Ingest** | `ingest.py` (this repo) | Pull raw records from a source into the domain's table (status `pending`) |
| **2 — Enrich** | claude.ai chat (MCP) | Claude reads each pending record and assigns **structural tags** — what the record explicitly *is* |
| **3 — Connect** | claude.ai chat (MCP) | Claude searches for a **concept** latently across a domain and writes **derived tags** — connections discovered later |

Ingest is idempotent and resumable — re-running picks up new/updated source
records and never overwrites tags written by Phases 2 & 3.

The two tag layers are kept strictly separate on every record:

- `structural_tags` — added by Phase 2; what the record explicitly is.
- `derived_tags` — added by Phase 3; latent connections to a concept, each with
  reasoning and the specific passage that matched.

### How Claude knows how to tag each domain

Each domain entry in `domains.yaml` carries two guidance fields:

- `enrichment_context` — a plain-language description of the domain (governing
  law, key concepts) that orients Claude before it tags.
- `structural_tag_categories` — the valid category values structural tags may use.

On every ingest the pipeline writes both into the `research_domains` registry.
The i.syncedsys research API (`GET /api/research/domains`) and the MCP serve them
to Claude, so tagging stays consistent without any per-domain code.

## Prerequisites

- **Python 3.11+**
- The **foundation migrations must already be run** on the research Supabase
  project (run the SQL files in this repo's `migrations/` folder in order — `001`
  then `002`). They create `research_domains`, `concepts`, the `update_updated_at()`
  function every domain table's trigger depends on, and the per-domain guidance
  columns the API/MCP serve to Claude.
- For the **higher-court precedents backfill** only, also run `003` (creates
  `precedents`, `classification_queue`, `ingestion_progress` and the pgvector
  extension). See [Higher-court precedents](#higher-court-precedents-bespoke-pipeline).
- A research Supabase project (separate from the main Syncedsys product DB).

## Setup

```bash
git clone <this repo>
cd syncedsys-pipeline
pip install -r requirements.txt
cp .env.example .env.local      # then fill in your keys
```

`.env.local` needs:

- `RESEARCH_SUPABASE_URL` / `RESEARCH_SUPABASE_SERVICE_KEY` — Supabase client (DML).
- `RESEARCH_DATABASE_URL` (+ `RESEARCH_DB_PASSWORD`) — direct Postgres connection
  used for DDL: the Supabase Python client can't create tables, so `psycopg2`
  does. Find it under Supabase → Project Settings → Database → Connection string
  (Session pooler).

Run all scripts **from the project root** so `lib/` and `adapters/` import
correctly.

## Adding a new domain

1. Add one entry to `domains.yaml` (copy the template at the bottom of the file):

   ```yaml
   straffratt:
     display_name: "Straffrätt"
     table_name: "straffratt"
     source_adapter: "domstolsverket"
     source_config:
       base_url: "https://rattspraxis.etjanst.domstol.se"
       court_codes: ["HDO"]
       search_queries: ["uppsåt", "nödvärn", "påföljd"]
       page_size: 50
       request_delay_seconds: 0.5
     extra_columns:
       malnummer: text
       lagrum: jsonb
     enrichment_context: |
       Swedish criminal-law cases from HD. Governing law: Brottsbalken (BrB)...
     structural_tag_categories:
       - principle
       - procedural
       - outcome
   ```

2. Ingest:

   ```bash
   python ingest.py --domain straffratt
   ```

The table is created automatically on first ingest (base schema + your
`extra_columns` + indexes), and the domain — including its `enrichment_context`
and `structural_tag_categories` — is registered in `research_domains`. No other
files change. To tag the new records, ask Claude in claude.ai: *"enrich the
pending records in straffratt"*.

To add a new **source type** (e.g. an academic API), drop an
`adapters/<name>.py` with a `fetch(source_config: dict) -> list[dict]` function
and reference it as `source_adapter: "<name>"`.

## Usage

### Phase 1 — Ingest
```bash
python ingest.py --domain arv_testamente
python ingest.py --domain arv_testamente --limit 20      # cap records (after fetch)
python ingest.py --domain arv_testamente --dry-run       # print, don't write
```
Idempotent: new records are inserted (status `pending`); existing records have
their source fields refreshed while enrichment columns are preserved.

### Phases 2 & 3 — Enrich and Connect (in claude.ai)
These no longer run from this repo. In claude.ai chat, with the research MCP
connected:

- **Enrich** — *"enrich the pending records in arv_testamente"*. Claude fetches
  the `pending` records, assigns structural tags using the domain's
  `enrichment_context` / `structural_tag_categories`, writes them back, and marks
  each one `done`.
- **Connect** — *"find cases about retroaktivitetsprincipen in arv_testamente"*.
  Claude searches the domain for a concept and appends derived tags, each with
  reasoning and the matching passage.

### Status
```bash
python status.py
```
Prints record counts and Phase 2 progress per domain, plus every concept with its
run count and the domains it has been applied to.

## Higher-court precedents (bespoke pipeline)

The `higher_court_precedents` domain is **not** a base-schema domain table. It
backfills the *entire* Domstolsverket "Sök rättspraxis" corpus — every higher
court (HD, HFD, the hovrätter and kammarrätter, Arbetsdomstolen, MÖD, PMÖD,
Migrationsöverdomstolen…), **unfiltered** — into the single `precedents` table
(migration `003`). Legal-area classification happens *after* ingest, so there
are no per-area domains and no court/keyword/area filter at ingest. It generates
embeddings with a **local** model (no paid API) and populates a
similarity-ranked `classification_queue`.

**Storage architecture**: heavy content lives in **Cloudflare R2**, not Postgres.
PDF originals (`raw_pdf_path`) and the extracted plain text (`full_text_path`)
are stored in R2; the Postgres row keeps only the structured fields, a short
summary, the R2 pointers and the `embedding`. So Postgres stays a thin,
searchable index over content that lives in object storage (the whole table is
roughly embeddings + HNSW + short fields), which keeps it well within the
free-tier disk budget. R2 is therefore **required** for this pipeline.

Because of the bespoke schema (canonical_id upsert key, `vector(768)` embedding,
similarity ranking, resumable cursor), it runs through its own CLI —
`backfill_precedents.py` — not `ingest.py` (which refuses it):

```bash
python backfill_precedents.py --phase probe                 # log total + API shape
python backfill_precedents.py --phase backfill [--limit N]  # resumable, idempotent pull
python backfill_precedents.py --phase embed    [--limit N]  # local embeddings (null only)
python backfill_precedents.py --phase queue    [--seed "…"] # rank ALL precedents
python backfill_precedents.py --phase all                   # full corpus, one shot
```

- **Resumable / idempotent**: a per-source cursor in `ingestion_progress` is
  saved after each page; restart resumes from it. Upsert is on `canonical_id`,
  and **never** overwrites `area` (classification) or `embedding` (embed phase).
- **Content → R2**: records served as PDF (domar/beslut since March 2025) have
  their original archived to R2 (`raw_pdf_path`) and text extracted with PyMuPDF;
  records with an inline HTML body (referat) have that body stripped to text. In
  both cases the extracted text is uploaded to R2 (`full_text_path`) — never
  stored in Postgres. R2 is **required**; the backfill aborts without it.
- **Embeddings**: local `sentence-transformers` (default
  `KBLab/sentence-bert-swedish-cased`, 768-dim to match the column). Model +
  dimension are configurable in `domains.yaml → source_config.embeddings`. The
  embed phase reads each body back from R2 (`full_text_path`) to embed it.
- **Queue ranking, never gating**: `priority` is the max cosine similarity to the
  seed phrase(s) (`source_config.seed_phrases`, or `--seed`). **Every** precedent
  gets a queue row — low-similarity ones are ranked last, never dropped.
- **Classification / grouping access** (migration `004`): the corpus is registered
  in `research_domains` and carries the base-schema tag columns, so the existing
  i.syncedsys research **MCP / API / UI** can browse, search, enrich
  (`structural_tags`), and connect concepts (`derived_tags`) over it with no code
  change. The embeddings + queue decide the *order* of work; the tags (and `area`)
  hold the resulting *groupings*.
- **Extra env**: needs `RESEARCH_DATABASE_URL` (+ `RESEARCH_DB_PASSWORD`) for the
  embed/queue phases (pgvector writes + cosine ranking) and the R2 vars (content
  storage; see `.env.example`). Extra deps: `boto3`, `pymupdf`,
  `sentence-transformers`.
- **Two-phase by design**: this is the backfill path; a future delta mode over
  the Sök-rättspraxis RSS feed reuses the adapter's `normalize()` unchanged.

## Project layout

```
syncedsys-pipeline/
├── domains.yaml          # domain registry — the single source of truth
├── ingest.py             # Phase 1 — ingest (base-schema domain tables)
├── backfill_precedents.py# bespoke higher-court precedents corpus CLI
├── status.py             # dashboard
├── adapters/             # one file per source type, each exposing fetch()
│   ├── domstolsverket.py            # keyword-bound rättspraxis -> domain tables
│   ├── domstolsverket_precedents.py # UNFILTERED corpus -> precedents (probe/page/PDF)
│   └── manual.py         # ingest a local JSON array of base-schema records
└── lib/
    ├── registry.py       # load + validate domains.yaml
    ├── schema.py         # generate CREATE TABLE SQL for a domain
    ├── db.py             # Supabase (DML) + psycopg2 (DDL/direct) + registry updates
    ├── precedents.py     # precedents orchestration: backfill / embed / queue
    ├── embeddings.py     # local sentence-transformers wrapper
    ├── pdf_text.py       # PyMuPDF text extraction
    └── r2.py             # Cloudflare R2 client (mirror of the app's lib/r2.ts)
```

## Notes

- **Domain-agnostic core**: `ingest.py` knows nothing about any specific domain.
  All domain knowledge lives in `domains.yaml` and the adapters.
- **Phases 2 & 3 run in claude.ai**: enrichment and concept-connection are
  performed by Claude through the research MCP, using the per-domain guidance the
  pipeline publishes to `research_domains`. This repo contains no model or API
  client.
- **Safe table resolution**: the API and pipeline always resolve a table name
  from the `research_domains` registry — never from raw user input.
