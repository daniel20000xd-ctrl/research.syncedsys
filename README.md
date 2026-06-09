# syncedsys-pipeline

A domain-agnostic research data pipeline. A small local CLI (no server, no
scheduler, no GUI) that ingests, enriches, and connects research data across
independent domains — legal cases, academic studies, research projects — each in
its own table in a dedicated Supabase project (`syncedsys-research`).

Everything is driven by a single registry, **`domains.yaml`**. Adding a new
domain means adding one entry to that file — **zero code changes**.

## The three phases

| Phase | Script | What it does | When to run |
|-------|--------|--------------|-------------|
| **1 — Ingest** | `ingest.py` | Pull raw records from a source into the domain's table | When you want new/updated source data |
| **2 — Enrich** | `enrich.py` | Haiku reads each record and assigns **structural tags** (what the record explicitly *is*) | After ingest |
| **3 — Connect** | `connect.py` | Haiku searches for a **concept** latently across a domain and writes **derived tags** (connections discovered later) | Any time, with any concept |

Each phase is independently runnable and resumable — re-running picks up where it
left off, and ingest never overwrites enrichment.

The two tag layers are kept strictly separate on every record:

- `structural_tags` — added by Phase 2; what the record explicitly is.
- `derived_tags` — added by Phase 3; latent connections to a concept, each with
  reasoning and the specific passage that matched.

## Prerequisites

- **Python 3.11+**
- The **foundation migration must already be run** on the research Supabase
  project (`migrations/001_research_foundation.sql`, in this repo's `migrations/`
  folder). It creates `research_domains`, `concepts`, and the `update_updated_at()`
  function that every domain table's trigger depends on.
- A research Supabase project (separate from the main Syncedsys product DB) and
  an Anthropic API key.

## Setup

```bash
git clone <this repo>
cd syncedsys-pipeline
pip install -r requirements.txt
cp .env.example .env.local      # then fill in your keys
```

`.env.local` needs:

- `RESEARCH_SUPABASE_URL` / `RESEARCH_SUPABASE_SERVICE_KEY` — Supabase client (DML).
- `RESEARCH_DATABASE_URL` — direct Postgres connection string (used for DDL: the
  Supabase Python client can't create tables, so `psycopg2` does). Find it under
  Supabase → Project Settings → Database → Connection string (URI).
- `ANTHROPIC_API_KEY` — for Haiku enrichment.

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
       court_codes: ["HD"]
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

2. Ingest and enrich:

   ```bash
   python ingest.py --domain straffratt
   python enrich.py --domain straffratt
   ```

The table is created automatically on first ingest (base schema + your
`extra_columns` + indexes), and the domain is registered in `research_domains`.
No other files change.

To add a new **source type** (e.g. an academic API), drop a
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

### Phase 2 — Enrich
```bash
python enrich.py --domain arv_testamente                 # 50 pending records
python enrich.py --domain arv_testamente --batch-size 200
python enrich.py --domain arv_testamente --limit 10
python enrich.py --domain arv_testamente --force         # re-tag everything
```
One Haiku call per record. Records that error get `phase2_status = 'error'` with
the message stored; the run continues. Re-run to process the next batch.

### Phase 3 — Connect (the concept system)
```bash
# First run defines the concept (stored in the `concepts` table):
python connect.py --domain arv_testamente \
  --concept "retroaktivitetsprincipen" \
  --description "Cases touching whether a newer rule may be applied to facts predating it."

# Re-run the same concept on another domain later — description is reused:
python connect.py --domain migration --concept "retroaktivitetsprincipen"

# Tune the relevance bar:
python connect.py --domain arv_testamente --concept "laglottsskydd" --threshold 0.8
```
If you name a new concept without `--description`, you'll be prompted to type one
(end with a single `.` on its own line). A concept is defined once and can be
re-run on any domain at any time; each record is searched for a given concept
only once (resumable).

### Status
```bash
python status.py
```
Prints record counts, Phase 2 progress per domain, and every concept with its run
count and the domains it has been applied to.

## Project layout

```
syncedsys-pipeline/
├── domains.yaml          # domain registry — the single source of truth
├── ingest.py             # Phase 1
├── enrich.py             # Phase 2
├── connect.py            # Phase 3
├── status.py             # dashboard
├── adapters/             # one file per source type, each exposing fetch()
│   ├── domstolsverket.py
│   └── manual.py         # ingest a local JSON array of base-schema records
└── lib/
    ├── registry.py       # load + validate domains.yaml
    ├── schema.py         # generate CREATE TABLE SQL for a domain
    ├── db.py             # Supabase (DML) + psycopg2 (DDL) + registry updates
    └── haiku.py          # Anthropic Haiku wrapper (classify / search)
```

## Notes

- **Domain-agnostic core**: `ingest.py`, `enrich.py`, `connect.py` know nothing
  about any specific domain. All domain knowledge lives in `domains.yaml` and the
  adapters.
- **Safe table resolution**: the API and pipeline always resolve a table name
  from the `research_domains` registry — never from raw user input.
- **Model**: enrichment and concept search use `claude-haiku-4-5-20251001` at
  temperature 0, with a small inter-call delay and exponential backoff on rate
  limits.
