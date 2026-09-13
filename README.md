# research.syncedsys — Swedish case-law corpus

~17.4k Swedish higher-court decisions (Domstolsverket "Sök rättspraxis"), used in exactly two ways:

1. **Enrich** — `enrich.py`, run deliberately, evaluates every case against a *concept* with the
   Claude API and records the verdict in a ledger.
2. **Search** — `mcp_server.py`, a read-only MCP server with four tools.

The cases are fixed; the concept layer is what grows. Everything else is out of scope.

## Layout

| path | role |
|---|---|
| `migrations/005_concept_corpus.sql` | v2 schema: `cases`, `concepts`, `concept_evaluations`, roles, `mcp_*` views (001–004 are v1 history) |
| `migrate.py` | applies a migration as `postgres` and sets the role passwords from `.env.local` |
| `ingest.py` | `backfill` / `embed` / `verify` from Domstolsverket into `cases` |
| `adapters/domstolsverket_precedents.py` | the source API: paging, PDFs, normalization |
| `concepts/<key>.yaml` | concept definitions (inserted on first run, immutable after) |
| `enrich.py` | the one enrichment runner |
| `mcp_server.py` | read-only MCP server (stdio) |
| `lib/` | `db` (role connections), `r2` (Cloudflare R2), `embeddings` (local KBLab), `pdf_text` |
| `legacy-v1/` | superseded v1 code, reference only |

## Setup

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
cp .env.example .env.local   # fill in; PIPELINE_/MCP_READER_DB_PASSWORD can be any strong random strings
.venv/Scripts/python migrate.py migrations/005_concept_corpus.sql
```

Run every script from the repo root.

## Corpus

```bash
python ingest.py backfill   # full pull; re-running over unchanged data writes nothing
python ingest.py embed      # KBLab embeddings for cases without one
python ingest.py verify     # per-court totals, Domstolsverket vs database
```

Postgres is a thin index over R2 (`precedents/<court>/<source_id>/`): `text.txt` (extracted text),
PDF originals and `raw.json` (the untouched source record). `cases.raw_data` is that record minus
the `innehall` body. A case that vanishes from the source is reported, never deleted.

Source limitation: referat go back to 1981, but full judgments (domar och beslut) only exist from
4 March 2025. No ingestion strategy changes that.

## Concepts and enrichment

A concept file, `concepts/<key>.yaml`:

```yaml
strategy: vector_llm        # lexical | vector_llm | sweep | manual
candidate_depth: 150        # vector_llm: nearest cases to the exemplars that Claude reads
exemplars: [<case id or source_id>, ...]
patterns: ['(?i)regex', ...]  # lexical only
definition: |               # terse; sent at escalation level 0
  ...
prompt_template: |          # definition + edge cases + worked examples; sent verbatim at levels 1-3
  ...
```

```bash
python enrich.py --concept <key>              # also the repair command after any failure
python enrich.py --concept <key> --depth 400  # deepen a vector_llm window; only new candidates cost
python enrich.py --concept <key> --batch      # level-0 reads via the Message Batches API (half price; sweeps always do)
python enrich.py --all
python enrich.py --status
```

Concepts are immutable (enforced by grants). To revise one, save it as `<key>_2` and run that;
rename a key freely in SQL. The runner never takes a case list: each pass computes what the ledger
still lacks, loops while it makes progress (backoff 5 s / 30 s / 2 min) and exits non-zero when it
stops with work left (2 if the API spend limit is reached).

Escalation — an abstention, confidence in 0.35–0.75, or a "match" without a verbatim quote moves
the case one level up on the next pass: 0 Haiku 4.5 (headnote) → 1 Sonnet 5 (headnote, expanded
prompt) → 2 Sonnet 5 (full text) → 3 Opus 5 (full text, both sides) → 4 owner review
(`method = 'abstain' and escalation_level = 4`).

Ledger `method`: `lexical`, `llm_candidate`, `llm_sweep`, `out_of_candidate_window` (deliberately
never read), `abstain`, `error`, `manual` (never overwritten). Authoring loop: run a concept on a
shallow window, read ~20 matches' and ~20 non-matches' `evidence`, then revise as a new concept.

## MCP server

`search_cases`, `get_case`, `get_case_text`, `list_concepts` — read-only, as the `mcp_reader`
role, which can only select from the `mcp_*` views (`source_area_code` and `raw_data` are not in
them). Semantic queries use the same local KBLab model as the corpus, so the server runs locally
over stdio (Claude Code / Claude Desktop, not claude.ai).

```bash
claude mcp add --scope user case-law -- A:/Projects/syncedsys-pipeline/.venv/Scripts/python.exe A:/Projects/syncedsys-pipeline/mcp_server.py
```

## Database roles

| role | access | used by |
|---|---|---|
| `postgres` | owner | `migrate.py` only |
| `pipeline` | read/write `cases`, `concept_evaluations`; `concepts` insert/delete + rename only | `ingest.py`, `enrich.py` |
| `mcp_reader` | `SELECT` on `mcp_cases`, `mcp_case_search`, `mcp_concepts`, `mcp_concept_matches` | `mcp_server.py` |

RLS is on with zero policies; `anon`/`authenticated` have no grants on the corpus.
