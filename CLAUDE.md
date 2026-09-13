# CLAUDE.md — research.syncedsys (case-law corpus v2)

Swedish higher-court corpus in the `syncedsys-research` Supabase project (ref `fjxhwxynrffpxchwxclo`),
content in Cloudflare R2. Two verbs only: **enrich** (`enrich.py`) and **search** (`mcp_server.py`).
Architecture spec: `syncedsys-architecture.md` (owner's document). v1 code is in `legacy-v1/` — do not
extend it. See README.md for commands.

## Invariants — do not regress

- **All ingest upserts on `source_id`**, and a re-run over unchanged data must write 0 rows (the
  `where ... is distinct from` clause in `ingest.py`). Never `DELETE FROM cases`; vanished source
  records are reported only.
- **`area` is set only by classification; `source_area_code` is never filterable.** It is absent from
  every `mcp_*` view by construction — keep it that way when editing views.
- **Postgres stays a thin index.** No full text in Postgres. `raw_data` = source record minus
  `innehall` (storing the body once pushed this free-tier DB past 500 MB into read-only mode); the
  untouched record is R2 `raw.json`. Current size ~0.4 GB of the 0.5 GB cap — check
  `pg_database_size` before adding anything heavy.
- **Concepts are immutable.** `pipeline` has table-level SELECT/INSERT/DELETE and column-level
  UPDATE(key) only. A column-level REVOKE does nothing against a table-level grant, so never grant
  table-level UPDATE on `concepts`. `postgres` (owner) can still edit — don't.
- **`id` is the foreign key; `key` is a label.** Never reference a concept by key from data.
- **Ledger honesty.** `out_of_candidate_window` means deliberately not read; `manual` rows are never
  overwritten (upsert guard); `error`/`abstain` rows are queue state that the next pass overwrites.
- **MCP server is read-only and four tools.** New needs become parameters, not tools; never add a
  tool-discovery tool. Writes would go in a separate server.
- Credentials only in `.env.local` (gitignored). Role passwords are set by `migrate.py`, never in SQL.

## How enrichment works (details the code relies on)

- Work query = cases with no ledger row, `error`, `abstain` below level 4, or (vector_llm)
  `out_of_candidate_window` rows that fall inside the current window. `--depth` deepens the window
  without mutating `candidate_depth`.
- Level 0 sends `definition`; levels 1–3 send `prompt_template` verbatim, both after the fixed
  `RULES` text in `enrich.py`. Levels 0–1 see headnote + metadata (court, case number, title, date,
  `lagrumLista`, `nyckelordLista`); levels 2–3 add the R2 full text.
- The model returns `probability_applies`, stored as the ledger's `confidence`. Never rename it back to
  "confidence": Haiku read that as certainty in its verdict ("no_match, 0.95") and the first test run
  escalated 141 of 150 clear rejections. 0.35–0.75, a verdict contradicting the probability,
  or a match whose evidence isn't a verbatim quote (whitespace/quote-normalized) →
  `abstain`; the reason goes in `error_message`. A level-3 abstention is stored at level 4 (review).
- Structured outputs (`output_config.format`) on all three models; Opus 5 calls go through
  `client.beta.messages` with `server-side-fallback-2026-07-01` + `fallbacks: "default"` (refusals).
- `anthropic.Anthropic(max_retries=0)`: the runner retries itself so the spend-cap 429
  (`error.details.error_code == enforced_spend_limit_reached`, no retry-after) aborts instead of
  retrying. A Console-set spend limit is a 400 "You have reached your specified…" — also aborts.
- Concurrency ramps from 2 to 16 over ~2 minutes (acceleration limits) and pauses when any
  `anthropic-ratelimit-*-remaining` drops below 10%.
- `derived_tags` = matched concept ids, rebuilt from the ledger at the end of each run.
- Sweeps and `--batch` send level-0 reads through the Message Batches API; escalations stay
  synchronous. Submitted batch ids live in `.batches/<concept_id>.json` (gitignored) so an
  interrupted run collects the results instead of paying twice — never delete it while a batch is pending.
- The window-edge hint is a heuristic, not proof: on `kringgaende_av_laglott`, depth 300 showed 0
  matches among its last 50 candidates while RH 2008:47 — a case the concept's own examples call a
  match — sat at rank 313. When a lexical signal exists (e.g. statute references in
  `raw_data->'lagrumLista'`), cross-check the window against it before trusting its depth.

## Gotchas

- **Connections:** only the session pooler works (`aws-0-eu-west-1.pooler.supabase.com:5432`); the
  direct `db.<ref>` host is IPv6-only. Custom roles log in as `<role>.<project-ref>` (`lib/db.py`).
- **Free-tier pause:** after ~a week idle the project pauses: pooler says `tenant/user ... not found`
  and PostgREST returns 502. Restore in the Supabase dashboard (possible for 90 days).
- **Supabase default privileges** grant every new table and view to `anon`/`authenticated`; RLS
  protects tables but NOT views. Any new view needs `revoke all ... from anon, authenticated`.
- **pgvector HNSW** returns at most `hnsw.ef_search` rows (default 40): set `ef_search` and
  `iterative_scan` before ANN queries with large limits or filters (done in `enrich.py` and the server).
- **Embeddings:** local `KBLab/sentence-bert-swedish-cased` (768d, max 384 tokens, so effectively
  summary + title + start of body). Query and corpus must use the same model; that is why the MCP
  server is local stdio rather than on Vercel.
- **MCP SDK v2** (`mcp>=2.2`): `from mcp.server import MCPServer`; raise
  `mcp.server.mcpserver.exceptions.ToolError` for messages the model should see — any other
  exception reaches the client only as "Error executing tool <name>". Nothing may print to stdout.
- **Anthropic SDK 1.x** (httpx2): raw headers via `.with_raw_response`, `.parse()` for the message.
- **Background runs:** stdout redirected to a file is block-buffered — progress lines appear only at
  exit. Watch the database instead (`select count(*) from cases`).
- **Domstolsverket API:** `POST /api/v1/sok`, 0-based `sidIndex`, court codes like `HDO` (not `HD`);
  PDFs at `GET /api/v1/bilagor/{URL-encoded fillagringId}` (plural `bilagor`). Rename upstream = breakage.
- The Grep tool skips gitignored paths (`.venv/`); grep the installed SDKs with Bash.
- Pending owner decisions: dropping the v1 tables (`precedents`, `classification_queue`,
  `research_domains`, `arv_testamente`, `ingestion_progress`, `legacy_v1_concepts`, ~190 MB), the hub
  `library_items` legal_case copies, and the i.syncedsys research routes/pages that read v1 tables.
  Area classification for cases added after v1 has no v2 runner yet.
- **111 v1 precedents are not in `cases`** because their source ids no longer exist at Domstolsverket
  (95 of them classified): 15 were republished under a new id with the same case number, 11 were
  duplicate publications of a decision that did carry over, ~85 post-2025 HD judgment records have no
  match (likely superseded by a referat). Their classifications live only in `precedents` — decide
  before dropping it.
