# CLAUDE.md — research.syncedsys (case-law corpus v2)

Swedish higher-court corpus in the `syncedsys-research` Supabase project (ref `fjxhwxynrffpxchwxclo`),
content in Cloudflare R2. Two verbs only: **enrich** (`enrich.py`) and **search** (`mcp_server.py`).
Architecture spec: `C:\Users\Danie\Downloads\syncedsys-architecture.md` (owner's document). v1 code is in
`legacy-v1/` — do not extend it.

**v2 lives on branch `corpus-v2`** — not merged; `main` still holds the v1 code and a v1 CLAUDE.md.
Work from the repo root (`A:\Projects\syncedsys-pipeline`) with the venv interpreter:
`./.venv/Scripts/python.exe` in Bash, `.venv\Scripts\python.exe` in PowerShell.

Last known state (2026-09-17 — trust `enrich.py --status` over this): 17,356 cases, all embedded; one
concept, `kringgaende_av_laglott` (vector_llm, window 350, 10 matches, window edge suggests going
deeper); database 389 MB of the 500 MB free-tier cap.

## Enrich a concept — runbook

### 1. Preflight

1. `python enrich.py --status` lists every concept's coverage and proves the database is up.
   `tenant/user pipeline.fjxhwxynrffpxchwxclo not found` (or a PostgREST 502) means the free-tier project
   auto-paused: ask the owner to restore it at https://supabase.com/dashboard/project/fjxhwxynrffpxchwxclo,
   then retry. Don't loop on it.
2. `.env.local` must set `RESEARCH_DATABASE_URL`, `PIPELINE_DB_PASSWORD`, `ANTHROPIC_API_KEY` and the R2
   vars (`CF_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`). The Anthropic key is shared with the
   hub's in-app Claude, so enrichment spend counts against the same monthly cap.
3. No other `enrich.py` may be running on the same concept — two runs read the same pending cases twice:
   `Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -like '*enrich.py*' }`
4. If the corpus may be stale: `python ingest.py verify` (exit 1 = the source has cases the database
   lacks), then `python ingest.py backfill` (~15 min) and `python ingest.py embed`. Do this before creating a
   vector_llm concept — a case without an embedding can't enter a candidate window.
5. Each concept adds one ledger row per case (~170 bytes measured on 2026-09-17: `concept_evaluations`
   was 23 MB across 8 concepts × 17,356 cases). Keep
   `select pg_size_pretty(pg_database_size(current_database()))` well under 500 MB — `cases` (embeddings +
   their HNSW index) and, until dropped, legacy `precedents` are the real space, not the ledger.

### 2. Write `concepts/<key>.yaml`

Start from `concepts/kringgaende_av_laglott.yaml`. The filename is the key: snake_case ASCII, never reused.

| field | meaning |
|---|---|
| `strategy` | `vector_llm` — the default, for ideas without fixed wording · `lexical` — fixed wording such as a statute reference · `sweep` — every case; only when vector recall can't be trusted, and ask the owner first · `manual` |
| `definition` | terse: what matches and what doesn't. The only concept text Haiku sees at level 0 |
| `prompt_template` | the definition plus *räknas som match / räknas inte som match / gränsfall / exempel* sections; sent verbatim at levels 1–3. Writing it in Swedish works well |
| `exemplars` | vector_llm: 2–3 known hits, as `cases.id` or `source_id` |
| `candidate_depth` | vector_llm, required integer: start at 150–300 |
| `patterns` | lexical: Python regexes (prefix `(?i)` for case-insensitive) over title, headnote and full text |

Find exemplars by title or case number and confirm `embedded` is true. An unknown or unembedded exemplar
only logs a warning, and if none resolve the window falls back to embedding the definition — a weaker anchor.

```sql
select id, title, decision_date, embedding is not null as embedded, left(summary, 160)
from cases where title ilike '%NJA 2022 s. 277%' or case_number ilike '%T 1234-19%';
```

**Review the file before the first run.** The concept row is inserted on first run and is immutable from
then on: if a later edit changes any parsed field, the runner exits with `differs in: …`. Revise by saving
`<key>_2`, a new concept with its own ledger. Never edit or delete the old row.

### 3. Run

```bash
./.venv/Scripts/python.exe -u enrich.py --concept <key>
```

- Long runs: redirect to a log and keep `-u`, or nothing prints until exit. Each pass prints one line;
  in between, watch progress with
  `select method, count(*) from concept_evaluations where concept_id = (select id from concepts where key = '<key>') group by method;`
- `--depth N` deepens a vector_llm window; only newly surfaced cases are read. Don't combine it with `--all`.
- `--batch` sends level-0 reads through the Message Batches API: half price, results in minutes to hours
  while the runner polls every 60 s. Sweeps always batch.
- `--all` reruns every concept at its own depth — the repair pass after a systemic fix.
- Exit 0 clean · 1 stopped with work left (the output lists the errored cases) · 2 API spend limit reached
  (every request fails until the cap resets or the owner raises it — stop and tell the owner).
- Measured cost on 2026-09-13: 150 candidates at level 0 ≈ $0.25–0.28; 50 via `--batch` ≈ $0.04.
  Estimates: a level-2 Sonnet 5 full-text read costs a few cents per case, a level-3 Opus 5 read ~$0.10; a
  full sweep ≈ $14 at level 0 plus escalations. `lexical` costs no API money but downloads each pending
  case's full text from R2 (~400 MB on a first run). `lexical` and `sweep` haven't run on real data yet.

### 4. Review the results

The run ends with `Coverage: …` and `Window edge: …` lines. Then read the verdicts with SQL, as the
pipeline role (writes also need `conn.commit()`), or in the Supabase SQL editor:

```bash
./.venv/Scripts/python.exe - <<'EOF'
from dotenv import load_dotenv; load_dotenv('.env.local')
from lib import db
conn = db.connect(); cur = conn.cursor()
cur.execute(r"""select 1""")
for row in cur.fetchall(): print(row)
conn.close()
EOF
```

```sql
-- matches, with their verbatim evidence
select c.title, e.escalation_level, round(e.confidence::numeric, 2) as p, e.evidence
from concept_evaluations e join cases c on c.id = e.case_id join concepts k on k.id = e.concept_id
where k.key = '<key>' and e.matched order by c.decision_date;

-- the closest rejections among cases Claude read (non-matches rarely carry evidence: judge by headnote)
select c.title, round(e.confidence::numeric, 2) as p, c.summary
from concept_evaluations e join cases c on c.id = e.case_id join concepts k on k.id = e.concept_id
where k.key = '<key>' and e.matched = false and e.method <> 'out_of_candidate_window'
order by e.confidence desc limit 20;

-- errors and abstentions; escalation_level 4 is the owner's review list
select c.title, e.method, e.escalation_level, e.error_message
from concept_evaluations e join cases c on c.id = e.case_id join concepts k on k.id = e.concept_id
where k.key = '<key>' and e.method in ('abstain', 'error') order by e.escalation_level desc;

-- recall check against a lexical signal, here cases citing 7 kap. 4 § ärvdabalken (SFS 1958:637)
select c.title, e.method, e.matched
from cases c join concept_evaluations e on e.case_id = c.id
join concepts k on k.id = e.concept_id and k.key = '<key>'
where exists (select 1 from jsonb_array_elements(coalesce(c.raw_data->'lagrumLista', '[]')) l
              where l->>'sfsNummer' = '1958:637' and l->>'referens' ~* '^7 kap\.? 4 §');
```

Then:
- Matches latch onto a surface feature, or cases the definition should cover are rejected → write
  `<key>_2`, adding the missed cases as exemplars. Don't patch ledger rows.
- The window edge reports matches, or the recall check finds hits marked `out_of_candidate_window` → rerun
  with a larger `--depth`. A clean window edge alone is not proof (see How enrichment works).
- `error` rows → fix the cause if it is structural (e.g. a missing R2 object), then rerun the same command.
- Level-4 abstentions → the owner decides. Record the decision as a `manual` row, which no automated run
  overwrites:

```sql
insert into concept_evaluations (case_id, concept_id, matched, confidence, evidence, method)
select c.id, k.id, true, 1.0, 'owner review', 'manual'  -- false, 0.0 to record a rejection
from cases c, concepts k where c.title = '<title>' and k.key = '<key>'
on conflict (case_id, concept_id) do update set
  matched = excluded.matched, confidence = excluded.confidence, evidence = excluded.evidence,
  method = 'manual', escalation_level = 0, model = null, error_message = null, evaluated_at = now();
```

### 5. Stop or repair a run

- Stop (kills the venv launcher and its child process):
  `Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -like '*enrich.py --concept <key>*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }`
  Rerunning the same command resumes; up to 25 unflushed verdicts are simply read again. Leave
  `.batches/<concept_id>.json` in place while a batch is pending, or its paid results are lost.
- A runner bug wrote wrong verdicts → fix the code, put the affected rows back in the queue instead of
  deleting them, and rerun (precedent: the 2026-09-13 `probability_applies` fix):

```sql
update concept_evaluations
set method = 'error', matched = null, confidence = null, evidence = null, escalation_level = 0,
    model = null, error_message = 'reset for retry: <why>', evaluated_at = now()
where concept_id = (select id from concepts where key = '<key>') and <which rows>;
```

- Never delete a concept or ledger rows without the owner: deleting a concept cascades its whole ledger.

### 6. Finish

Results are searchable immediately — the MCP server reads the ledger through views, and `derived_tags` is
rebuilt at the end of every run. Commit the concept file (and any fix) on `corpus-v2` and push. The MCP
server is also the quickest way to find exemplars and read matches (`search_cases`, `get_case`,
`list_concepts`); check `claude mcp list`, and register it with
`claude mcp add --scope user case-law -- A:/Projects/syncedsys-pipeline/.venv/Scripts/python.exe A:/Projects/syncedsys-pipeline/mcp_server.py`.

## Invariants — do not regress

- **All ingest upserts on `source_id`**, and a re-run over unchanged data must write 0 rows (the
  `where ... is distinct from` clause in `ingest.py`). Never `DELETE FROM cases`; vanished source
  records are reported only.
- **`area` is set only by classification; `source_area_code` is never filterable.** It is absent from
  every `mcp_*` view by construction — keep it that way when editing views.
- **Postgres stays a thin index.** No full text in Postgres. `raw_data` = source record minus
  `innehall` (storing the body once pushed this free-tier DB past 500 MB into read-only mode); the
  untouched record is R2 `raw.json`.
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
- Sweeps and `--batch` keep submitted batch ids in `.batches/<concept_id>.json` (gitignored) so an
  interrupted run collects the results instead of paying twice; escalations above level 0 stay synchronous.
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
- **Background runs:** when output goes to a file, run Python with `-u`; otherwise it is block-buffered
  and progress lines appear only at exit.
- **Domstolsverket API:** `POST /api/v1/sok`, 0-based `sidIndex`, court codes like `HDO` (not `HD`);
  PDFs at `GET /api/v1/bilagor/{URL-encoded fillagringId}` (plural `bilagor`). Rename upstream = breakage.
- The Grep tool skips gitignored paths (`.venv/`); grep the installed SDKs with Bash.

## Open owner decisions — don't act on these without the owner

- Merging `corpus-v2` into `main`.
- Dropping the v1 tables (`precedents`, `classification_queue`, `research_domains`, `arv_testamente`,
  `ingestion_progress`, `legacy_v1_concepts`, ~190 MB), the hub `library_items` legal_case copies, and the
  i.syncedsys research routes/pages that read the v1 tables.
- **111 v1 precedents are not in `cases`** because their source ids no longer exist at Domstolsverket
  (95 of them classified): 15 were republished under a new id with the same case number, 11 were
  duplicate publications of a decision that did carry over, ~85 post-2025 HD judgment records have no
  match (likely superseded by a referat). Their classifications live only in `precedents` — decide
  before dropping it.
- Area classification: 2,532 cases have no `area`, and v2 has no area runner.
