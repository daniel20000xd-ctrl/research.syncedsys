#!/usr/bin/env python3
"""Enrich the corpus against one concept, or all of them.

    python enrich.py --concept <key> [--depth N] [--batch]
    python enrich.py --all [--batch]
    python enrich.py --status

The runner never takes a case list. Each pass asks the ledger (concept_evaluations) what is
still missing for the concept — the gap is the work queue — so a crash, a fixed bug, newly
ingested cases or a deeper candidate window are all repaired by re-running the same command.

A concept is defined once in concepts/<key>.yaml and inserted on its first run. Concepts are
immutable: to revise one, save it under a new key (<key>_2) and run that.

Strategies:
  lexical     regex patterns over title, summary and the R2 full text; deterministic
  vector_llm  the candidate_depth cases nearest the exemplars are adjudicated by Claude, the
              rest recorded as out_of_candidate_window; --depth N deepens the window and only
              newly surfaced cases cost anything
  sweep       every case adjudicated by Claude; level 0 goes through the Message Batches API
  manual      owner-set rows only; the runner reports coverage

Escalation ladder — an abstention, a confidence in the 0.35-0.75 band, or a "match" whose
evidence is not a verbatim quote is retried one level up on the next pass:
  0  Haiku 4.5   headnote + metadata   concept definition
  1  Sonnet 5    headnote + metadata   prompt_template
  2  Sonnet 5    + full text from R2   prompt_template, adaptive thinking
  3  Opus 5      + full text from R2   prompt_template, adaptive thinking, both sides argued
  4  flagged for owner review (method = 'abstain', escalation_level = 4)

--batch sends level-0 work through the Message Batches API (half price, separate rate limits;
always on for sweeps). Submitted batch ids are kept in .batches/ so an interrupted run collects
them instead of paying twice.

The ledger's `confidence` is the model's probability_applies (not certainty in its verdict).
Exit status: 0 clean, 1 stopped with work remaining, 2 spend limit reached.
Run from the repo root.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

for _stream in (sys.stdout, sys.stderr):
    _stream.reconfigure(encoding="utf-8")

import anthropic  # noqa: E402
import psycopg2.extras  # noqa: E402
import yaml  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv(".env.local")

from lib import db, r2  # noqa: E402

CONCEPTS_DIR = Path("concepts")
BATCH_DIR = Path(".batches")
IMMUTABLE_FIELDS = ("definition", "strategy", "patterns", "exemplars", "prompt_template", "candidate_depth")
STRATEGIES = {"lexical", "vector_llm", "sweep", "manual"}
METHODS = {"lexical": "lexical", "vector_llm": "llm_candidate", "sweep": "llm_sweep"}
READ_METHODS = ("lexical", "llm_candidate", "llm_sweep", "manual")
REVIEW_LEVEL = 4
UNCERTAIN_BAND = (0.35, 0.75)
MAX_PASSES = 8
PASS_BACKOFF = (5, 30, 120)
MAX_CONCURRENCY = 16
ATTEMPTS = 5
FLUSH_EVERY = 25
BATCH_CHUNK = 10_000
BATCH_POLL_SECONDS = 60
FALLBACK_BETA = "server-side-fallback-2026-07-01"


@dataclass(frozen=True)
class Level:
    model: str
    full_text: bool
    expanded: bool
    thinking: str | None
    max_tokens: int
    both_sides: bool = False


LEVELS = {
    0: Level("claude-haiku-4-5", full_text=False, expanded=False, thinking=None, max_tokens=1024),
    1: Level("claude-sonnet-5", full_text=False, expanded=True, thinking="disabled", max_tokens=1024),
    2: Level("claude-sonnet-5", full_text=True, expanded=True, thinking="adaptive", max_tokens=16000),
    3: Level("claude-opus-5", full_text=True, expanded=True, thinking="adaptive", max_tokens=16000, both_sides=True),
}
PRICES = {"claude-haiku-4-5": (1.0, 5.0), "claude-sonnet-5": (2.0, 10.0), "claude-opus-5": (5.0, 25.0)}

RULES = """You decide whether ONE Swedish court decision matches ONE concept. The concept text below is the only standard: apply it as written, not a broader or narrower idea of the topic.

Return:
- reasoning: one or two sentences.
- evidence: one contiguous passage copied exactly from the case material you were shown — no ellipses, no paraphrase, no added quotation marks. Required for "match". Otherwise quote the most relevant passage, or "".
- verdict: "match", "no_match" or "uncertain".
- probability_applies: your probability, from 0 to 1, that the concept applies to this case — near 0 for a clear no_match, near 1 for a clear match.

"uncertain" is a correct answer, not a failure. Give it when the case is genuinely borderline under the concept, or when what you were shown cannot settle the question (for example the headnote is silent on a point the full reasoning would decide). Uncertain cases are escalated to a stronger reader with more of the judgment. Do not guess to avoid it.

Never answer "match" without a passage you can quote exactly; without one the answer is "uncertain" or "no_match"."""

BOTH_SIDES = ("Before deciding, set out the strongest argument that the concept applies to this case and the "
              "strongest argument that it does not, then weigh them against the concept text.")

VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning": {"type": "string"},
        "evidence": {"type": "string"},
        "verdict": {"type": "string", "enum": ["match", "no_match", "uncertain"]},
        # Not "confidence": models read that as certainty in their verdict ("no_match, 0.95").
        "probability_applies": {"type": "number"},
    },
    "required": ["reasoning", "evidence", "verdict", "probability_applies"],
    "additionalProperties": False,
}

_UPSERT = """
insert into concept_evaluations (case_id, concept_id, matched, confidence, evidence, method,
                                 escalation_level, model, error_message)
values %s
on conflict (case_id, concept_id) do update set
  matched = excluded.matched, confidence = excluded.confidence, evidence = excluded.evidence,
  method = excluded.method, escalation_level = excluded.escalation_level, model = excluded.model,
  error_message = excluded.error_message, evaluated_at = now()
where concept_evaluations.method <> 'manual'
"""
_CASE_COLUMNS = """c.id, c.court, c.raw_data->'domstol'->>'domstolNamn', c.case_number, c.title, c.decision_date,
                   c.summary, c.full_text_path, c.raw_data->'lagrumLista', c.raw_data->'nyckelordLista'"""


class SpendLimitReached(Exception):
    pass


@dataclass
class PassStats:
    evaluated: int = 0
    escalated: int = 0
    errors: int = 0

    def count(self, row: tuple, method: str) -> None:
        self.evaluated += row[5] == method
        self.escalated += row[5] == "abstain"
        self.errors += row[5] == "error"

    def absorb(self, other: PassStats) -> None:
        self.evaluated += other.evaluated
        self.escalated += other.escalated
        self.errors += other.errors


# ──────────────────────────────────────────────────────────────────────────────
# Database helpers (each opens its own connection so with_retry can reconnect)
# ──────────────────────────────────────────────────────────────────────────────
def _query(sql: str, params: tuple = ()) -> list[tuple]:
    def run():
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if cur.description else []
            conn.commit()
            return rows
        finally:
            conn.close()
    return db.with_retry(run)


def _write(rows: list[tuple]) -> None:
    if not rows:
        return

    def run():
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _UPSERT, rows, page_size=500,
                                               template="(%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s)")
            conn.commit()
        finally:
            conn.close()
    db.with_retry(run)


def _fetch_cases(ids: list[str]) -> dict[str, tuple]:
    rows = _query(f"select {_CASE_COLUMNS} from cases c where c.id = any(%s::uuid[])", (ids,))
    return {str(r[0]): r for r in rows}


# ──────────────────────────────────────────────────────────────────────────────
# Concepts
# ──────────────────────────────────────────────────────────────────────────────
def _validate(key: str, spec: dict) -> None:
    missing = [f for f in ("definition", "strategy", "prompt_template") if not spec.get(f)]
    if missing:
        sys.exit(f"concepts/{key}.yaml is missing: {', '.join(missing)}")
    if spec["strategy"] not in STRATEGIES:
        sys.exit(f"strategy must be one of {sorted(STRATEGIES)}")
    if spec["strategy"] == "vector_llm" and not isinstance(spec.get("candidate_depth"), int):
        sys.exit("vector_llm concepts need an integer candidate_depth")
    if spec["strategy"] == "lexical":
        if not spec.get("patterns"):
            sys.exit("lexical concepts need patterns")
        for pattern in spec["patterns"]:
            re.compile(pattern)


def load_concept(key: str) -> dict:
    path = CONCEPTS_DIR / f"{key}.yaml"
    spec = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else None
    columns = ("id", "key", *IMMUTABLE_FIELDS)
    rows = _query(f"select {', '.join(columns)} from concepts where key = %s", (key,))
    if not rows:
        if spec is None:
            sys.exit(f"No concept '{key}' in the database and no {path}.")
        _validate(key, spec)
        json_or_none = lambda v: psycopg2.extras.Json(v) if v is not None else None  # noqa: E731
        rows = _query(
            f"insert into concepts (key, {', '.join(IMMUTABLE_FIELDS)}) values (%s, %s, %s, %s, %s, %s, %s) "
            f"returning {', '.join(columns)}",
            (key, spec["definition"], spec["strategy"], json_or_none(spec.get("patterns")),
             json_or_none(spec.get("exemplars")), spec["prompt_template"], spec.get("candidate_depth")),
        )
        print(f"[concept] inserted '{key}' from {path}")
    concept = dict(zip(columns, rows[0]))
    if spec is not None:
        changed = [f for f in IMMUTABLE_FIELDS if spec.get(f) != concept[f]]
        if changed:
            sys.exit(f"Concept '{key}' is immutable, but {path} differs in: {', '.join(changed)}. "
                     f"Save the revision under a new key (e.g. {key}_2) and run that.")
    return concept


# ──────────────────────────────────────────────────────────────────────────────
# Rate limits, spend and usage
# ──────────────────────────────────────────────────────────────────────────────
class Throttle:
    """Ramped concurrency plus header-driven backpressure, shared by every worker."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._in_flight = 0
        self._resume_at = 0.0
        self._started = time.monotonic()

    def _allowed(self) -> int:
        # Acceleration limits punish a cold burst, so concurrency ramps up over ~2 minutes.
        return min(MAX_CONCURRENCY, 2 + int((time.monotonic() - self._started) / 10))

    def acquire(self) -> None:
        with self._cond:
            while self._in_flight >= self._allowed() or time.monotonic() < self._resume_at:
                self._cond.wait(timeout=1.0)
            self._in_flight += 1

    def release(self) -> None:
        with self._cond:
            self._in_flight -= 1
            self._cond.notify_all()

    def pause(self, seconds: float) -> None:
        with self._cond:
            self._resume_at = max(self._resume_at, time.monotonic() + seconds)

    def observe(self, headers) -> None:
        for kind in ("requests", "input-tokens", "output-tokens"):
            limit = headers.get(f"anthropic-ratelimit-{kind}-limit")
            remaining = headers.get(f"anthropic-ratelimit-{kind}-remaining")
            reset = headers.get(f"anthropic-ratelimit-{kind}-reset")
            if limit and remaining and reset and int(remaining) < 0.1 * int(limit):
                until = datetime.fromisoformat(reset.replace("Z", "+00:00")) - datetime.now(timezone.utc)
                self.pause(min(max(until.total_seconds(), 1.0), 60.0))


class Usage:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: dict[tuple[str, bool], list[int]] = {}

    def add(self, model: str, usage, batch: bool = False) -> None:
        with self._lock:
            t = self._tokens.setdefault((model, batch), [0, 0, 0, 0])
            t[0] += usage.input_tokens or 0
            t[1] += usage.output_tokens or 0
            t[2] += usage.cache_creation_input_tokens or 0
            t[3] += usage.cache_read_input_tokens or 0

    def report(self) -> str:
        lines, total = [], 0.0
        for (model, batch), (fresh, out, written, cached) in sorted(self._tokens.items()):
            price_in, price_out = PRICES[model]
            cost = (fresh * price_in + written * price_in * 1.25 + cached * price_in * 0.1 + out * price_out) / 1e6
            cost *= 0.5 if batch else 1.0
            total += cost
            lines.append(f"  {model}{' (batch)' if batch else ''}: {fresh + written + cached:,} input "
                         f"({cached:,} cached), {out:,} output  ~${cost:.2f}")
        return "\n".join(["API usage:", *lines, f"  estimated cost ~${total:.2f}"]) if lines else "API usage: none"


USAGE = Usage()


def _spend_limit(error: anthropic.APIStatusError) -> bool:
    body = error.body if isinstance(error.body, dict) else {}
    details = (body.get("error") or {}).get("details") or body.get("details") or {}
    if details.get("error_code") == "enforced_spend_limit_reached":
        return True
    # A spend limit set in the Console answers 400 invalid_request_error, with no error code.
    return error.status_code == 400 and "reached your specified" in (error.message or "")


# ──────────────────────────────────────────────────────────────────────────────
# Adjudication
# ──────────────────────────────────────────────────────────────────────────────
def _listing(value) -> str:
    items = []
    for item in value or []:
        if isinstance(item, dict):
            item = item.get("referens") or item.get("namn") or json.dumps(item, ensure_ascii=False)
        items.append(str(item))
    return "; ".join(items)


def _case_material(case: tuple, level: Level) -> str:
    _, court, court_name, case_number, title, decision_date, summary, text_path, lagrum, keywords = case
    material = "\n".join([
        f"court: {court}" + (f" ({court_name})" if court_name else ""),
        f"case_number: {case_number or ''}",
        f"title: {title or ''}",
        f"decision_date: {decision_date or ''}",
        f"lagrum: {_listing(lagrum)}",
        f"keywords: {_listing(keywords)}",
        "",
        "headnote:",
        summary or "(none)",
    ])
    if level.full_text:
        if not text_path:
            raise LookupError("case has no full_text_path")
        material += "\n\nfull text:\n" + r2.download_text(text_path)
    return material


def _build_request(concept: dict, case: tuple, level: Level) -> tuple[dict, str]:
    material = _case_material(case, level)
    user = f"<case>\n{material}\n</case>" + (f"\n\n{BOTH_SIDES}" if level.both_sides else "")
    section = concept["prompt_template"] if level.expanded else concept["definition"]
    request = {
        "model": level.model,
        "max_tokens": level.max_tokens,
        "system": [{"type": "text", "text": f"{RULES}\n\n## Concept\n\n{section.strip()}",
                    "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": user}],
        "output_config": {"format": {"type": "json_schema", "schema": VERDICT_SCHEMA}},
    }
    if level.thinking:
        request["thinking"] = {"type": level.thinking}
    return request, material


_PUNCTUATION = str.maketrans({"”": '"', "“": '"', "„": '"', "’": "'", "‘": "'",
                              "–": "-", "—": "-", " ": " "})


def _normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text.translate(_PUNCTUATION)).strip().casefold()


def _verbatim(evidence: str, material: str) -> bool:
    quote = _normalized(evidence.strip().strip('"').strip())
    return len(quote) >= 12 and quote in _normalized(material)


def _interpret(result: dict, material: str) -> tuple[bool | None, float, str, str | None]:
    """(matched, confidence, evidence, abstain_reason); the ledger's confidence is probability_applies."""
    confidence = min(max(float(result.get("probability_applies", 0.5)), 0.0), 1.0)
    evidence = (result.get("evidence") or "").strip()
    verdict = result.get("verdict")
    low, high = UNCERTAIN_BAND
    if verdict == "uncertain":
        return None, confidence, evidence, "model abstained"
    if low <= confidence <= high:
        return None, confidence, evidence, f"probability_applies {confidence:.2f} inside the uncertain band"
    if verdict == "match" and confidence < low or verdict == "no_match" and confidence > high:
        return None, confidence, evidence, f"verdict {verdict} contradicts probability_applies {confidence:.2f}"
    if verdict == "match" and not _verbatim(evidence, material):
        return None, confidence, evidence, "match without a verbatim quote"
    return verdict == "match", confidence, evidence, None


def _row(concept: dict, case: tuple, level_no: int, outcome: str = "error", message: str | None = None,
         matched: bool | None = None, confidence: float | None = None, evidence: str | None = None,
         escalation: int | None = None) -> tuple:
    return (case[0], concept["id"], matched, confidence, evidence or None, outcome,
            level_no if escalation is None else escalation, LEVELS[level_no].model, message)


def _outcome(concept: dict, case: tuple, level_no: int, method: str, message, material: str) -> tuple:
    if message.stop_reason == "refusal":
        return _row(concept, case, level_no, message="refused by the model")
    text = next((b.text for b in message.content if b.type == "text"), "")
    try:
        result = json.loads(text)
    except ValueError:
        return _row(concept, case, level_no, message=f"unparseable output (stop_reason={message.stop_reason})")
    matched, confidence, evidence, reason = _interpret(result, material)
    if reason:
        escalation = REVIEW_LEVEL if level_no == max(LEVELS) else level_no
        return _row(concept, case, level_no, "abstain", reason, confidence=confidence, evidence=evidence,
                    escalation=escalation)
    return _row(concept, case, level_no, method, matched=matched, confidence=confidence, evidence=evidence)


def _evaluate(client: anthropic.Anthropic, throttle: Throttle, concept: dict, case: tuple,
              level_no: int, method: str) -> tuple:
    level = LEVELS[level_no]
    try:
        request, material = _build_request(concept, case, level)
    except Exception as e:  # noqa: BLE001 — recorded as an error row, retried next pass
        return _row(concept, case, level_no, message=f"full text unavailable ({case[7]}): {str(e)[:200]}")

    last_error = ""
    for attempt in range(ATTEMPTS):
        throttle.acquire()
        try:
            if level.model == "claude-opus-5":
                raw = client.beta.messages.with_raw_response.create(
                    **request, betas=[FALLBACK_BETA], extra_body={"fallbacks": "default"})
            else:
                raw = client.messages.with_raw_response.create(**request)
            throttle.observe(raw.headers)
            message = raw.parse()
        except anthropic.RateLimitError as e:
            if _spend_limit(e):
                raise SpendLimitReached(e.message) from e
            throttle.pause(float(e.response.headers.get("retry-after") or 30))
            last_error = f"429: {e.message}"
            continue
        except anthropic.BadRequestError as e:
            if _spend_limit(e):
                raise SpendLimitReached(e.message) from e
            return _row(concept, case, level_no, message=f"400: {e.message}"[:500])
        except (anthropic.InternalServerError, anthropic.APIConnectionError) as e:
            last_error = f"{type(e).__name__}: {e}"[:500]
            time.sleep(min(5 * 2 ** attempt, 60))
            continue
        except anthropic.APIStatusError as e:
            return _row(concept, case, level_no, message=f"{e.status_code}: {e.message}"[:500])
        finally:
            throttle.release()
        USAGE.add(level.model, message.usage)
        return _outcome(concept, case, level_no, method, message, material)
    return _row(concept, case, level_no, message=f"failed after {ATTEMPTS} attempts: {last_error}")


def _llm_pass(client: anthropic.Anthropic, concept: dict, todo: dict[str, int], method: str) -> PassStats:
    stats, buffer, throttle = PassStats(), [], Throttle()
    cases = _fetch_cases(list(todo))

    def collect(result: tuple) -> None:
        nonlocal buffer
        stats.count(result, method)
        buffer.append(result)
        if len(buffer) >= FLUSH_EVERY:
            _write(buffer)
            buffer = []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as pool:
        futures = {pool.submit(_evaluate, client, throttle, concept, cases[cid], level, method)
                   for cid, level in todo.items() if cid in cases}
        done = set()
        try:
            for future in as_completed(futures):
                done.add(future)
                collect(future.result())
        except SpendLimitReached:
            for future in futures - done:
                future.cancel()
            for future in futures - done:
                if not future.cancelled() and not isinstance(future.exception(), SpendLimitReached):
                    collect(future.result())
            _write(buffer)
            raise
    _write(buffer)
    return stats


def _batch_pass(client: anthropic.Anthropic, concept: dict, case_ids: list[str], method: str) -> PassStats:
    """Level-0 adjudication through the Message Batches API; collects pending batches first."""
    state = BATCH_DIR / f"{concept['id']}.json"
    batch_ids = json.loads(state.read_text()) if state.exists() else []
    if not batch_ids and case_ids:
        cases = _fetch_cases(case_ids)
        requests = [{"custom_id": cid, "params": _build_request(concept, case, LEVELS[0])[0]}
                    for cid, case in cases.items()]
        BATCH_DIR.mkdir(exist_ok=True)
        for i in range(0, len(requests), BATCH_CHUNK):
            try:
                batch = client.messages.batches.create(requests=requests[i:i + BATCH_CHUNK])
            except anthropic.APIStatusError as e:
                if _spend_limit(e):
                    raise SpendLimitReached(e.message) from e
                raise
            batch_ids.append(batch.id)
            state.write_text(json.dumps(batch_ids))
            print(f"  submitted batch {batch.id} ({len(requests[i:i + BATCH_CHUNK])} requests)")

    stats = PassStats()
    for batch_id in list(batch_ids):
        while (batch := client.messages.batches.retrieve(batch_id)).processing_status != "ended":
            counts = batch.request_counts
            print(f"  batch {batch_id}: {counts.succeeded + counts.errored} done, {counts.processing} processing")
            time.sleep(BATCH_POLL_SECONDS)
        results = list(client.messages.batches.results(batch_id))
        cases = _fetch_cases([r.custom_id for r in results])
        rows = []
        for result in results:
            case = cases.get(result.custom_id)
            if case is None:
                continue
            if result.result.type == "succeeded":
                message = result.result.message
                USAGE.add(LEVELS[0].model, message.usage, batch=True)
                rows.append(_outcome(concept, case, 0, method, message, _case_material(case, LEVELS[0])))
            else:
                detail = getattr(result.result, "error", None)
                rows.append(_row(concept, case, 0, message=f"batch request {result.result.type}: {detail}"[:500]))
            stats.count(rows[-1], method)
        _write(rows)
        batch_ids.remove(batch_id)
        if batch_ids:
            state.write_text(json.dumps(batch_ids))
        else:
            state.unlink()
    return stats


def _lexical_pass(concept: dict, todo: dict[str, int]) -> PassStats:
    patterns = [re.compile(p) for p in concept["patterns"]]
    cases = _fetch_cases(list(todo))

    def one(case: tuple) -> tuple:
        case_id, title, summary, text_path = case[0], case[4], case[6], case[7]
        try:
            body = r2.download_text(text_path) if text_path else ""
        except Exception as e:  # noqa: BLE001
            return (case_id, concept["id"], None, None, None, "error", 0, None,
                    f"full text unavailable ({text_path}): {str(e)[:200]}")
        text = "\n".join(p for p in (title, summary, body) if p)
        for pattern in patterns:
            if hit := pattern.search(text):
                snippet = text[max(0, hit.start() - 200):hit.end() + 200].strip()
                return (case_id, concept["id"], True, 1.0, snippet, "lexical", 0, None, None)
        return (case_id, concept["id"], False, 1.0, None, "lexical", 0, None, None)

    with ThreadPoolExecutor(max_workers=16) as pool:
        rows = list(pool.map(one, cases.values()))
    _write(rows)
    errors = sum(r[5] == "error" for r in rows)
    return PassStats(evaluated=len(rows) - errors, errors=errors)


# ──────────────────────────────────────────────────────────────────────────────
# The work query and the convergence loop
# ──────────────────────────────────────────────────────────────────────────────
def _candidate_window(concept: dict, depth: int) -> list[str]:
    exemplars = [str(x) for x in concept["exemplars"] or []]

    def run():
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                vectors = []
                if exemplars:
                    cur.execute("select embedding::text from cases where (id::text = any(%s) or source_id = any(%s)) "
                                "and embedding is not null", (exemplars, exemplars))
                    vectors = [r[0] for r in cur.fetchall()]
                    if len(vectors) < len(exemplars):
                        print(f"  warning: {len(exemplars) - len(vectors)} exemplar(s) missing or not embedded")
                if not vectors:
                    from lib.embeddings import Embedder, to_pgvector
                    vectors = [to_pgvector(Embedder().embed_queries([concept["definition"]])[0])]
                cur.execute("set local hnsw.ef_search = 1000")
                cur.execute("set local hnsw.iterative_scan = strict_order")
                best: dict[str, float] = {}
                for vector in vectors:
                    cur.execute("select id, embedding <=> %s::vector from cases where embedding is not null "
                                "order by embedding <=> %s::vector limit %s", (vector, vector, depth))
                    for case_id, distance in cur.fetchall():
                        best[case_id] = min(distance, best.get(case_id, 2.0))
            conn.rollback()
            return [cid for cid, _ in sorted(best.items(), key=lambda kv: kv[1])[:depth]]
        finally:
            conn.close()
    return db.with_retry(run)


def _pending(concept: dict, window: list[str] | None) -> dict[str, int]:
    """case_id -> escalation level to attempt, for everything the ledger still lacks."""
    sql = """select c.id, e.method, e.escalation_level from cases c
             left join concept_evaluations e on e.case_id = c.id and e.concept_id = %s
             where e.case_id is null or e.method = 'error' or (e.method = 'abstain' and e.escalation_level < %s)"""
    params: tuple = (concept["id"], REVIEW_LEVEL)
    if window is not None:
        sql += " or (e.method = 'out_of_candidate_window' and c.id = any(%s::uuid[]))"
        params += (window,)
    todo = {}
    for case_id, method, level in _query(sql, params):
        todo[str(case_id)] = level + 1 if method == "abstain" else level if method == "error" else 0
    return todo


def _coverage(concept_id: str) -> str:
    (read, matched, out_of_window, abstaining, review, errored, total), = _query(
        f"""select count(*) filter (where method in {READ_METHODS}), count(*) filter (where matched),
                   count(*) filter (where method = 'out_of_candidate_window'),
                   count(*) filter (where method = 'abstain' and escalation_level < {REVIEW_LEVEL}),
                   count(*) filter (where method = 'abstain' and escalation_level >= {REVIEW_LEVEL}),
                   count(*) filter (where method = 'error'), (select count(*) from cases)
            from concept_evaluations where concept_id = %s""", (concept_id,))
    return (f"Coverage: {read:,} / {total:,} read — matched {matched:,}, out of window {out_of_window:,}, "
            f"abstaining {abstaining:,}, for review {review:,}, errored {errored:,}")


def run_concept(client: anthropic.Anthropic, key: str, depth: int | None, batch: bool) -> int:
    concept = load_concept(key)
    strategy = concept["strategy"]
    print(f"\n== {key} ({strategy})")
    if strategy == "manual":
        print(_coverage(concept["id"]))
        return 0

    window = None
    if strategy == "vector_llm":
        effective = max(depth or 0, concept["candidate_depth"])
        window = _candidate_window(concept, effective)
        marked = _query("""insert into concept_evaluations (case_id, concept_id, matched, method)
                           select c.id, %s, false, 'out_of_candidate_window' from cases c
                           where not (c.id = any(%s::uuid[]))
                           on conflict (case_id, concept_id) do nothing returning 1""", (concept["id"], window))
        print(f"  candidate window: {len(window)} cases; {len(marked):,} newly recorded as out_of_candidate_window")

    batch = batch or strategy == "sweep"
    todo, pass_no = _pending(concept, window), 0
    while todo or (batch and (BATCH_DIR / f"{concept['id']}.json").exists()):
        pass_no += 1
        if strategy == "lexical":
            stats = _lexical_pass(concept, todo)
        else:
            stats = PassStats()
            level_zero = [cid for cid, level in todo.items() if level == 0] if batch else []
            if level_zero or (batch and (BATCH_DIR / f"{concept['id']}.json").exists()):
                stats.absorb(_batch_pass(client, concept, level_zero, METHODS[strategy]))
            synchronous = {cid: level for cid, level in todo.items() if not (batch and level == 0)}
            if synchronous:
                stats.absorb(_llm_pass(client, concept, synchronous, METHODS[strategy]))
        next_todo = _pending(concept, window)
        print(f"Pass {pass_no}: {len(todo)} remaining -> {stats.evaluated} evaluated, "
              f"{stats.escalated} escalated, {stats.errors} errors")
        progressed = len(next_todo) < len(todo) or stats.escalated > 0
        todo = next_todo
        if not todo:
            break
        if not progressed:
            print("  [no progress]")
            break
        if pass_no >= MAX_PASSES:
            print(f"  [stopped after {MAX_PASSES} passes]")
            break
        time.sleep(PASS_BACKOFF[min(pass_no - 1, len(PASS_BACKOFF) - 1)])

    _query("""update cases c set derived_tags = coalesce(m.ids, '[]'::jsonb)
              from (select c2.id, (select jsonb_agg(e.concept_id order by e.concept_id) from concept_evaluations e
                                   where e.case_id = c2.id and e.matched) as ids from cases c2) m
              where m.id = c.id and c.derived_tags is distinct from coalesce(m.ids, '[]'::jsonb)""")

    if todo:
        print(f"\nStopped: {len(todo)} case(s) could not be finished")
        for title, source_id, message in _query(
                """select c.title, c.source_id, e.error_message from concept_evaluations e join cases c on c.id = e.case_id
                   where e.concept_id = %s and e.method = 'error' order by c.title limit 20""", (concept["id"],)):
            print(f"   {title or source_id}: {message}")
    print(_coverage(concept["id"]))
    if window:
        edge = window[-min(50, len(window)):]
        (edge_matches,), = _query("select count(*) from concept_evaluations where concept_id = %s and matched "
                                  "and case_id = any(%s::uuid[])", (concept["id"], edge))
        hint = f" — ranking not bottomed out; consider --depth {len(window) * 2}" if edge_matches else ""
        print(f"Window edge: {edge_matches} of the last {len(edge)} candidates matched{hint}")
    return 1 if todo else 0


def status() -> None:
    rows = _query(f"""select c.key, c.strategy, count(*) filter (where e.method in {READ_METHODS}),
                             count(*) filter (where e.matched), count(*) filter (where e.method = 'out_of_candidate_window'),
                             count(*) filter (where e.method = 'abstain'), count(*) filter (where e.method = 'error'),
                             max(e.evaluated_at)
                      from concepts c left join concept_evaluations e on e.concept_id = c.id
                      group by c.id order by c.created_at""")
    (total,), = _query("select count(*) from cases")
    print(f"{total:,} cases")
    print(f"{'concept':<32}{'strategy':<12}{'read':>8}{'matched':>9}{'window-':>9}{'abstain':>9}{'error':>7}  last run")
    for key, strategy, read, matched, out_of_window, abstaining, errored, last_run in rows:
        when = f"{last_run:%Y-%m-%d %H:%M}" if last_run else "never"
        print(f"{key:<32}{strategy:<12}{read:>8}{matched:>9}{out_of_window:>9}{abstaining:>9}{errored:>7}  {when}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich the corpus against concepts")
    what = ap.add_mutually_exclusive_group(required=True)
    what.add_argument("--concept", metavar="KEY")
    what.add_argument("--all", action="store_true")
    what.add_argument("--status", action="store_true")
    ap.add_argument("--depth", type=int, help="deepen a vector_llm candidate window beyond candidate_depth")
    ap.add_argument("--batch", action="store_true", help="send level-0 work through the Message Batches API")
    args = ap.parse_args()

    if args.status:
        status()
        return
    client = anthropic.Anthropic(max_retries=0)
    keys = [args.concept] if args.concept else [k for (k,) in _query("select key from concepts order by created_at")]
    exit_code = 0
    try:
        for key in keys:
            exit_code = max(exit_code, run_concept(client, key, args.depth, args.batch))
    except SpendLimitReached as e:
        print(f"\nABORTED: API spend limit reached; every further request fails until access resumes.\n{e}")
        exit_code = 2
    print(USAGE.report())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
