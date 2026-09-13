#!/usr/bin/env python3
"""Read-only MCP server over the case-law corpus. Four tools, nothing else:

  search_cases   hybrid / lexical / semantic search with concept, court, area and date filters
  get_case       one decision: headnote, tags, matched concepts with their evidence
  get_case_text  the full text from R2, paginated
  list_concepts  every concept with its definition and coverage

Connects as mcp_reader (SELECT on the mcp_* views only) and embeds queries with the same local
KBLab model as the corpus. stdio transport — stdout is the protocol wire, so log to stderr only.

  claude mcp add --scope user case-law -- A:/Projects/syncedsys-pipeline/.venv/Scripts/python.exe A:/Projects/syncedsys-pipeline/mcp_server.py
"""
from __future__ import annotations

import base64
import json
import os
import sys
import threading
import uuid
from datetime import date
from pathlib import Path
from typing import Annotated, Literal

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env.local")
os.environ.setdefault("HF_HUB_OFFLINE", "1")  # the model is cached locally; skip Hub lookups on every start

from mcp.server import MCPServer  # noqa: E402
from mcp.server.mcpserver.exceptions import ToolError  # noqa: E402
from mcp.types import ToolAnnotations  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from adapters.domstolsverket_precedents import detail_url  # noqa: E402
from lib import db, r2  # noqa: E402
from lib.embeddings import Embedder, to_pgvector  # noqa: E402

PAGE_CHARS = 12_000
POOL = 200
RRF_K = 60
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)

mcp = MCPServer(
    "case-law",
    instructions=(
        "Read-only corpus of ~17k Swedish higher-court decisions. Map an idea to concept keys with "
        "list_concepts, retrieve with search_cases, verify with get_case and get_case_text. Whenever a "
        "search filters on concepts, tell the user the concept_coverage numbers."
    ),
)

_embedder = Embedder()
_embedder_lock = threading.Lock()


def _query_vector(text: str) -> str:
    with _embedder_lock:
        return to_pgvector(_embedder.embed_queries([text])[0])


def _rows(sql: str, params: list | tuple = ()) -> list[tuple]:
    def run():
        conn = db.connect("mcp_reader")
        try:
            with conn.cursor() as cur:
                cur.execute("set local hnsw.ef_search = 400")
                cur.execute("set local hnsw.iterative_scan = relaxed_order")
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.rollback()
            conn.close()
    return db.with_retry(run)


class CaseHit(BaseModel):
    case_id: str
    court: str
    case_number: str | None
    title: str | None
    summary: str | None
    decision_date: str | None
    area: str | None
    matched_concepts: list[str]


class ConceptCoverage(BaseModel):
    key: str
    matched_count: int
    evaluated_count: int
    unevaluated_count: int = Field(description="Cases never read for this concept, including those outside its candidate window.")


class SearchResult(BaseModel):
    results: list[CaseHit]
    next_cursor: str | None
    has_more: bool
    concept_coverage: list[ConceptCoverage]


class ConceptMatch(BaseModel):
    key: str
    confidence: float | None
    evidence: str | None
    method: str


class Case(BaseModel):
    case_id: str
    court: str
    case_number: str | None
    title: str | None
    summary: str | None
    decision_date: str | None
    area: str | None
    structural_tags: list[dict]
    matched_concepts: list[ConceptMatch]
    source_url: str
    has_full_text: bool


class CaseText(BaseModel):
    case_id: str
    page: int
    total_pages: int
    text: str


class Concept(BaseModel):
    key: str
    definition: str
    strategy: str
    evaluated_count: int
    matched_count: int
    out_of_window_count: int
    abstaining_count: int
    abstaining_at_max_count: int
    errored_count: int
    unevaluated_count: int
    last_run_at: str | None


class ConceptList(BaseModel):
    total_cases: int
    concepts: list[Concept]


def _concepts() -> list[tuple]:
    return _rows("""select id, key, definition, strategy, evaluated_count, matched_count, out_of_window_count,
                           abstaining_count, abstaining_at_max_count, errored_count, unevaluated_count, last_run_at
                    from mcp_concepts order by key""")


def _uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise ToolError(f"'{value}' is not a case_id (expected a UUID from search_cases).") from None


def _date(value: str | None, name: str) -> str | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise ToolError(f"{name} must be YYYY-MM-DD, got '{value}'.") from None


def _fuse(*rankings: list[str]) -> list[str]:
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, case_id in enumerate(ranking):
            scores[case_id] = scores.get(case_id, 0.0) + 1.0 / (RRF_K + rank + 1)
    return sorted(scores, key=scores.__getitem__, reverse=True)


@mcp.tool(annotations=READ_ONLY)
def search_cases(
    query: Annotated[str, Field(description="Free text: a legal question, a statute reference such as '7 kap. 4 § ärvdabalken', a citation or a case number. Leave empty to list by filters, newest first.")] = "",
    concepts: Annotated[list[str] | None, Field(description="Concept keys from list_concepts; a case must match all of them.")] = None,
    court: Annotated[str | None, Field(description="Court code, e.g. HDO (Högsta domstolen), HFD, ADO, MMOD, PMOD, MIOD. get_case shows a decision's code.")] = None,
    area: Annotated[str | None, Field(description="Legal area set by classification, e.g. arv_testamente, patenträtt, straffrätt.")] = None,
    date_from: Annotated[str | None, Field(description="Earliest decision date, YYYY-MM-DD.")] = None,
    date_to: Annotated[str | None, Field(description="Latest decision date, YYYY-MM-DD.")] = None,
    mode: Annotated[Literal["hybrid", "lexical", "semantic"], Field(description="hybrid fuses full-text and embedding rankings; lexical for exact wording; semantic for ideas without fixed wording.")] = "hybrid",
    limit: Annotated[int, Field(ge=1, le=50)] = 10,
    cursor: Annotated[str | None, Field(description="next_cursor from the previous call with the same arguments.")] = None,
) -> SearchResult:
    """Search Swedish higher-court decisions: HD, HFD, AD, MÖD, PMÖD, MIG, hovrätter and kammarrätter; referat from 1981, full judgments only from March 2025.

    Concepts are ideas already evaluated across the corpus; call list_concepts first when the user describes an idea rather than wording. A concept only finds cases it has read, so when filtering on concepts, report concept_coverage (unevaluated_count) alongside the results. Ranked searches return at most the top 200 per ranking.
    """
    known = {key: (cid, matched, evaluated, unevaluated)
             for cid, key, _, _, evaluated, matched, _, _, _, _, unevaluated, _ in _concepts()}
    unknown = [k for k in concepts or [] if k not in known]
    if unknown:
        raise ToolError(f"Unknown concept key(s): {', '.join(unknown)}. Valid keys: {', '.join(sorted(known)) or 'none yet'}.")
    concept_ids = [known[k][0] for k in concepts or []]

    clauses, params = ["true"], []
    for clause, value in (("c.court = %s", court), ("c.area = %s", area),
                          ("c.decision_date >= %s::date", _date(date_from, "date_from")),
                          ("c.decision_date <= %s::date", _date(date_to, "date_to"))):
        if value:
            clauses.append(clause)
            params.append(value)
    if concept_ids:
        clauses.append("c.id in (select case_id from mcp_concept_matches where concept_id = any(%s::uuid[]) "
                       "group by case_id having count(distinct concept_id) = %s)")
        params += [concept_ids, len(concept_ids)]
    where = " and ".join(clauses)

    try:
        offset = json.loads(base64.urlsafe_b64decode(cursor))["offset"] if cursor else 0
    except (ValueError, KeyError, TypeError):
        raise ToolError("Invalid cursor; pass next_cursor exactly as returned.") from None

    text = query.strip()
    if not text:
        ids = [r[0] for r in _rows(f"select c.id from mcp_cases c where {where} "
                                   "order by c.decision_date desc nulls last, c.id limit %s offset %s",
                                   params + [limit + 1, offset])]
        has_more, page_ids = len(ids) > limit, ids[:limit]
    else:
        rankings = []
        if mode in ("hybrid", "lexical"):
            like = f"%{text}%"
            rankings.append([r[0] for r in _rows(
                f"select c.id from mcp_cases c where {where} and (c.case_number ilike %s or c.title ilike %s) "
                "order by c.decision_date desc limit 50", params + [like, like])])
            rankings.append([r[0] for r in _rows(
                f"""select c.id from mcp_cases c join mcp_case_search s on s.id = c.id
                    where {where} and s.fts @@ websearch_to_tsquery('swedish', %s)
                    order by ts_rank_cd(s.fts, websearch_to_tsquery('swedish', %s)) desc, c.decision_date desc
                    limit {POOL}""", params + [text, text])])
        if mode in ("hybrid", "semantic"):
            vector = _query_vector(text)
            rankings.append([r[0] for r in _rows(
                f"""select c.id from mcp_cases c join mcp_case_search s on s.id = c.id
                    where {where} and s.embedding is not null
                    order by s.embedding <=> %s::vector limit {POOL}""", params + [vector])])
        fused = _fuse(*rankings)
        page_ids, has_more = fused[offset:offset + limit], offset + limit < len(fused)

    names = {cid: key for key, (cid, *_) in known.items()}
    matches: dict[str, list[str]] = {}
    hits = {}
    if page_ids:
        for case_id, concept_id in _rows("select case_id, concept_id from mcp_concept_matches where case_id = any(%s::uuid[])", [page_ids]):
            matches.setdefault(case_id, []).append(names[concept_id])
        for case_id, court_code, case_number, title, summary, decided, case_area in _rows(
                "select id, court, case_number, title, summary, decision_date, area from mcp_cases where id = any(%s::uuid[])", [page_ids]):
            hits[case_id] = CaseHit(case_id=case_id, court=court_code, case_number=case_number, title=title,
                                    summary=summary, decision_date=decided.isoformat() if decided else None,
                                    area=case_area, matched_concepts=sorted(matches.get(case_id, [])))
    return SearchResult(
        results=[hits[i] for i in page_ids if i in hits],
        next_cursor=base64.urlsafe_b64encode(json.dumps({"offset": offset + limit}).encode()).decode() if has_more else None,
        has_more=has_more,
        concept_coverage=[ConceptCoverage(key=k, matched_count=known[k][1], evaluated_count=known[k][2],
                                          unevaluated_count=known[k][3]) for k in concepts or []],
    )


@mcp.tool(annotations=READ_ONLY)
def get_case(case_id: Annotated[str, Field(description="case_id from search_cases.")]) -> Case:
    """One decision: headnote, legal area, structural tags, and every concept it matched with the verbatim evidence passage and confidence."""
    rows = _rows("""select id, court, case_number, title, summary, decision_date, area, structural_tags,
                           source_id, full_text_path is not null
                    from mcp_cases where id = %s""", [_uuid(case_id)])
    if not rows:
        raise ToolError(f"No case with case_id {case_id}.")
    cid, court, case_number, title, summary, decided, area, tags, source_id, has_text = rows[0]
    names = {concept_id: key for concept_id, key, *_ in _concepts()}
    matched = [ConceptMatch(key=names[concept_id], confidence=confidence, evidence=evidence, method=method)
               for concept_id, confidence, evidence, method in _rows(
                   "select concept_id, confidence, evidence, method from mcp_concept_matches where case_id = %s", [cid])]
    return Case(case_id=cid, court=court, case_number=case_number, title=title, summary=summary,
                decision_date=decided.isoformat() if decided else None, area=area, structural_tags=tags or [],
                matched_concepts=sorted(matched, key=lambda m: m.key), source_url=detail_url(source_id),
                has_full_text=has_text)


@mcp.tool(annotations=READ_ONLY)
def get_case_text(
    case_id: Annotated[str, Field(description="case_id from search_cases.")],
    page: Annotated[int, Field(ge=1, description="1-based page; each page is up to 12,000 characters.")] = 1,
) -> CaseText:
    """Full text of a decision, paginated. Before 4 March 2025 this is the published referat; from then on, the judgment itself. Use it to verify a passage before relying on it."""
    rows = _rows("select full_text_path from mcp_cases where id = %s", [_uuid(case_id)])
    if not rows:
        raise ToolError(f"No case with case_id {case_id}.")
    if not rows[0][0]:
        raise ToolError("This case has no stored full text.")
    text = r2.download_text(rows[0][0])
    total_pages = max(1, -(-len(text) // PAGE_CHARS))
    if page > total_pages:
        raise ToolError(f"page {page} is past the end; this text has {total_pages} page(s).")
    return CaseText(case_id=case_id, page=page, total_pages=total_pages,
                    text=text[(page - 1) * PAGE_CHARS:page * PAGE_CHARS])


@mcp.tool(annotations=READ_ONLY)
def list_concepts() -> ConceptList:
    """Every concept — an idea evaluated across the corpus so it can be used as a search_cases filter — with its definition and coverage.

    unevaluated_count cases were never read for that concept (including cases outside a vector concept's candidate window), so a concept can miss them; abstaining_at_max_count cases await the owner's review.
    """
    (total,), = _rows("select count(*) from mcp_cases")
    return ConceptList(total_cases=total, concepts=[
        Concept(key=key, definition=definition, strategy=strategy, evaluated_count=evaluated, matched_count=matched,
                out_of_window_count=out_of_window, abstaining_count=abstaining, abstaining_at_max_count=review,
                errored_count=errored, unevaluated_count=unevaluated,
                last_run_at=last_run.isoformat() if last_run else None)
        for _, key, definition, strategy, evaluated, matched, out_of_window, abstaining, review, errored,
            unevaluated, last_run in _concepts()
    ])


if __name__ == "__main__":
    # Warm the model through the same lock queries use, so a first query can't load it a second time.
    threading.Thread(target=_query_vector, args=("uppvärmning",), daemon=True).start()
    mcp.run()
