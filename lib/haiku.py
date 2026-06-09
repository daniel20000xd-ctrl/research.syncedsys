"""Anthropic Haiku client wrapper. All Haiku calls go through this module.

Two task functions (classify_record for Phase 2, search_concept for Phase 3)
plus a prompt-formatting helper. Both task functions are defensive: a parse or
API failure degrades to a safe empty/negative result rather than aborting a run.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from anthropic import Anthropic, APIStatusError, RateLimitError

MODEL = "claude-haiku-4-5-20251001"
CALL_DELAY_SECONDS = 0.2  # small delay between API calls
MAX_RETRIES = 3

# Base-schema columns — used to derive "extra" (domain-specific) fields from a
# record row when we only have the row, not the domain config (Phase 3).
_BASE_FIELDS = {
    "id", "external_id", "source_name", "source_url", "domain",
    "title", "summary", "record_date", "raw_data",
    "structural_tags", "derived_tags",
    "phase2_status", "phase2_ran_at", "phase2_error", "phase3_concept_ids",
    "ingested_at", "updated_at",
}

_client: Anthropic | None = None


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY must be set (see .env.example).")
        _client = Anthropic(api_key=key)
    return _client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _call(prompt: str, max_tokens: int) -> str:
    """Call Haiku (temperature 0) with retries + exponential backoff."""
    client = _get_client()
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(CALL_DELAY_SECONDS)
            resp = client.messages.create(
                model=MODEL,
                max_tokens=max_tokens,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.content[0].text
        except (RateLimitError, APIStatusError) as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
                delay *= 2
    raise last_err  # exhausted retries


def _parse_json(text: str):
    """Parse JSON from a model response, tolerating stray code fences."""
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`").strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # Last resort: slice the outermost array/object.
        for open_c, close_c in (("[", "]"), ("{", "}")):
            i, j = s.find(open_c), s.rfind(close_c)
            if i != -1 and j != -1 and j > i:
                return json.loads(s[i : j + 1])
        raise


def format_extra_fields(record: dict, extra_columns) -> str:
    """Format domain-specific extra fields for inclusion in a prompt.

    Only non-null, non-empty values are included. JSONB array values are joined
    with commas; objects are rendered as compact JSON. `extra_columns` may be any
    iterable of column names (a dict's keys work).
    """
    lines = []
    for col in (extra_columns or {}):
        value = record.get(col)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            if not value:
                continue
            rendered = ", ".join(str(v) for v in value)
        elif isinstance(value, dict):
            if not value:
                continue
            rendered = json.dumps(value, ensure_ascii=False)
        else:
            rendered = str(value).strip()
            if not rendered:
                continue
        label = col.replace("_", " ").capitalize()
        lines.append(f"{label}: {rendered}")
    return "\n".join(lines)


def classify_record(record: dict, domain_config: dict) -> list[dict]:
    """Phase 2 — ask Haiku for structural tags. Returns a list of tag dicts.

    Each returned tag carries an `added_at` ISO timestamp. Returns [] on parse
    failure or when no tags qualify.
    """
    extra_columns = domain_config.get("extra_columns") or {}
    categories = domain_config.get("structural_tag_categories") or []
    prompt = (
        "You are classifying a record in a structured knowledge database.\n\n"
        f"Domain: {domain_config.get('display_name', '')}\n"
        "Context:\n"
        f"{domain_config.get('enrichment_context', '')}\n\n"
        "Record:\n"
        f"Title: {record.get('title') or ''}\n"
        f"Summary: {record.get('summary') or ''}\n"
        f"{format_extra_fields(record, extra_columns)}\n\n"
        f"Assign structural tags from the following categories: {', '.join(categories)}\n\n"
        "Return ONLY valid JSON — no markdown, no explanation, nothing else:\n"
        "[\n"
        '  {"tag": "tag_name", "category": "category_name", "confidence": 0.0}\n'
        "]\n\n"
        "Rules:\n"
        "- Only tags with confidence >= 0.7\n"
        "- Tags in the same language as the record content (Swedish for Swedish content)\n"
        "- Tag names: concise (1-3 words), lowercase\n"
        "- Return [] if no tags qualify"
    )
    try:
        tags = _parse_json(_call(prompt, max_tokens=500))
        if not isinstance(tags, list):
            raise ValueError("expected a JSON array")
    except Exception as e:  # noqa: BLE001 — degrade gracefully
        print(f"  ! classify_record error: {e}")
        return []

    out = []
    for tag in tags:
        if not isinstance(tag, dict) or "tag" not in tag:
            continue
        tag["added_at"] = _now_iso()
        out.append(tag)
    return out


def search_concept(record: dict, concept_name: str, concept_description: str) -> dict:
    """Phase 3 — ask Haiku whether a record is latently relevant to a concept.

    Returns {relevant, confidence, reasoning, specific_passage}. Returns a safe
    negative result on parse failure.
    """
    fallback = {
        "relevant": False,
        "confidence": 0.0,
        "reasoning": "parse error",
        "specific_passage": None,
    }
    extra_columns = {k: "" for k in record if k not in _BASE_FIELDS}
    prompt = (
        "You are searching for latent relevance to a concept in a knowledge record.\n\n"
        f"Concept: {concept_name}\n"
        "Description:\n"
        f"{concept_description}\n\n"
        "Record:\n"
        f"Title: {record.get('title') or ''}\n"
        f"Summary: {record.get('summary') or ''}\n"
        f"{format_extra_fields(record, extra_columns)}\n\n"
        "Is this record relevant to the concept above, even if not explicitly tagged?\n\n"
        "Return ONLY valid JSON — no markdown, no explanation, nothing else:\n"
        "{\n"
        '  "relevant": true,\n'
        '  "confidence": 0.0,\n'
        '  "reasoning": "max 80 words",\n'
        '  "specific_passage": "most relevant excerpt or null"\n'
        "}"
    )
    try:
        result = _parse_json(_call(prompt, max_tokens=300))
        if not isinstance(result, dict):
            raise ValueError("expected a JSON object")
    except Exception as e:  # noqa: BLE001 — degrade gracefully
        print(f"  ! search_concept error: {e}")
        return dict(fallback)

    return {
        "relevant": bool(result.get("relevant", False)),
        "confidence": float(result.get("confidence", 0.0) or 0.0),
        "reasoning": result.get("reasoning") or "",
        "specific_passage": result.get("specific_passage"),
    }
