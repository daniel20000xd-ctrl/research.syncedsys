"""Source adapter: Domstolsverket "Sök rättspraxis" open data — FULL backfill.

Unlike `domstolsverket.py` (keyword-bound, base-schema domain tables), this
adapter pulls the **entire higher-court corpus UNFILTERED** into the bespoke
`precedents` table. Legal-area classification happens later — never at ingest.

Probed live (see canonical_id / pagination notes below), not assumed:
  * Search:   POST {base}/api/v1/sok   body {filter:{}, sidIndex, antalPerSida,
              sortorder:'publiceringstid', asc:true} -> {publiceringLista, total}.
              An EMPTY filter returns every record the API exposes (all higher
              courts: HD/HDO, HFD, the hovrätter, kammarrätter, AD/ADO, MÖD, PMÖD,
              MIG…); total was 17258 at probe time. No court/keyword/area filter.
  * Paging:   `sidIndex` is a 0-based PAGE index, `antalPerSida` the page size;
              `total` is constant across pages. We sort publiceringstid ASC so the
              page index is a STABLE resume cursor — new publications append at the
              end (higher pages), leaving already-fetched pages put across days.
  * PDFs:     records served as PDF carry bilagaLista:[{filnamn, fillagringId}]
              and an empty `innehall`. Download with GET
              {base}/api/v1/bilagor/{URL-ENCODED fillagringId}, Accept
              application/pdf (the path id MUST be percent-encoded; raw slashes 404).
  * Detail:   stable public web URL is {base}/sok/publicering/{id}.

Two-phase by design: `probe()`, `iter_pages()` and `download_bilaga()` are the
backfill surface; `normalize()` is source-shape -> precedents-row and is shared,
so a future delta mode over the Sök-rättspraxis RSS feed can reuse it unchanged.
"""
from __future__ import annotations

import re
import time
from urllib.parse import quote

import requests

SEARCH_PATH = "/api/v1/sok"
BILAGA_PATH = "/api/v1/bilagor/{lagring_id}"  # GET, Accept application/pdf
DETAIL_PATH = "/sok/publicering/{id}"         # public SPA web URL
SORT_ORDER = "publiceringstid"

_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Syncedsys-Research/1.0 (research backfill; contact daniel20000xd@gmail.com)",
}
_TIMEOUT = 60
_RETRY_STATUSES = {429, 500, 502, 503, 504}
_BACKOFF_BASE = 1.5
_BACKOFF_MAX = 60.0


# ──────────────────────────────────────────────────────────────────────────────
# HTTP with polite exponential backoff
# ──────────────────────────────────────────────────────────────────────────────
def _request(method: str, url: str, *, headers: dict, json=None,
             max_retries: int = 6) -> requests.Response:
    """One HTTP call with exponential backoff on 429/5xx and transient errors."""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.request(
                method, url, json=json, headers=headers, timeout=_TIMEOUT
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            if attempt == max_retries:
                raise
        else:
            if resp.status_code not in _RETRY_STATUSES:
                resp.raise_for_status()
                return resp
            if attempt == max_retries:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if (retry_after or "").isdigit() else 0.0
            wait = max(wait, min(_BACKOFF_BASE * (2 ** attempt), _BACKOFF_MAX))
            time.sleep(wait)
            continue
        wait = min(_BACKOFF_BASE * (2 ** attempt), _BACKOFF_MAX)
        print(f"    transient error ({last_exc.__class__.__name__}); retry in {wait:.1f}s")
        time.sleep(wait)
    raise last_exc  # unreachable, but keeps type-checkers happy


def _search_body(page: int, page_size: int) -> dict:
    # filter:{} == unfiltered. asc -> stable page cursor (see module docstring).
    return {
        "filter": {},
        "sidIndex": page,
        "antalPerSida": page_size,
        "sortorder": SORT_ORDER,
        "asc": True,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Step 0 — probe
# ──────────────────────────────────────────────────────────────────────────────
def probe(source_config: dict) -> dict:
    """Hit the API once and report the real total + response/pagination shape.

    Returns a dict the caller logs before pulling. Inspects the actual response;
    nothing here is assumed.
    """
    base_url = source_config["base_url"].rstrip("/")
    page_size = int(source_config.get("page_size", 100))
    resp = _request("POST", base_url + SEARCH_PATH, headers=_HEADERS,
                    json=_search_body(0, 1))
    data = resp.json()
    pubs = data.get("publiceringLista") or []
    total = data.get("total")
    return {
        "total": total,
        "page_size": page_size,
        "pages_to_fetch": (
            (int(total) + page_size - 1) // page_size if isinstance(total, int) else None
        ),
        "response_keys": sorted(data.keys()) if isinstance(data, dict) else None,
        "record_keys": sorted(pubs[0].keys()) if pubs else [],
        "pagination": "POST /api/v1/sok; sidIndex=0-based page, antalPerSida=page size",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Backfill — resumable page iterator
# ──────────────────────────────────────────────────────────────────────────────
def iter_pages(source_config: dict, start_page: int = 0):
    """Yield (page_index, raw_records, total) from `start_page` to the end.

    A polite inter-request delay runs between pages. The caller persists
    page_index after each page (ingestion_progress.last_cursor) for resume.
    """
    base_url = source_config["base_url"].rstrip("/")
    page_size = int(source_config.get("page_size", 100))
    delay = float(source_config.get("request_delay_seconds", 0.5))
    url = base_url + SEARCH_PATH

    page = start_page
    while True:
        resp = _request("POST", url, headers=_HEADERS, json=_search_body(page, page_size))
        data = resp.json()
        pubs = data.get("publiceringLista") or []
        total = data.get("total")
        if not pubs:
            break
        yield page, pubs, total
        page += 1
        if len(pubs) < page_size:
            break
        if isinstance(total, int) and page * page_size >= total:
            break
        time.sleep(delay)


def download_bilaga(base_url: str, fillagring_id: str) -> bytes:
    """Download one attachment (PDF) by its fillagringId. Bytes, with backoff."""
    url = base_url.rstrip("/") + BILAGA_PATH.format(lagring_id=quote(fillagring_id, safe=""))
    resp = _request("GET", url, headers={**_HEADERS, "Accept": "application/pdf"})
    return resp.content


def detail_url(base_url: str, record_id: str) -> str:
    return base_url.rstrip("/") + DETAIL_PATH.format(id=record_id)


# ──────────────────────────────────────────────────────────────────────────────
# málnummer / reference normalization
# ──────────────────────────────────────────────────────────────────────────────
_MALNR_RE = re.compile(r"^\s*([A-Za-zÅÄÖåäö]+)\s*(\d+)\s*-\s*(\d+)\s*$")


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _norm_malnummer(mal: str) -> str:
    """Canonicalize a single case number, e.g. 't 1234-19' -> 'T 1234-19'."""
    m = _MALNR_RE.match(mal or "")
    if m:
        return f"{m.group(1).upper()} {m.group(2)}-{m.group(3)}"
    return _norm_ws(mal or "")


def normalize_reference(raw: dict) -> str | None:
    """Best human citation for a record, per court format.

    Preference: published referat citation (NJA / HFD ref. / RÅ / AD nr …) →
    Arbetsdomstolen domsnummer (AD 124/93) → first normalized målnummer
    (T/Ö/B/M {number}-{year} for hovrätt/kammarrätt/MÖD etc.).
    """
    ref = raw.get("referatNummerLista") or []
    if ref:
        return _norm_ws(str(ref[0]))
    ad = raw.get("arbetsdomstolenDomsnummer")
    if ad:
        return f"AD {_norm_ws(str(ad))}"
    mal = raw.get("malNummerLista") or []
    if mal:
        return _norm_malnummer(str(mal[0]))
    return None


# ──────────────────────────────────────────────────────────────────────────────
# HTML → text (inline `innehall` for referat)
# ──────────────────────────────────────────────────────────────────────────────
def _strip_html(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</(?:p|div|h[1-6]|li|tr)>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    for entity, char in (
        ("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"),
        ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'"),
    ):
        s = s.replace(entity, char)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip() or None


def _doc_type(raw: dict, bilagor: list[dict]) -> str | None:
    """Advisory doc_type from publiceringsform, refined dom/beslut via filename."""
    form = (raw.get("publiceringsform") or "").upper()
    if form == "REFERAT":
        return "referat"
    if form == "NOTIS":
        return "notis"
    if form == "DOM_ELLER_BESLUT":
        names = " ".join((b.get("filnamn") or "") for b in bilagor).lower()
        has_dom = "dom" in names
        has_beslut = "beslut" in names
        if has_dom and not has_beslut:
            return "dom"
        if has_beslut and not has_dom:
            return "beslut"
        return "dom_eller_beslut"
    return form.lower() or None


# ──────────────────────────────────────────────────────────────────────────────
# normalize — source record → precedents row (shared by backfill & future delta)
# ──────────────────────────────────────────────────────────────────────────────
def normalize(raw: dict, base_url: str) -> dict:
    """Map one PubliceringDTO to a precedents-shaped dict.

    Sets only source/advisory fields. `area` and `embedding` are NEVER set here —
    owned by classification / the embed phase, and must survive re-ingest. The
    body is NOT a Postgres column: `_full_text` (inline HTML body for referat;
    None for PDF-only) and `_bilagor` are internal — the orchestrator uploads the
    text and any PDF originals to R2, sets full_text_path / raw_pdf_path, and
    drops both before the DB write.
    """
    domstol = raw.get("domstol") or {}
    bilagor = [
        {"filnamn": b.get("filnamn"), "fillagring_id": b.get("fillagringId")}
        for b in (raw.get("bilagaLista") or [])
        if b.get("fillagringId")
    ]

    # canonical_id: prefer the stable source document id (UUID); fall back to the
    # group correlation id, then a court-scoped normalized reference.
    record_id = raw.get("id")
    ref_norm = normalize_reference(raw)
    canonical_id = (
        str(record_id) if record_id
        else str(raw.get("gruppKorrelationsnummer")) if raw.get("gruppKorrelationsnummer")
        else (f"{domstol.get('domstolKod') or 'NA'}:{ref_norm}" if ref_norm else None)
    )

    mal = raw.get("malNummerLista") or []
    ad = raw.get("arbetsdomstolenDomsnummer")
    malnummer_raw_parts = [str(x) for x in mal]
    if ad:
        malnummer_raw_parts.append(f"AD {ad}")

    ref_list = raw.get("referatNummerLista") or []
    court_raw = domstol.get("domstolNamn")
    title = _norm_ws(str(ref_list[0])) if ref_list else _norm_ws(
        " ".join(p for p in [court_raw, ref_norm] if p)
    ) or ref_norm or court_raw

    pub = raw.get("publiceringstid")
    rattsomrade = [x for x in (raw.get("rattsomradeLista") or []) if x]

    return {
        "canonical_id": canonical_id,
        "court": domstol.get("domstolKod"),
        "court_raw": court_raw,
        "doc_type": _doc_type(raw, bilagor),
        "malnummer_raw": "; ".join(malnummer_raw_parts) or None,
        "malnummer_normalized": ref_norm,
        "decision_date": raw.get("avgorandedatum") or None,
        "publication_date": (pub[:10] if isinstance(pub, str) and pub else None),
        "source_area_code": "; ".join(rattsomrade) or None,  # ADVISORY — never filter
        "title": title or None,
        "summary": _strip_html(raw.get("sammanfattning")),  # short headnote — kept in PG
        "source_url": detail_url(base_url, str(record_id)) if record_id else None,
        # raw_pdf_path / full_text_path are set by the orchestrator after R2 upload.
        "metadata": {
            "keywords": raw.get("nyckelordLista") or [],
            "lagrum": raw.get("lagrumLista") or [],
            "forarbeten": raw.get("forarbeteLista") or [],
            "rattsomrade": rattsomrade,           # full source area list (advisory)
            "referat_numbers": ref_list,
            "typ": raw.get("typ"),                # PREJUDIKAT / VAGLEDANDE… (advisory)
            "publiceringsform": raw.get("publiceringsform"),
            "grupp_korrelationsnummer": raw.get("gruppKorrelationsnummer"),
            "attachments": [b.get("filnamn") for b in (raw.get("bilagaLista") or [])],
            # The full source envelope is intentionally NOT stored — the body lives
            # in R2 (full_text_path) and the record is re-fetchable by canonical_id.
            # Migration 003 has no raw_data/full_text column by design.
        },
        # internal — consumed by the orchestrator, dropped before the DB write:
        "_full_text": _strip_html(raw.get("innehall")),  # inline body (referat); None for PDF-only
        "_bilagor": bilagor,
    }
