"""Source adapter: Domstolsverket "Sök rättspraxis" open data — the whole corpus.

Pulls the ENTIRE higher-court corpus unfiltered into `cases`. Legal-area classification
happens later — never at ingest.

Probed live, not assumed:
  * Search:   POST {base}/api/v1/sok   body {filter:{}, sidIndex, antalPerSida,
              sortorder:'publiceringstid', asc:true} -> {publiceringLista, total}.
              An EMPTY filter returns every record the API exposes (all higher courts);
              filter.domstolKodLista narrows it to courts (court codes: HDO, not HD).
  * Paging:   `sidIndex` is a 0-based PAGE index, `antalPerSida` the page size; `total` is
              constant across pages. Sorting publiceringstid ASC keeps pages stable — new
              publications append at the end.
  * PDFs:     records served as PDF carry bilagaLista:[{filnamn, fillagringId}] and an empty
              `innehall`. Download with GET {base}/api/v1/bilagor/{URL-ENCODED fillagringId},
              Accept application/pdf (the path id MUST be percent-encoded; raw slashes 404).
  * Detail:   stable public web URL is {base}/sok/publicering/{id}.
  * Coverage: referat back to 1981, but domar och beslut only from 4 March 2025 — a property
              of the source, not the pipeline.
"""
from __future__ import annotations

import re
import time
from urllib.parse import quote

import requests

BASE_URL = "https://rattspraxis.etjanst.domstol.se"
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


def _search_body(page: int, page_size: int, courts: list[str] | None = None) -> dict:
    return {
        "filter": {"domstolKodLista": courts} if courts else {},
        "sidIndex": page,
        "antalPerSida": page_size,
        "sortorder": SORT_ORDER,
        "asc": True,
    }


def total(court: str | None = None) -> int:
    """How many records the API exposes right now, optionally for one court code."""
    resp = _request("POST", BASE_URL + SEARCH_PATH, headers=_HEADERS,
                    json=_search_body(0, 1, [court] if court else None))
    return int(resp.json()["total"])


def iter_pages(page_size: int = 100, delay: float = 0.5):
    """Yield (page_index, raw_records, total) over the whole corpus."""
    url = BASE_URL + SEARCH_PATH
    page = 0
    while True:
        data = _request("POST", url, headers=_HEADERS, json=_search_body(page, page_size)).json()
        pubs = data.get("publiceringLista") or []
        total_hits = data.get("total")
        if not pubs:
            break
        yield page, pubs, total_hits
        page += 1
        if len(pubs) < page_size or (isinstance(total_hits, int) and page * page_size >= total_hits):
            break
        time.sleep(delay)


def download_bilaga(fillagring_id: str) -> bytes:
    """Download one attachment (PDF) by its fillagringId."""
    url = BASE_URL + BILAGA_PATH.format(lagring_id=quote(fillagring_id, safe=""))
    return _request("GET", url, headers={**_HEADERS, "Accept": "application/pdf"}).content


def detail_url(source_id: str) -> str:
    return BASE_URL + DETAIL_PATH.format(id=source_id)


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


# ──────────────────────────────────────────────────────────────────────────────
# normalize — source record → cases row
# ──────────────────────────────────────────────────────────────────────────────
def normalize(raw: dict) -> dict:
    """Map one PubliceringDTO to a `cases` row plus internal `_full_text` / `_bilagor`.

    Only source fields are set: area, structural_tags, derived_tags and embedding belong to
    classification, enrichment and the embed step, and must survive re-ingest. `raw_data`
    is the record minus the `innehall` body — the body lives in R2 as extracted text and
    the untouched record as raw.json, so Postgres stays a thin index.
    """
    domstol = raw.get("domstol") or {}
    record_id = raw.get("id")
    reference = normalize_reference(raw)
    # Prefer the stable source document id (UUID); fall back to the group correlation id,
    # then a court-scoped reference. Must stay identical to v1's canonical_id.
    source_id = (
        str(record_id) if record_id
        else str(raw["gruppKorrelationsnummer"]) if raw.get("gruppKorrelationsnummer")
        else f"{domstol.get('domstolKod') or 'NA'}:{reference}" if reference
        else None
    )

    case_numbers = [_norm_malnummer(str(m)) for m in raw.get("malNummerLista") or []]
    if raw.get("arbetsdomstolenDomsnummer"):
        case_numbers.append(f"AD {_norm_ws(str(raw['arbetsdomstolenDomsnummer']))}")

    referat = raw.get("referatNummerLista") or []
    title = _norm_ws(str(referat[0])) if referat else _norm_ws(
        " ".join(p for p in (domstol.get("domstolNamn"), reference) if p)
    ) or None

    return {
        "source_id": source_id,
        "court": domstol.get("domstolKod"),
        "case_number": "; ".join(case_numbers) or None,
        "title": title,
        "summary": _strip_html(raw.get("sammanfattning")),
        "decision_date": raw.get("avgorandedatum") or None,
        "source_area_code": "; ".join(x for x in raw.get("rattsomradeLista") or [] if x) or None,
        "raw_data": {k: v for k, v in raw.items() if k != "innehall"},
        "_full_text": _strip_html(raw.get("innehall")),
        "_bilagor": [
            {"filnamn": b.get("filnamn"), "fillagring_id": b.get("fillagringId")}
            for b in raw.get("bilagaLista") or []
            if b.get("fillagringId")
        ],
    }
