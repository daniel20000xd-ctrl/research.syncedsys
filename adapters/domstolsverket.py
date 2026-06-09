"""Source adapter for the Domstolsverket rättspraxis API.

Interface: fetch(source_config: dict) -> list[dict]

Records are normalized to the base-schema shape (plus the domain-specific extra
columns declared in domains.yaml). The exact API response shape is not publicly
documented, so endpoint discovery and field extraction are defensive.
"""
from __future__ import annotations

import time

import requests

# Candidate endpoint paths, tried in order until one returns HTTP 200.
_ENDPOINT_PATHS = ["/api/avgoranden", "/api/v1/avgoranden", "/api/search"]
_HEADERS = {"Accept": "application/json", "User-Agent": "Syncedsys-Research/1.0"}
_TIMEOUT = 30


def _params(query: str, court: str, page: int, page_size: int) -> dict:
    return {
        "fritext": query,
        "domstolskod": court,
        "page": page,
        "size": page_size,
        "sortering": "datumDesc",
    }


def _resolve_endpoint(base_url: str, court: str, query: str, page_size: int) -> str:
    """Return the first endpoint path that responds 200, else raise a clear error."""
    base = base_url.rstrip("/")
    attempts = []
    for path in _ENDPOINT_PATHS:
        url = f"{base}{path}"
        try:
            resp = requests.get(
                url, params=_params(query, court, 0, page_size), headers=_HEADERS, timeout=_TIMEOUT
            )
        except requests.RequestException as e:
            attempts.append(f"{url} -> {e.__class__.__name__}")
            continue
        if resp.status_code == 200:
            return url
        attempts.append(f"{url} -> HTTP {resp.status_code}")
    raise RuntimeError(
        "No working Domstolsverket endpoint found. Tried:\n  " + "\n  ".join(attempts)
    )


def _extract_results(payload):
    """Return (results_list, total_or_None) from an unknown response shape."""
    if isinstance(payload, list):
        return payload, None
    if not isinstance(payload, dict):
        return [], None
    results = []
    for key in ("results", "avgoranden", "data", "items", "hits", "content"):
        if isinstance(payload.get(key), list):
            results = payload[key]
            break
    total = None
    for key in ("total", "totalAntal", "totalHits", "totalElements", "antal", "count"):
        if isinstance(payload.get(key), int):
            total = payload[key]
            break
    return results, total


def _normalize(raw: dict) -> dict:
    ext = raw.get("id") or raw.get("malnummer") or raw.get("avgörandeId")
    return {
        # Base schema fields
        "external_id": str(ext) if ext is not None else None,
        "source_name": "domstolsverket",
        "source_url": raw.get("url") or raw.get("permalink"),
        "title": raw.get("rubrik") or raw.get("heading"),
        "summary": raw.get("sammanfattning") or raw.get("summary"),
        "record_date": raw.get("avgörandeDatum") or raw.get("avgörandedatum"),
        # Domain-specific extra fields (match extra_columns in domains.yaml)
        "malnummer": raw.get("malnummer") or raw.get("målnummer"),
        "referat": raw.get("referat") or raw.get("referatnummer"),
        "domstolskod": raw.get("domstolskod"),
        "domstol": raw.get("domstol") or raw.get("domstolsnamn"),
        "lagrum": raw.get("lagrum") or raw.get("lagrumshänvisningar") or [],
        "sokord": raw.get("sökord") or raw.get("sokord") or [],
        # Always include the full raw record
        "raw_data": raw,
    }


def fetch(source_config: dict) -> list[dict]:
    base_url = source_config["base_url"]
    court_codes = source_config.get("court_codes") or []
    queries = source_config.get("search_queries") or []
    page_size = int(source_config.get("page_size", 50))
    delay = float(source_config.get("request_delay_seconds", 0.5))

    if not court_codes or not queries:
        raise ValueError(
            "domstolsverket source_config requires non-empty court_codes and search_queries"
        )

    endpoint = _resolve_endpoint(base_url, court_codes[0], queries[0], page_size)

    seen: set[str] = set()
    records: list[dict] = []

    for court in court_codes:
        for query in queries:
            page = 0
            while True:
                resp = requests.get(
                    endpoint,
                    params=_params(query, court, page, page_size),
                    headers=_HEADERS,
                    timeout=_TIMEOUT,
                )
                resp.raise_for_status()
                time.sleep(delay)  # respect delay between all requests

                results, total = _extract_results(resp.json())
                if not results:
                    break

                for raw in results:
                    record = _normalize(raw)
                    ext = record["external_id"]
                    if not ext or ext in seen:  # dedup across queries; skip id-less
                        continue
                    seen.add(ext)
                    records.append(record)

                # Stop when the page is short or we've covered `total`.
                if len(results) < page_size:
                    break
                if total is not None and (page + 1) * page_size >= total:
                    break
                page += 1

    return records
