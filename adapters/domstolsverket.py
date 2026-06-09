"""Source adapter for the Domstolsverket rättspraxis API.

Interface: fetch(source_config: dict) -> list[dict]

Search is POST {base_url}/api/v1/sok with a filtered body; the response is
{ total, publiceringLista: PubliceringDTO[] }. Each PubliceringDTO is normalized
to the base-schema shape plus the domain-specific extra columns from domains.yaml.

source_config keys:
  base_url              e.g. https://rattspraxis.etjanst.domstol.se
  court_codes           domstolKod list, e.g. ["HDO"] (Högsta domstolen)
  search_queries        nyckelordLista tags (OR logic), e.g. ["Arv", "Testamente"]
  page_size             antalPerSida
  request_delay_seconds delay between page requests
"""
from __future__ import annotations

import re
import time

import requests

SEARCH_PATH = "/api/v1/sok"
_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Syncedsys-Research/1.0",
}
_TIMEOUT = 30


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


def _normalize(raw: dict) -> dict:
    ext = raw.get("id") or raw.get("gruppKorrelationsnummer")
    domstol = raw.get("domstol") or {}
    mal = raw.get("malNummerLista") or []
    ref = raw.get("referatNummerLista") or []
    return {
        # Base schema fields
        "external_id": str(ext) if ext is not None else None,
        "source_name": "domstolsverket",
        "source_url": None,  # the API exposes no stable public web URL
        "title": (ref[0] if ref else None) or raw.get("benamning") or (mal[0] if mal else None),
        "summary": _strip_html(raw.get("sammanfattning")),
        "record_date": raw.get("avgorandedatum"),
        # Domain-specific extra columns (match extra_columns in domains.yaml)
        "malnummer": ", ".join(mal) if mal else None,
        "referat": ", ".join(ref) if ref else None,
        "domstolskod": domstol.get("domstolKod"),
        "domstol": domstol.get("domstolNamn"),
        "lagrum": raw.get("lagrumLista") or [],
        "sokord": raw.get("nyckelordLista") or [],
        # Full original record
        "raw_data": raw,
    }


def fetch(source_config: dict) -> list[dict]:
    base_url = source_config["base_url"].rstrip("/")
    courts = source_config.get("court_codes") or []
    keywords = source_config.get("search_queries") or []
    page_size = int(source_config.get("page_size", 50))
    delay = float(source_config.get("request_delay_seconds", 0.5))

    if not keywords:
        raise ValueError(
            "domstolsverket source_config requires a non-empty search_queries list "
            "(matched against the API's nyckelordLista tags)"
        )

    url = f"{base_url}{SEARCH_PATH}"
    seen: set[str] = set()
    records: list[dict] = []
    page = 0

    while True:
        body = {
            "filter": {
                "sokordLista": keywords,
                **({"domstolKodLista": courts} if courts else {}),
            },
            "sidIndex": page,
            "antalPerSida": page_size,
            "sortorder": "publiceringstid",
            "asc": True,
        }
        resp = requests.post(url, json=body, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        time.sleep(delay)

        data = resp.json()
        pubs = data.get("publiceringLista") or []
        total = data.get("total")
        if not pubs:
            break

        for raw in pubs:
            record = _normalize(raw)
            ext = record["external_id"]
            if not ext or ext in seen:  # dedup; skip id-less
                continue
            seen.add(ext)
            records.append(record)

        page += 1
        if len(pubs) < page_size:
            break
        if isinstance(total, int) and page * page_size >= total:
            break

    return records
