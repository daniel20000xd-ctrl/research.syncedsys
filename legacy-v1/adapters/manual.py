"""Adapter for manually-prepared JSON files.

Interface: fetch(source_config: dict) -> list[dict]

source_config must contain `file_path`: a JSON file holding an array of records
already in the base-schema shape. Each record needs at least `external_id` and
`title`; all other fields are optional.
"""
from __future__ import annotations

import json
from pathlib import Path


def fetch(source_config: dict) -> list[dict]:
    file_path = source_config.get("file_path")
    if not file_path:
        raise ValueError("manual adapter requires 'file_path' in source_config")

    path = Path(file_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Manual input file not found: {path}")

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Manual input file is not valid JSON: {path} ({e})") from e

    if not isinstance(data, list):
        raise ValueError(
            f"Manual input file must contain a JSON array, got {type(data).__name__}: {path}"
        )

    for i, record in enumerate(data):
        if not isinstance(record, dict):
            raise ValueError(f"Record {i} is not a JSON object")
        if not record.get("external_id") or not record.get("title"):
            raise ValueError(
                f"Record {i} must have at least 'external_id' and 'title'"
            )

    return data
