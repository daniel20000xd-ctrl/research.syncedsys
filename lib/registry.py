"""Domain registry loader and validator.

Reads domains.yaml — the single source of truth — and returns typed domain
config dicts. No domain-specific logic lives here; only generic loading and
validation. Every script in the pipeline gets its domain config from here.
"""
from __future__ import annotations

from pathlib import Path

import yaml

# domains.yaml lives at the project root, one level up from this file.
REGISTRY_PATH = Path(__file__).parent.parent / "domains.yaml"

# Fields every domain must define for the generic pipeline to operate on it.
REQUIRED_FIELDS = (
    "display_name",
    "table_name",
    "source_adapter",
    "source_config",
    "structural_tag_categories",
)


def load_registry() -> dict:
    """Read domains.yaml and return the full registry dict."""
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Domain registry not found at {REGISTRY_PATH}")
    with REGISTRY_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    domains = data.get("domains")
    if not isinstance(domains, dict):
        raise ValueError("domains.yaml must contain a top-level 'domains' mapping")
    # Drop any null entries (e.g. an empty/placeholder key parsed as None).
    data["domains"] = {k: v for k, v in domains.items() if v is not None}
    return data


def get_domain(domain_key: str) -> dict:
    """Return config for a single domain. Raises ValueError if not found."""
    domains = load_registry()["domains"]
    if domain_key not in domains:
        available = ", ".join(sorted(domains)) or "(none)"
        raise ValueError(
            f"Domain '{domain_key}' not found in domains.yaml. Available: {available}"
        )
    config = domains[domain_key]
    validate_domain(config)
    return config


def list_domains() -> list[str]:
    """Return all domain keys."""
    return list(load_registry()["domains"].keys())


def validate_domain(config: dict) -> None:
    """Validate that required fields are present. Raises ValueError if not."""
    if not isinstance(config, dict):
        raise ValueError("Domain config must be a mapping")
    missing = [
        field
        for field in REQUIRED_FIELDS
        if field not in config or config[field] in (None, "", [], {})
    ]
    if missing:
        name = config.get("display_name", "?") if isinstance(config, dict) else "?"
        raise ValueError(
            f"Domain '{name}' is missing required field(s): {', '.join(missing)}"
        )
    extra = config.get("extra_columns")
    if extra is not None and not isinstance(extra, dict):
        raise ValueError("extra_columns must be a mapping of column_name: type")
