"""Generates CREATE TABLE SQL (plus indexes and the updated_at trigger) for a
domain, from its config dict.

The base schema mirrors the contract documented in the base-schema comment
block of migrations/001_research_foundation.sql. Domain-specific columns from
domains.yaml (extra_columns) are appended after the base columns. The output is
idempotent — safe to run on every ingest.
"""
from __future__ import annotations

# domains.yaml column type -> Postgres type.
TYPE_MAP = {
    "text": "TEXT",
    "jsonb": "JSONB",
    "int": "INTEGER",
    "date": "DATE",
    "float": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
}

# Base schema columns — exact types/defaults from the foundation contract.
BASE_COLUMNS = [
    "id                 UUID PRIMARY KEY DEFAULT gen_random_uuid()",
    "external_id        TEXT NOT NULL",
    "source_name        TEXT NOT NULL",
    "source_url         TEXT",
    "domain             TEXT NOT NULL",
    "title              TEXT",
    "summary            TEXT",
    "record_date        DATE",
    "raw_data           JSONB NOT NULL DEFAULT '{}'",
    "structural_tags    JSONB NOT NULL DEFAULT '[]'",
    "derived_tags       JSONB NOT NULL DEFAULT '[]'",
    "phase2_status      TEXT NOT NULL DEFAULT 'pending'",
    "phase2_ran_at      TIMESTAMPTZ",
    "phase2_error       TEXT",
    "phase3_concept_ids JSONB NOT NULL DEFAULT '[]'",
    "ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()",
    "updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()",
]


def _map_type(yaml_type: str) -> str:
    try:
        return TYPE_MAP[yaml_type]
    except KeyError:
        raise ValueError(
            f"Unknown extra_columns type '{yaml_type}'. "
            f"Valid types: {', '.join(TYPE_MAP)}"
        ) from None


def get_create_table_sql(domain_key: str, config: dict) -> str:
    """Return idempotent DDL (table + indexes + trigger) for a domain.

    All index/constraint/trigger names are prefixed with the table name to
    avoid collisions across domain tables.
    """
    table = config["table_name"]
    extra_columns = config.get("extra_columns") or {}

    extra_defs = [f"{name} {_map_type(coltype)}" for name, coltype in extra_columns.items()]
    columns_sql = ",\n  ".join(BASE_COLUMNS + extra_defs)
    unique_name = f"{table}_external_id_domain_key"

    statements = [
        # Table — IF NOT EXISTS so re-runs are safe.
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {columns_sql},\n"
        f"  CONSTRAINT {unique_name} UNIQUE (external_id, domain)\n);",
        # Full-text search over title + summary (Swedish config).
        f"CREATE INDEX IF NOT EXISTS {table}_fts_idx ON {table} "
        f"USING GIN (to_tsvector('swedish', coalesce(title,'') || ' ' || coalesce(summary,'')));",
        # phase2_status — queried on every enrich run.
        f"CREATE INDEX IF NOT EXISTS {table}_phase2_status_idx ON {table} (phase2_status);",
        # phase3_concept_ids — checked to skip already-searched records.
        f"CREATE INDEX IF NOT EXISTS {table}_phase3_concept_ids_idx ON {table} "
        f"USING GIN (phase3_concept_ids);",
        # record_date — newest-first listing.
        f"CREATE INDEX IF NOT EXISTS {table}_record_date_idx ON {table} (record_date DESC);",
        # structural / derived tag containment queries.
        f"CREATE INDEX IF NOT EXISTS {table}_structural_tags_idx ON {table} "
        f"USING GIN (structural_tags);",
        f"CREATE INDEX IF NOT EXISTS {table}_derived_tags_idx ON {table} "
        f"USING GIN (derived_tags);",
        # updated_at trigger (function defined in the foundation migration).
        # CREATE TRIGGER has no IF NOT EXISTS before PG14, so drop-then-create.
        f"DROP TRIGGER IF EXISTS {table}_set_updated_at ON {table};\n"
        f"CREATE TRIGGER {table}_set_updated_at BEFORE UPDATE ON {table} "
        f"FOR EACH ROW EXECUTE FUNCTION update_updated_at();",
    ]

    return "\n\n".join(statements) + "\n"
