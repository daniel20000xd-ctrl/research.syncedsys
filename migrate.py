#!/usr/bin/env python3
"""Apply SQL migrations as postgres, then set the corpus role passwords.

    python migrate.py migrations/005_concept_corpus.sql

Everything runs in one transaction. Passwords for the pipeline / mcp_reader roles come
from .env.local (PIPELINE_DB_PASSWORD, MCP_READER_DB_PASSWORD), never from a migration.
Run from the repo root.
"""
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv(".env.local")

from lib import db  # noqa: E402

ROLE_PASSWORDS = {"pipeline": "PIPELINE_DB_PASSWORD", "mcp_reader": "MCP_READER_DB_PASSWORD"}


def main() -> None:
    ap = argparse.ArgumentParser(description="Apply SQL migrations and set role passwords")
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()

    conn = db.connect("admin")
    try:
        with conn.cursor() as cur:
            for path in args.files:
                with open(path, encoding="utf-8") as f:
                    cur.execute(f.read())
                print(f"applied {path}")
            cur.execute("select rolname from pg_roles where rolname = any(%s)", (list(ROLE_PASSWORDS),))
            for (role,) in cur.fetchall():
                password = os.environ.get(ROLE_PASSWORDS[role])
                if not password:
                    sys.exit(f"{ROLE_PASSWORDS[role]} is not set in .env.local")
                cur.execute(sql.SQL("alter role {} with password {}").format(
                    sql.Identifier(role), sql.Literal(password)))
                print(f"password set for {role}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
