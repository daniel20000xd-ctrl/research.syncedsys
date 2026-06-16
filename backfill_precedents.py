#!/usr/bin/env python3
"""Backfill the higher-court precedent corpus (Domstolsverket "Sök rättspraxis").

This is the bespoke pipeline for the `precedents` table (created by
migrations/003_precedents_corpus.sql) — NOT a base-schema domain table, so it
does not run through ingest.py. The generic ingest.py refuses this domain and
points here.

Phases (run independently or together with --phase all):
  probe     hit the API once; log total hit count + response/pagination shape
  backfill  resumable, idempotent unfiltered pull -> precedents (PDFs -> R2 + text)
  embed     embed every precedent where embedding is null, using a LOCAL model
  queue     rank ALL precedents by similarity to seed phrase(s) -> classification_queue

Usage:
  python backfill_precedents.py --phase probe
  python backfill_precedents.py --phase backfill [--limit N] [--dry-run]
  python backfill_precedents.py --phase embed    [--limit N] [--dry-run]
  python backfill_precedents.py --phase queue    [--seed "..." --seed "..."] [--dry-run]
  python backfill_precedents.py --phase all      [--limit N]

Run from the repo root (imports of lib/ and adapters/ are cwd-relative).
"""
from __future__ import annotations

import argparse

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()  # fall back to .env

from lib import precedents, registry  # noqa: E402 — after dotenv

DEFAULT_DOMAIN = "higher_court_precedents"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill the higher-court precedent corpus into `precedents`."
    )
    parser.add_argument(
        "--phase", choices=["probe", "backfill", "embed", "queue", "all"],
        default="all", help="which phase(s) to run (default: all)",
    )
    parser.add_argument("--domain", default=DEFAULT_DOMAIN,
                        help=f"domain_key from domains.yaml (default: {DEFAULT_DOMAIN})")
    parser.add_argument("--limit", type=int, default=None,
                        help="cap records (backfill) / embeddings (embed) for testing")
    parser.add_argument("--seed", action="append", default=None,
                        help="seed phrase for queue ranking (repeatable; overrides domains.yaml)")
    parser.add_argument("--dry-run", action="store_true",
                        help="probe/inspect only; write nothing to the DB or R2")
    args = parser.parse_args()

    config = registry.get_domain(args.domain)
    if config.get("pipeline") != "precedents":
        raise SystemExit(
            f"Domain '{args.domain}' is not a precedents-pipeline domain "
            f"(expected `pipeline: precedents` in domains.yaml)."
        )

    if args.phase in ("probe",):
        precedents.probe(config)
    if args.phase in ("backfill", "all"):
        precedents.backfill(config, domain_key=args.domain, limit=args.limit, dry_run=args.dry_run)
    if args.phase in ("embed", "all"):
        precedents.embed(config, limit=args.limit, dry_run=args.dry_run)
    if args.phase in ("queue", "all"):
        precedents.populate_queue(config, seeds=args.seed, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
