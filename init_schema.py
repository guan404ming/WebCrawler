#!/usr/bin/env python3
"""
Create all SQLAlchemy tables in Postgres (one-time per empty database).

Docker Compose only starts Postgres; it does NOT run migrations. Until you run
this script (or the README snippet), tables such as `domain_state` do not exist.

Examples:
  docker exec -w /app scheduler_control python init_schema.py

  # From host: defaults in ingest.yaml use hostname `postgres`; on the host we
  # auto-rewrite to 127.0.0.1:5433 (see host_dsn.py). Override if needed:
  INIT_SCHEMA_DSN=postgresql+psycopg2://crawler:crawler@127.0.0.1:5433/crawlerdb \\
    uv run --group seed python init_schema.py

Optional env:
  INIT_SCHEMA_DSN  overrides postgres.dsn from config (skips auto host rewrite)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from host_dsn import adjust_postgres_dsn_for_host, print_pg_auth_failure_hint

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.db import (  # noqa: E402
    Base,
    DomainState,
    DomainStatsDaily,
    SummaryDaily,
    content_feature_current_table,
    content_feature_history_table,
    url_event_counter_table,
    url_state_current_table,
    url_state_history_table,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Create crawler DB schema (all shards).")
    ap.add_argument(
        "--config",
        type=Path,
        default=ROOT / "containers/scheduler_ingest/config/ingest.yaml",
        help="YAML containing postgres.dsn and router.num_shards",
    )
    ap.add_argument("--dsn", help="SQLAlchemy DSN (overrides config and INIT_SCHEMA_DSN)")
    args = ap.parse_args()

    raw = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    router = raw["router"]
    pg = raw["postgres"]
    num_shards = int(router["num_shards"])
    dsn = args.dsn or os.environ.get("INIT_SCHEMA_DSN") or str(pg["dsn"])
    if not args.dsn and not os.environ.get("INIT_SCHEMA_DSN"):
        dsn, note = adjust_postgres_dsn_for_host(dsn)
        if note:
            print(note, file=sys.stderr)

    for i in range(num_shards):
        url_state_current_table(i)
        url_state_history_table(i)
        url_event_counter_table(i)
        content_feature_current_table(i)
        content_feature_history_table(i)

    engine = create_engine(dsn, future=True)
    try:
        Base.metadata.create_all(engine)
    except OperationalError as e:
        print_pg_auth_failure_hint(dsn, e)
        raise SystemExit(2) from e
    print(f"Schema OK ({num_shards} shards, DSN host taken from connection).")


if __name__ == "__main__":
    main()
