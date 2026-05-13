"""
Migration: add `idx_domain_state_shard_score` on domain_state.

The offerer's global refill path (`peek_global_candidates` in
GoldenDiscoveryRankerV1Strategy / ReadOnlyStrategy) runs:

    SELECT domain_id, shard_id, domain_score
    FROM domain_state
    WHERE shard_id BETWEEN :shard_start AND :shard_end
      AND ...
    ORDER BY domain_score DESC NULLS LAST, domain_id
    LIMIT :limit

Without an index this falls back to a Seq Scan over the full 11.5M-row
domain_state table per refill, which is unworkable once more than a
single offerer is running.

This index covers both the BETWEEN filter (leading column) and the
ORDER BY (subsequent columns), so the planner walks it in index order
and stops at LIMIT. domain_id is included to give a stable tiebreaker
that matches the SQL.

CREATE INDEX CONCURRENTLY does not lock writers, so the ingestor /
router that touches domain_state continues to run during the build.

Usage:
    uv run scripts/migrate_add_domain_state_shard_score_index.py [--dry-run]
"""

import argparse
import logging

import psycopg2

try:
    from scripts.constants import CRAWLERDB
except ModuleNotFoundError:
    from constants import CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

INDEX_NAME = "idx_domain_state_shard_score"


def create_index_sql() -> str:
    return (
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
        "ON domain_state (shard_id, domain_score DESC NULLS LAST, domain_id)"
    )


def create_index(conn, dry_run: bool) -> None:
    sql = create_index_sql()
    if dry_run:
        log.info("[DRY-RUN] %s", sql)
        return

    previous_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.autocommit = previous_autocommit


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Add idx_domain_state_shard_score to support the offerer's "
            "global peek_global_candidates query"
        )
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)

    try:
        create_index(conn, args.dry_run)
        if args.dry_run:
            log.info("[DRY-RUN] Would create index %s", INDEX_NAME)
        else:
            log.info("Done: created index %s", INDEX_NAME)
    except Exception:
        if not conn.autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
