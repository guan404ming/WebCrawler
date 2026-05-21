"""
Bootstrap the docker-compose.test.yml mock crawlerdb.

Runs once before sitemap_patroller + scheduler_ingest_test start. Steps:
  1. Connect to **prod** crawlerdb (read-only SELECT) and pull a small
     sample of `domain_state` rows where domain_score >= --score-min.
  2. Connect to the **mock** crawlerdb and INSERT the sample (idempotent
     ON CONFLICT). Optionally seed a few rows into shard_split_subdomain
     so the patroller's load_sharding_config sees a non-empty whitelist.
  3. Apply scripts/migrate_add_domain_sitemap.STATEMENTS against the mock
     to exercise the migration itself — this is the test's whole point.

The prod connection is strictly SELECT-only; this script never issues
INSERT / UPDATE / DELETE / DDL against the prod DSN. The mock connection
is the only one we write to.

Usage:
    python -m scripts.test_bootstrap_mock_db \
        --prod-dsn 'postgresql://crawler:crawler@host.docker.internal:5432/crawlerdb' \
        --mock-dsn 'postgresql://crawler:crawler@test_postgres:5432/crawlerdb_test' \
        [--score-min 0.95] [--domain-limit 20]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import psycopg2

# Make the sibling migration module importable (scripts/ is added to path
# via PYTHONPATH=/app and the module living under scripts/).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import migrate_add_domain_sitemap as mig  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def fetch_prod_golden_domains(prod_dsn: str, score_min: float, limit: int) -> list[tuple[str, int, float]]:
    """Strictly read-only. Returns [(domain, shard_id, domain_score), ...]."""
    conn = psycopg2.connect(prod_dsn)
    try:
        # Belt-and-braces: explicitly mark this session read-only so even an
        # accidental UPDATE would error out.
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT domain, shard_id, COALESCE(domain_score, 0.0)
                FROM domain_state
                WHERE domain_score >= %s
                ORDER BY domain_score DESC, domain
                LIMIT %s
                """,
                (score_min, limit),
            )
            return [(str(r[0]), int(r[1]), float(r[2])) for r in cur.fetchall()]
    finally:
        conn.close()


def fetch_prod_split_subdomains(prod_dsn: str) -> list[str]:
    """Read-only snapshot of the production shard_split_subdomain whitelist.
    Needed so the patroller's shard routing matches prod exactly."""
    conn = psycopg2.connect(prod_dsn)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT host FROM shard_split_subdomain")
            return [str(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def seed_mock(
    mock_dsn: str,
    domains: list[tuple[str, int, float]],
    split_subdomains: list[str],
) -> dict:
    counters = {"domains_inserted": 0, "split_inserted": 0}
    conn = psycopg2.connect(mock_dsn)
    try:
        with conn.cursor() as cur:
            for domain, shard_id, score in domains:
                cur.execute(
                    """
                    INSERT INTO domain_state (domain, shard_id, domain_score)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (domain) DO UPDATE SET
                        shard_id     = EXCLUDED.shard_id,
                        domain_score = EXCLUDED.domain_score
                    RETURNING (xmax = 0)
                    """,
                    (domain, shard_id, score),
                )
                if bool(cur.fetchone()[0]):
                    counters["domains_inserted"] += 1
            for host in split_subdomains:
                cur.execute(
                    "INSERT INTO shard_split_subdomain (host) VALUES (%s) "
                    "ON CONFLICT (host) DO NOTHING RETURNING host",
                    (host,),
                )
                if cur.fetchone() is not None:
                    counters["split_inserted"] += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return counters


def apply_sitemap_migration(mock_dsn: str) -> None:
    """Run scripts/migrate_add_domain_sitemap.STATEMENTS against the mock.
    This is the actual migration test the user asked for."""
    conn = psycopg2.connect(mock_dsn)
    try:
        with conn.cursor() as cur:
            for sql in mig.STATEMENTS:
                cur.execute(sql)
                log.info("migration ok: %s", sql.strip().splitlines()[0])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prod-dsn", required=True, help="psycopg2 DSN for the prod crawlerdb (read-only).")
    parser.add_argument("--mock-dsn", required=True, help="psycopg2 DSN for the mock crawlerdb (writable).")
    parser.add_argument("--score-min", type=float, default=0.95,
                        help="Pull domain_state rows with domain_score >= this (default 0.95 = T0+T1).")
    parser.add_argument("--domain-limit", type=int, default=20,
                        help="Cap on rows copied from prod (default 20; keep small for fast tests).")
    args = parser.parse_args()

    log.info("[1/3] Pulling up to %d golden domains from PROD (read-only)...", args.domain_limit)
    domains = fetch_prod_golden_domains(args.prod_dsn, args.score_min, args.domain_limit)
    log.info("       fetched %d domains", len(domains))
    if not domains:
        log.warning("PROD returned 0 domains at score_min=%.2f. "
                    "Has update_golden_domain_scores.py run?", args.score_min)

    log.info("[1b/3] Pulling shard_split_subdomain whitelist from PROD (read-only)...")
    split_subdomains = fetch_prod_split_subdomains(args.prod_dsn)
    log.info("       fetched %d split-subdomain entries", len(split_subdomains))

    log.info("[2/3] Seeding MOCK with the sample...")
    counters = seed_mock(args.mock_dsn, domains, split_subdomains)
    log.info("       domains_inserted=%d  split_inserted=%d  (rest were already present)",
             counters["domains_inserted"], counters["split_inserted"])

    log.info("[3/3] Applying scripts/migrate_add_domain_sitemap against MOCK...")
    apply_sitemap_migration(args.mock_dsn)

    log.info("Bootstrap complete.")


if __name__ == "__main__":
    main()
