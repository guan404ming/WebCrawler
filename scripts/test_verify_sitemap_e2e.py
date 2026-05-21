"""
Verify the sitemap test stack produced end-to-end state.

Inspects the mock crawlerdb populated by docker-compose.test.yml's
`sitemap` profile and prints a summary of what happened. Designed to be
run repeatedly while the stack is up — it is read-only.

Usage:
    uv run scripts/test_verify_sitemap_e2e.py \
        --mock-dsn 'postgresql://crawler:crawler@127.0.0.1:5433/crawlerdb_test'
        [--ipc-dir ./.test-data/ipc]

Exit code 0: every check produced non-empty evidence.
Exit code 1: at least one check is empty (the test has not completed
yet, or something is wrong — re-check container logs first).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import psycopg2

DEFAULT_IPC_DIR = Path("./.test-data/ipc")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def query_one(cur, sql: str, params: tuple = ()) -> tuple:
    cur.execute(sql, params)
    return cur.fetchone()


def query_all(cur, sql: str, params: tuple = ()) -> list[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def check_domain_state(cur) -> int:
    """How many golden domains did the bootstrap copy in?"""
    section("domain_state (sample copied from prod)")
    (n,) = query_one(cur, "SELECT COUNT(*) FROM domain_state WHERE domain_score >= 0.95")
    print(f"  rows with domain_score >= 0.95: {n}")
    if n == 0:
        print("  ⚠  empty — did db_bootstrap run? Did prod return any rows at score>=0.95?")
        return 0
    rows = query_all(
        cur,
        "SELECT domain, shard_id, domain_score "
        "FROM domain_state WHERE domain_score >= 0.95 "
        "ORDER BY domain_score DESC, domain LIMIT 5",
    )
    for r in rows:
        print(f"  - {r[0]:40s} shard={r[1]:3d}  score={r[2]:.3f}")
    return n


def check_domain_sitemap(cur) -> int:
    """How many sitemap URLs has the discover worker registered?"""
    section("domain_sitemap (created by our migration, populated by discover)")
    (total,) = query_one(cur, "SELECT COUNT(*) FROM domain_sitemap")
    (patrolled,) = query_one(
        cur, "SELECT COUNT(*) FROM domain_sitemap WHERE last_patrolled_at IS NOT NULL"
    )
    print(f"  total rows:        {total}")
    print(f"  patrolled at least once: {patrolled}")
    if total == 0:
        print("  ⚠  empty — did sitemap_discover complete one sweep?")
        return 0
    rows = query_all(
        cur,
        "SELECT sitemap_url, status, last_url_count, last_new_count, last_patrolled_at "
        "FROM domain_sitemap "
        "ORDER BY last_patrolled_at DESC NULLS LAST "
        "LIMIT 5",
    )
    print("  most-recent rows:")
    for r in rows:
        url, status, n_url, n_new, patrolled_at = r
        url_short = url if len(url) <= 60 else url[:57] + "..."
        print(f"    [{status or 'pending':12s}]  url={n_url}  new={n_new}  at={patrolled_at}  {url_short}")

    section("domain_sitemap status mix (last 24h of patrols)")
    rows = query_all(
        cur,
        "SELECT status, COUNT(*) FROM domain_sitemap "
        "WHERE last_patrolled_at IS NOT NULL "
        "GROUP BY status ORDER BY 2 DESC",
    )
    for status, count in rows:
        print(f"  {status or '<none>':14s}  {count}")
    return patrolled


def check_ipc_files(ipc_dir: Path) -> int:
    """Did the patrol worker actually emit IPC files?"""
    section(f"IPC files under {ipc_dir} (sitemap-emitted)")
    if not ipc_dir.exists():
        print(f"  ⚠  {ipc_dir} does not exist")
        return 0
    files = sorted(ipc_dir.glob("crawl_result/ingestor_*/*/*/*_sitemap_*.jsonl"))
    print(f"  matching files: {len(files)}")
    for f in files[:5]:
        # Show relative path for readability.
        try:
            print(f"  - {f.relative_to(ipc_dir.parent)}")
        except ValueError:
            print(f"  - {f}")
    if not files:
        print("  ⚠  no sitemap JSONL files emitted yet. The patrol loop "
              "may not have produced any urlset hits, or the worker has not "
              "completed its first pass.")
    return len(files)


def check_url_state(cur) -> int:
    """Did the ingestor consume any of those files into url_state_current?"""
    section("url_state_current_* (ingestor consumption — sample shards)")
    # Spot-check 8 sample shards instead of all 256 for speed.
    sample_shards = (0, 32, 64, 96, 128, 160, 192, 224)
    total_new = 0
    for shard in sample_shards:
        t = f"url_state_current_{shard:03d}"
        try:
            (n,) = query_one(cur, f"SELECT COUNT(*) FROM {t} WHERE discovery_source_type = 2")
        except psycopg2.errors.UndefinedTable:
            print(f"  ⚠  table {t} missing")
            continue
        print(f"  {t}: {n} rows with discovery_source_type=2 (sitemap)")
        total_new += n
    if total_new == 0:
        print("  ⚠  no sitemap-source rows in any sampled shard.")
        print("     This is expected immediately after startup — the ingestor")
        print("     only consumes folders older than 2 * interval_minutes (2 min in test).")
    return total_new


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mock-dsn", required=True,
                        help="psycopg2 DSN for the mock crawlerdb (read-only inspection).")
    parser.add_argument("--ipc-dir", type=Path, default=DEFAULT_IPC_DIR,
                        help=f"Bind-mount root for the test IPC tree (default {DEFAULT_IPC_DIR}).")
    args = parser.parse_args()

    conn = psycopg2.connect(args.mock_dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            n_domains = check_domain_state(cur)
            n_patrolled = check_domain_sitemap(cur)
            n_url_state = check_url_state(cur)
    finally:
        conn.close()

    n_files = check_ipc_files(args.ipc_dir)

    section("Summary")
    checks = {
        "domains seeded":            n_domains > 0,
        "sitemaps patrolled":        n_patrolled > 0,
        "IPC files written":         n_files > 0,
        "ingestor consumed (any)":   n_url_state > 0,
    }
    for label, ok in checks.items():
        print(f"  [{'OK' if ok else '..'}]  {label}")
    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        print(f"\n  {len(failed)} check(s) still pending: {failed}")
        return 1
    print("\n  all checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
