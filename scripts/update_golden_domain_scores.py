"""
Golden Set Domain Score Tiering

Writes domain_state.domain_score from each domain's presence across metricdb
golden batches. Designed to run after every new metric batch arrives.

Tiers (highest wins):
    1.0   T0  domain appeared in EVERY metric batch
    0.95  T1  domain appeared in the LAST 2 metric batches (consecutive by id),
              and is not already in T0
    0.8   T2  domain appeared in any metric batch, and is not in T0 or T1
    0.0   T3  default — domain never appeared in any metric batch

Strategy:
    1. Pull (host, batch_id) pairs from metricdb.
    2. Apply the production sharder's shard_key() so each host collapses to the
       same key that domain_state.domain uses (eTLD+1 for non-split subdomains,
       full host for entries in the `shard_split_subdomain` DB whitelist).
    3. Compute T0 / T1 / T2 disjoint sets.
    4. In one crawlerdb transaction:
        a. Reset every row whose domain_score is currently {1.0, 0.95, 0.8}
           back to 0.0 (so domains that have dropped out of any tier are
           demoted correctly).
        b. Write SCORE_T2 to T2 rows, SCORE_T1 to T1 rows, SCORE_T0 to T0 rows.

The full pipeline is idempotent — running twice in a row yields the same state.

Usage:
    python scripts/update_golden_domain_scores.py [--dry-run]
"""

import argparse
import logging
import re
from pathlib import Path

import psycopg2

from constants import CRAWLERDB, METRICDB
from libs.db.sharding.key import load_sharding_config, shard_key

SCORE_T0 = 1.0    # every batch
SCORE_T1 = 0.95   # last 2 consecutive batches
SCORE_T2 = 0.8    # any batch
TIER_SCORES = (SCORE_T0, SCORE_T1, SCORE_T2)

INGEST_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_ingest/config/ingest.yaml"
)

# Pulls just the host (authority) portion. URL params / fragments are ignored
# because metric_url.url is already stripped to the canonical URL.
_HOST_RE = re.compile(r"^https?://([^/?#]+)")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def fetch_host_batch_presence(metric_cur) -> dict[str, set[int]]:
    """Return mapping host -> set of batch_ids the host appeared in."""
    metric_cur.execute(
        """
        SELECT mu.url, mq.batch_id
        FROM metric_url mu
        JOIN metric_queries mq ON mq.id = mu.query_id
        """
    )
    out: dict[str, set[int]] = {}
    for url, batch_id in metric_cur.fetchall():
        if not url:
            continue
        m = _HOST_RE.match(url)
        if not m:
            continue
        host = m.group(1).lower()
        out.setdefault(host, set()).add(batch_id)
    return out


def collapse_to_domain_keys(
    host_batches: dict[str, set[int]],
    split_subdomains: set[str],
) -> dict[str, set[int]]:
    """Map raw host -> production domain key via shard_key(). Multiple hosts
    can collapse to the same key when the `shard_split_subdomain` whitelist
    does not list them."""
    out: dict[str, set[int]] = {}
    for host, batches in host_batches.items():
        key = shard_key(host, split_subdomains)
        out.setdefault(key, set()).update(batches)
    return out


def fetch_all_batch_ids(metric_cur) -> list[int]:
    metric_cur.execute("SELECT id FROM metric_batches ORDER BY id ASC")
    return [r[0] for r in metric_cur.fetchall()]


def classify_tiers(
    domain_batches: dict[str, set[int]],
    all_batch_ids: list[int],
) -> tuple[set[str], set[str], set[str]]:
    """Place each domain in the highest tier it qualifies for. Returns
    (T0, T1, T2) as disjoint sets. Domains that never appeared in any batch
    are absent from all returned sets (they stay at score 0.0)."""
    if not all_batch_ids:
        return set(), set(), set()
    full = set(all_batch_ids)
    last_two = set(all_batch_ids[-2:]) if len(all_batch_ids) >= 2 else set()

    t0: set[str] = set()
    t1: set[str] = set()
    t2: set[str] = set()
    for dom, batches in domain_batches.items():
        if not batches:
            continue
        if batches == full:
            t0.add(dom)
        elif last_two and last_two.issubset(batches):
            t1.add(dom)
        else:
            t2.add(dom)
    return t0, t1, t2


def apply_scores(
    crawler_cur,
    t0: set[str],
    t1: set[str],
    t2: set[str],
) -> dict:
    """Reset previously-tiered rows then write new tier scores. Sets are
    expected to be disjoint (output of classify_tiers)."""
    crawler_cur.execute(
        "UPDATE domain_state SET domain_score = 0.0 WHERE domain_score = ANY(%s)",
        (list(TIER_SCORES),),
    )
    reset_rowcount = crawler_cur.rowcount

    counts: dict[str, int] = {"reset": reset_rowcount}
    for label, score, dom_set in (
        ("t2", SCORE_T2, t2),
        ("t1", SCORE_T1, t1),
        ("t0", SCORE_T0, t0),
    ):
        if not dom_set:
            counts[label] = 0
            continue
        crawler_cur.execute(
            "UPDATE domain_state SET domain_score = %s WHERE domain = ANY(%s)",
            (score, list(dom_set)),
        )
        counts[label] = crawler_cur.rowcount
    return counts


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Update domain_state.domain_score tiers from metric_url batch "
            "presence (T0=1.0 all batches, T1=0.95 last-2 consec, T2=0.8 ever)."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute tiers and print summary without writing crawlerdb",
    )
    args = parser.parse_args()

    metric_conn = psycopg2.connect(**METRICDB)
    try:
        crawler_conn = psycopg2.connect(**CRAWLERDB)
    except Exception:
        metric_conn.close()
        raise

    try:
        metric_cur = metric_conn.cursor()
        crawler_cur = crawler_conn.cursor()

        # New signature (NTU-CSIE5376/WebCrawler#28): split_subdomains now
        # lives in the `shard_split_subdomain` table on crawlerdb, not a yaml
        # file. Pass the open psycopg2 connection so load_sharding_config can
        # read it.
        _overrides, split_subdomains = load_sharding_config(INGEST_CONFIG, crawler_conn)

        log.info("Pulling host -> batch presence from metricdb...")
        raw_hosts = fetch_host_batch_presence(metric_cur)
        log.info("  raw hosts seen: %d", len(raw_hosts))

        domain_batches = collapse_to_domain_keys(raw_hosts, split_subdomains)
        log.info("  effective domain keys after shard_key(): %d", len(domain_batches))

        all_batch_ids = fetch_all_batch_ids(metric_cur)
        log.info("  metric batches: %s", all_batch_ids)

        t0, t1, t2 = classify_tiers(domain_batches, all_batch_ids)
        untiered = len(domain_batches) - len(t0) - len(t1) - len(t2)
        log.info(
            "Tier sizes: T0=%d  T1=%d  T2=%d  (rest in golden set but not tier-eligible=%d)",
            len(t0), len(t1), len(t2), untiered,
        )

        if args.dry_run:
            log.info(
                "[DRY-RUN] Skipping UPDATE. Sample T0 (up to 5): %s",
                list(t0)[:5],
            )
            log.info("[DRY-RUN] Sample T1 (up to 5): %s", list(t1)[:5])
            log.info("[DRY-RUN] Sample T2 (up to 5): %s", list(t2)[:5])
            return

        counts = apply_scores(crawler_cur, t0, t1, t2)
        crawler_conn.commit()
        log.info(
            "Done: reset=%d  t2_written=%d  t1_written=%d  t0_written=%d",
            counts["reset"], counts["t2"], counts["t1"], counts["t0"],
        )

    except Exception:
        crawler_conn.rollback()
        raise
    finally:
        metric_conn.close()
        crawler_conn.close()


if __name__ == "__main__":
    main()
