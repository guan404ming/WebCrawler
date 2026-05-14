from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from psycopg2.extras import execute_values
from sqlalchemy.orm import sessionmaker

from libs.scoring.golden_discovery_runtime import GoldenDiscoveryRuntimeScorer


logger = logging.getLogger("golden_discovery_ranker_v1")


@dataclass(frozen=True)
class GoldenDiscoveryRankerConfig:
    total_shards: int
    num_workers: int
    worker_id: int
    batch_size: int
    scan_interval_sec: int
    max_batches_per_shard: int
    # Domain-priority steering: when enabled, each batch only picks unscored
    # URLs from domains with `domain_state.domain_score > 0` (i.e. domains
    # that have appeared in a golden batch). Ordered by domain_score DESC so
    # higher-tier golden domains are scored first. When disabled, the scorer
    # keeps the legacy first_seen-ASC behavior.
    domain_priority_steering_enabled: bool = False


class GoldenDiscoveryRankerService:
    def __init__(
        self,
        cfg: GoldenDiscoveryRankerConfig,
        Session: sessionmaker,
        scorer: GoldenDiscoveryRuntimeScorer,
    ):
        self.cfg = cfg
        self.Session = Session
        self.scorer = scorer

    @staticmethod
    def _table(shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def _shard_ids(self) -> range:
        return range(self.cfg.worker_id, self.cfg.total_shards, self.cfg.num_workers)

    def _score_batch(self, shard_id: int) -> int:
        table = self._table(shard_id)

        with self.Session.begin() as sess:
            with sess.connection().connection.cursor() as cur:
                if self.cfg.domain_priority_steering_enabled:
                    # Score only URLs whose domain has a golden-tier score.
                    # `d.domain_score > 0` is the gate — domain_state default
                    # is 0.0, so this picks up exactly the rows that the
                    # tier-write cron has tagged.
                    cur.execute(
                        f"""
                        SELECT u.url
                        FROM {table} u
                        JOIN domain_state d ON d.domain_id = u.domain_id
                        WHERE u.should_crawl = TRUE
                          AND u.url_score_updated_at IS NULL
                          AND d.domain_score > 0
                        ORDER BY d.domain_score DESC NULLS LAST,
                                 u.first_seen ASC NULLS LAST
                        FOR UPDATE OF u SKIP LOCKED
                        LIMIT %s
                        """,
                        (self.cfg.batch_size,),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT url
                        FROM {table}
                        WHERE should_crawl = TRUE
                          AND url_score_updated_at IS NULL
                        ORDER BY first_seen ASC NULLS LAST
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                        """,
                        (self.cfg.batch_size,),
                    )
                urls = [row[0] for row in cur.fetchall()]
                if not urls:
                    return 0

                scores = self.scorer.score_many(urls)
                rows = list(zip(urls, scores))

                # Keep score history compact: the ranker refreshes
                # current.url_score in place and uses url_score_updated_at as
                # the only completion bit.
                execute_values(
                    cur,
                    f"""
                    UPDATE {table} AS u
                    SET
                        url_score = v.score::double precision,
                        url_score_updated_at = CURRENT_TIMESTAMP
                    FROM (VALUES %s) AS v(url, score)
                    WHERE u.url = v.url
                    """,
                    rows,
                    page_size=len(rows),
                )
                return len(rows)

    def run_once(self) -> dict[str, int]:
        totals = {"scored_urls": 0, "scored_batches": 0}

        for shard_id in self._shard_ids():
            batches = 0
            while batches < self.cfg.max_batches_per_shard:
                count = self._score_batch(shard_id)
                if count == 0:
                    break
                batches += 1
                totals["scored_batches"] += 1
                totals["scored_urls"] += count

        logger.info(
            "golden_discovery_ranker_v1.run_once",
            extra={
                "event": "golden_discovery_ranker_v1.run_once",
                "worker_id": self.cfg.worker_id,
                **totals,
            },
        )
        return totals

    def run_forever(self) -> None:
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error(
                    "golden_discovery_ranker_v1.error",
                    extra={
                        "event": "golden_discovery_ranker_v1.error",
                        "worker_id": self.cfg.worker_id,
                        "error": str(e),
                    },
                )
            time.sleep(self.cfg.scan_interval_sec)
