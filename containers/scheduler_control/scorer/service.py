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

    def _shard_ids(self) -> list[int]:
        """Order in which this worker visits shards on each run_once.

        Every worker visits every shard. Workers stagger their starting
        offset by `total_shards // num_workers` so the four (or N) of
        them don't all queue up on shard 0 first — but the static
        partition is gone, so a worker that finishes its starting
        section keeps walking and picks up shards that another worker
        would previously have owned. URL-level FOR UPDATE SKIP LOCKED
        inside `_score_batch` handles the resulting concurrent claims
        on the same shard.

        Replaces the prior static partition (worker N saw shards
        [N, N + num_workers, ...]) which could not reassign work when
        one worker drew a disproportionate share of the very large
        (256M-row) shards while other workers sat idle after draining
        their own assignment.
        """
        if self.cfg.num_workers <= 0:
            return list(range(self.cfg.total_shards))
        step = max(1, self.cfg.total_shards // self.cfg.num_workers)
        offset = (self.cfg.worker_id * step) % self.cfg.total_shards
        return [
            (offset + i) % self.cfg.total_shards
            for i in range(self.cfg.total_shards)
        ]

    def _score_batch(self, shard_id: int) -> int:
        table = self._table(shard_id)

        with self.Session.begin() as sess:
            with sess.connection().connection.cursor() as cur:
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
                scored = len(rows)

        logger.info(
            "golden_discovery_ranker_v1.score_batch",
            extra={
                "event": "golden_discovery_ranker_v1.score_batch",
                "worker_id": self.cfg.worker_id,
                "shard_id": shard_id,
                "scored_urls": scored,
            },
        )
        return scored

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
