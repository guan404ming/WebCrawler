from __future__ import annotations

from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .base import CandidateDomain, SelectionStrategy


class ReadOnlyStrategy(SelectionStrategy):
    """Test-only strategy: pulls eligible URLs from the DB without touching it.

    Unlike ExampleStrategy this performs no UPDATE/INSERT, so running it against
    a live production database does not flip should_crawl, advance
    last_scheduled, or append to url_event_counter_*. The same rows may be
    re-selected on every scan, which is fine for generating log traffic in the
    Loki test stack.
    """

    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _table(self, shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def select_by_domain(
        self,
        shard_id: int,
        exclude_domain_ids: set[int],
        per_domain_cap: int,
        max_domains: int,
    ) -> dict[int, list[str]]:
        if max_domains <= 0 or per_domain_cap <= 0:
            return {}

        table = self._table(shard_id)

        exclude_clause = ""
        params: dict = {
            "max_domains": max_domains,
            "per_domain_cap": per_domain_cap,
        }
        if exclude_domain_ids:
            exclude_clause = "AND domain_id NOT IN :exclude"
            params["exclude"] = tuple(exclude_domain_ids)

        sql = text(f"""
        WITH eligible_domains AS (
            SELECT DISTINCT domain_id
            FROM {table}
            WHERE should_crawl = TRUE
              {exclude_clause}
            ORDER BY domain_id
            LIMIT :max_domains
        )
        SELECT u.url, u.domain_id
        FROM eligible_domains d,
        LATERAL (
            SELECT url, domain_id
            FROM {table}
            WHERE should_crawl = TRUE AND domain_id = d.domain_id
            ORDER BY url_score DESC NULLS LAST,
                     domain_score DESC NULLS LAST,
                     last_scheduled ASC NULLS FIRST,
                     first_seen ASC
            LIMIT :per_domain_cap
        ) u;
        """)

        with self.Session() as sess:
            rows = sess.execute(sql, params).fetchall()

        result: dict[int, list[str]] = defaultdict(list)
        for r in rows:
            result[r.domain_id].append(r.url)
        return dict(result)

    def peek_global_candidates(
        self,
        limit: int,
        exclude_domain_ids: set[int],
        shard_start: int,
        shard_end: int,
    ) -> list[CandidateDomain]:
        if limit <= 0 or shard_start > shard_end:
            return []

        shard_exists = " OR ".join(
            f"(d.shard_id = {sid} AND EXISTS ("
            f"SELECT 1 FROM url_state_current_{sid:03d} u "
            f"WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE"
            f"))"
            for sid in range(shard_start, shard_end + 1)
        )

        sql = text(f"""
        SELECT d.domain_id, d.shard_id, d.domain_score
        FROM domain_state d
        WHERE d.shard_id BETWEEN :shard_start AND :shard_end
          AND (d.crawl_paused_until IS NULL OR d.crawl_paused_until <= NOW())
          AND d.domain_id <> ALL(:exclude_arr)
          AND ({shard_exists})
        ORDER BY d.domain_score DESC NULLS LAST, d.domain_id
        LIMIT :limit
        """)

        params = {
            "limit": limit,
            "shard_start": shard_start,
            "shard_end": shard_end,
            "exclude_arr": list(exclude_domain_ids) if exclude_domain_ids else [],
        }

        with self.Session() as sess:
            rows = sess.execute(sql, params).fetchall()

        return [
            CandidateDomain(
                domain_id=r.domain_id,
                shard_id=r.shard_id,
                domain_score=float(r.domain_score) if r.domain_score is not None else 0.0,
            )
            for r in rows
        ]

    def claim_domain_urls(
        self,
        shard_id: int,
        domain_id: int,
        per_domain_cap: int,
    ) -> list[str]:
        if per_domain_cap <= 0:
            return []

        table = self._table(shard_id)

        sql = text(f"""
        SELECT url
        FROM {table}
        WHERE should_crawl = TRUE AND domain_id = :domain_id
        ORDER BY url_score DESC NULLS LAST,
                 domain_score DESC NULLS LAST,
                 last_scheduled ASC NULLS FIRST,
                 first_seen ASC
        LIMIT :per_domain_cap
        """)

        params = {
            "domain_id": domain_id,
            "per_domain_cap": per_domain_cap,
        }

        with self.Session() as sess:
            rows = sess.execute(sql, params).fetchall()

        return [r.url for r in rows]
