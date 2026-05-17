from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from containers.scheduler_control.offerer.selection.golden_discovery_ranker_v1_strategy import (
    GoldenDiscoveryRankerV1Strategy,
)
from containers.scheduler_control.scorer import service as scorer_service
from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig,
    GoldenDiscoveryRankerService,
)
from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from scripts import migrate_add_url_score_updated_at as migration


class GoldenDiscoveryMigrationSqlTest(unittest.TestCase):
    def test_unscored_index_is_partial_current_shard_index(self):
        sql = migration.create_golden_discovery_unscored_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v1_unscored",
            sql,
        )
        self.assertIn(
            "ON url_state_current_007 (first_seen ASC NULLS LAST)",
            sql,
        )
        self.assertIn(
            "WHERE should_crawl = TRUE AND url_score_updated_at IS NULL",
            sql,
        )
        self.assertNotIn("url_state_history", sql)

    def test_selection_index_is_partial_current_shard_index(self):
        sql = migration.create_golden_discovery_selection_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v1_selection",
            sql,
        )
        self.assertIn(
            "ON url_state_current_007 (domain_id, "
            "((CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END)), "
            "url_score DESC NULLS LAST, "
            "domain_score DESC NULLS LAST, "
            "last_scheduled ASC NULLS FIRST, "
            "first_seen ASC)",
            sql,
        )
        self.assertIn("WHERE should_crawl = TRUE", sql)
        self.assertNotIn("url_state_history", sql)

    def test_column_migration_targets_current_and_history_shards(self):
        tables = list(migration.iter_state_tables(num_shards=2))

        self.assertEqual(
            tables,
            [
                "url_state_current_000",
                "url_state_current_001",
                "url_state_history_000",
                "url_state_history_001",
            ],
        )
        self.assertEqual(
            migration.add_url_score_updated_at_sql("url_state_current_000"),
            "ALTER TABLE url_state_current_000 "
            "ADD COLUMN IF NOT EXISTS url_score_updated_at TIMESTAMPTZ",
        )


class _FakeResult:
    def fetchall(self):
        return [SimpleNamespace(domain_id=7, url="https://example.com/a")]


class _FakeOffererSession:
    def __init__(self):
        self.calls = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.calls.append((str(sql), params))
        return _FakeResult()

    def commit(self):
        self.committed = True


class _FakeOffererSessionFactory:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return self.session


class GoldenDiscoveryOffererStrategyTest(unittest.TestCase):
    def test_strategy_prefers_refreshed_ranker_scores_in_sql(self):
        session = _FakeOffererSession()
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        selected = strategy.select_by_domain(
            shard_id=3,
            exclude_domain_ids={11, 22},
            per_domain_cap=2,
            max_domains=5,
        )

        self.assertEqual(selected, {7: ["https://example.com/a"]})
        self.assertTrue(session.committed)

        sql, params = session.calls[0]
        self.assertIn("FROM url_state_current_003", sql)
        self.assertIn(
            "MAX(CASE WHEN url_score_updated_at IS NOT NULL THEN url_score END)",
            sql,
        )
        self.assertNotIn("WHEN MAX(CASE", sql)
        self.assertIn("NOT EXISTS", sql)
        self.assertIn("FROM domain_state d", sql)
        self.assertIn("d.crawl_paused_until > NOW()", sql)
        self.assertIn(
            "CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END",
            sql,
        )
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params["exclude"], (11, 22))
        self.assertEqual(params["per_domain_cap"], 2)
        self.assertEqual(params["max_domains"], 5)


class _FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return [
            ("https://example.com/a",),
            ("https://example.com/b",),
        ]


class _FakeRawConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class _FakeSqlAlchemyConnection:
    def __init__(self, cursor):
        self.connection = _FakeRawConnection(cursor)


class _FakeScorerSession:
    def __init__(self, cursor):
        self._cursor = cursor

    def connection(self):
        return _FakeSqlAlchemyConnection(self._cursor)


class _FakeBeginContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeScorerSessionFactory:
    def __init__(self, cursor):
        self.session = _FakeScorerSession(cursor)

    def begin(self):
        return _FakeBeginContext(self.session)


class _FakeScorer:
    def score_many(self, urls):
        self.urls = urls
        return [0.25, 0.75]


class GoldenDiscoveryRankerServiceTest(unittest.TestCase):
    def test_scorer_claims_unscored_urls_and_updates_timestamp(self):
        cursor = _FakeCursor()
        fake_scorer = _FakeScorer()
        cfg = GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=4,
            worker_id=0,
            batch_size=1000,
            scan_interval_sec=60,
            max_batches_per_shard=1,
        )
        service = GoldenDiscoveryRankerService(
            cfg=cfg,
            Session=_FakeScorerSessionFactory(cursor),
            scorer=fake_scorer,
        )
        execute_values_calls = []

        def fake_execute_values(cur, sql, rows, page_size):
            execute_values_calls.append((cur, sql, rows, page_size))

        with patch.object(scorer_service, "execute_values", fake_execute_values):
            count = service._score_batch(5)

        self.assertEqual(count, 2)
        self.assertEqual(
            fake_scorer.urls,
            ["https://example.com/a", "https://example.com/b"],
        )

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_005", select_sql)
        self.assertIn("url_score_updated_at IS NULL", select_sql)
        self.assertIn("ORDER BY first_seen ASC NULLS LAST", select_sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", select_sql)
        self.assertEqual(select_params, (1000,))

        _, update_sql, rows, page_size = execute_values_calls[0]
        self.assertIn("url_score_updated_at = CURRENT_TIMESTAMP", update_sql)
        self.assertEqual(
            rows,
            [
                ("https://example.com/a", 0.25),
                ("https://example.com/b", 0.75),
            ],
        )
        self.assertEqual(page_size, 2)


def _make_steering_service(cursor, *, steering: bool, batch_size: int = 100):
    return GoldenDiscoveryRankerService(
        cfg=GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=1,
            worker_id=0,
            batch_size=batch_size,
            scan_interval_sec=60,
            max_batches_per_shard=1,
            domain_priority_steering_enabled=steering,
        ),
        Session=_FakeScorerSessionFactory(cursor),
        scorer=_FakeScorer(),
    )


class GoldenDiscoveryRankerSteeringTest(unittest.TestCase):
    def test_steering_disabled_uses_legacy_first_seen_query(self):
        cursor = _FakeCursor()
        service = _make_steering_service(cursor, steering=False, batch_size=1000)

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(7)

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_007", select_sql)
        self.assertIn("ORDER BY first_seen ASC NULLS LAST", select_sql)
        self.assertNotIn("domain_state", select_sql)
        self.assertNotIn("domain_score", select_sql)
        self.assertEqual(select_params, (1000,))

    def test_steering_enabled_joins_domain_state_and_filters_by_score(self):
        cursor = _FakeCursor()
        service = _make_steering_service(cursor, steering=True, batch_size=1000)

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(7)

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_007 u", select_sql)
        self.assertIn("JOIN domain_state d ON d.domain_id = u.domain_id", select_sql)
        self.assertIn("d.domain_score > 0", select_sql)
        self.assertIn("ORDER BY d.domain_score DESC NULLS LAST", select_sql)
        self.assertIn("u.first_seen ASC NULLS LAST", select_sql)
        self.assertIn("FOR UPDATE OF u SKIP LOCKED", select_sql)
        self.assertEqual(select_params, (1000,))


class _FakeInlineRanker:
    def __init__(self):
        self.calls = []
        self.urls = []

    def score_many(self, urls):
        self.urls = list(urls)
        self.calls.append(list(urls))
        return [0.42 + i / 100 for i, _ in enumerate(urls)]


class _FakeLinkCursor:
    def __init__(self, existing_urls=()):
        self.existing_urls = set(existing_urls)
        self.executed = []
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        urls = params[0] if params else []
        self._rows = [(url,) for url in urls if url in self.existing_urls]

    def fetchall(self):
        return self._rows


class GoldenDiscoveryIngestInlineScoringTest(unittest.TestCase):
    def test_new_links_are_inserted_with_inline_ranker_scores_when_enabled(self):
        ranker = _FakeInlineRanker()
        db = IngestDB(Session=None, inline_ranker=ranker)
        cursor = _FakeLinkCursor(existing_urls={"https://example.com/b"})
        execute_values_calls = []

        records = [
            (
                0,
                {
                    "url": "https://example.com/a",
                    "domain_id": 7,
                    "domain_score": 0.1,
                    "discovered_from": "https://example.com/",
                },
            ),
            (
                1,
                {
                    "url": "https://example.com/b",
                    "domain_id": 7,
                    "domain_score": 0.1,
                    "discovered_from": "https://example.com/",
                },
            ),
        ]

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            materialized_rows = list(rows)
            execute_values_calls.append((sql, materialized_rows, page_size, fetch))
            if fetch:
                return [
                    (row[0], row[0] not in cursor.existing_urls)
                    for row in materialized_rows
                ]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            result = db._bulk_links(cur=cursor, shard_id=3, items=records)

        self.assertEqual(result, [(0, True), (1, False)])
        self.assertEqual(ranker.calls, [["https://example.com/a"]])
        self.assertIn("FROM url_state_current_003", cursor.executed[0][0])

        current_sql, current_rows, _, current_fetch = execute_values_calls[0]
        self.assertTrue(current_fetch)
        self.assertIn("url_score, url_score_updated_at", current_sql)
        self.assertEqual(current_rows[0][3], 0.42)
        self.assertEqual(current_rows[1][3], 0.0)
        self.assertIsNotNone(current_rows[0][4])
        self.assertIsNone(current_rows[1][4])

        history_sql, history_rows, _, history_fetch = execute_values_calls[1]
        self.assertFalse(history_fetch)
        self.assertIn("url_score, url_score_updated_at", history_sql)
        self.assertEqual(history_rows, [current_rows[0]])

    def test_new_links_fall_back_to_unscored_when_inline_ranker_times_out(self):
        ranker = _FakeInlineRanker()
        db = IngestDB(
            Session=None,
            inline_ranker=ranker,
            inline_score_timeout_sec=0.0,
        )
        cursor = _FakeLinkCursor()
        execute_values_calls = []
        records = [
            (
                0,
                {
                    "url": "https://example.com/a",
                    "domain_id": 7,
                    "domain_score": 0.1,
                },
            )
        ]

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            materialized_rows = list(rows)
            execute_values_calls.append((sql, materialized_rows, page_size, fetch))
            if fetch:
                return [(row[0], True) for row in materialized_rows]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            result = db._bulk_links(cur=cursor, shard_id=3, items=records)

        self.assertEqual(result, [(0, True)])
        self.assertEqual(ranker.calls, [])
        self.assertEqual(cursor.executed, [])
        current_rows = execute_values_calls[0][1]
        self.assertEqual(current_rows[0][3], 0.0)
        self.assertIsNone(current_rows[0][4])

    def test_new_links_remain_unscored_when_inline_ranker_is_disabled(self):
        db = IngestDB(Session=None, inline_ranker=None)
        cursor = _FakeLinkCursor()
        execute_values_calls = []
        records = [
            (
                0,
                {
                    "url": "https://example.com/a",
                    "domain_id": 7,
                    "domain_score": 0.1,
                },
            )
        ]

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            materialized_rows = list(rows)
            execute_values_calls.append((sql, materialized_rows, page_size, fetch))
            if fetch:
                return [(row[0], True) for row in materialized_rows]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            result = db._bulk_links(cur=cursor, shard_id=3, items=records)

        self.assertEqual(result, [(0, True)])
        self.assertEqual(cursor.executed, [])
        current_rows = execute_values_calls[0][1]
        self.assertEqual(current_rows[0][3], 0.0)
        self.assertIsNone(current_rows[0][4])


if __name__ == "__main__":
    unittest.main()
