from __future__ import annotations

import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.ipc.bus import MessageConsumer

from .db_ops import apply_stats_delta


class StatsAggregatorService:
    def __init__(self, consumer: MessageConsumer, postgres_dsn: str):
        self.consumer = consumer
        self.engine = create_engine(
            postgres_dsn,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_size=2,
            max_overflow=1,
            pool_timeout=30,
            future=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 5,
                "keepalives_count": 5
            },
        )
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)

    def run_forever(self):
        print("[stats] started", flush=True)
        while True:
            messages = self.consumer.poll("stats_delta", 0, max_messages=10)
            if not messages:
                time.sleep(5)
                continue

            for delta in messages:
                with self.Session() as session:
                    try:
                        apply_stats_delta(session, delta)
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        print(f"[stats] ERROR: {e}", flush=True)

            print(f"[stats] processed {len(messages)} deltas", flush=True)
