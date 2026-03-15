from __future__ import annotations

import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.ipc.bus import create_consumer
from libs.stats.delta_writer import StatsDeltaWriter

from .service import IngestService
from .db_ops import IngestDB


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--ingestor-id", type=int, required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)
    ingestor_cfg = require(raw, "ingestor")
    pg = require(raw, "postgres")
    ipc = raw.get("ipc", {})

    engine = create_engine(
        str(require(pg, "dsn")),
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
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    consumer = create_consumer(ipc, group="ingestor", consumer_name=f"ingestor_{args.ingestor_id:02d}")
    db = IngestDB(Session)
    stats = StatsDeltaWriter(require(ingestor_cfg, "stats_dir"))

    svc = IngestService(args.ingestor_id, db, consumer, stats)
    svc.run_forever()


if __name__ == "__main__":
    main()
