from __future__ import annotations

import argparse

from libs.config.loader import load_yaml, require
from libs.ipc.bus import create_consumer

from .service import StatsAggregatorService


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)
    pg = require(raw, "postgres")
    ipc = raw.get("ipc", {})

    consumer = create_consumer(ipc, group="stats", consumer_name="stats_00")
    svc = StatsAggregatorService(consumer, str(require(pg, "dsn")))
    svc.run_forever()


if __name__ == "__main__":
    main()
