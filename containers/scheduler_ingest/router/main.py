from __future__ import annotations

import argparse

from libs.config.loader import load_yaml
from libs.ipc.bus import create_producer, create_consumer

from .service import RouterService, load_router_config


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--router-id", type=int, required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)
    ipc = raw.get("ipc", {})

    cfg = load_router_config(args.config, args.router_id)
    consumer = create_consumer(ipc, group="router", consumer_name=f"router_{args.router_id:02d}")
    producer = create_producer(ipc)

    svc = RouterService(cfg, consumer, producer)
    svc.run_forever()


if __name__ == "__main__":
    main()
