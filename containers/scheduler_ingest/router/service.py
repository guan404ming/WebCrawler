from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, InterfaceError

from libs.config.loader import load_yaml, require
from libs.ipc.bus import MessageProducer, MessageConsumer

from .routing import ShardRouter
from .domain_resolver import DomainResolver


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="replace")).hexdigest()


@dataclass(frozen=True)
class RouterConfig:
    router_id: int

    num_shards: int
    shards_per_ingestor: int
    interval_minutes: int
    poll_interval_sec: int

    domain_overrides: Dict[str, int]
    postgres_dsn: str


class RouterService:
    def __init__(self, cfg: RouterConfig, consumer: MessageConsumer, producer: MessageProducer):
        self.cfg = cfg
        self.consumer = consumer
        self.producer = producer
        self.sharder = ShardRouter(
            num_shards=self.cfg.num_shards,
            shards_per_ingestor=self.cfg.shards_per_ingestor,
            domain_overrides=self.cfg.domain_overrides,
        )
        self.engine = create_engine(
            self.cfg.postgres_dsn,
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
        self._domain_cache: dict[str, tuple[int, float]] = {}

    def _resolve_domain(self, domain: str, shard_id: int) -> tuple[int, float]:
        if domain in self._domain_cache:
            return self._domain_cache[domain]

        for attempt in range(3):
            try:
                with self.Session() as sess:
                    resolver = DomainResolver(sess)
                    with sess.begin():
                        domain_id, domain_score = resolver.ensure_and_get(domain, shard_id)
                self._domain_cache[domain] = (domain_id, domain_score)
                return domain_id, domain_score
            except (OperationalError, InterfaceError) as e:
                if attempt == 2:
                    raise
                try:
                    self.engine.dispose()
                except Exception:
                    pass
                time.sleep(0.2 * (2 ** attempt))

    def _process_record(self, rec: dict) -> None:
        domain = rec.get("domain")
        status = rec.get("status")
        content = rec.get("content")
        outlinks = rec.get("outlinks", [])

        shard_id = self.sharder.domain_to_shard(domain)
        ingestor_id = self.sharder.shard_to_ingestor(shard_id)

        content_hash = None
        if status == "ok" and isinstance(content, str):
            content_hash = sha1_hex(content)

        try:
            domain_id, _ = self._resolve_domain(domain, shard_id)
        except Exception as e:
            print(f"[router {self.cfg.router_id:02d}] domain resolve error {domain}: {e}", flush=True)
            return

        new_outlinks = []
        for link in outlinks:
            processed = self._process_link(link)
            if processed:
                new_outlinks.append(processed)

        out = {
            "url": rec.get("url"),
            "status": status,
            "fetched_at": rec.get("fetched_at"),
            "fail_reason": rec.get("fail_reason"),
            # "content": content,
            "outlinks": new_outlinks,
            "shard_id": shard_id,
            "domain_id": domain_id,
            "content_hash": content_hash,
        }

        self.producer.send("ingest_input", ingestor_id, out)

    def _process_link(self, link: Dict[str, str]) -> Optional[Dict[str, Any]]:
        url = link.get("url")
        domain = link.get("domain")
        anchor = link.get("anchor")
        if not url:
            return None

        shard_id = self.sharder.domain_to_shard(domain)
        ingestor_id = self.sharder.shard_to_ingestor(shard_id)

        try:
            domain_id, domain_score = self._resolve_domain(domain, shard_id)

            self.producer.send("ingest_input", ingestor_id, {
                "url": url,
                "status": "new",
                "shard_id": shard_id,
                "domain_id": domain_id,
                "domain_score": domain_score,
            })

            return {"url": url, "domain_id": domain_id, "anchor": anchor}
        except Exception as e:
            print(f"[router {self.cfg.router_id:02d}] process link error {link}: {e}", flush=True)
            return None

    def run_forever(self) -> None:
        print(f"[router {self.cfg.router_id:02d}] started", flush=True)
        while True:
            messages = self.consumer.poll("crawl_result", self.cfg.router_id, max_messages=100)
            if not messages:
                time.sleep(self.cfg.poll_interval_sec)
                continue

            for rec in messages:
                self._process_record(rec)


def load_router_config(path: str, router_id: int) -> RouterConfig:
    raw = load_yaml(path)
    r = require(raw, "router")
    pg = require(raw, "postgres")

    return RouterConfig(
        router_id=router_id,
        num_shards=int(require(r, "num_shards")),
        shards_per_ingestor=int(require(r, "shards_per_ingestor")),
        interval_minutes=int(r.get("interval_minutes", 10)),
        poll_interval_sec=int(r.get("poll_interval_sec", 5)),
        domain_overrides=r.get("domain_overrides", {}) or {},
        postgres_dsn=str(require(pg, "dsn")),
    )
