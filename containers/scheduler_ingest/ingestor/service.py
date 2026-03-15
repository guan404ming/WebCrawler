from __future__ import annotations

import time
from collections import defaultdict

from libs.ipc.bus import MessageConsumer
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import IngestDB


class IngestService:
    def __init__(self, ingestor_id: int, db: IngestDB, consumer: MessageConsumer, stats: StatsDeltaWriter):
        self.ingestor_id = ingestor_id
        self.db = db
        self.consumer = consumer
        self.stats = stats

    def run_forever(self) -> None:
        print(f"[ingestor {self.ingestor_id:02d}] started", flush=True)
        while True:
            messages = self.consumer.poll("ingest_input", self.ingestor_id, max_messages=100)
            if not messages:
                time.sleep(2)
                continue

            counters = defaultdict(int)
            domains = {}

            for rec in messages:
                try:
                    if rec.get("status") == "new":
                        if self.db.process_link(rec):
                            counters["new_links"] += 1
                        continue

                    result = self.db.process_result(rec)
                    if not result:
                        continue

                    domains.setdefault(result.domain_id, defaultdict(int))

                    if result.new_link:
                        counters["new_links"] += 1
                    if result.is_ok:
                        counters["num_fetch_ok"] += 1
                        domains[result.domain_id]["num_fetch_ok"] += 1
                    else:
                        counters["num_fetch_fail"] += 1
                        domains[result.domain_id]["num_fetch_fail"] += 1
                    if result.is_upd:
                        counters["num_content_update"] += 1
                        domains[result.domain_id]["num_content_update"] += 1
                    if result.fail_reason:
                        counters.setdefault("fail_reasons", defaultdict(int))[result.fail_reason] += 1
                        domains[result.domain_id].setdefault("fail_reasons", defaultdict(int))[result.fail_reason] += 1
                except Exception as e:
                    print(f"[ingestor {self.ingestor_id:02d}] ERROR: {e}", flush=True)
                    counters["error_count"] += 1
                    counters["ingest_error"] += 1

            domains.pop(None, None)
            self.stats.write(source="ingestor", counters=counters, domains=domains)
            print(f"[ingestor {self.ingestor_id:02d}] processed {len(messages)} records", flush=True)
