from __future__ import annotations

import time

from libs.ipc.bus import MessageConsumer
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import FeatureDB
from .extract_basic import extract_basic


class ExtractService:
    def __init__(self, extractor_id: int, db: FeatureDB, consumer: MessageConsumer, stats: StatsDeltaWriter):
        self.extractor_id = extractor_id
        self.db = db
        self.consumer = consumer
        self.stats = stats

    def run_forever(self) -> None:
        print(f"[extractor {self.extractor_id:02d}] started", flush=True)
        while True:
            messages = self.consumer.poll("ingest_input", self.extractor_id, max_messages=100)
            if not messages:
                time.sleep(2)
                continue

            processed = 0
            error = 0

            for rec in messages:
                try:
                    if rec.get("status") == "ok":
                        feat = extract_basic(rec)
                        self.db.process(feat)
                        processed += 1
                except Exception as e:
                    print(f"[extractor {self.extractor_id:02d}] ERROR: {e}", flush=True)
                    error += 1

            if error:
                self.stats.write(
                    source="extractor",
                    counters={"error_count": error, "extract_error": error},
                )
            if processed > 0:
                print(f"[extractor {self.extractor_id:02d}] processed {processed} features", flush=True)
