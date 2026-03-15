from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass

from libs.ipc.bus import MessageProducer
from libs.stats.delta_writer import StatsDeltaWriter, now_iso
from .selection.base import SelectionStrategy
from .batching import round_robin_mix, chunk


@dataclass(frozen=True)
class OffererDerivation:
    total_shards: int
    shards_per_offerer: int

    def shard_range(self, offerer_id: int) -> tuple[int, int]:
        start = offerer_id * self.shards_per_offerer
        end = start + self.shards_per_offerer - 1
        if start < 0 or end >= self.total_shards:
            raise ValueError(f"Offerer {offerer_id} shard range out of bounds: {start}-{end}")
        return start, end


@dataclass(frozen=True)
class OffererConfig:
    offerer_id: int

    scan_interval_sec: int
    low_watermark_batches: int
    batch_size: int
    per_shard_select_cap: int

    stats_dir: str


class OffererService:
    def __init__(
        self,
        cfg: OffererConfig,
        deriv: OffererDerivation,
        selector: SelectionStrategy,
        producer: MessageProducer,
    ):
        self.cfg = cfg
        self.deriv = deriv
        self.selector = selector
        self.producer = producer
        self.stats = StatsDeltaWriter(stats_dir=cfg.stats_dir)

    def _refill_once_if_needed(self) -> dict:
        offerer_id = self.cfg.offerer_id
        partition = offerer_id

        shard_start, shard_end = self.deriv.shard_range(offerer_id)
        shard_ids = list(range(shard_start, shard_end + 1))

        per_shard_urls = defaultdict(list)
        total_picked = 0
        domain_counter = defaultdict(int)

        for sid in shard_ids:
            picked = self.selector.select_and_update(sid, self.cfg.per_shard_select_cap)
            total_picked += len(picked)
            for url, domain_id in picked:
                per_shard_urls[sid].append(url)
                domain_counter[domain_id] += 1

        if total_picked == 0:
            return {"action": "refill_empty", "picked_urls": 0}

        mixed = round_robin_mix(per_shard_urls)
        parts = chunk(mixed, self.cfg.batch_size)

        written = 0
        for part in parts:
            self.producer.send("url_queue", partition, {"generated_at": now_iso(), "urls": part})
            written += 1

        self.stats.write(
            source="offerer",
            counters={"num_scheduled": total_picked},
            domains={
                int(domain_id): {"num_scheduled": cnt}
                for domain_id, cnt in domain_counter.items()
            }
        )

        return {
            "action": "refill",
            "picked_urls": total_picked,
            "written_batches": written,
            "shards": {"start": shard_start, "end": shard_end},
        }

    def run_forever(self) -> None:
        while True:
            try:
                res = self._refill_once_if_needed()
                print(f"[offerer {self.cfg.offerer_id:02d}] {res}", flush=True)
            except Exception as e:
                print(f"[offerer {self.cfg.offerer_id:02d}] ERROR: {e}", flush=True)
                self.stats.write(
                    source="offerer",
                    counters={"offer_error": 1, "error_count": 1},
                )
            time.sleep(self.cfg.scan_interval_sec)
