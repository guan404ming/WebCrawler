"""
Periodic throughput stats extension for benchmarking.

Every THROUGHPUT_INTERVAL seconds (default 30), writes a JSON line to
  ipc/stats/crawler_{id}_throughput.jsonl
with request/response/item counts and average download latency.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from scrapy import signals
from scrapy.exceptions import NotConfigured


class ThroughputExtension:

    def __init__(self, stats_dir: str, crawler_id: int, interval: float):
        self.stats_dir = Path(stats_dir)
        self.crawler_id = crawler_id
        self.interval = interval
        self._reset_window()
        self._last_flush = time.monotonic()

    def _reset_window(self):
        self._req_started = 0
        self._resp_received = 0
        self._items_ok = 0
        self._items_fail = 0
        self._download_latency_sum = 0.0
        self._download_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        stats_dir = crawler.settings.get("RESULT_DIR_TEMPLATE", "/app/ipc/crawl_result/crawler_{id:02d}")
        stats_dir = "/app/ipc/stats"
        interval = float(crawler.settings.getfloat("THROUGHPUT_INTERVAL", 30))
        spider_args = dict(crawler.spidercls.__dict__.get("__init__defaults__", {}))
        crawler_id = int(getattr(crawler, "_crawler_id", 0))
        ext = cls(stats_dir, crawler_id, interval)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.request_reached_downloader, signal=signals.request_reached_downloader)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        self.crawler_id = int(getattr(spider, "crawler_id", 0))
        self._last_flush = time.monotonic()
        self._reset_window()

    def request_reached_downloader(self, request, spider):
        self._req_started += 1
        request.meta["_tp_t0"] = time.monotonic()

    def response_received(self, response, request, spider):
        self._resp_received += 1
        t0 = request.meta.get("_tp_t0")
        if t0 is not None:
            self._download_latency_sum += time.monotonic() - t0
            self._download_count += 1
        self._maybe_flush()

    def item_scraped(self, item, response, spider):
        if item.get("fail_reason"):
            self._items_fail += 1
        else:
            self._items_ok += 1

    def _maybe_flush(self):
        now = time.monotonic()
        if now - self._last_flush < self.interval:
            return
        self._flush(now)

    def _flush(self, now: float):
        elapsed = now - self._last_flush
        avg_lat = (self._download_latency_sum / self._download_count
                    if self._download_count > 0 else 0.0)
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "crawler_id": self.crawler_id,
            "window_sec": round(elapsed, 1),
            "req_started": self._req_started,
            "resp_received": self._resp_received,
            "items_ok": self._items_ok,
            "items_fail": self._items_fail,
            "avg_download_latency_ms": round(avg_lat * 1000, 1),
        }
        out_dir = self.stats_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"crawler_{self.crawler_id:02d}_throughput.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")

        self._reset_window()
        self._last_flush = now

    def spider_closed(self, spider, reason):
        self._flush(time.monotonic())
