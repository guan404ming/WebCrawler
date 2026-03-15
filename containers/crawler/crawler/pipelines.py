from __future__ import annotations

import time
from datetime import datetime, timezone

from libs.ipc.bus import create_producer


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class CrawlResultPipeline:
    """
    Sends crawl results to the IPC bus.
    Backend (filesystem or redis) is determined by config.
    """

    def __init__(self, ipc_config: dict):
        self.ipc_config = ipc_config
        self.producer = None
        self.crawler_id = None

    @classmethod
    def from_crawler(cls, crawler):
        ipc_config = crawler.settings.get("IPC_CONFIG", {})
        return cls(ipc_config)

    def open_spider(self, spider):
        self.crawler_id = spider.crawler_id
        self.producer = create_producer(self.ipc_config)

    def process_item(self, item, spider):
        content = item.get("content")
        rec = {
            "url": item.get("url"),
            "domain": item.get("domain"),
            "fetched_at": _now_iso(),
            "status": "ok" if content else "fail",
            "fail_reason": item.get("fail_reason"),
            "content_length": len(content) if content else 0,
            "outlinks": item.get("outlinks", []),
        }

        for attempt in range(5):
            try:
                self.producer.send("crawl_result", self.crawler_id, rec)
                return item
            except Exception:
                if attempt == 4:
                    raise
                self.producer = create_producer(self.ipc_config)
                time.sleep(0.1 * (2 ** attempt))

        return item
