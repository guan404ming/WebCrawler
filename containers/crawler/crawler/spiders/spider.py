from __future__ import annotations

import tldextract
from w3lib.url import canonicalize_url
from datetime import datetime

import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider, IgnoreRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError

from crawler.items import PageItem
from crawler.queue_consumer import QueueConsumer

ACCEPTED_CONTENT_TYPES = ["text/html", "application/xhtml+xml"]


def split_bench_url(url: str) -> tuple[str, str]:
    """
    Convert benchmark-tagged URLs like s042__https://example.com into:
      - source_url: original tagged URL for downstream DB matching
      - fetch_url: actual URL Scrapy should request
    """
    if "__http://" in url or "__https://" in url:
        _, fetch_url = url.split("__", 1)
        return url, fetch_url
    return url, url


class HtmlSpider(scrapy.Spider):
    name = "html_spider"

    def __init__(self, crawler_id: int = 0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crawler_id = int(crawler_id)
        self._inflight = 0
        self._max_inflight = 0
        self._max_transferring = 0
        self._max_slot_queue = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)

        qtpl = crawler.settings["URL_QUEUE_TEMPLATE"]
        spider.queue = QueueConsumer(queue_dir=qtpl.format(id=spider.crawler_id))
        spider.link_extractor = LinkExtractor(canonicalize=True)

        crawler.signals.connect(spider.on_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.req_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(spider.req_start, signal=signals.request_reached_downloader)
        crawler.signals.connect(spider.req_end, signal=signals.request_left_downloader)

        return spider

    def _set_inflight_stats(self):
        stats = getattr(self.crawler, "stats", None)
        if stats is None:
            return
        stats.set_value("inflight/current", self._inflight, spider=self)
        stats.set_value("inflight/max", self._max_inflight, spider=self)
        runtime = self._downloader_runtime()
        stats.set_value("transferring/current", runtime["transferring"], spider=self)
        stats.set_value("transferring/max", self._max_transferring, spider=self)
        stats.set_value("slot_queue/current", runtime["slot_queue"], spider=self)
        stats.set_value("slot_queue/max", self._max_slot_queue, spider=self)

    def _downloader_runtime(self) -> dict[str, int]:
        downloader = getattr(getattr(self.crawler, "engine", None), "downloader", None)
        slots = getattr(downloader, "slots", {}) or {}

        transferring = 0
        slot_queue = 0
        slot_active = 0

        for slot in slots.values():
            transferring += len(getattr(slot, "transferring", ()) or ())
            slot_queue += len(getattr(slot, "queue", ()) or ())
            slot_active += len(getattr(slot, "active", ()) or ())

        self._max_transferring = max(self._max_transferring, transferring)
        self._max_slot_queue = max(self._max_slot_queue, slot_queue)

        return {
            "transferring": transferring,
            "slot_queue": slot_queue,
            "slot_active": slot_active,
            "slots": len(slots),
        }

    def _runtime_suffix(self) -> str:
        runtime = self._downloader_runtime()
        return (
            f"inflight={self._inflight}, inflight_max={self._max_inflight}, "
            f"transferring={runtime['transferring']}, transferring_max={self._max_transferring}, "
            f"slot_queue={runtime['slot_queue']}, slot_queue_max={self._max_slot_queue}, "
            f"slot_active={runtime['slot_active']}, slots={runtime['slots']}"
        )

    def _log(self, message: str):
        print(f"[crawler-{self.crawler_id:02d}] {message}, {self._runtime_suffix()}", flush=True)

    def spider_opened(self, spider=None):
        self._set_inflight_stats()

    async def start(self):
        urls = self.queue.pop_batch()
        t = datetime.now()
        self._log(f"Get {len(urls)} new requests, time={t}")

        for u in urls:
            source_url, fetch_url = split_bench_url(u)
            yield scrapy.Request(
                url=fetch_url,
                callback=self.parse,
                errback=self.errback,
                meta={"source_url": source_url},
            )

    def on_idle(self):
        urls = self.queue.pop_batch()
        t = datetime.now()
        self._log(f"Get {len(urls)} new requests, time={t}")

        for u in urls:
            source_url, fetch_url = split_bench_url(u)
            self.crawler.engine.crawl(
                scrapy.Request(
                    url=fetch_url,
                    callback=self.parse,
                    errback=self.errback,
                    meta={"source_url": source_url},
                )
            )

        raise DontCloseSpider

    def _extract_domain(self, url):
        _, fetch_url = split_bench_url(url)
        extracted = tldextract.extract(fetch_url)
        domain = ".".join([p for p in [extracted.domain, extracted.suffix] if p])
        return domain


    def parse(self, response):
        source_url = response.meta.get("source_url", response.url)
        fetched_url = canonicalize_url(response.url)
        domain = self._extract_domain(fetched_url)

        ctype = response.headers.get("Content-Type", b"").decode().lower()
        if not any(t in ctype for t in ACCEPTED_CONTENT_TYPES):
            yield PageItem(
                url=source_url,
                domain=domain,
                fail_reason="NonHTML content-type",
                content=None,
                outlinks=[],
            )
            return

        outlinks = []

        for link in self.link_extractor.extract_links(response):
            if not link.nofollow:
                u = canonicalize_url(link.url)
                outlinks.append({
                    "url": u,
                    "domain": self._extract_domain(u),
                    "anchor": (link.text or "").strip()[:200]
                })

        yield PageItem(
            url=source_url,
            domain=domain,
            fail_reason=None,
            content=response.text,
            outlinks=outlinks,
        )

    def errback(self, failure):
        source_url = failure.request.meta.get("source_url", failure.request.url)
        fetched_url = canonicalize_url(failure.request.url)
        domain = self._extract_domain(fetched_url)

        item = PageItem(
            url=source_url,
            domain=domain,
            fail_reason=failure.type.__name__,
            content=None,
            outlinks=[],
        )

        if failure.check(HttpError):
            item["fail_reason"] = f"HttpError {failure.value.response.status}"
        elif failure.check(IgnoreRequest):
            item["fail_reason"] = f"IgnoreRequest {failure.getErrorMessage()}"
            if "exceeded DOWNLOAD_MAXSIZE" in item["fail_reason"]:
                item["fail_reason"] = f"IgnoreRequest exceeded DOWNLOAD_MAXSIZE"

        yield item

    def req_scheduled(self, request, spider=None):
        t = datetime.now()
        self._log(f"Request scheduled: time={t}, url={request.meta.get('source_url', request.url)}")

    def req_start(self, request, spider=None):
        t = datetime.now()
        self._inflight += 1
        self._max_inflight = max(self._max_inflight, self._inflight)
        self._set_inflight_stats()
        request.meta["t_down_start"] = t
        self._log(f"Download started: time={t}, url={request.meta.get('source_url', request.url)}")

    def req_end(self, request, spider=None):
        t = datetime.now()
        self._inflight = max(0, self._inflight - 1)
        self._set_inflight_stats()
        self._log(
            f"Download ended: time={t}, "
            f"url={request.meta.get('source_url', request.url)}, latency={t - request.meta.get('t_down_start', t)}"
        )
