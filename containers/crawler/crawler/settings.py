import json

BOT_NAME = "crawler"
LOG_LEVEL = "WARNING"

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter"

with open("domain_qps.json") as f:
    _DOMAIN_QPS = json.load(f)
_DEFAULT_QPS = _DOMAIN_QPS.pop("_default", {})

CONCURRENT_REQUESTS = 128
CONCURRENT_REQUESTS_PER_DOMAIN = _DEFAULT_QPS.get("concurrency", 4)
DOWNLOAD_DELAY = _DEFAULT_QPS.get("delay", 1.0)
DOWNLOAD_SLOTS = _DOMAIN_QPS
AUTOTHROTTLE_ENABLED = False

DNS_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 15
DOWNLOAD_MAXSIZE = 10 * 1024 * 1024
RETRY_ENABLED = True
RETRY_TIMES = 1

REDIRECT_ENABLED = True
COOKIES_ENABLED = False

EXTENSIONS = {
    "scrapy.extensions.logstats.LogStats": 500,
    "scrapy.extensions.memusage.MemoryUsage": 500,
}

ITEM_PIPELINES = {
    "crawler.pipelines.CrawlResultPipeline": 500,
}

# IPC config: override via environment or keep default filesystem
IPC_CONFIG = {"backend": "filesystem", "base_dir": "/data/ipc"}
