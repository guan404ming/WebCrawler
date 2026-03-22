# WebCrawler

A distributed web crawler built on Docker Compose with PostgreSQL, Scrapy, and a multi-stage ingest pipeline.

## Architecture Overview

```
[offerer x16] ──► url_queue/ ──► [crawler/scrapy x16] ──► crawl_result/
     ▲                                                           │
     │ (should_crawl=TRUE)                                       ▼
  [DB: url_state_current]          [router x16] ──► ingestor_result/
                                         │                      │
                                         ▼                      ▼
                                   [ingestor x16]   [feature_extractor x16]
                                         │
                                         ▼
                                   [stats_aggregator x1]
```

Four Docker services communicate via a shared IPC directory (`./ipc`):

| Service | Processes | Role |
|---|---|---|
| `postgres` | — | Persistent storage for all URL/domain/content state |
| `scheduler_control` | 16 offerers + 1 accounting rolloff | Reads DB, fills crawler queues |
| `crawler` | 16 Scrapy spiders | Fetches URLs, writes JSONL results |
| `scheduler_ingest` | 16 routers + 16 ingestors + 16 extractors + 1 stats aggregator | Processes results, writes back to DB |

See `docs/` for detailed architecture documentation.

---

## Prerequisites

- Docker Desktop (Mac: ensure File Sharing includes the project directory)
- Python 3.12+ (for running helper scripts on the host)

---

## Setup & Deployment

### 1. Configure IPC and Postgres paths

The default `docker-compose.yml` mounts local directories to avoid Docker File Sharing issues on Mac:

```yaml
volumes:
  - ./ipc:/data/ipc        # shared IPC directory
  - ./data/postgres:/var/lib/postgresql/data
```

Both `./ipc` and `./data/postgres` are created automatically on first run.

> On Linux production servers, these can be changed to absolute paths (e.g. `/data/ipc`) for better I/O performance.

### 2. Configure the PostgreSQL DSN

Both config files hardcode the DB host IP. Update it to match your environment:

**`containers/scheduler_control/config/control.yaml`**
**`containers/scheduler_ingest/config/ingest.yaml`**

```yaml
postgres:
  dsn: "postgresql+psycopg2://crawler:crawler@<HOST>:5432/crawlerdb"
```

| Environment | Value for `<HOST>` |
|---|---|
| Mac (Docker Desktop, host network) | `host.docker.internal` |
| Compose internal service name | `postgres` |
| Linux with fixed IP | e.g. `172.16.191.1` |

> The postgres service is exposed on host port `5433` (mapped from container `5432`).

### 3. Initialize the database schema

The application does **not** auto-migrate. You must create all tables before starting.

**`docker compose up` only starts Postgres with an empty `crawlerdb`; it does not create tables.** Until you run the steps below, helpers such as `seed_db.py` will fail with “relation `domain_state` does not exist”.

Shortcut (same effect as the inline script):

```bash
docker exec -w /app scheduler_control python init_schema.py
```

Run the following inside the `scheduler_control` container after `docker compose up` if you prefer a one-liner:

```bash
docker exec -it scheduler_control python -c "
import sys
sys.path.insert(0, '/app')
from libs.db.base import Base
from libs.db import (
    DomainState, DomainStatsDaily, SummaryDaily,
    url_state_current_table, url_state_history_table,
    url_event_counter_table, content_feature_current_table,
    content_feature_history_table,
)
from sqlalchemy import create_engine

DSN = 'postgresql+psycopg2://crawler:crawler@<HOST>:5432/crawlerdb'
engine = create_engine(DSN)

# Register all 256-shard table classes before create_all
for i in range(256):
    url_state_current_table(i)
    url_state_history_table(i)
    url_event_counter_table(i)
    content_feature_current_table(i)
    content_feature_history_table(i)

Base.metadata.create_all(engine)
print('Schema created.')
"
```

This creates:
- 3 non-sharded tables (`domain_state`, `domain_stats_daily`, `summary_daily`)
- 5 × 256 = **1280 sharded tables** across all shard families

### 4. Start the stack

```bash
docker compose up -d
```

Check all services are running:

```bash
docker compose ps
```

### 5. Seed initial URLs

The scheduler (`offerer`) pulls URLs from the database. On a fresh deployment the database is empty, so the crawler has nothing to fetch.

**Recommended: `seed_db.py`** — writes fake crawl-result records into the router's input directories. The router computes the correct shard via `MD5(domain) % 256`, inserts `domain_state` entries, and passes `status="new"` records to the ingestor, which inserts each URL into `url_state_current_{shard}` with `should_crawl=TRUE`. The offerer then schedules them for actual crawling.

```bash
# Requires tldextract (already installed in the containers)
docker exec -w /app scheduler_ingest python seed_db.py
```

The router will pick up the records within `interval_minutes * 2` (default: 20 minutes). Monitor progress with:

```bash
docker logs scheduler_ingest --tail 50 -f
```

After the seed cycle, verify URLs are in the database:

```bash
docker exec -it postgres psql -U crawler -d crawlerdb \
  -c "SELECT COUNT(*) FROM domain_state;"
```

---

> **Alternative: `seed_queue.py`** — injects URLs directly into the crawler queue files, bypassing the DB entirely. Useful for a quick smoke-test when the DB schema is not yet set up. The URLs are crawled immediately; their outlinks will populate the DB through the normal pipeline.
>
> ```bash
> python seed_queue.py   # runs on the host, no dependencies
> ```

---

## Monitoring

### Check container logs

```bash
docker logs crawler --tail 50 -f
docker logs scheduler_control --tail 50 -f
docker logs scheduler_ingest --tail 50 -f
```

### Watch live crawl output

```bash
ls ipc/crawl_result/crawler_00/
```

### Check for pipeline errors

```bash
ls ipc/stats/bad/
cat ipc/stats/bad/<filename>.json
```

Files in `ipc/stats/bad/` indicate stats aggregation failures or offerer DB errors. Common cause on a fresh deployment: the DB schema has not been initialized yet (see Step 3).

### Verify database is receiving data

```bash
docker exec -it postgres psql -U crawler -d crawlerdb \
  -c "SELECT COUNT(*) FROM domain_state;"
```

---

## Directory Structure

```
.
├── containers/
│   ├── crawler/            # Scrapy spider + supervisord config
│   ├── scheduler_control/  # Offerer + accounting rolloff + config
│   └── scheduler_ingest/   # Router, ingestor, extractor, stats + config
├── data/
│   └── postgres/           # Postgres data volume (auto-created)
├── docs/                   # Architecture documentation
├── ipc/                    # Shared IPC volume (auto-created)
│   ├── url_queue/          # Offerer → Crawler: batch URL files
│   ├── crawl_result/       # Crawler → Router: JSONL results
│   ├── progress/           # Per-worker progress checkpoints
│   └── stats/              # Stats delta files (bad/ = errors)
├── libs/                   # Shared Python libraries (db, config, ipc, stats)
├── seed_queue.py           # Helper: inject seed URLs into crawler queues
├── seed_urls.txt           # Seed URL list (one URL per line)
└── docker-compose.yml
```

---

## Configuration Reference

### `containers/scheduler_control/config/control.yaml`

| Key | Default | Description |
|---|---|---|
| `offerer.scan_interval_sec` | 300 | How often each offerer checks the queue depth |
| `offerer.low_watermark_batches` | 20 | Refill queues when below this many pending batches |
| `offerer.batch_size` | 512 | URLs per queue file |
| `offerer.per_shard_select_cap` | 4096 | Max URLs fetched per shard per refill cycle |
| `offerer.total_shards` | 256 | Must match DB schema shard count |
| `offerer.shards_per_offerer` | 16 | Shards assigned to each offerer process |
| `accounting.event_retention_days` | 90 | Days to retain URL event counters |
| `accounting.run_hour_utc` | 3 | UTC hour to run daily rolloff |

### `containers/scheduler_ingest/config/ingest.yaml`

| Key | Default | Description |
|---|---|---|
| `router.interval_minutes` | 10 | Time-bucket width for grouping crawl results |
| `router.num_shards` | 256 | Must match DB schema shard count |
| `router.shards_per_ingestor` | 16 | Shards routed to each ingestor |
| `router.domain_overrides` | see file | Force specific domains to a fixed shard |

---

## Sharding Notes

Shard assignment is deterministic and based on domain name:

```python
import hashlib
shard_id = int(hashlib.md5(domain.encode("utf-8")).hexdigest(), 16) % 256
```

**Do not manually insert rows** into `url_state_current_*` or `domain_state` with arbitrary shard values. Always let the router compute the shard, or use `seed_queue.py` to let the pipeline handle it correctly.
