"""
Seed sharded url_state tables directly in Postgres (no router / ingestor IPC).

For each line in seed_urls.txt: derive eTLD+1 like the crawler, compute shard_id
using the same rules as the router (ingest.yaml domain_overrides + md5(domain) %
num_shards), ensure domain_state, then INSERT into url_state_current_{shard} and
url_state_history_{shard} exactly like IngestDB.process_link.

Requirements:
  uv sync --group seed && uv run --group seed python seed_db.py
  (or: pip install tldextract pyyaml sqlalchemy psycopg2-binary — versions in pyproject.toml)

Run (example):
  docker exec -w /app scheduler_ingest python seed_db.py

Optional env:
  SEED_FILE          path to URL list (default /app/seed_urls.txt)
  SEED_CONFIG        path to ingest.yaml (default: repo path below)
  SEED_POSTGRES_DSN  overrides postgres.dsn from YAML; on host use port 5433, e.g.
                       postgresql+psycopg2://crawler:crawler@127.0.0.1:5433/crawlerdb
"""

from __future__ import annotations

import hashlib
import os
import sys
from collections import Counter
from pathlib import Path

import tldextract
import yaml
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from host_dsn import adjust_postgres_dsn_for_host, print_pg_auth_failure_hint


def _require_domain_state_table(engine) -> None:
    """Fail fast with setup hints if README schema init was never run."""
    if inspect(engine).has_table("domain_state"):
        return
    print(
        "ERROR: PostgreSQL has no `domain_state` table (schema not initialized).\n"
        "\n"
        "`docker compose up` only starts containers; it does NOT create tables.\n"
        "Run a one-time schema init, then seed again:\n"
        "  docker exec -w /app scheduler_control python init_schema.py\n"
        "\n"
        "From the host (Postgres published on port 5433):\n"
        "  INIT_SCHEMA_DSN=postgresql+psycopg2://crawler:crawler@127.0.0.1:5433/crawlerdb \\\n"
        "    uv run --group seed python init_schema.py\n"
        "\n"
        "Details: README.md section \"Initialize the database schema\".\n"
        "\n"
        "For seed_db.py on the host you can omit SEED_POSTGRES_DSN (we rewrite postgres -> 127.0.0.1:5433),\n"
        "or set it explicitly:\n"
        "  SEED_POSTGRES_DSN=postgresql+psycopg2://crawler:crawler@127.0.0.1:5433/crawlerdb",
        file=sys.stderr,
    )
    raise SystemExit(2)


def _default_config_path() -> Path:
    return Path(__file__).resolve().parent / "containers/scheduler_ingest/config/ingest.yaml"


SEED_FILE = Path(os.environ.get("SEED_FILE", "/app/seed_urls.txt"))


def extract_domain(url: str) -> str:
    """Extract eTLD+1 the same way the Scrapy spider does."""
    ext = tldextract.extract(url)
    return ".".join([p for p in [ext.domain, ext.suffix] if p])


def domain_to_shard(domain: str, num_shards: int, overrides: dict[str, int]) -> int:
    """Match ShardRouter.domain_to_shard (containers/scheduler_ingest/router/routing.py)."""
    d = domain or "unknown"
    if d in overrides:
        return int(overrides[d])
    h = hashlib.md5(d.encode("utf-8")).hexdigest()
    return int(h, 16) % num_shards


def seed_shard_stats(
    urls: list[str], num_shards: int, overrides: dict[str, int]
) -> tuple[int, int, int, Counter[int], Counter[int]]:
    """
    From the seed list only: empty-domain skips, valid URL count, distinct domain count,
    URLs per shard, and distinct domains per shard.
    """
    urls_per_shard: Counter[int] = Counter()
    domain_shard: dict[str, int] = {}
    skip_empty = 0
    for url in urls:
        domain = extract_domain(url)
        if not domain:
            skip_empty += 1
            continue
        shard_id = domain_to_shard(domain, num_shards, overrides)
        urls_per_shard[shard_id] += 1
        domain_shard[domain] = shard_id
    domains_per_shard = Counter(domain_shard.values())
    valid_urls = sum(urls_per_shard.values())
    distinct_domains = len(domain_shard)
    return skip_empty, valid_urls, distinct_domains, urls_per_shard, domains_per_shard


def print_shard_distribution(
    num_shards: int,
    distinct_domains: int,
    urls_per_shard: Counter[int],
    domains_per_shard: Counter[int],
) -> None:
    used = sorted(urls_per_shard.keys())
    print(f"\nDistinct domains (seed file, non-empty eTLD+1): {distinct_domains}")
    print(f"Shards with ≥1 URL: {len(used)} / {num_shards}")
    print(f"{'shard':>5}  {'urls':>6}  {'domains':>8}")
    print("-" * 24)
    for sid in used:
        print(f"{sid:5d}  {urls_per_shard[sid]:6d}  {domains_per_shard[sid]:8d}")
    empty = num_shards - len(used)
    if empty:
        print(f"(shards with 0 seed URLs: {empty})")


def _tcur(shard_id: int) -> str:
    return f"url_state_current_{shard_id:03d}"


def _this(shard_id: int) -> str:
    return f"url_state_history_{shard_id:03d}"


def ensure_domain(sess: Session, domain: str, shard_id: int) -> tuple[int, float]:
    """Match DomainResolver.ensure_and_get."""
    sess.execute(
        text("""
            INSERT INTO domain_state(domain, shard_id)
            VALUES (:domain, :shard_id)
            ON CONFLICT (domain) DO NOTHING
        """),
        {"domain": domain, "shard_id": shard_id},
    )
    row = sess.execute(
        text("""
            SELECT domain_id, COALESCE(domain_score, 0.0) AS domain_score
            FROM domain_state
            WHERE domain = :domain
        """),
        {"domain": domain},
    ).first()
    if row is None:
        raise RuntimeError(f"domain_state insert/select failed for domain={domain}")
    return int(row.domain_id), float(row.domain_score)


def insert_new_link(sess: Session, url: str, shard_id: int, domain_id: int, domain_score: float) -> bool:
    """Match IngestDB.process_link inserts; returns True if url was newly inserted."""
    tcur = _tcur(shard_id)
    th = _this(shard_id)
    inserted_url = sess.execute(
        text(f"""
            INSERT INTO {tcur} (url, domain_id, domain_score)
            VALUES (:url, :domain_id, :domain_score)
            ON CONFLICT (url) DO NOTHING
            RETURNING url;
        """),
        {"url": url, "domain_id": domain_id, "domain_score": domain_score},
    ).scalar_one_or_none()
    if inserted_url is None:
        return False
    sess.execute(
        text(f"""
            INSERT INTO {th} (url, domain_id, domain_score)
            VALUES (:url, :domain_id, :domain_score)
            RETURNING 1;
        """),
        {"url": url, "domain_id": domain_id, "domain_score": domain_score},
    )
    return True


def main() -> None:
    config_path = Path(os.environ["SEED_CONFIG"]) if os.environ.get("SEED_CONFIG") else _default_config_path()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    router = raw["router"]
    pg = raw["postgres"]
    num_shards = int(router["num_shards"])
    overrides_raw = router.get("domain_overrides") or {}
    overrides: dict[str, int] = {str(k): int(v) for k, v in overrides_raw.items()}
    dsn = os.environ.get("SEED_POSTGRES_DSN") or str(pg["dsn"])
    if not os.environ.get("SEED_POSTGRES_DSN"):
        dsn, note = adjust_postgres_dsn_for_host(dsn)
        if note:
            print(note, file=sys.stderr)

    urls = [u.strip() for u in SEED_FILE.read_text(encoding="utf-8").splitlines() if u.strip()]
    print(f"Loaded {len(urls)} seed URLs from {SEED_FILE}")
    print(f"Config: {config_path} (num_shards={num_shards})")

    pre_skip, valid_n, distinct_domains, urls_per_shard, domains_per_shard = seed_shard_stats(
        urls, num_shards, overrides
    )
    if pre_skip:
        print(f"(preview) lines with empty domain (excluded from shard stats): {pre_skip}")
    print(f"(preview) non-empty URL lines: {valid_n}")
    print_shard_distribution(num_shards, distinct_domains, urls_per_shard, domains_per_shard)

    engine = create_engine(dsn, pool_pre_ping=True, future=True)
    try:
        _require_domain_state_table(engine)
    except OperationalError as e:
        print_pg_auth_failure_hint(dsn, e)
        raise SystemExit(2) from e
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    domain_cache: dict[str, tuple[int, float]] = {}
    new_count = 0
    dup_count = 0
    skip_count = 0

    with SessionLocal() as sess:
        with sess.begin():
            for url in urls:
                domain = extract_domain(url)
                if not domain:
                    print(f"  skip (no domain): {url[:80]}...")
                    skip_count += 1
                    continue
                shard_id = domain_to_shard(domain, num_shards, overrides)
                if domain not in domain_cache:
                    domain_cache[domain] = ensure_domain(sess, domain, shard_id)
                domain_id, domain_score = domain_cache[domain]

                if insert_new_link(sess, url, shard_id, domain_id, domain_score):
                    new_count += 1
                else:
                    dup_count += 1

    print(f"\nDone. inserted={new_count}, already_present={dup_count}, skipped={skip_count}")


if __name__ == "__main__":
    main()
