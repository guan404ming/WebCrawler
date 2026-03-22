"""
Broadcast seed URLs into ALL 256 shards for benchmarking.

Each URL is prefixed with a shard tag (e.g. "s042__https://...") so the same
real URL can exist in every shard without unique-constraint conflicts. This
gives every offerer a full workload regardless of domain hash distribution.

Reuses domain_state from the real domain (one entry per eTLD+1) and assigns
a single shared domain_id; only the shard placement differs.

Usage:
  uv run --group seed python seed_bench.py
  # or inside container:
  docker exec -w /app scheduler_control python seed_bench.py

Optional env (same as seed_db.py):
  SEED_FILE, SEED_CONFIG, SEED_POSTGRES_DSN
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import tldextract
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from host_dsn import adjust_postgres_dsn_for_host, print_pg_auth_failure_hint


def _default_config_path() -> Path:
    return Path(__file__).resolve().parent / "containers/scheduler_ingest/config/ingest.yaml"


SEED_FILE = Path(os.environ.get("SEED_FILE", str(Path(__file__).resolve().parent / "seed_urls.txt")))


def extract_domain(url: str) -> str:
    ext = tldextract.extract(url)
    return ".".join([p for p in [ext.domain, ext.suffix] if p])


def _tcur(shard_id: int) -> str:
    return f"url_state_current_{shard_id:03d}"


def _this(shard_id: int) -> str:
    return f"url_state_history_{shard_id:03d}"


def ensure_domain(sess: Session, domain: str, shard_id: int) -> tuple[int, float]:
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


def main() -> None:
    config_path = Path(os.environ["SEED_CONFIG"]) if os.environ.get("SEED_CONFIG") else _default_config_path()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    router = raw["router"]
    pg = raw["postgres"]
    num_shards = int(router["num_shards"])
    dsn = os.environ.get("SEED_POSTGRES_DSN") or str(pg["dsn"])
    if not os.environ.get("SEED_POSTGRES_DSN"):
        dsn, note = adjust_postgres_dsn_for_host(dsn)
        if note:
            print(note, file=sys.stderr)

    urls = [u.strip() for u in SEED_FILE.read_text(encoding="utf-8").splitlines() if u.strip()]
    print(f"Loaded {len(urls)} seed URLs from {SEED_FILE}")
    print(f"Broadcasting into all {num_shards} shards ({len(urls) * num_shards} total rows)")

    engine = create_engine(dsn, pool_pre_ping=True, future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    domain_cache: dict[str, tuple[int, float]] = {}
    total_inserted = 0

    with SessionLocal() as sess:
        # Ensure all domains first (use shard 0 as canonical; shard_id in
        # domain_state is informational, the real shard is per-URL here).
        with sess.begin():
            for url in urls:
                domain = extract_domain(url)
                if not domain or domain in domain_cache:
                    continue
                domain_cache[domain] = ensure_domain(sess, domain, 0)
        print(f"Ensured {len(domain_cache)} distinct domains in domain_state")

        for shard_id in range(num_shards):
            tcur = _tcur(shard_id)
            th = _this(shard_id)
            shard_new = 0
            with sess.begin():
                for url in urls:
                    domain = extract_domain(url)
                    if not domain:
                        continue
                    domain_id, domain_score = domain_cache[domain]
                    tagged = f"s{shard_id:03d}__{url}"
                    inserted = sess.execute(
                        text(f"""
                            INSERT INTO {tcur} (url, domain_id, domain_score, should_crawl)
                            VALUES (:url, :domain_id, :domain_score, TRUE)
                            ON CONFLICT (url) DO NOTHING
                            RETURNING url;
                        """),
                        {"url": tagged, "domain_id": domain_id, "domain_score": domain_score},
                    ).scalar_one_or_none()
                    if inserted is not None:
                        sess.execute(
                            text(f"""
                                INSERT INTO {th} (url, domain_id, domain_score)
                                VALUES (:url, :domain_id, :domain_score)
                            """),
                            {"url": tagged, "domain_id": domain_id, "domain_score": domain_score},
                        )
                        shard_new += 1

            total_inserted += shard_new
            if (shard_id + 1) % 32 == 0 or shard_id == num_shards - 1:
                print(f"  shards 0..{shard_id}: {total_inserted} rows inserted so far")

    print(f"\nDone. Total inserted: {total_inserted} (expected ~{len(urls) * num_shards})")


if __name__ == "__main__":
    main()
