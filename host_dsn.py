"""
Rewrite compose-style postgres DSN when scripts run on the host (not in Docker).

ingest.yaml uses host `postgres`, which only resolves on the Compose network.
This repo maps container 5432 -> host 5433 (see docker-compose.yml).
"""

from __future__ import annotations

import sys
from pathlib import Path


def adjust_postgres_dsn_for_host(dsn: str) -> tuple[str, str | None]:
    """
    If not inside a container and DSN host is `postgres`, return a DSN aimed at
    localhost:5433. Otherwise return dsn unchanged.

    Returns (effective_dsn, optional short note for stdout/stderr).
    """
    if Path("/.dockerenv").exists():
        return dsn, None

    from sqlalchemy.engine.url import make_url

    u = make_url(dsn)
    if u.host != "postgres":
        return dsn, None

    port = u.port
    new_port = 5433 if port in (None, 5432) else port
    new_u = u.set(host="127.0.0.1", port=new_port)
    note = (
        f"Host run: using {new_u.host}:{new_u.port} instead of postgres:{port or 5432} "
        "(see docker-compose.yml port mapping)."
    )
    # str(URL) masks the password as "***", which would be sent literally to psycopg2.
    return new_u.render_as_string(hide_password=False), note


def print_pg_auth_failure_hint(dsn: str, err: BaseException) -> None:
    """If err looks like a Postgres auth failure, explain credential mismatch to stderr."""
    msg = str(getattr(err, "orig", err))
    if "password authentication failed" not in msg and "authentication failed" not in msg.lower():
        return
    from sqlalchemy.engine.url import make_url

    try:
        u = make_url(dsn)
        user = u.username or "?"
        host = u.host or "?"
        port = u.port or 5432
        db = u.database or "?"
        masked = f"{u.drivername}://{user}:***@{host}:{port}/{db}"
    except Exception:
        masked = "(could not parse DSN)"
        u = None
    print(
        "\nPostgreSQL accepted the TCP connection but rejected this login.\n"
        f"  DSN (password masked as ***): {masked}\n",
        file=sys.stderr,
    )
    if u is not None and u.password is None:
        print(
            "  Parsed URL has no password. Check for typos in the DSN or URL-encode special\n"
            "  characters in the password.\n",
            file=sys.stderr,
        )
    print(
        "Scripts use the same defaults as docker-compose.yml: user `crawler`, password\n"
        "`crawler`, database `crawlerdb`. DataGrip must use the same pair, or set:\n"
        "  INIT_SCHEMA_DSN=postgresql+psycopg2://USER:PASS@127.0.0.1:5433/crawlerdb\n"
        "\n"
        "Quick check from the host (uses password `crawler`):\n"
        "  PGPASSWORD=crawler psql -h 127.0.0.1 -p 5433 -U crawler -d crawlerdb -c 'select 1'\n"
        "If that fails, the DB was initialized with different credentials or another\n"
        "Postgres is bound to 5433.\n"
        "\n"
        "If POSTGRES_PASSWORD was changed after ./data/postgres was first created, the\n"
        "volume still holds the old password until you remove that volume (data loss).\n",
        file=sys.stderr,
    )
