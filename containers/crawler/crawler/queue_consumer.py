from __future__ import annotations

import re
from pathlib import Path

from libs.ipc.jsonio import read_json

_DOMAIN_FILE_RE = re.compile(r"^domain_(\d+)\.json$")


class QueueConsumer:
    """
    Per-domain queue consumer:
      - list domain_*.json files
      - read then DELETE immediately
      - return {domain_name: [urls]}
    """

    def __init__(self, queue_dir: str):
        self.queue_dir = Path(queue_dir)

    def pop_domain_batches(self, limit: int = 0) -> dict[str, list[str]]:
        """
        Read up to `limit` domain files (0 = all available).
        Returns {domain_name: [url, ...]}.
        Each file is deleted immediately after reading.
        """
        if not self.queue_dir.exists():
            return {}

        files = sorted(
            f for f in self.queue_dir.iterdir()
            if _DOMAIN_FILE_RE.match(f.name)
        )
        if not files:
            return {}

        if limit > 0:
            files = files[:limit]

        result: dict[str, list[str]] = {}
        for p in files:
            try:
                data = read_json(p)
            except Exception:
                continue
            try:
                p.unlink()
            except FileNotFoundError:
                continue

            urls = data.get("urls")
            if not isinstance(urls, list) or not urls:
                continue

            domain = str(data.get("domain_id", p.stem))
            result[domain] = [str(u) for u in urls]

        return result

    def pop_batch(self) -> list[str]:
        """
        Legacy compatibility: pop a single domain file and return flat URL list.
        """
        batches = self.pop_domain_batches(limit=1)
        if not batches:
            return []
        return next(iter(batches.values()))
