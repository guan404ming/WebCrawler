#!/usr/bin/env python3
"""
Live IPC throughput monitor for the crawler benchmark.

Prints a snapshot every INTERVAL seconds showing file counts and deltas
across url_queue, crawl_result/crawler_*, and crawl_result/ingestor_* trees.
Also tracks how many JSONL result records have been written by crawlers,
so benchmark logs include total results and result QPS instead of only file counts.

All snapshots are appended to a JSONL log file for later visualization with
plot_bench.py.

Usage:
  python monitor_ipc.py [--ipc-root ./ipc] [--interval 2] [--batch-size 512] \
                         [--log bench_logs/ipc_N16.jsonl]
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path


class JsonlLineCounter:
    """Incrementally count JSONL lines for append-only files."""

    def __init__(self) -> None:
        self._cache: dict[Path, tuple[int, int]] = {}

    @staticmethod
    def _count_newlines(fh, chunk_size: int = 1024 * 1024) -> int:
        total = 0
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                return total
            total += chunk.count(b"\n")

    def count_file(self, path: Path) -> int:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            self._cache.pop(path, None)
            return 0

        cached = self._cache.get(path)
        if cached is None or size < cached[0]:
            with open(path, "rb") as fh:
                lines = self._count_newlines(fh)
        elif size == cached[0]:
            return cached[1]
        else:
            with open(path, "rb") as fh:
                fh.seek(cached[0])
                lines = cached[1] + self._count_newlines(fh)

        self._cache[path] = (size, lines)
        return lines

    def prune(self, live_paths: set[Path]) -> None:
        for path in list(self._cache):
            if path not in live_paths:
                self._cache.pop(path, None)


def count_files(root: Path) -> tuple[int, int]:
    """Return (file_count, dir_count) under root matching *.json + *.jsonl."""
    if not root.exists():
        return 0, 0
    json_files = list(root.rglob("*.json"))
    jsonl_files = list(root.rglob("*.jsonl"))
    all_files = json_files + jsonl_files
    dirs = {f.parent for f in all_files}
    return len(all_files), len(dirs)


def count_tree(root: Path, prefix: str) -> tuple[int, int]:
    """Count files across all subdirs matching prefix (e.g. 'crawler_')."""
    if not root.exists():
        return 0, 0
    total_files = 0
    total_dirs = 0
    for d in sorted(root.iterdir()):
        if d.is_dir() and d.name.startswith(prefix):
            fc, _ = count_files(d)
            total_files += fc
            total_dirs += 1
    return total_files, total_dirs


def count_jsonl_records(root: Path, prefix: str, line_counter: JsonlLineCounter) -> int:
    """Count total JSONL records under subdirs matching prefix."""
    if not root.exists():
        line_counter.prune(set())
        return 0

    total_records = 0
    live_paths: set[Path] = set()
    for d in sorted(root.iterdir()):
        if d.is_dir() and d.name.startswith(prefix):
            for path in d.rglob("*.jsonl"):
                live_paths.add(path)
                total_records += line_counter.count_file(path)

    line_counter.prune(live_paths)
    return total_records


def snapshot(ipc: Path, crawler_result_counter: JsonlLineCounter) -> dict[str, dict[str, int]]:
    url_queue = ipc / "url_queue"
    crawl_result = ipc / "crawl_result"
    url_queue_files, url_queue_dirs = count_tree(url_queue, "crawler_")
    crawler_out_files, crawler_out_dirs = count_tree(crawl_result, "crawler_")
    ingestor_in_files, ingestor_in_dirs = count_tree(crawl_result, "ingestor_")
    return {
        "url_queue": {
            "files": url_queue_files,
            "dirs": url_queue_dirs,
        },
        "crawler_out": {
            "files": crawler_out_files,
            "dirs": crawler_out_dirs,
            "records": count_jsonl_records(crawl_result, "crawler_", crawler_result_counter),
        },
        "ingestor_in": {
            "files": ingestor_in_files,
            "dirs": ingestor_in_dirs,
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Live IPC throughput monitor")
    ap.add_argument("--ipc-root", type=Path, default=Path("./ipc"))
    ap.add_argument("--interval", type=float, default=2.0, help="Seconds between snapshots")
    ap.add_argument("--batch-size", type=int, default=512, help="URLs per queue batch file")
    ap.add_argument("--log", type=Path, default=None,
                    help="JSONL log path (default: bench_logs/ipc_<timestamp>.jsonl)")
    args = ap.parse_args()

    ipc = args.ipc_root.resolve()
    interval = args.interval
    batch_size = args.batch_size

    log_path: Path = args.log or Path("bench_logs") / f"ipc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_fh = open(log_path, "a", encoding="utf-8")

    print(f"Monitoring {ipc} every {interval}s (batch_size={batch_size})")
    print(f"Logging to {log_path}")
    print("Press Ctrl+C to stop.\n")

    crawler_result_counter = JsonlLineCounter()

    prev = snapshot(ipc, crawler_result_counter)
    prev_time = time.monotonic()
    t0_wall = time.time()

    while True:
        time.sleep(interval)
        now = snapshot(ipc, crawler_result_counter)
        now_time = time.monotonic()
        dt = now_time - prev_time
        elapsed = time.time() - t0_wall

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        rec: dict = {
            "ts": ts,
            "elapsed_s": round(elapsed, 1),
            "interval_s": round(dt, 2),
            "batch_size": batch_size,
        }

        print(f"=== {ts} (interval={dt:.1f}s, elapsed={elapsed:.0f}s) ===")

        for key, label in [
            ("url_queue", "url_queue      "),
            ("crawler_out", "crawl_result   "),
            ("ingestor_in", "ingestor_input "),
        ]:
            files_now = now[key]["files"]
            dirs_now = now[key]["dirs"]
            files_prev = prev[key]["files"]
            delta = files_now - files_prev
            rate = delta / dt if dt > 0 else 0
            sign = "+" if delta >= 0 else ""
            print(f"  {label} {files_now:6d} files in {dirs_now:3d} dirs  "
                  f"(delta: {sign}{delta}, {rate:+.1f}/s)")
            rec[f"{key}_files"] = files_now
            rec[f"{key}_dirs"] = dirs_now
            rec[f"{key}_delta"] = delta
            rec[f"{key}_rate"] = round(rate, 2)

        result_now = now["crawler_out"].get("records", 0)
        result_prev = prev["crawler_out"].get("records", 0)
        result_delta = result_now - result_prev
        result_qps = result_delta / dt if dt > 0 else 0.0
        result_sign = "+" if result_delta >= 0 else ""
        print(f"  --- total results: {result_now}  (delta: {result_sign}{result_delta}, qps={result_qps:.1f})")
        rec["crawler_out_records"] = result_now
        rec["crawler_out_records_delta"] = result_delta
        rec["crawler_out_qps"] = round(result_qps, 2)

        q_delta = prev["url_queue"]["files"] - now["url_queue"]["files"]
        est_urls_per_s = 0.0
        if q_delta > 0 and dt > 0:
            batches_per_s = q_delta / dt
            est_urls_per_s = batches_per_s * batch_size
            print(f"  --- est. consume: {batches_per_s:.1f} batches/s = ~{est_urls_per_s:.0f} URLs/s")
        rec["est_urls_per_s"] = round(est_urls_per_s, 1)

        log_fh.write(json.dumps(rec) + "\n")
        log_fh.flush()

        print()
        prev = now
        prev_time = now_time


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
