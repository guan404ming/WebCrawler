#!/usr/bin/env python3
"""
Visualize crawler benchmark data from monitor_ipc.py and Scrapy throughput logs.

Reads:
  - bench_logs/ipc_*.jsonl   (IPC monitor snapshots)
  - ipc/stats/crawler_*_throughput.jsonl  (Scrapy per-worker stats)

Usage:
  uv run --group bench python plot_bench.py
  uv run --group bench python plot_bench.py --ipc-logs bench_logs/ipc_N16.jsonl bench_logs/ipc_N32.jsonl
  uv run --group bench python plot_bench.py --scrapy-dir ./ipc/stats

Output: bench_logs/bench_report.png (multi-panel figure)
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def load_jsonl(path: Path) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def infer_n_from_filename(name: str) -> str:
    """Try to extract N from filenames like ipc_N16_... or ipc_20260322_..."""
    m = re.search(r"N(\d+)", name)
    return f"N={m.group(1)}" if m else name


def load_scrapy_stats(stats_dir: Path) -> list[dict]:
    """Load and merge all crawler_*_throughput.jsonl files."""
    all_recs = []
    for f in sorted(stats_dir.glob("crawler_*_throughput.jsonl")):
        all_recs.extend(load_jsonl(f))
    return sorted(all_recs, key=lambda r: r.get("ts", ""))


def plot_ipc_comparison(ax_queue: plt.Axes, ax_rate: plt.Axes,
                        ipc_logs: list[Path]) -> None:
    """Plot IPC file counts and estimated URL consume rate across runs."""
    for log_path in ipc_logs:
        recs = load_jsonl(log_path)
        if not recs:
            continue
        label = infer_n_from_filename(log_path.stem)
        elapsed = [r["elapsed_s"] for r in recs]
        queue_files = [r.get("url_queue_files", 0) for r in recs]
        crawl_files = [r.get("crawler_out_files", 0) for r in recs]
        est_urls = [r.get("est_urls_per_s", 0) for r in recs]

        ax_queue.plot(elapsed, queue_files, label=f"{label} url_queue", linewidth=1.5)
        ax_queue.plot(elapsed, crawl_files, label=f"{label} crawl_result",
                      linewidth=1.5, linestyle="--")
        ax_rate.plot(elapsed, est_urls, label=label, linewidth=1.5)

    ax_queue.set_xlabel("Elapsed (s)")
    ax_queue.set_ylabel("File count")
    ax_queue.set_title("IPC File Counts Over Time")
    ax_queue.legend(fontsize=8)
    ax_queue.grid(True, alpha=0.3)

    ax_rate.set_xlabel("Elapsed (s)")
    ax_rate.set_ylabel("Est. URLs/s consumed")
    ax_rate.set_title("Estimated Crawl Throughput (URLs/s)")
    ax_rate.legend(fontsize=8)
    ax_rate.grid(True, alpha=0.3)


def plot_scrapy_throughput(ax_resp: plt.Axes, ax_lat: plt.Axes,
                           stats_dir: Path) -> None:
    """Plot aggregate Scrapy responses/s and average download latency."""
    recs = load_scrapy_stats(stats_dir)
    if not recs:
        ax_resp.text(0.5, 0.5, "No Scrapy throughput data found",
                     ha="center", va="center", transform=ax_resp.transAxes)
        ax_lat.text(0.5, 0.5, "No Scrapy throughput data found",
                    ha="center", va="center", transform=ax_lat.transAxes)
        return

    # Aggregate per timestamp window: sum responses, weighted-avg latency
    by_ts: dict[str, dict] = {}
    for r in recs:
        ts = r["ts"][:19]  # truncate to second
        if ts not in by_ts:
            by_ts[ts] = {"resp": 0, "items_ok": 0, "items_fail": 0,
                         "lat_sum": 0.0, "lat_count": 0, "window": 0.0}
        by_ts[ts]["resp"] += r.get("resp_received", 0)
        by_ts[ts]["items_ok"] += r.get("items_ok", 0)
        by_ts[ts]["items_fail"] += r.get("items_fail", 0)
        lat = r.get("avg_download_latency_ms", 0)
        cnt = r.get("resp_received", 0)
        by_ts[ts]["lat_sum"] += lat * cnt
        by_ts[ts]["lat_count"] += cnt
        by_ts[ts]["window"] = max(by_ts[ts]["window"], r.get("window_sec", 30))

    timestamps = sorted(by_ts.keys())
    resp_per_s = []
    avg_latency = []
    items_ok = []
    for ts in timestamps:
        d = by_ts[ts]
        w = d["window"] if d["window"] > 0 else 30
        resp_per_s.append(d["resp"] / w)
        items_ok.append(d["items_ok"] / w)
        avg_latency.append(d["lat_sum"] / d["lat_count"] if d["lat_count"] > 0 else 0)

    x = list(range(len(timestamps)))

    ax_resp.bar(x, resp_per_s, width=0.8, alpha=0.7, label="responses/s", color="#2196F3")
    ax_resp.bar(x, items_ok, width=0.8, alpha=0.5, label="items_ok/s", color="#4CAF50")
    ax_resp.set_xlabel("Window index")
    ax_resp.set_ylabel("Rate (/s)")
    ax_resp.set_title("Scrapy Aggregate Throughput (all crawlers)")
    ax_resp.legend(fontsize=8)
    ax_resp.grid(True, alpha=0.3, axis="y")

    ax_lat.plot(x, avg_latency, color="#FF5722", linewidth=1.5, marker="o", markersize=3)
    ax_lat.set_xlabel("Window index")
    ax_lat.set_ylabel("Avg download latency (ms)")
    ax_lat.set_title("Scrapy Avg Download Latency")
    ax_lat.grid(True, alpha=0.3)


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot crawler benchmark results")
    ap.add_argument("--ipc-logs", type=Path, nargs="*", default=None,
                    help="IPC JSONL log files (default: all in bench_logs/)")
    ap.add_argument("--scrapy-dir", type=Path, default=Path("./ipc/stats"),
                    help="Directory with crawler_*_throughput.jsonl files")
    ap.add_argument("--out", type=Path, default=Path("bench_logs/bench_report.png"),
                    help="Output image path")
    args = ap.parse_args()

    ipc_logs = args.ipc_logs
    if ipc_logs is None:
        log_dir = Path("bench_logs")
        ipc_logs = sorted(log_dir.glob("ipc_*.jsonl")) if log_dir.exists() else []

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Crawler Scale Benchmark", fontsize=16, fontweight="bold")
    plt.subplots_adjust(hspace=0.35, wspace=0.25)

    if ipc_logs:
        plot_ipc_comparison(axes[0, 0], axes[0, 1], ipc_logs)
    else:
        for ax in axes[0]:
            ax.text(0.5, 0.5, "No IPC logs found in bench_logs/",
                    ha="center", va="center", transform=ax.transAxes)

    plot_scrapy_throughput(axes[1, 0], axes[1, 1], args.scrapy_dir)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    print(f"Saved to {args.out}")
    plt.close(fig)


if __name__ == "__main__":
    main()
