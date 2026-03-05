"""
Performance/overhead benchmark.

Measures for each micro workload × DB:
  1) baseline  – no analysis, no delays
  2) analysis  – run heuristic selection but force delay=0
  3) full      – heuristic K=3 with real delays

Reports: txns/sec, median latency, p95 latency.
"""

from __future__ import annotations

import csv
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.experiments.db_adapter import make_adapter
from scripts.experiments.micro_runner import MicroRunner, TrialResult
from scripts.experiments.micro_workloads import MICRO_WORKLOADS


@dataclass
class OverheadRow:
    db: str
    workload: str
    mode: str  # baseline, analysis, full
    txns_per_sec: float
    median_latency_ms: float
    p95_latency_ms: float
    total_txns: int
    total_time_s: float


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(q * (len(s) - 1))
    return s[min(idx, len(s) - 1)]


def run_overhead_benchmark(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    mode: str,
    duration_s: float = 15.0,
    n_workers: int = 4,
    delay_ms: float = 5.0,
    **adapter_kwargs: Any,
) -> OverheadRow:
    """Run overhead benchmark for one mode."""
    wl = MICRO_WORKLOADS[workload_name]

    if mode == "baseline":
        strategy = "stress"
        actual_delay = 0.0
    elif mode == "analysis":
        strategy = "heuristic"
        actual_delay = 0.0  # Analysis-only: compute delay target but inject 0
    elif mode == "full":
        strategy = "heuristic"
        actual_delay = delay_ms
    else:
        raise ValueError(f"Unknown mode: {mode}")

    runner = MicroRunner(
        workload=wl, db=db, dsn=dsn, n_workers=n_workers,
        variant="buggy", strategy=strategy, delay_ms=actual_delay,
        heuristic_k=3, **adapter_kwargs,
    )

    latencies: List[float] = []
    start = time.monotonic()
    txns = 0

    while (time.monotonic() - start) < duration_s:
        tr = runner.run_trial()
        latencies.append(tr.elapsed_ms)
        txns += 1

    total_time = time.monotonic() - start
    tps = txns / total_time if total_time > 0 else 0.0
    median = statistics.median(latencies) if latencies else 0.0
    p95 = _percentile(latencies, 0.95)

    return OverheadRow(
        db=db,
        workload=workload_name,
        mode=mode,
        txns_per_sec=round(tps, 2),
        median_latency_ms=round(median, 2),
        p95_latency_ms=round(p95, 2),
        total_txns=txns,
        total_time_s=round(total_time, 2),
    )


def write_overhead_csv(rows: List[OverheadRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["db", "workload", "mode", "txns_per_sec", "median_latency_ms",
                     "p95_latency_ms", "total_txns", "total_time_s"])
        for r in rows:
            w.writerow([r.db, r.workload, r.mode, r.txns_per_sec, r.median_latency_ms,
                         r.p95_latency_ms, r.total_txns, r.total_time_s])


def write_overhead_md(rows: List[OverheadRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Overhead Benchmark Results", ""]
    lines.append("| DB | Workload | Mode | Txns/sec | Median (ms) | P95 (ms) | Total Txns | Time (s) |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|")
    for r in rows:
        lines.append(
            f"| {r.db} | {r.workload} | {r.mode} | {r.txns_per_sec} | "
            f"{r.median_latency_ms} | {r.p95_latency_ms} | {r.total_txns} | {r.total_time_s} |"
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
