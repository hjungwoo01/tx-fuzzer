"""
Micro workload runner with stress baseline, random delays, and heuristic fuzzer.

Implements coverage-over-time sampling and produces CSV + plots.

Strategies:
  - stress: N threads loop for T seconds, OS scheduler only (no delays)
  - random: random delay (0-K ms) injected before each transaction step
  - heuristic: deterministic delay injection with K rotations (scheduling fuzzer)
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DBAdapter, ExecResult, make_adapter
from scripts.experiments.error_taxonomy import ErrorCategory
from scripts.experiments.micro_workloads import MICRO_WORKLOADS, MicroWorkload


@dataclass
class CoveragePoint:
    time_s: float
    db: str
    workload: str
    strategy: str
    rep: int
    unique_states: int
    txns_done: int
    bugs_found: int


@dataclass
class TrialResult:
    outcomes: List[Dict[str, Any]]
    canonical_state: Dict[str, Any]
    canonical_hash: str
    bug_found: bool
    bug_reason: str
    elapsed_ms: float
    error_categories: List[str] = field(default_factory=list)


def _make_connections(
    db: str, dsn: str, n: int, **kwargs: Any
) -> List[DBAdapter]:
    """Create n independent connections."""
    adapters = []
    for _ in range(n):
        a = make_adapter(db, dsn, **kwargs)
        adapters.append(a)
    return adapters


class MicroRunner:
    """Run a micro workload trial: setup, execute workers, observe, check oracle."""

    def __init__(
        self,
        workload: MicroWorkload,
        db: str,
        dsn: str,
        n_workers: int = 4,
        variant: str = "buggy",
        strategy: str = "stress",
        delay_ms: float = 0.0,
        heuristic_k: int = 3,
        **adapter_kwargs: Any,
    ) -> None:
        self.workload = workload
        self.db = db
        self.dsn = dsn
        self.n_workers = n_workers
        self.variant = variant
        self.strategy = strategy
        self.delay_ms = delay_ms
        self.heuristic_k = heuristic_k
        self.adapter_kwargs = adapter_kwargs
        self._rotation = 0

    def _inject_delay(self, worker_id: int) -> None:
        """Inject delay based on strategy (scheduling-only, no data mutation)."""
        if self.strategy == "stress":
            return  # no delay
        if self.strategy == "random":
            d = random.uniform(0, self.delay_ms) / 1000.0
            if d > 0:
                time.sleep(d)
        elif self.strategy == "heuristic":
            # Rotate which worker gets the delay
            target = self._rotation % self.n_workers
            if worker_id == target:
                d = self.delay_ms / 1000.0
                if d > 0:
                    time.sleep(d)

    def run_trial(self) -> TrialResult:
        """Execute one trial of the micro workload."""
        t0 = time.monotonic()
        # Create a shared setup adapter
        setup_adapter = make_adapter(self.db, self.dsn, **self.adapter_kwargs)
        try:
            setup_adapter.setup(self.workload.setup_sql(self.db))
        finally:
            setup_adapter.close()

        # Create per-worker adapters
        adapters = _make_connections(self.db, self.dsn, self.n_workers, **self.adapter_kwargs)
        outcomes: List[Dict[str, Any]] = [{}] * self.n_workers
        error_cats: List[str] = []

        fn = self.workload.run_buggy if self.variant == "buggy" else self.workload.run_fixed

        # Run workers concurrently using threads (per-session FIFO preserved)
        barrier = threading.Barrier(self.n_workers, timeout=10)
        threads = []

        def worker(idx: int) -> None:
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                pass
            self._inject_delay(idx)
            try:
                result = fn(adapters[idx], idx)
                outcomes[idx] = result
                ec = result.get("error", "OK")
                if ec != "OK":
                    error_cats.append(ec)
            except Exception as e:
                outcomes[idx] = {"outcome": "exception", "error": str(e)}
                error_cats.append("OTHER")

        for i in range(self.n_workers):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=30)

        # Rotate heuristic
        self._rotation += 1

        # Read canonical state
        read_adapter = make_adapter(self.db, self.dsn, **self.adapter_kwargs)
        try:
            # Pass outcomes to canonical for error-category coverage
            import inspect
            sig = inspect.signature(self.workload.canonical)
            if len(sig.parameters) > 1:
                state = self.workload.canonical(read_adapter, outcomes)
            else:
                state = self.workload.canonical(read_adapter)
        finally:
            read_adapter.close()

        # Cleanup worker adapters
        for a in adapters:
            try:
                a.close()
            except Exception:
                pass

        # Check oracle (unified interface)
        bug_found, bug_reason = self.workload.oracle(outcomes, state)

        elapsed = (time.monotonic() - t0) * 1000
        h = self.workload.canonical_hash(state)

        return TrialResult(
            outcomes=outcomes,
            canonical_state=state,
            canonical_hash=h,
            bug_found=bug_found,
            bug_reason=bug_reason,
            elapsed_ms=elapsed,
            error_categories=error_cats,
        )


def run_coverage_session(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    strategy: str,
    variant: str = "buggy",
    duration_s: float = 20.0,
    n_workers: int = 4,
    delay_ms: float = 5.0,
    heuristic_k: int = 3,
    rep: int = 0,
    sample_interval_s: float = 0.5,
    **adapter_kwargs: Any,
) -> Tuple[List[CoveragePoint], List[TrialResult]]:
    """Run a micro workload for duration_s, sampling coverage every sample_interval_s."""
    wl = MICRO_WORKLOADS[workload_name]
    runner = MicroRunner(
        workload=wl, db=db, dsn=dsn, n_workers=n_workers,
        variant=variant, strategy=strategy, delay_ms=delay_ms,
        heuristic_k=heuristic_k, **adapter_kwargs,
    )

    coverage_points: List[CoveragePoint] = []
    trial_results: List[TrialResult] = []
    unique_hashes: set = set()
    bugs_found = 0
    txns_done = 0
    start = time.monotonic()
    last_sample = start

    while (time.monotonic() - start) < duration_s:
        tr = runner.run_trial()
        trial_results.append(tr)
        txns_done += 1
        unique_hashes.add(tr.canonical_hash)
        if tr.bug_found:
            bugs_found += 1

        now = time.monotonic()
        if (now - last_sample) >= sample_interval_s:
            elapsed = now - start
            coverage_points.append(CoveragePoint(
                time_s=round(elapsed, 3),
                db=db,
                workload=workload_name,
                strategy=strategy,
                rep=rep,
                unique_states=len(unique_hashes),
                txns_done=txns_done,
                bugs_found=bugs_found,
            ))
            last_sample = now

    # Final sample
    elapsed = time.monotonic() - start
    coverage_points.append(CoveragePoint(
        time_s=round(elapsed, 3),
        db=db,
        workload=workload_name,
        strategy=strategy,
        rep=rep,
        unique_states=len(unique_hashes),
        txns_done=txns_done,
        bugs_found=bugs_found,
    ))

    return coverage_points, trial_results


def write_coverage_csv(points: List[CoveragePoint], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time_s", "db", "workload", "strategy", "rep", "unique_states", "txns_done", "bugs_found"])
        for p in points:
            w.writerow([p.time_s, p.db, p.workload, p.strategy, p.rep, p.unique_states, p.txns_done, p.bugs_found])


def plot_coverage(points: List[CoveragePoint], output_dir: Path, db: str) -> List[str]:
    """Generate coverage-over-time plots per workload for one DB."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[WARN] matplotlib not available, skipping plots", file=sys.stderr)
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    generated = []

    workloads = sorted(set(p.workload for p in points if p.db == db))
    for wl_name in workloads:
        fig, ax = plt.subplots(figsize=(8, 5))
        strategies = sorted(set(p.strategy for p in points if p.db == db and p.workload == wl_name))
        colors = {"stress": "tab:blue", "random": "tab:orange", "heuristic": "tab:green"}

        for strat in strategies:
            strat_points = [p for p in points if p.db == db and p.workload == wl_name and p.strategy == strat]
            # Average across reps at each time point
            by_time: Dict[float, List[int]] = {}
            for p in strat_points:
                by_time.setdefault(p.time_s, []).append(p.unique_states)
            times = sorted(by_time.keys())
            avg_states = [sum(by_time[t]) / len(by_time[t]) for t in times]
            ax.plot(times, avg_states, label=strat, color=colors.get(strat, None), linewidth=2)

        ax.set_title(f"Coverage: {wl_name} ({db})")
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Unique Canonical States")
        ax.legend()
        ax.grid(True, alpha=0.3)
        out = output_dir / f"coverage_{db}_{wl_name}.png"
        fig.tight_layout()
        fig.savefig(out, dpi=140)
        plt.close(fig)
        generated.append(str(out))

    return generated
