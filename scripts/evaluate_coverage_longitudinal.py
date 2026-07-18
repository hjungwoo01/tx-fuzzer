#!/usr/bin/env python3
"""
Longitudinal State-Coverage Analyzer

Addresses reviewer request: "Move beyond speed-to-first-bug and show the
asymptotic behavior of the schedulers over extended runs."

Runs the Stress, Random, and Heuristic schedulers (plus the pool-based
variants) for an extended duration and emits a time-series CSV of
(timestamp, cumulative_unique_states) per scheduler.

Output:
  <output_dir>/coverage_timeseries.csv   — raw time-series
  <output_dir>/coverage_longitudinal.csv — per-strategy summary

The CSV is suitable for plotting a line graph:
  X = elapsed time (s), Y = cumulative unique FinalStateHash values.

Usage:
  python scripts/evaluate_coverage_longitudinal.py --db sqlite
  python scripts/evaluate_coverage_longitudinal.py --db sqlite --duration 600 --reps 3
  python scripts/evaluate_coverage_longitudinal.py --db postgres mysql sqlite --duration 300
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.micro_runner import (
    CoveragePoint,
    MicroRunner,
    TrialResult,
    run_coverage_session,
)
from scripts.experiments.micro_workloads import MICRO_WORKLOADS

from scripts.schedule_pool import (
    FinalStateHash,
    SchedulePool,
    PoolConfig,
    ScheduleMutator,
    MutationConfig,
)
from scripts.schedule_pool.base_strategy import RandomStrategy, HeuristicStrategy
from scripts.schedule_pool.event_id import get_workload_event_ids
from scripts.schedule_pool.runner import (
    SchedulePoolRunner,
    run_pool_search,
)
from scripts.schedule_pool.retention import RetentionPolicy
from scripts.schedule_pool.schedule import Schedule, ScheduleArtifact


# ---------------------------------------------------------------------------
# Time-series data point
# ---------------------------------------------------------------------------

@dataclass
class TimeSeriesPoint:
    """One sample in the coverage time-series."""
    timestamp_s: float       # seconds since campaign start
    db: str
    workload: str
    strategy: str
    rep: int
    cumulative_unique_states: int
    cumulative_trials: int
    cumulative_bugs: int


# ---------------------------------------------------------------------------
# Extended coverage session using MicroRunner (stress/random/heuristic)
# ---------------------------------------------------------------------------

def run_baseline_coverage(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    strategy: str,
    duration_s: float,
    n_workers: int,
    delay_ms: float,
    heuristic_k: int,
    rep: int,
    sample_interval_s: float,
    **adapter_kwargs: Any,
) -> List[TimeSeriesPoint]:
    """Run a baseline strategy (stress/random/heuristic) and sample coverage."""
    wl = MICRO_WORKLOADS[workload_name]
    runner = MicroRunner(
        workload=wl,
        db=db,
        dsn=dsn,
        n_workers=n_workers,
        variant="buggy",
        strategy=strategy,
        delay_ms=delay_ms,
        heuristic_k=heuristic_k,
        **adapter_kwargs,
    )

    points: List[TimeSeriesPoint] = []
    unique_hashes: Set[str] = set()
    bugs_found = 0
    trials = 0
    start = time.monotonic()
    last_sample = start

    while (time.monotonic() - start) < duration_s:
        tr = runner.run_trial()
        trials += 1
        h = FinalStateHash.compute(tr.canonical_state).value
        unique_hashes.add(h)
        if tr.bug_found:
            bugs_found += 1

        now = time.monotonic()
        if (now - last_sample) >= sample_interval_s:
            elapsed = now - start
            points.append(TimeSeriesPoint(
                timestamp_s=round(elapsed, 3),
                db=db,
                workload=workload_name,
                strategy=strategy,
                rep=rep,
                cumulative_unique_states=len(unique_hashes),
                cumulative_trials=trials,
                cumulative_bugs=bugs_found,
            ))
            last_sample = now

    # Final point
    elapsed = time.monotonic() - start
    points.append(TimeSeriesPoint(
        timestamp_s=round(elapsed, 3),
        db=db,
        workload=workload_name,
        strategy=strategy,
        rep=rep,
        cumulative_unique_states=len(unique_hashes),
        cumulative_trials=trials,
        cumulative_bugs=bugs_found,
    ))
    return points


# ---------------------------------------------------------------------------
# Pool-based coverage session
# ---------------------------------------------------------------------------

def run_pool_coverage(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    strategy_name: str,
    base_strategy,
    duration_s: float,
    n_workers: int,
    seed: int,
    rep: int,
    sample_interval_s: float,
    **adapter_kwargs: Any,
) -> List[TimeSeriesPoint]:
    """Run pool-based search and sample coverage time-series."""
    coverage_points, _, _ = run_pool_search(
        db=db,
        dsn=dsn,
        workload_name=workload_name,
        variant="buggy",
        duration_s=duration_s,
        n_workers=n_workers,
        seed=seed,
        base_strategy=base_strategy,
        sample_interval_s=sample_interval_s,
        rep=rep,
        **adapter_kwargs,
    )

    return [
        TimeSeriesPoint(
            timestamp_s=cp.time_s,
            db=db,
            workload=workload_name,
            strategy=strategy_name,
            rep=rep,
            cumulative_unique_states=cp.unique_states,
            cumulative_trials=cp.txns_done,
            cumulative_bugs=cp.bugs_found,
        )
        for cp in coverage_points
    ]


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

TS_FIELDS = [
    "timestamp_s", "db", "workload", "strategy", "rep",
    "cumulative_unique_states", "cumulative_trials", "cumulative_bugs",
]


def write_timeseries_csv(points: List[TimeSeriesPoint], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TS_FIELDS)
        writer.writeheader()
        for p in points:
            writer.writerow({
                "timestamp_s": p.timestamp_s,
                "db": p.db,
                "workload": p.workload,
                "strategy": p.strategy,
                "rep": p.rep,
                "cumulative_unique_states": p.cumulative_unique_states,
                "cumulative_trials": p.cumulative_trials,
                "cumulative_bugs": p.cumulative_bugs,
            })


def write_summary_csv(points: List[TimeSeriesPoint], path: Path) -> None:
    """Write per-strategy final-state summary aggregated across reps."""
    # Group by (db, workload, strategy) and take final point per rep
    groups: Dict[Tuple[str, str, str], List[TimeSeriesPoint]] = defaultdict(list)
    rep_final: Dict[Tuple[str, str, str, int], TimeSeriesPoint] = {}
    for p in points:
        key = (p.db, p.workload, p.strategy, p.rep)
        if key not in rep_final or p.timestamp_s > rep_final[key].timestamp_s:
            rep_final[key] = p

    for (db, wl, strat, _rep), p in rep_final.items():
        groups[(db, wl, strat)].append(p)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "db", "workload", "strategy", "reps",
            "final_states_mean", "final_states_std",
            "final_trials_mean", "final_bugs_mean",
            "duration_s",
        ])
        for (db, wl, strat), group in sorted(groups.items()):
            states = [p.cumulative_unique_states for p in group]
            trials = [p.cumulative_trials for p in group]
            bugs = [p.cumulative_bugs for p in group]
            durations = [p.timestamp_s for p in group]

            def _mean(xs: list) -> str:
                return f"{statistics.mean(xs):.2f}" if xs else ""

            def _std(xs: list) -> str:
                return f"{statistics.stdev(xs):.2f}" if len(xs) >= 2 else "0.00"

            writer.writerow([
                db, wl, strat, len(group),
                _mean(states), _std(states),
                _mean(trials), _mean(bugs),
                _mean(durations),
            ])


# ---------------------------------------------------------------------------
# DB availability
# ---------------------------------------------------------------------------

def _db_available(db: str, dsn: str) -> bool:
    try:
        a = make_adapter(db, dsn)
        a.begin()
        a.exec("SELECT 1")
        a.commit()
        a.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]

# Strategies: baseline (no pool) + pool-augmented
BASELINE_STRATEGIES = ["stress", "random", "heuristic"]
POOL_STRATEGIES = ["pool(random)", "pool(heuristic)"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Longitudinal State-Coverage Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/evaluate_coverage_longitudinal.py --db sqlite --duration 600
  python scripts/evaluate_coverage_longitudinal.py --db postgres --duration 300 --reps 3
  python scripts/evaluate_coverage_longitudinal.py --db sqlite --strategies stress random heuristic
        """,
    )
    parser.add_argument(
        "--db", nargs="+", default=["sqlite"],
        help="Database(s) to test (default: sqlite)",
    )
    parser.add_argument(
        "--workloads", nargs="+", default=None,
        help="Workload names (default: all three)",
    )
    parser.add_argument(
        "--strategies", nargs="+", default=None,
        help="Strategies to run. Options: stress, random, heuristic, pool(random), pool(heuristic). Default: all",
    )
    parser.add_argument(
        "--duration", type=float, default=600.0,
        help="Campaign duration in seconds (default: 600 = 10 minutes)",
    )
    parser.add_argument(
        "--reps", type=int, default=3,
        help="Repetitions per (strategy, workload) (default: 3)",
    )
    parser.add_argument(
        "--n-workers", type=int, default=4,
        help="Concurrent sessions per trial (default: 4)",
    )
    parser.add_argument(
        "--sample-interval", type=float, default=2.0,
        help="Seconds between coverage samples (default: 2.0)",
    )
    parser.add_argument(
        "--delay-ms", type=float, default=15.0,
        help="Delay parameter for random/heuristic baselines (default: 15.0)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Base random seed (default: 42)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: results/longitudinal/)",
    )
    args = parser.parse_args()

    workloads = args.workloads or WORKLOADS
    strategies = args.strategies or (BASELINE_STRATEGIES + POOL_STRATEGIES)
    output_dir = Path(args.output_dir) if args.output_dir else ROOT / "results" / "longitudinal"
    output_dir.mkdir(parents=True, exist_ok=True)

    dsns: Dict[str, str] = {}
    for db in args.db:
        env_key = f"{db.upper()}_DSN"
        dsns[db] = os.getenv(env_key, DEFAULT_DSNS.get(db, ""))

    print("=" * 70)
    print("IsoTest Longitudinal State-Coverage Analyzer")
    print("=" * 70)
    print(f"  Databases:    {args.db}")
    print(f"  Strategies:   {strategies}")
    print(f"  Workloads:    {workloads}")
    print(f"  Duration:     {args.duration}s per campaign")
    print(f"  Reps:         {args.reps}")
    print(f"  Sample rate:  every {args.sample_interval}s")
    print(f"  Output:       {output_dir}")
    print("=" * 70)

    all_points: List[TimeSeriesPoint] = []
    total = len(args.db) * len(strategies) * len(workloads) * args.reps
    done = 0

    for db in args.db:
        dsn = dsns.get(db, "")
        if not _db_available(db, dsn):
            print(f"\n  SKIP {db} (not available)")
            continue

        for wl_name in workloads:
            if wl_name not in MICRO_WORKLOADS:
                print(f"  SKIP unknown workload: {wl_name}")
                continue

            for strat in strategies:
                for rep in range(args.reps):
                    done += 1
                    seed = args.seed + rep
                    print(
                        f"\n[{done}/{total}] "
                        f"{db} / {strat} / {wl_name} / rep={rep} "
                        f"(seed={seed}, {args.duration}s)"
                    )

                    t0 = time.monotonic()

                    if strat in BASELINE_STRATEGIES:
                        points = run_baseline_coverage(
                            db=db,
                            dsn=dsn,
                            workload_name=wl_name,
                            strategy=strat,
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            delay_ms=args.delay_ms,
                            heuristic_k=3,
                            rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    elif strat == "pool(random)":
                        points = run_pool_coverage(
                            db=db,
                            dsn=dsn,
                            workload_name=wl_name,
                            strategy_name=strat,
                            base_strategy=RandomStrategy(max_delay_ms=args.delay_ms * 2),
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            seed=seed,
                            rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    elif strat == "pool(heuristic)":
                        points = run_pool_coverage(
                            db=db,
                            dsn=dsn,
                            workload_name=wl_name,
                            strategy_name=strat,
                            base_strategy=HeuristicStrategy(
                                k=4,
                                min_delay_ms=3.0,
                                max_delay_ms=25.0,
                            ),
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            seed=seed,
                            rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    else:
                        print(f"  SKIP unknown strategy: {strat}")
                        continue

                    wall = time.monotonic() - t0
                    all_points.extend(points)

                    if points:
                        final = points[-1]
                        print(
                            f"  -> {final.cumulative_trials} trials, "
                            f"{final.cumulative_unique_states} unique states, "
                            f"{final.cumulative_bugs} bugs "
                            f"({wall:.1f}s wall)"
                        )

    # Write outputs
    ts_path = output_dir / "coverage_timeseries.csv"
    write_timeseries_csv(all_points, ts_path)
    print(f"\nTime-series: {ts_path}")

    summary_path = output_dir / "coverage_longitudinal.csv"
    write_summary_csv(all_points, summary_path)
    print(f"Summary:     {summary_path}")

    # Print final summary table
    print("\n" + "=" * 70)
    print("FINAL COVERAGE SUMMARY")
    print("=" * 70)

    # Get final point per (db, wl, strat, rep)
    rep_final: Dict[Tuple[str, str, str, int], TimeSeriesPoint] = {}
    for p in all_points:
        key = (p.db, p.workload, p.strategy, p.rep)
        if key not in rep_final or p.timestamp_s > rep_final[key].timestamp_s:
            rep_final[key] = p

    groups: Dict[Tuple[str, str, str], List[TimeSeriesPoint]] = defaultdict(list)
    for (db, wl, strat, _), p in rep_final.items():
        groups[(db, wl, strat)].append(p)

    header = f"{'DB':<10} {'Workload':<20} {'Strategy':<18} {'States':<12} {'Trials':<10} {'Bugs':<8}"
    print(header)
    print("-" * len(header))
    for (db, wl, strat), group in sorted(groups.items()):
        states = [p.cumulative_unique_states for p in group]
        trials = [p.cumulative_trials for p in group]
        bugs = [p.cumulative_bugs for p in group]
        s_str = f"{statistics.mean(states):.1f}"
        t_str = f"{statistics.mean(trials):.0f}"
        b_str = f"{statistics.mean(bugs):.0f}"
        print(f"{db:<10} {wl:<20} {strat:<18} {s_str:<12} {t_str:<10} {b_str:<8}")

    print(f"\nDone. {len(all_points)} data points across {done} campaigns.")


if __name__ == "__main__":
    main()
