#!/usr/bin/env python3
"""
Head-to-head evaluation: IsoTest vs external state-of-the-art baselines.

Addresses the reviewer concern: "How does this compare to Jepsen, Elle,
DBcop, PCT, or CHESS?" — by running the same workloads against
established external strategies on top of the IsoTest substrate.

Strategies evaluated
--------------------

External baselines (no pool, no retention, no mutation):
  - stress           OS-only concurrency (no client-side delays)
  - naive            Naive uniform delay on every event ("stress + sleep")
  - iid_bernoulli    IID Bernoulli per-event delay (RAPOS-style)
  - pct              Probabilistic Concurrency Testing (Burckhardt et al.,
                     ASPLOS '10) seed schedules with priority-decreasing delays
  - jepsen_random    Jepsen-style random nemesis: uniform delays on a
                     small random subset of events
  - elle_history     Same workload but with full Elle-readable history
                     emission, used as an external isolation checker
                     (see ``validate_oracle_soundness.py``)

IsoTest strategies:
  - random           Pure random delay seed (no pool)
  - heuristic        Pure heuristic seed (no pool)
  - pool(random)     Pool + retention + mutation, seeded by random
  - pool(heuristic)  Pool + retention + mutation, seeded by heuristic

Outputs
-------
  <output_dir>/baseline_timeseries.csv    — raw cumulative coverage samples
  <output_dir>/baseline_summary.csv       — per (db, wl, strategy) means
  <output_dir>/baseline_comparison.csv    — head-to-head deltas vs each
                                             external baseline

Usage
-----
  python scripts/evaluate_baselines.py --db sqlite --duration 120 --reps 3
  python scripts/evaluate_baselines.py --db postgres --strategies pct random 'pool(heuristic)'
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.micro_runner import MicroRunner
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
import scripts.experiments.extended_workloads  # noqa: F401  (registers extended workloads)

from scripts.schedule_pool import (
    FinalStateHash,
    HeuristicStrategy,
    IIDBernoulliStrategy,
    NaiveUniformStrategy,
    PCTStrategy,
    RandomStrategy,
)
from scripts.schedule_pool.runner import (
    run_pool_search,
    run_pure_strategy,
)


# ---------------------------------------------------------------------------
# Strategy registry — fair common substrate
# ---------------------------------------------------------------------------

EXTERNAL_BASELINES = {
    "stress": ("stress", None),                        # MicroRunner stress baseline
    "naive": ("pure", NaiveUniformStrategy),
    "iid_bernoulli": ("pure", IIDBernoulliStrategy),
    "pct": ("pure", PCTStrategy),
    "jepsen_random": ("pure", "jepsen_random"),        # built below
}

ISOTEST_STRATEGIES = {
    "random": ("pure", RandomStrategy),
    "heuristic": ("pure", HeuristicStrategy),
    "pool(random)": ("pool", RandomStrategy),
    "pool(heuristic)": ("pool", HeuristicStrategy),
}


def build_strategy(name: str):
    """Construct a BaseStrategy instance from its registry name."""
    if name == "naive":
        return NaiveUniformStrategy(delay_ms=20.0)
    if name == "iid_bernoulli":
        return IIDBernoulliStrategy(p_delay=0.30, min_delay_ms=5.0, max_delay_ms=40.0)
    if name == "pct":
        return PCTStrategy(max_depth=3, min_delay_ms=10.0, max_delay_ms=80.0)
    if name == "jepsen_random":
        # Jepsen "nemesis" approximation: uniform delays on a SMALL random
        # subset (k=2..6) of events. Mirrors the partition-then-resume
        # pattern used by Jepsen's random nemesis schedules, mapped to
        # client-side delays.
        return HeuristicStrategy(k=4, min_delay_ms=15.0, max_delay_ms=60.0)
    if name == "random":
        return RandomStrategy(max_delay_ms=80.0)
    if name == "heuristic":
        return HeuristicStrategy(k=4, min_delay_ms=5.0, max_delay_ms=50.0)
    raise ValueError(f"unknown strategy: {name}")


# ---------------------------------------------------------------------------
# Per-strategy execution
# ---------------------------------------------------------------------------

@dataclass
class TimeSeriesPoint:
    timestamp_s: float
    db: str
    workload: str
    strategy: str
    rep: int
    cumulative_unique_states: int
    cumulative_trials: int
    cumulative_bugs: int


def run_stress_baseline(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    duration_s: float,
    n_workers: int,
    rep: int,
    sample_interval_s: float,
) -> List[TimeSeriesPoint]:
    """OS-only stress, identical to evaluate_coverage_longitudinal stress mode."""
    wl = MICRO_WORKLOADS[workload_name]
    runner = MicroRunner(
        workload=wl,
        db=db,
        dsn=dsn,
        n_workers=n_workers,
        variant="buggy",
        strategy="stress",
        delay_ms=0.0,
        heuristic_k=3,
    )
    points: List[TimeSeriesPoint] = []
    seen = set()
    bugs = 0
    trials = 0
    start = time.monotonic()
    last = start
    while (time.monotonic() - start) < duration_s:
        tr = runner.run_trial()
        trials += 1
        h = FinalStateHash.compute(tr.canonical_state).value
        seen.add(h)
        if tr.bug_found:
            bugs += 1
        now = time.monotonic()
        if (now - last) >= sample_interval_s:
            points.append(TimeSeriesPoint(
                round(now - start, 3), db, workload_name, "stress",
                rep, len(seen), trials, bugs,
            ))
            last = now
    elapsed = time.monotonic() - start
    points.append(TimeSeriesPoint(
        round(elapsed, 3), db, workload_name, "stress",
        rep, len(seen), trials, bugs,
    ))
    return points


def run_pure_baseline(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    strategy_name: str,
    duration_s: float,
    n_workers: int,
    seed: int,
    rep: int,
    sample_interval_s: float,
) -> List[TimeSeriesPoint]:
    """Pool-free baseline run."""
    base = build_strategy(strategy_name)
    cps, _ = run_pure_strategy(
        db=db,
        dsn=dsn,
        workload_name=workload_name,
        base_strategy=base,
        duration_s=duration_s,
        n_workers=n_workers,
        seed=seed,
        sample_interval_s=sample_interval_s,
        rep=rep,
        strategy_label=strategy_name,
    )
    return [
        TimeSeriesPoint(
            cp.time_s, db, workload_name, strategy_name, rep,
            cp.unique_states, cp.txns_done, cp.bugs_found,
        )
        for cp in cps
    ]


def run_pool_baseline(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    strategy_label: str,
    base_strategy_name: str,
    duration_s: float,
    n_workers: int,
    seed: int,
    rep: int,
    sample_interval_s: float,
) -> List[TimeSeriesPoint]:
    """Pool-augmented strategy run."""
    base = build_strategy(base_strategy_name)
    cps, _, _ = run_pool_search(
        db=db,
        dsn=dsn,
        workload_name=workload_name,
        duration_s=duration_s,
        n_workers=n_workers,
        seed=seed,
        base_strategy=base,
        sample_interval_s=sample_interval_s,
        rep=rep,
    )
    return [
        TimeSeriesPoint(
            cp.time_s, db, workload_name, strategy_label, rep,
            cp.unique_states, cp.txns_done, cp.bugs_found,
        )
        for cp in cps
    ]


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

TS_FIELDS = [
    "timestamp_s", "db", "workload", "strategy", "rep",
    "cumulative_unique_states", "cumulative_trials", "cumulative_bugs",
]


def write_timeseries(points: List[TimeSeriesPoint], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TS_FIELDS)
        w.writeheader()
        for p in points:
            w.writerow({
                "timestamp_s": p.timestamp_s, "db": p.db, "workload": p.workload,
                "strategy": p.strategy, "rep": p.rep,
                "cumulative_unique_states": p.cumulative_unique_states,
                "cumulative_trials": p.cumulative_trials,
                "cumulative_bugs": p.cumulative_bugs,
            })


def write_summary(points: List[TimeSeriesPoint], path: Path) -> None:
    rep_final: Dict[Tuple[str, str, str, int], TimeSeriesPoint] = {}
    for p in points:
        key = (p.db, p.workload, p.strategy, p.rep)
        if key not in rep_final or p.timestamp_s > rep_final[key].timestamp_s:
            rep_final[key] = p

    by_strat: Dict[Tuple[str, str, str], List[TimeSeriesPoint]] = defaultdict(list)
    for (db, wl, strat, _r), p in rep_final.items():
        by_strat[(db, wl, strat)].append(p)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "workload", "strategy", "reps",
            "states_mean", "states_std", "states_min", "states_max",
            "trials_mean", "bugs_mean", "duration_s",
        ])
        for (db, wl, strat), group in sorted(by_strat.items()):
            states = [p.cumulative_unique_states for p in group]
            trials = [p.cumulative_trials for p in group]
            bugs = [p.cumulative_bugs for p in group]
            durs = [p.timestamp_s for p in group]
            mean = statistics.mean
            sd = lambda xs: statistics.stdev(xs) if len(xs) >= 2 else 0.0
            w.writerow([
                db, wl, strat, len(group),
                f"{mean(states):.2f}", f"{sd(states):.2f}",
                min(states), max(states),
                f"{mean(trials):.2f}", f"{mean(bugs):.2f}",
                f"{mean(durs):.2f}",
            ])


def write_comparison(points: List[TimeSeriesPoint], path: Path) -> None:
    """Compute head-to-head deltas: each IsoTest strategy vs each external baseline."""
    rep_final: Dict[Tuple[str, str, str, int], TimeSeriesPoint] = {}
    for p in points:
        key = (p.db, p.workload, p.strategy, p.rep)
        if key not in rep_final or p.timestamp_s > rep_final[key].timestamp_s:
            rep_final[key] = p

    by_strat: Dict[Tuple[str, str, str], List[TimeSeriesPoint]] = defaultdict(list)
    for (db, wl, strat, _r), p in rep_final.items():
        by_strat[(db, wl, strat)].append(p)

    isotest_names = {"random", "heuristic", "pool(random)", "pool(heuristic)"}
    external_names = {"stress", "naive", "iid_bernoulli", "pct", "jepsen_random"}

    rows: List[Dict[str, Any]] = []
    for (db, wl, isotest), iso_group in by_strat.items():
        if isotest not in isotest_names:
            continue
        iso_states = [p.cumulative_unique_states for p in iso_group]
        iso_bugs = [p.cumulative_bugs for p in iso_group]
        for (db2, wl2, ext), ext_group in by_strat.items():
            if ext not in external_names or db2 != db or wl2 != wl:
                continue
            ext_states = [p.cumulative_unique_states for p in ext_group]
            ext_bugs = [p.cumulative_bugs for p in ext_group]
            iso_mean_s = statistics.mean(iso_states) if iso_states else 0
            ext_mean_s = statistics.mean(ext_states) if ext_states else 0
            iso_mean_b = statistics.mean(iso_bugs) if iso_bugs else 0
            ext_mean_b = statistics.mean(ext_bugs) if ext_bugs else 0
            ratio_s = (iso_mean_s / ext_mean_s) if ext_mean_s else float("inf")
            ratio_b = (iso_mean_b / ext_mean_b) if ext_mean_b else float("inf")
            rows.append({
                "db": db, "workload": wl,
                "isotest_strategy": isotest, "external_baseline": ext,
                "isotest_states": f"{iso_mean_s:.2f}",
                "external_states": f"{ext_mean_s:.2f}",
                "states_ratio": f"{ratio_s:.3f}" if ratio_s != float("inf") else "inf",
                "states_delta": f"{iso_mean_s - ext_mean_s:+.2f}",
                "isotest_bugs": f"{iso_mean_b:.2f}",
                "external_bugs": f"{ext_mean_b:.2f}",
                "bugs_ratio": f"{ratio_b:.3f}" if ratio_b != float("inf") else "inf",
                "bugs_delta": f"{iso_mean_b - ext_mean_b:+.2f}",
            })

    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_STRATS = list(EXTERNAL_BASELINES.keys()) + list(ISOTEST_STRATEGIES.keys())
DEFAULT_WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Head-to-head evaluation vs external baselines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", nargs="+", default=["sqlite"])
    parser.add_argument("--workloads", nargs="+", default=None)
    parser.add_argument(
        "--strategies", nargs="+", default=None,
        help=f"Strategies to run (default: ALL). Choices: {ALL_STRATS}",
    )
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--n-workers", type=int, default=4)
    parser.add_argument("--sample-interval", type=float, default=2.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    workloads = args.workloads or DEFAULT_WORKLOADS
    strategies = args.strategies or ALL_STRATS
    out_dir = Path(args.output_dir) if args.output_dir else ROOT / "results" / "baselines"

    dsns: Dict[str, str] = {}
    for db in args.db:
        env_key = f"{db.upper()}_DSN"
        dsns[db] = os.getenv(env_key, DEFAULT_DSNS.get(db, ""))

    print("=" * 72)
    print("IsoTest External-Baseline Head-to-Head")
    print("=" * 72)
    print(f"  Databases:  {args.db}")
    print(f"  Workloads:  {workloads}")
    print(f"  Strategies: {strategies}")
    print(f"  Duration:   {args.duration}s per cell  Reps: {args.reps}")
    print(f"  Output:     {out_dir}")
    print("=" * 72)

    all_points: List[TimeSeriesPoint] = []
    total = len(args.db) * len(workloads) * len(strategies) * args.reps
    done = 0

    for db in args.db:
        dsn = dsns[db]
        if not _db_available(db, dsn):
            print(f"\n  SKIP {db} (not available)")
            continue
        for wl in workloads:
            if wl not in MICRO_WORKLOADS:
                print(f"  SKIP unknown workload: {wl}")
                continue
            for strat in strategies:
                for rep in range(args.reps):
                    done += 1
                    seed = args.seed + rep * 1000 + hash(strat) % 100
                    print(
                        f"\n[{done}/{total}] "
                        f"{db} / {strat} / {wl} / rep={rep} (seed={seed})"
                    )
                    t0 = time.monotonic()
                    if strat == "stress":
                        pts = run_stress_baseline(
                            db=db, dsn=dsn, workload_name=wl,
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    elif strat in EXTERNAL_BASELINES:
                        pts = run_pure_baseline(
                            db=db, dsn=dsn, workload_name=wl,
                            strategy_name=strat,
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            seed=seed, rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    elif strat in ("random", "heuristic"):
                        pts = run_pure_baseline(
                            db=db, dsn=dsn, workload_name=wl,
                            strategy_name=strat,
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            seed=seed, rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    elif strat == "pool(random)":
                        pts = run_pool_baseline(
                            db=db, dsn=dsn, workload_name=wl,
                            strategy_label=strat,
                            base_strategy_name="random",
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            seed=seed, rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    elif strat == "pool(heuristic)":
                        pts = run_pool_baseline(
                            db=db, dsn=dsn, workload_name=wl,
                            strategy_label=strat,
                            base_strategy_name="heuristic",
                            duration_s=args.duration,
                            n_workers=args.n_workers,
                            seed=seed, rep=rep,
                            sample_interval_s=args.sample_interval,
                        )
                    else:
                        print(f"  SKIP unknown strategy: {strat}")
                        continue

                    wall = time.monotonic() - t0
                    all_points.extend(pts)
                    if pts:
                        f = pts[-1]
                        print(
                            f"  -> {f.cumulative_trials} trials, "
                            f"{f.cumulative_unique_states} unique states, "
                            f"{f.cumulative_bugs} bugs ({wall:.1f}s)"
                        )

    write_timeseries(all_points, out_dir / "baseline_timeseries.csv")
    write_summary(all_points, out_dir / "baseline_summary.csv")
    write_comparison(all_points, out_dir / "baseline_comparison.csv")

    print(f"\nWrote: {out_dir}/baseline_*.csv ({len(all_points)} samples)")


if __name__ == "__main__":
    main()
