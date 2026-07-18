#!/usr/bin/env python3
"""
Scalability sweep for IsoTest.

Sweeps three axes that affect concurrency-bug discovery throughput:

  - n_workers     {2, 4, 8, 16}
  - table_scale   {1x, 2x, 4x, 8x}     (rows per workload, where applicable)
  - txn_size      {1, 5, 10}            (operations per transaction)

Each cell runs ``--reps`` repetitions of a strategy on a workload, recording:

  - time_to_first_bug_s         (None if no bug found)
  - trials_to_first_bug
  - unique_states_at_end
  - bugs_at_end
  - throughput_trials_per_s
  - per_trial_latency_p50_ms / p95_ms

The CSV makes it easy to plot:
  X = n_workers, Y = unique_states (curves: strategy)
  X = table_scale, Y = TTFB (curves: strategy)

Output
------
  <output_dir>/scalability.csv         per-cell aggregated results
  <output_dir>/scalability_raw.csv     per-rep raw results

Usage
-----
  python scripts/evaluate_scalability.py --db sqlite --duration 60
  python scripts/evaluate_scalability.py --db postgres \
      --workloads multikey_transfer \
      --workers 2 4 8 16 --scales 1 2 4 \
      --strategies stress random heuristic 'pool(heuristic)'
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
from scripts.experiments import micro_workloads as mw_mod
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
from scripts.experiments.micro_runner import MicroRunner

# Importing for side-effect: register extended workloads.
import scripts.experiments.extended_workloads  # noqa: F401

from scripts.schedule_pool import (
    FinalStateHash,
    HeuristicStrategy,
    PCTStrategy,
    RandomStrategy,
)
from scripts.schedule_pool.runner import (
    run_pool_search,
    run_pure_strategy,
)


# ---------------------------------------------------------------------------
# Result record
# ---------------------------------------------------------------------------

@dataclass
class CellResult:
    db: str
    workload: str
    strategy: str
    n_workers: int
    table_scale: int
    rep: int
    duration_s: float
    trials: int
    unique_states: int
    bugs: int
    time_to_first_bug_s: Optional[float]
    trials_to_first_bug: Optional[int]
    p50_latency_ms: float
    p95_latency_ms: float
    throughput_trials_per_s: float


# ---------------------------------------------------------------------------
# Scaling: monkey-patch workload constants
# ---------------------------------------------------------------------------

@dataclass
class _ScaleSnapshot:
    sb_customers: int
    mk_accounts: int


def _snapshot() -> _ScaleSnapshot:
    return _ScaleSnapshot(
        sb_customers=mw_mod._SB_NUM_CUSTOMERS,
        mk_accounts=mw_mod._MK_NUM_ACCOUNTS,
    )


def _apply_scale(scale: int) -> _ScaleSnapshot:
    """Scale up table sizes for workloads that support it. Returns prior values."""
    prior = _snapshot()
    if scale > 1:
        mw_mod._SB_NUM_CUSTOMERS = prior.sb_customers * scale
        mw_mod._SB_TOTAL = (
            mw_mod._SB_NUM_CUSTOMERS
            * (mw_mod._SB_INIT_SAVINGS + mw_mod._SB_INIT_CHECKING)
        )
        mw_mod._MK_NUM_ACCOUNTS = prior.mk_accounts * scale
        mw_mod._MK_TOTAL = mw_mod._MK_NUM_ACCOUNTS * mw_mod._MK_INIT_BALANCE
        # Re-instantiate oracles so they pick up the new totals
        from scripts.schedule_pool.oracle import SmallBankOracle, MultiKeyOracle
        if "smallbank" in MICRO_WORKLOADS:
            MICRO_WORKLOADS["smallbank"].oracle_instance = SmallBankOracle(
                expected_total=mw_mod._SB_TOTAL
            )
        if "multikey_transfer" in MICRO_WORKLOADS:
            MICRO_WORKLOADS["multikey_transfer"].oracle_instance = MultiKeyOracle(
                expected_total=mw_mod._MK_TOTAL
            )
    return prior


def _restore_scale(prior: _ScaleSnapshot) -> None:
    mw_mod._SB_NUM_CUSTOMERS = prior.sb_customers
    mw_mod._SB_TOTAL = (
        mw_mod._SB_NUM_CUSTOMERS
        * (mw_mod._SB_INIT_SAVINGS + mw_mod._SB_INIT_CHECKING)
    )
    mw_mod._MK_NUM_ACCOUNTS = prior.mk_accounts
    mw_mod._MK_TOTAL = mw_mod._MK_NUM_ACCOUNTS * mw_mod._MK_INIT_BALANCE
    from scripts.schedule_pool.oracle import SmallBankOracle, MultiKeyOracle
    if "smallbank" in MICRO_WORKLOADS:
        MICRO_WORKLOADS["smallbank"].oracle_instance = SmallBankOracle(
            expected_total=mw_mod._SB_TOTAL
        )
    if "multikey_transfer" in MICRO_WORKLOADS:
        MICRO_WORKLOADS["multikey_transfer"].oracle_instance = MultiKeyOracle(
            expected_total=mw_mod._MK_TOTAL
        )


# ---------------------------------------------------------------------------
# Cell runner
# ---------------------------------------------------------------------------

def _percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def run_cell_stress(
    *, db: str, dsn: str, workload_name: str,
    n_workers: int, duration_s: float, rep: int,
) -> CellResult:
    wl = MICRO_WORKLOADS[workload_name]
    runner = MicroRunner(
        workload=wl, db=db, dsn=dsn, n_workers=n_workers,
        variant="buggy", strategy="stress", delay_ms=0.0,
    )
    seen = set()
    bugs = 0
    trials = 0
    ttfb: Optional[float] = None
    trials_to_first: Optional[int] = None
    latencies: List[float] = []
    start = time.monotonic()
    while (time.monotonic() - start) < duration_s:
        tr = runner.run_trial()
        trials += 1
        latencies.append(tr.elapsed_ms)
        seen.add(FinalStateHash.compute(tr.canonical_state).value)
        if tr.bug_found:
            bugs += 1
            if ttfb is None:
                ttfb = time.monotonic() - start
                trials_to_first = trials
    elapsed = time.monotonic() - start
    return CellResult(
        db=db, workload=workload_name, strategy="stress",
        n_workers=n_workers, table_scale=0, rep=rep,
        duration_s=elapsed, trials=trials, unique_states=len(seen), bugs=bugs,
        time_to_first_bug_s=ttfb, trials_to_first_bug=trials_to_first,
        p50_latency_ms=_percentile(latencies, 50),
        p95_latency_ms=_percentile(latencies, 95),
        throughput_trials_per_s=trials / max(elapsed, 1e-6),
    )


def run_cell_pure(
    *, db: str, dsn: str, workload_name: str, strategy_name: str,
    n_workers: int, duration_s: float, rep: int, seed: int,
) -> CellResult:
    base = _build_strategy(strategy_name)
    cps, trs = run_pure_strategy(
        db=db, dsn=dsn, workload_name=workload_name,
        base_strategy=base, duration_s=duration_s,
        n_workers=n_workers, seed=seed, sample_interval_s=duration_s,
        rep=rep, strategy_label=strategy_name,
    )
    return _aggregate_cell(
        db, workload_name, strategy_name, n_workers, rep,
        cps[-1] if cps else None, trs,
    )


def run_cell_pool(
    *, db: str, dsn: str, workload_name: str, strategy_label: str,
    base_strategy_name: str, n_workers: int,
    duration_s: float, rep: int, seed: int,
) -> CellResult:
    base = _build_strategy(base_strategy_name)
    cps, trs, _ = run_pool_search(
        db=db, dsn=dsn, workload_name=workload_name,
        duration_s=duration_s, n_workers=n_workers,
        seed=seed, base_strategy=base,
        sample_interval_s=duration_s, rep=rep,
    )
    return _aggregate_cell(
        db, workload_name, strategy_label, n_workers, rep,
        cps[-1] if cps else None, trs,
    )


def _build_strategy(name: str):
    if name == "random":
        return RandomStrategy(max_delay_ms=80.0)
    if name == "heuristic":
        return HeuristicStrategy(k=4, min_delay_ms=5.0, max_delay_ms=50.0)
    if name == "pct":
        return PCTStrategy(max_depth=3, min_delay_ms=10.0, max_delay_ms=80.0)
    raise ValueError(f"unknown base strategy {name}")


def _aggregate_cell(
    db: str, workload: str, strategy: str, n_workers: int,
    rep: int, last_cp, trs,
) -> CellResult:
    duration = last_cp.time_s if last_cp else 0.0
    trials = last_cp.txns_done if last_cp else 0
    unique_states = last_cp.unique_states if last_cp else 0
    bugs = last_cp.bugs_found if last_cp else 0
    ttfb: Optional[float] = None
    trials_to_first: Optional[int] = None
    if trs:
        cumulative_t = 0
        running_wall = 0.0
        for i, tr in enumerate(trs):
            cumulative_t += 1
            running_wall += tr.elapsed_ms / 1000.0
            if tr.bug_found and ttfb is None:
                ttfb = running_wall
                trials_to_first = cumulative_t
                break
    latencies = [tr.elapsed_ms for tr in trs]
    return CellResult(
        db=db, workload=workload, strategy=strategy,
        n_workers=n_workers, table_scale=0, rep=rep,
        duration_s=duration, trials=trials,
        unique_states=unique_states, bugs=bugs,
        time_to_first_bug_s=ttfb, trials_to_first_bug=trials_to_first,
        p50_latency_ms=_percentile(latencies, 50),
        p95_latency_ms=_percentile(latencies, 95),
        throughput_trials_per_s=trials / max(duration, 1e-6),
    )


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

RAW_FIELDS = [
    "db", "workload", "strategy", "n_workers", "table_scale", "rep",
    "duration_s", "trials", "unique_states", "bugs",
    "time_to_first_bug_s", "trials_to_first_bug",
    "p50_latency_ms", "p95_latency_ms", "throughput_trials_per_s",
]


def _write_raw(rows: List[CellResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(RAW_FIELDS)
        for r in rows:
            w.writerow([
                r.db, r.workload, r.strategy, r.n_workers, r.table_scale, r.rep,
                f"{r.duration_s:.3f}", r.trials, r.unique_states, r.bugs,
                f"{r.time_to_first_bug_s:.3f}" if r.time_to_first_bug_s is not None else "",
                r.trials_to_first_bug if r.trials_to_first_bug is not None else "",
                f"{r.p50_latency_ms:.2f}", f"{r.p95_latency_ms:.2f}",
                f"{r.throughput_trials_per_s:.3f}",
            ])


def _write_summary(rows: List[CellResult], path: Path) -> None:
    """Aggregate over reps within each (db, wl, strat, workers, scale) cell."""
    groups: Dict[Tuple[str, str, str, int, int], List[CellResult]] = defaultdict(list)
    for r in rows:
        groups[(r.db, r.workload, r.strategy, r.n_workers, r.table_scale)].append(r)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "workload", "strategy", "n_workers", "table_scale", "n_reps",
            "trials_mean", "states_mean", "states_std", "bugs_mean",
            "ttfb_mean_s", "ttfb_min_s", "ttfb_finds",
            "throughput_mean",
            "p50_latency_mean", "p95_latency_mean",
        ])
        for (db, wl, strat, nw, sc), group in sorted(groups.items()):
            mean = statistics.mean
            std = lambda xs: statistics.stdev(xs) if len(xs) >= 2 else 0.0
            ttfbs = [g.time_to_first_bug_s for g in group if g.time_to_first_bug_s is not None]
            row = [
                db, wl, strat, nw, sc, len(group),
                f"{mean([g.trials for g in group]):.2f}",
                f"{mean([g.unique_states for g in group]):.2f}",
                f"{std([g.unique_states for g in group]):.2f}",
                f"{mean([g.bugs for g in group]):.2f}",
                f"{mean(ttfbs):.3f}" if ttfbs else "",
                f"{min(ttfbs):.3f}" if ttfbs else "",
                len(ttfbs),
                f"{mean([g.throughput_trials_per_s for g in group]):.3f}",
                f"{mean([g.p50_latency_ms for g in group]):.2f}",
                f"{mean([g.p95_latency_ms for g in group]):.2f}",
            ]
            w.writerow(row)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

DEFAULT_WORKLOADS = ["bank_overdraft", "multikey_transfer"]
DEFAULT_STRATS = ["stress", "random", "heuristic", "pct", "pool(heuristic)"]


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", nargs="+", default=["sqlite"])
    parser.add_argument("--workloads", nargs="+", default=None)
    parser.add_argument("--strategies", nargs="+", default=None)
    parser.add_argument("--workers", nargs="+", type=int,
                        default=[2, 4, 8, 16],
                        help="Worker counts to sweep")
    parser.add_argument("--scales", nargs="+", type=int,
                        default=[1, 2, 4],
                        help="Table-size scale factors (multiplies # rows)")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--reps", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    workloads = args.workloads or DEFAULT_WORKLOADS
    strategies = args.strategies or DEFAULT_STRATS
    out_dir = Path(args.output_dir) if args.output_dir else ROOT / "results" / "scalability"

    dsns: Dict[str, str] = {}
    for db in args.db:
        env_key = f"{db.upper()}_DSN"
        dsns[db] = os.getenv(env_key, DEFAULT_DSNS.get(db, ""))

    print("=" * 72)
    print("IsoTest Scalability Sweep")
    print("=" * 72)
    print(f"  Databases:    {args.db}")
    print(f"  Workloads:    {workloads}")
    print(f"  Strategies:   {strategies}")
    print(f"  Worker grid:  {args.workers}")
    print(f"  Scale grid:   {args.scales}")
    print(f"  Duration:     {args.duration}s per cell  Reps: {args.reps}")
    print(f"  Output:       {out_dir}")
    print("=" * 72)

    rows: List[CellResult] = []
    total = (
        len(args.db) * len(workloads) * len(strategies)
        * len(args.workers) * len(args.scales) * args.reps
    )
    done = 0

    for db in args.db:
        dsn = dsns[db]
        if not _db_available(db, dsn):
            print(f"\n  SKIP {db}")
            continue
        for wl in workloads:
            if wl not in MICRO_WORKLOADS:
                print(f"  SKIP unknown workload: {wl}")
                continue
            for scale in args.scales:
                prior = _apply_scale(scale)
                try:
                    for nw in args.workers:
                        for strat in strategies:
                            for rep in range(args.reps):
                                done += 1
                                seed = args.seed + rep * 1000 + nw * 7 + scale * 13
                                print(
                                    f"\n[{done}/{total}] "
                                    f"{db}/{wl} scale={scale}x workers={nw} "
                                    f"strat={strat} rep={rep}"
                                )
                                t0 = time.monotonic()
                                if strat == "stress":
                                    r = run_cell_stress(
                                        db=db, dsn=dsn, workload_name=wl,
                                        n_workers=nw, duration_s=args.duration,
                                        rep=rep,
                                    )
                                elif strat in ("random", "heuristic", "pct"):
                                    r = run_cell_pure(
                                        db=db, dsn=dsn, workload_name=wl,
                                        strategy_name=strat,
                                        n_workers=nw, duration_s=args.duration,
                                        rep=rep, seed=seed,
                                    )
                                elif strat in ("pool(random)", "pool(heuristic)"):
                                    base = "random" if "random" in strat else "heuristic"
                                    r = run_cell_pool(
                                        db=db, dsn=dsn, workload_name=wl,
                                        strategy_label=strat,
                                        base_strategy_name=base,
                                        n_workers=nw, duration_s=args.duration,
                                        rep=rep, seed=seed,
                                    )
                                else:
                                    print(f"  SKIP unknown strategy {strat}")
                                    continue
                                r.table_scale = scale
                                rows.append(r)
                                print(
                                    f"  -> {r.trials} trials, "
                                    f"{r.unique_states} states, "
                                    f"{r.bugs} bugs, "
                                    f"thr={r.throughput_trials_per_s:.1f}/s "
                                    f"({time.monotonic()-t0:.1f}s)"
                                )
                finally:
                    _restore_scale(prior)

    _write_raw(rows, out_dir / "scalability_raw.csv")
    _write_summary(rows, out_dir / "scalability.csv")
    print(f"\nWrote: {out_dir}/scalability.csv ({len(rows)} cells)")


if __name__ == "__main__":
    main()
