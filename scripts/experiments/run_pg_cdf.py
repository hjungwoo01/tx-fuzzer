#!/usr/bin/env python3
"""
PG UCO CDF experiment: 5 reps × 4 strategies, 3 PG 12.3 containers in parallel.

Produces time-to-first-bug CDF data for the hardened PostgreSQL
update_conflict_out case study.  Each rep gets a 2h budget.

3 containers run jobs concurrently via a ThreadPoolExecutor.
Results are written incrementally so partial runs are usable.

Usage:
  python scripts/experiments/run_pg_cdf.py [--output-dir DIR] [--dry-run] [--smoke]
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.run_final_eval import (
    _pg_uco_trial_with_delays_hard,
    run_case_pool_search,
    CasePoolResult,
    PG_UCO_EVENTS_HARD,
)
from scripts.schedule_pool import (
    SchedulePool,
    ScheduleMutator,
    MutationConfig,
)
from scripts.schedule_pool.pool import PoolConfig

try:
    from scripts.discover_update_conflict_out import (
        setup_schema as _uco_setup_schema,
        reset_tables as _uco_reset_tables,
    )
    _HAS_UCO = True
except ImportError:
    _HAS_UCO = False


# 3 PG 12.3 buggy containers
PG_DSNS = [
    "postgres://postgres:postgres@localhost:54323/postgres",
    "postgres://postgres:postgres@localhost:54327/postgres",
    "postgres://postgres:postgres@localhost:54328/postgres",
]
PG_CONTROL_DSN = "postgres://postgres:postgres@localhost:54324/postgres"

STRATEGIES = ["stress", "random", "heuristic", "schedule_pool"]
REPS = 5
SEEDS = [42, 10042, 20042, 30042, 40042]
REP_BUDGET_S = 7200.0  # 2 hours per rep

REPLAY_COUNT = 20  # replay follow-up per bug-finding schedule


@dataclass
class CDFRow:
    strategy: str
    rep: int
    seed: int
    bug_found: bool
    ttfb_s: Optional[float]
    trials_to_first: Optional[int]
    total_trials: int
    total_time_s: float
    replay_buggy_hits: Optional[int] = None
    replay_buggy_total: Optional[int] = None
    replay_buggy_rate: Optional[float] = None
    replay_control_hits: Optional[int] = None
    replay_control_total: Optional[int] = None
    schedule_json: Optional[str] = None
    pool_size: Optional[int] = None
    container_port: Optional[int] = None


def _gen_strategy_delays(strategy: str, event_ids: List[str], rng: random.Random) -> Dict[str, float]:
    if strategy == "stress":
        return {}
    elif strategy == "random":
        return {eid: rng.uniform(0, 0.100) for eid in event_ids}
    elif strategy == "heuristic":
        k = min(4, len(event_ids))
        chosen = rng.sample(event_ids, k)
        return {eid: rng.uniform(0.005, 0.050) for eid in chosen}
    return {}


def _run_replay(dsn: str, delays: Dict[str, float], count: int) -> int:
    """Run exact replays and return number of bug hits."""
    hits = 0
    for _ in range(count):
        try:
            _uco_setup_schema(dsn)
            r = _pg_uco_trial_with_delays_hard(dsn, delays)
            if r.get("bug_found"):
                hits += 1
        except Exception:
            pass
    return hits


def run_one_job(strategy: str, rep: int, seed: int, dsn: str,
                budget_s: float) -> CDFRow:
    """Run a single (strategy, rep) on a given DSN. Thread-safe."""
    port = int(dsn.split(":")[3].split("/")[0])
    label = f"{strategy}/rep{rep} (:{port})"
    print(f"  [START] {label}  seed={seed}  budget={budget_s:.0f}s", flush=True)

    _uco_setup_schema(dsn)

    if strategy == "schedule_pool":
        pr = run_case_pool_search(
            trial_fn=lambda d, _dsn=dsn: _pg_uco_trial_with_delays_hard(_dsn, d),
            event_ids=PG_UCO_EVENTS_HARD,
            db="postgres",
            case_name="update_conflict_out",
            duration_s=budget_s,
            seed=seed,
            hot_events=["foo_commit", "bar_commit", "foo_select",
                        "bar_select", "trouble_update", "trouble_abort"],
        )

        # Replay follow-up if bug found
        replay_buggy_hits = None
        replay_buggy_total = None
        replay_control_hits = None
        replay_control_total = None
        schedule_json = None

        if pr.bug_found and pr.first_bug_schedule:
            delays = pr.first_bug_schedule.to_delay_map()
            schedule_json = json.dumps(delays, sort_keys=True)
            replay_buggy_hits = _run_replay(dsn, delays, REPLAY_COUNT)
            replay_buggy_total = REPLAY_COUNT
            replay_control_hits = _run_replay(PG_CONTROL_DSN, delays, REPLAY_COUNT)
            replay_control_total = REPLAY_COUNT

        row = CDFRow(
            strategy=strategy, rep=rep, seed=seed,
            bug_found=pr.bug_found,
            ttfb_s=pr.ttfb_s,
            trials_to_first=pr.trials_to_first,
            total_trials=pr.total_trials,
            total_time_s=pr.total_time_s,
            replay_buggy_hits=replay_buggy_hits,
            replay_buggy_total=replay_buggy_total,
            replay_buggy_rate=(replay_buggy_hits / REPLAY_COUNT
                               if replay_buggy_hits is not None else None),
            replay_control_hits=replay_control_hits,
            replay_control_total=replay_control_total,
            schedule_json=schedule_json,
            pool_size=pr.pool_size,
            container_port=port,
        )
    else:
        # stress / random / heuristic
        rng = random.Random(seed + rep)
        start = time.monotonic()
        trial = 0
        bug_found = False
        ttfb_s = None
        trials_to_first = None
        first_delays = None

        for trial in range(1, 1_000_000):
            if (time.monotonic() - start) > budget_s:
                break
            delays = _gen_strategy_delays(strategy, PG_UCO_EVENTS_HARD, rng)
            _uco_reset_tables(dsn)
            r = _pg_uco_trial_with_delays_hard(dsn, delays)
            if r["bug_found"]:
                bug_found = True
                ttfb_s = time.monotonic() - start
                trials_to_first = trial
                first_delays = delays
                break

        total_time = time.monotonic() - start

        # Replay follow-up if bug found
        replay_buggy_hits = None
        replay_buggy_total = None
        replay_control_hits = None
        replay_control_total = None
        schedule_json = None

        if bug_found and first_delays is not None:
            schedule_json = json.dumps(first_delays, sort_keys=True)
            replay_buggy_hits = _run_replay(dsn, first_delays, REPLAY_COUNT)
            replay_buggy_total = REPLAY_COUNT
            replay_control_hits = _run_replay(PG_CONTROL_DSN, first_delays, REPLAY_COUNT)
            replay_control_total = REPLAY_COUNT

        row = CDFRow(
            strategy=strategy, rep=rep, seed=seed,
            bug_found=bug_found,
            ttfb_s=ttfb_s,
            trials_to_first=trials_to_first,
            total_trials=trial,
            total_time_s=total_time,
            replay_buggy_hits=replay_buggy_hits,
            replay_buggy_total=replay_buggy_total,
            replay_buggy_rate=(replay_buggy_hits / REPLAY_COUNT
                               if replay_buggy_hits is not None else None),
            replay_control_hits=replay_control_hits,
            replay_control_total=replay_control_total,
            schedule_json=schedule_json,
            container_port=port,
        )

    elapsed_str = f"{row.total_time_s:.1f}s"
    if row.bug_found:
        print(f"  [DONE]  {label}  BUG at {row.ttfb_s:.1f}s  trial#{row.trials_to_first}  "
              f"replay={row.replay_buggy_hits}/{row.replay_buggy_total}  "
              f"({elapsed_str} total)", flush=True)
    else:
        print(f"  [DONE]  {label}  NO BUG  trials={row.total_trials}  ({elapsed_str} total)", flush=True)

    return row


@dataclass
class Job:
    strategy: str
    rep: int
    seed: int
    priority: int  # lower = run first


def build_jobs() -> List[Job]:
    """Build all jobs, ordered so fast ones go first and slow ones last."""
    jobs = []
    for strat in STRATEGIES:
        for rep_idx in range(REPS):
            # Priority: stress=0 (fast), heuristic=1 (fast), schedule_pool=2 (slow), random=3 (slowest)
            priority = {"stress": 0, "heuristic": 1, "schedule_pool": 2, "random": 3}[strat]
            jobs.append(Job(
                strategy=strat, rep=rep_idx,
                seed=SEEDS[rep_idx],
                priority=priority,
            ))
    jobs.sort(key=lambda j: (j.priority, j.rep))
    return jobs


def run_experiment(output_dir: Path, dry_run: bool = False,
                   budget_s: float = REP_BUDGET_S) -> List[CDFRow]:
    """Run all jobs across 3 containers in parallel."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "pg_cdf_results.csv"

    jobs = build_jobs()
    print(f"\n{'='*60}")
    print(f"  PG UCO CDF Experiment")
    print(f"  {len(jobs)} jobs = {len(STRATEGIES)} strategies × {REPS} reps")
    print(f"  Budget per rep: {budget_s:.0f}s ({budget_s/3600:.1f}h)")
    print(f"  Containers: {len(PG_DSNS)} × PG 12.3")
    print(f"  Output: {output_dir}")
    print(f"{'='*60}")

    if dry_run:
        for j in jobs:
            print(f"  [DRY] {j.strategy}/rep{j.rep}  seed={j.seed}  prio={j.priority}")
        return []

    # DSN pool: round-robin assignment managed by the executor
    dsn_lock = threading.Lock()
    dsn_available = list(PG_DSNS)  # available DSNs
    dsn_condition = threading.Condition(dsn_lock)

    def acquire_dsn() -> str:
        with dsn_condition:
            while not dsn_available:
                dsn_condition.wait()
            return dsn_available.pop(0)

    def release_dsn(dsn: str) -> None:
        with dsn_condition:
            dsn_available.append(dsn)
            dsn_condition.notify()

    results: List[CDFRow] = []
    results_lock = threading.Lock()

    # Write CSV header
    header = [
        "strategy", "rep", "seed", "bug_found", "ttfb_s",
        "trials_to_first", "total_trials", "total_time_s",
        "replay_buggy_hits", "replay_buggy_total", "replay_buggy_rate",
        "replay_control_hits", "replay_control_total",
        "schedule_json", "pool_size", "container_port",
    ]
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()

    def _run_job(job: Job) -> CDFRow:
        dsn = acquire_dsn()
        try:
            row = run_one_job(job.strategy, job.rep, job.seed, dsn, budget_s)
        finally:
            release_dsn(dsn)

        # Append to CSV incrementally
        with results_lock:
            results.append(row)
            with open(csv_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=header)
                w.writerow(asdict(row))

        # Also write per-job JSON
        job_dir = output_dir / "jobs" / job.strategy
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / f"rep{job.rep}.json").write_text(
            json.dumps(asdict(row), indent=2, default=str), encoding="utf-8")
        return row

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=len(PG_DSNS)) as pool:
        futures = {pool.submit(_run_job, j): j for j in jobs}
        for fut in as_completed(futures):
            job = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                print(f"  [ERROR] {job.strategy}/rep{job.rep}: {exc}", flush=True)

    wall_time = time.monotonic() - t0
    print(f"\n{'='*60}")
    print(f"  All {len(jobs)} jobs complete in {wall_time:.0f}s ({wall_time/3600:.1f}h)")
    print(f"  Results: {csv_path}")
    print(f"{'='*60}")

    # Summary table
    _print_summary(results, output_dir)

    return results


def _print_summary(results: List[CDFRow], output_dir: Path) -> None:
    """Print and save a summary table."""
    lines = []
    lines.append(f"\n{'Strategy':<15} {'Rep':<5} {'Bug?':<6} {'TTFB(s)':<12} "
                 f"{'Trials':<8} {'Replay':<12}")
    lines.append("-" * 65)

    for strat in STRATEGIES:
        strat_rows = sorted([r for r in results if r.strategy == strat],
                           key=lambda r: r.rep)
        for r in strat_rows:
            ttfb_str = f"{r.ttfb_s:.1f}" if r.ttfb_s is not None else "—"
            replay_str = (f"{r.replay_buggy_hits}/{r.replay_buggy_total}"
                         if r.replay_buggy_total else "—")
            lines.append(f"{r.strategy:<15} {r.rep:<5} "
                        f"{'YES' if r.bug_found else 'NO':<6} "
                        f"{ttfb_str:<12} {r.total_trials:<8} {replay_str:<12}")

        # Aggregate
        found = [r for r in strat_rows if r.bug_found]
        if found:
            ttfbs = [r.ttfb_s for r in found]
            median = sorted(ttfbs)[len(ttfbs) // 2]
            lines.append(f"  → {len(found)}/{len(strat_rows)} found  "
                        f"median TTFB={median:.1f}s  "
                        f"min={min(ttfbs):.1f}s  max={max(ttfbs):.1f}s")
        else:
            lines.append(f"  → 0/{len(strat_rows)} found")
        lines.append("")

    text = "\n".join(lines)
    print(text)
    (output_dir / "summary.txt").write_text(text, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="PG UCO CDF experiment")
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--smoke", action="store_true",
                        help="Quick smoke test: 2 reps, 30s budget")
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(args.output_dir) if args.output_dir else ROOT / "results" / f"pg_cdf_{ts}"

    budget = REP_BUDGET_S
    if args.smoke:
        global REPS, SEEDS
        REPS = 2
        SEEDS = SEEDS[:2]
        budget = 30.0

    run_experiment(out, dry_run=args.dry_run, budget_s=budget)


if __name__ == "__main__":
    main()
