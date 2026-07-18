#!/usr/bin/env python3
"""
Delay-Sensitivity & Robustness Benchmark

Addresses reviewer concern: "How sensitive are results to the hardcoded
3–25 ms delay range, particularly in high-variance environments?"

Systematically runs exploration + replay campaigns across delay profiles
and records per-profile metrics:

  - Time-to-First-Bug (TTFB) in seconds
  - Replay Success Rate (fraction of replays that reproduce the bug)
  - State Coverage (unique FinalStateHash count at campaign end)

Output: CSV matrix at <output_dir>/sensitivity_matrix.csv

Usage:
  python scripts/evaluate_sensitivity.py --db sqlite
  python scripts/evaluate_sensitivity.py --db postgres --duration 120 --reps 5
  python scripts/evaluate_sensitivity.py --db sqlite --profiles 1-10 3-25 10-100 50-200
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.micro_runner import CoveragePoint, TrialResult
from scripts.experiments.micro_workloads import MICRO_WORKLOADS

from scripts.schedule_pool import (
    Schedule,
    ScheduleArtifact,
    DelayPoint,
    SchedulePool,
    ScheduleMutator,
    MutationConfig,
    PoolConfig,
    RetentionPolicy,
    FinalStateHash,
)
from scripts.schedule_pool.base_strategy import RandomStrategy, HeuristicStrategy
from scripts.schedule_pool.event_id import get_workload_event_ids
from scripts.schedule_pool.runner import SchedulePoolRunner, replay_schedule


# ---------------------------------------------------------------------------
# Delay profile definition
# ---------------------------------------------------------------------------

@dataclass
class DelayProfile:
    """A named delay range to benchmark."""
    name: str
    min_ms: float
    max_ms: float

    def __str__(self) -> str:
        return self.name


DEFAULT_PROFILES = [
    DelayProfile("1-5ms", 1.0, 5.0),
    DelayProfile("3-25ms", 3.0, 25.0),
    DelayProfile("10-100ms", 10.0, 100.0),
    DelayProfile("50-500ms", 50.0, 500.0),
]


def parse_profile(spec: str) -> DelayProfile:
    """Parse a profile spec like '3-25' into a DelayProfile."""
    spec = spec.strip().rstrip("ms")
    lo, hi = spec.split("-")
    lo_f, hi_f = float(lo), float(hi)
    return DelayProfile(name=f"{lo}-{hi}ms", min_ms=lo_f, max_ms=hi_f)


# ---------------------------------------------------------------------------
# Per-profile campaign result
# ---------------------------------------------------------------------------

@dataclass
class ProfileResult:
    """Aggregated result for one (profile, workload, db, rep) campaign."""
    profile: str
    workload: str
    db: str
    rep: int
    duration_s: float
    ttfb_s: Optional[float]        # None if no bug found
    replay_success_rate: Optional[float]  # None if no bug schedule to replay
    unique_states: int
    total_trials: int


# ---------------------------------------------------------------------------
# Campaign runner
# ---------------------------------------------------------------------------

def run_sensitivity_campaign(
    *,
    profile: DelayProfile,
    db: str,
    dsn: str,
    workload_name: str,
    duration_s: float,
    n_workers: int,
    seed: int,
    n_replays: int,
    **adapter_kwargs: Any,
) -> ProfileResult:
    """Run one exploration campaign under a specific delay profile.

    Uses the HeuristicStrategy with min/max from the profile so the
    generated seed schedules respect the profile's delay range.  The
    MutationConfig's add_delay bounds are also clamped to the profile.
    """
    wl = MICRO_WORKLOADS[workload_name]
    rng = random.Random(seed)
    all_events = get_workload_event_ids(workload_name, n_workers)

    # Strategy seeded with the profile's delay range
    strategy = HeuristicStrategy(
        k=4,
        min_delay_ms=profile.min_ms,
        max_delay_ms=profile.max_ms,
    )

    # Mutation config clamped to profile range
    mut_cfg = MutationConfig(
        add_delay_min_ms=profile.min_ms,
        add_delay_max_ms=profile.max_ms,
        max_per_event_delay_ms=profile.max_ms * 2,
        max_total_delay_ms=profile.max_ms * n_workers * 3,
    )

    pool = SchedulePool(config=PoolConfig())
    mutator = ScheduleMutator(all_event_ids=all_events, config=mut_cfg, rng=rng)
    retention = RetentionPolicy(pool)

    runner = SchedulePoolRunner(
        workload=wl,
        db=db,
        dsn=dsn,
        n_workers=n_workers,
        variant="buggy",
        **adapter_kwargs,
    )

    unique_hashes: set = set()
    total_trials = 0
    first_bug_time: Optional[float] = None
    first_bug_schedule: Optional[Schedule] = None
    start = time.monotonic()

    while (time.monotonic() - start) < duration_s:
        # Sample or seed
        parent = pool.sample(rng, workload=workload_name, db=db)
        if parent is not None:
            parent_schedule = parent.schedule
        else:
            parent_schedule = strategy.seed(all_events, rng)

        candidate = mutator.mutate(parent_schedule)

        tr = runner.run_trial_with_schedule(candidate)
        total_trials += 1
        unique_hashes.add(tr.canonical_hash)

        record = ScheduleArtifact(
            db=db,
            workload=workload_name,
            variant="buggy",
            strategy=f"sensitivity_{profile.name}",
            seed=seed,
            schedule=candidate,
            discovered_state_hash=tr.canonical_hash,
            bug_found=tr.bug_found,
            bug_reason=tr.bug_reason,
        )
        retention.accept(record)

        if tr.bug_found and first_bug_time is None:
            first_bug_time = time.monotonic() - start
            first_bug_schedule = candidate

    # Replay phase: measure replay success rate for the first bug schedule
    replay_rate: Optional[float] = None
    if first_bug_schedule is not None and n_replays > 0:
        replay_result = replay_schedule(
            db=db,
            dsn=dsn,
            workload_name=workload_name,
            schedule=first_bug_schedule,
            variant="buggy",
            n_workers=n_workers,
            n_replays=n_replays,
            **adapter_kwargs,
        )
        replay_rate = replay_result["bug_rate"]

    return ProfileResult(
        profile=profile.name,
        workload=workload_name,
        db=db,
        rep=seed,
        duration_s=duration_s,
        ttfb_s=round(first_bug_time, 4) if first_bug_time is not None else None,
        replay_success_rate=round(replay_rate, 4) if replay_rate is not None else None,
        unique_states=len(unique_hashes),
        total_trials=total_trials,
    )


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "profile",
    "workload",
    "db",
    "rep",
    "duration_s",
    "ttfb_s",
    "replay_success_rate",
    "unique_states",
    "total_trials",
]


def write_results_csv(results: List[ProfileResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "profile": r.profile,
                "workload": r.workload,
                "db": r.db,
                "rep": r.rep,
                "duration_s": r.duration_s,
                "ttfb_s": r.ttfb_s if r.ttfb_s is not None else "",
                "replay_success_rate": r.replay_success_rate if r.replay_success_rate is not None else "",
                "unique_states": r.unique_states,
                "total_trials": r.total_trials,
            })


def write_summary_csv(results: List[ProfileResult], path: Path) -> None:
    """Write aggregated summary (mean/std across reps) per profile+workload."""
    from collections import defaultdict

    groups: Dict[Tuple[str, str, str], List[ProfileResult]] = defaultdict(list)
    for r in results:
        groups[(r.profile, r.workload, r.db)].append(r)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "profile", "workload", "db", "reps",
            "ttfb_mean_s", "ttfb_std_s", "ttfb_found_pct",
            "replay_rate_mean", "replay_rate_std",
            "coverage_mean", "coverage_std",
            "trials_mean",
        ])
        for (profile, workload, db), group in sorted(groups.items()):
            n = len(group)
            ttfbs = [r.ttfb_s for r in group if r.ttfb_s is not None]
            replays = [r.replay_success_rate for r in group if r.replay_success_rate is not None]
            coverages = [r.unique_states for r in group]
            trials = [r.total_trials for r in group]

            def _mean(xs: list) -> str:
                return f"{statistics.mean(xs):.4f}" if xs else ""

            def _std(xs: list) -> str:
                return f"{statistics.stdev(xs):.4f}" if len(xs) >= 2 else ""

            writer.writerow([
                profile, workload, db, n,
                _mean(ttfbs), _std(ttfbs),
                f"{len(ttfbs)/n:.2f}" if n > 0 else "",
                _mean(replays), _std(replays),
                _mean(coverages), _std(coverages),
                _mean(trials),
            ])


# ---------------------------------------------------------------------------
# DB availability check
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delay-Sensitivity & Robustness Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/evaluate_sensitivity.py --db sqlite
  python scripts/evaluate_sensitivity.py --db sqlite --profiles 1-10 3-25 10-100 50-200
  python scripts/evaluate_sensitivity.py --db postgres mysql sqlite --duration 120 --reps 5
        """,
    )
    parser.add_argument(
        "--db", nargs="+", default=["sqlite"],
        help="Database(s) to test (default: sqlite)",
    )
    parser.add_argument(
        "--profiles", nargs="+", default=None,
        help="Delay profiles as MIN-MAX (e.g. 3-25 10-100). Default: 1-5 3-25 10-100 50-500",
    )
    parser.add_argument(
        "--workloads", nargs="+", default=None,
        help="Workload names (default: all three micro workloads)",
    )
    parser.add_argument(
        "--duration", type=float, default=60.0,
        help="Campaign duration in seconds per (profile, workload, rep) (default: 60)",
    )
    parser.add_argument(
        "--reps", type=int, default=3,
        help="Number of repetitions per (profile, workload) (default: 3)",
    )
    parser.add_argument(
        "--n-workers", type=int, default=4,
        help="Concurrent sessions per trial (default: 4)",
    )
    parser.add_argument(
        "--n-replays", type=int, default=20,
        help="Number of replays for measuring replay success rate (default: 20)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Base random seed (each rep uses seed + rep) (default: 42)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: results/sensitivity/)",
    )
    args = parser.parse_args()

    # Parse profiles
    if args.profiles:
        profiles = [parse_profile(p) for p in args.profiles]
    else:
        profiles = list(DEFAULT_PROFILES)

    workloads = args.workloads or WORKLOADS

    output_dir = Path(args.output_dir) if args.output_dir else ROOT / "results" / "sensitivity"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve DSNs
    dsns: Dict[str, str] = {}
    for db in args.db:
        env_key = f"{db.upper()}_DSN"
        dsns[db] = os.getenv(env_key, DEFAULT_DSNS.get(db, ""))

    print("=" * 70)
    print("IsoTest Delay-Sensitivity & Robustness Benchmark")
    print("=" * 70)
    print(f"  Databases:  {args.db}")
    print(f"  Profiles:   {[str(p) for p in profiles]}")
    print(f"  Workloads:  {workloads}")
    print(f"  Duration:   {args.duration}s per campaign")
    print(f"  Reps:       {args.reps}")
    print(f"  Replays:    {args.n_replays} per bug schedule")
    print(f"  Output:     {output_dir}")
    print("=" * 70)

    all_results: List[ProfileResult] = []
    total_campaigns = len(args.db) * len(profiles) * len(workloads) * args.reps
    done = 0

    for db in args.db:
        dsn = dsns.get(db, "")
        if not _db_available(db, dsn):
            print(f"\n  SKIP {db} (not available at {dsn})")
            continue

        for profile in profiles:
            for wl_name in workloads:
                if wl_name not in MICRO_WORKLOADS:
                    print(f"  SKIP unknown workload: {wl_name}")
                    continue

                for rep in range(args.reps):
                    done += 1
                    campaign_seed = args.seed + rep
                    print(
                        f"\n[{done}/{total_campaigns}] "
                        f"{db} / {profile} / {wl_name} / rep={rep} "
                        f"(seed={campaign_seed})"
                    )

                    t0 = time.monotonic()
                    result = run_sensitivity_campaign(
                        profile=profile,
                        db=db,
                        dsn=dsn,
                        workload_name=wl_name,
                        duration_s=args.duration,
                        n_workers=args.n_workers,
                        seed=campaign_seed,
                        n_replays=args.n_replays,
                    )
                    elapsed = time.monotonic() - t0
                    all_results.append(result)

                    bug_str = (
                        f"TTFB={result.ttfb_s:.2f}s"
                        if result.ttfb_s is not None
                        else "no bug"
                    )
                    replay_str = (
                        f"replay={result.replay_success_rate:.0%}"
                        if result.replay_success_rate is not None
                        else "n/a"
                    )
                    print(
                        f"  → {result.total_trials} trials, "
                        f"{result.unique_states} states, "
                        f"{bug_str}, {replay_str} "
                        f"({elapsed:.1f}s wall)"
                    )

    # Write outputs
    raw_path = output_dir / "sensitivity_matrix.csv"
    write_results_csv(all_results, raw_path)
    print(f"\nRaw results:    {raw_path}")

    summary_path = output_dir / "sensitivity_summary.csv"
    write_summary_csv(all_results, summary_path)
    print(f"Summary:        {summary_path}")

    # Print summary table to stdout
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    from collections import defaultdict
    groups: Dict[Tuple[str, str, str], List[ProfileResult]] = defaultdict(list)
    for r in all_results:
        groups[(r.db, r.profile, r.workload)].append(r)

    header = f"{'DB':<10} {'Profile':<12} {'Workload':<20} {'TTFB(s)':<12} {'Replay%':<10} {'States':<8}"
    print(header)
    print("-" * len(header))
    for (db, profile, wl), group in sorted(groups.items()):
        ttfbs = [r.ttfb_s for r in group if r.ttfb_s is not None]
        replays = [r.replay_success_rate for r in group if r.replay_success_rate is not None]
        coverages = [r.unique_states for r in group]

        ttfb_str = f"{statistics.mean(ttfbs):.2f}" if ttfbs else "—"
        replay_str = f"{statistics.mean(replays):.0%}" if replays else "—"
        cov_str = f"{statistics.mean(coverages):.1f}"
        print(f"{db:<10} {profile:<12} {wl:<20} {ttfb_str:<12} {replay_str:<10} {cov_str:<8}")

    print(f"\nDone. {len(all_results)} campaigns completed.")


if __name__ == "__main__":
    main()
