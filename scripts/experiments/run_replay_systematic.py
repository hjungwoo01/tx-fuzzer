"""
Systematic replay/determinism evaluation.

Goal: establish empirical reproducibility evidence across the full matrix:
  - 3 DBs (postgres, mysql, sqlite) × 3 micro workloads × up to 5 schedules
  - 20 buggy replays + 20 control replays per schedule
  - Case study replays: PG UCO, MySQL NRR, SQLite dirty read

Outputs:
  results/replay_systematic_<timestamp>/
    determinism_matrix.csv      — full 9-cell matrix
    case_study_replay.csv       — case study replays
    summary.txt                 — human-readable summary
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import make_adapter
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
from scripts.schedule_pool import (
    Schedule, SchedulePool, PoolConfig, ScheduleMutator, MutationConfig,
    ScheduleRecord,
)
from scripts.schedule_pool.base_strategy import RandomStrategy
from scripts.schedule_pool.runner import (
    SchedulePoolRunner, run_pool_search, replay_schedule,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MICRO_DSNS = {
    "postgres": "postgres://postgres:postgres@localhost:54323/postgres",
    "mysql": "mysql://root:root@localhost:33080/test",
    "sqlite": "file:replay_eval_shared?mode=memory&cache=shared",
}

WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]

DISCOVERY_BUDGET_S = 60.0       # seconds per discovery run
N_SCHEDULES_TARGET = 5          # distinct bug-triggering schedules per cell
N_REPLAYS = 20                  # replays per schedule × variant
N_WORKERS = 4
SEED_BASE = 42

ADAPTER_KWARGS = {
    "sqlite": {"sqlite_shared_cache": True, "sqlite_read_uncommitted": True},
}

# Case study configuration
CASE_PG_BUGGY_DSN = "postgres://postgres:postgres@localhost:54323/postgres"
CASE_PG_CONTROL_DSN = "postgres://postgres:postgres@localhost:54324/postgres"
CASE_MYSQL_DSN = "mysql://root:root@localhost:33080/test"
CASE_SQLITE_URI = "file:replay_eval_case_shared?mode=memory&cache=shared"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ReplayRow:
    db: str
    workload: str
    schedule_id: str
    schedule_idx: int       # 0-based index among found schedules
    delay_count: int
    total_delay_ms: float
    buggy_replays: int
    buggy_hits: int
    buggy_rate: float
    control_replays: int
    control_hits: int
    control_rate: float
    stable: bool
    unique_hashes_buggy: int
    unique_hashes_control: int


@dataclass
class CaseReplayRow:
    db: str
    case_name: str
    schedule_idx: int
    delay_count: int
    total_delay_ms: float
    schedule_json: str
    buggy_replays: int
    buggy_hits: int
    buggy_rate: float
    control_replays: int
    control_hits: int
    control_rate: float
    stable: bool


# ---------------------------------------------------------------------------
# Helper: discover multiple bug schedules
# ---------------------------------------------------------------------------

def discover_bug_schedules(
    db: str,
    dsn: str,
    workload_name: str,
    n_target: int,
    budget_s: float,
    seed: int,
    **adapter_kwargs: Any,
) -> List[Schedule]:
    """
    Run pool search, collecting up to n_target distinct bug-triggering
    schedules by running multiple discovery passes with different seeds.
    """
    found: List[Schedule] = []
    found_hashes: Set[str] = set()

    for attempt in range(n_target * 3):  # up to 3x attempts
        if len(found) >= n_target:
            break
        s = seed + attempt * 7919

        try:
            _, _, pool = run_pool_search(
                db=db,
                dsn=dsn,
                workload_name=workload_name,
                variant="buggy",
                duration_s=budget_s,
                n_workers=N_WORKERS,
                seed=s,
                base_strategy=RandomStrategy(max_delay_ms=100.0),
                **adapter_kwargs,
            )

            for rec in pool.list(workload=workload_name, db=db, bug_only=True):
                h = rec.schedule.content_hash()
                if h not in found_hashes:
                    found_hashes.add(h)
                    found.append(rec.schedule)
                    if len(found) >= n_target:
                        break
        except Exception as e:
            print(f"    [WARN] Discovery attempt {attempt} failed: {e}")

    return found


# ---------------------------------------------------------------------------
# Micro workload matrix
# ---------------------------------------------------------------------------

def run_micro_matrix(output_dir: Path) -> List[ReplayRow]:
    """Run the full 3×3 micro workload determinism matrix."""
    rows: List[ReplayRow] = []

    for db in ["postgres", "mysql", "sqlite"]:
        dsn = MICRO_DSNS[db]
        kw = ADAPTER_KWARGS.get(db, {})

        for wl_name in WORKLOADS:
            label = f"{db}/{wl_name}"
            print(f"\n{'='*60}")
            print(f"  {label}: discovering up to {N_SCHEDULES_TARGET} schedules...")
            print(f"{'='*60}")

            schedules = discover_bug_schedules(
                db=db, dsn=dsn, workload_name=wl_name,
                n_target=N_SCHEDULES_TARGET,
                budget_s=DISCOVERY_BUDGET_S,
                seed=SEED_BASE, **kw,
            )

            if not schedules:
                print(f"    No bug schedules found for {label}")
                rows.append(ReplayRow(
                    db=db, workload=wl_name, schedule_id="none",
                    schedule_idx=0, delay_count=0, total_delay_ms=0,
                    buggy_replays=0, buggy_hits=0, buggy_rate=0.0,
                    control_replays=0, control_hits=0, control_rate=0.0,
                    stable=False, unique_hashes_buggy=0, unique_hashes_control=0,
                ))
                continue

            print(f"    Found {len(schedules)} distinct bug schedule(s)")

            for idx, sched in enumerate(schedules):
                sid = sched.content_hash()[:12]
                print(f"\n    [{idx+1}/{len(schedules)}] Schedule {sid} "
                      f"({sched.delay_count} delays, {sched.total_delay_ms:.1f}ms)")

                # Replay on buggy
                print(f"      Buggy replay ({N_REPLAYS}x)...", end=" ", flush=True)
                buggy = replay_schedule(
                    db=db, dsn=dsn, workload_name=wl_name,
                    schedule=sched, variant="buggy",
                    n_workers=N_WORKERS, n_replays=N_REPLAYS, **kw,
                )
                print(f"→ {buggy['bug_count']}/{buggy['total']}")

                # Replay on control (fixed code, same DB)
                print(f"      Control replay ({N_REPLAYS}x)...", end=" ", flush=True)
                control = replay_schedule(
                    db=db, dsn=dsn, workload_name=wl_name,
                    schedule=sched, variant="fixed",
                    n_workers=N_WORKERS, n_replays=N_REPLAYS, **kw,
                )
                print(f"→ {control['bug_count']}/{control['total']}")

                stable = (
                    buggy["bug_count"] == buggy["total"]
                    and control["bug_count"] == 0
                )

                rows.append(ReplayRow(
                    db=db, workload=wl_name, schedule_id=sid,
                    schedule_idx=idx,
                    delay_count=sched.delay_count,
                    total_delay_ms=round(sched.total_delay_ms, 2),
                    buggy_replays=buggy["total"],
                    buggy_hits=buggy["bug_count"],
                    buggy_rate=round(buggy["bug_rate"], 4),
                    control_replays=control["total"],
                    control_hits=control["bug_count"],
                    control_rate=round(control["bug_rate"], 4),
                    stable=stable,
                    unique_hashes_buggy=buggy["unique_canonical_hashes"],
                    unique_hashes_control=control["unique_canonical_hashes"],
                ))

    return rows


# ---------------------------------------------------------------------------
# Case study replays
# ---------------------------------------------------------------------------

def run_case_study_replays(output_dir: Path) -> List[CaseReplayRow]:
    """Replay case study schedules on buggy + control."""
    from scripts.experiments.run_final_eval import (
        _pg_uco_trial_with_delays,
        _mysql_rr_trial_with_delays,
        _sqlite_dirty_trial_with_delays,
        run_case_pool_search,
        PG_UCO_EVENTS, MYSQL_RR_EVENTS, SQLITE_DIRTY_EVENTS,
    )
    from scripts.discover_update_conflict_out import setup_schema as _uco_setup_schema

    rows: List[CaseReplayRow] = []
    cases = [
        {
            "db": "postgres",
            "case_name": "update_conflict_out",
            "event_ids": PG_UCO_EVENTS,
            "trial_fn": lambda d: _pg_uco_trial_with_delays(CASE_PG_BUGGY_DSN, d),
            "buggy_trial_fn": lambda d: _pg_uco_trial_with_delays(CASE_PG_BUGGY_DSN, d),
            "control_trial_fn": lambda d: _pg_uco_trial_with_delays(CASE_PG_CONTROL_DSN, d),
            "setup_fn": lambda: _uco_setup_schema(CASE_PG_BUGGY_DSN),
            "setup_control_fn": lambda: _uco_setup_schema(CASE_PG_CONTROL_DSN),
            "hot_events": ["foo_commit", "bar_commit", "foo_select",
                           "bar_select", "trouble_update", "trouble_abort"],
        },
        {
            "db": "mysql",
            "case_name": "rr_nonexistent_row",
            "event_ids": MYSQL_RR_EVENTS,
            "trial_fn": lambda d: _mysql_rr_trial_with_delays(CASE_MYSQL_DSN, d),
            "buggy_trial_fn": lambda d: _mysql_rr_trial_with_delays(CASE_MYSQL_DSN, d),
            "control_trial_fn": None,  # no MySQL control container
            "setup_fn": None,
            "setup_control_fn": None,
            "hot_events": ["t1_begin", "t1_update", "t3_snap", "t3_update"],
        },
        {
            "db": "sqlite",
            "case_name": "narrow_dirty_read",
            "event_ids": SQLITE_DIRTY_EVENTS,
            "trial_fn": lambda d: _sqlite_dirty_trial_with_delays(CASE_SQLITE_URI, d),
            "buggy_trial_fn": lambda d: _sqlite_dirty_trial_with_delays(CASE_SQLITE_URI, d),
            "control_trial_fn": None,  # no separate control for SQLite
            "setup_fn": None,
            "setup_control_fn": None,
            "hot_events": ["writer_begin", "writer_update", "reader_begin",
                           "reader_select"],
        },
    ]

    for case in cases:
        db = case["db"]
        case_name = case["case_name"]
        print(f"\n{'='*60}")
        print(f"  Case study: {db}/{case_name}")
        print(f"{'='*60}")

        # Discover bug schedules
        found_schedules: List[Dict[str, float]] = []
        found_hashes: Set[str] = set()

        for attempt in range(N_SCHEDULES_TARGET * 3):
            if len(found_schedules) >= N_SCHEDULES_TARGET:
                break
            seed = SEED_BASE + attempt * 7919

            if case.get("setup_fn"):
                try:
                    case["setup_fn"]()
                except Exception:
                    pass

            print(f"    Discovery attempt {attempt} (seed={seed})...")
            try:
                pr = run_case_pool_search(
                    trial_fn=case["trial_fn"],
                    event_ids=case["event_ids"],
                    db=db,
                    case_name=case_name,
                    duration_s=DISCOVERY_BUDGET_S,
                    seed=seed,
                    hot_events=case.get("hot_events"),
                )

                if pr.bug_found and pr.first_bug_schedule:
                    delays = pr.first_bug_schedule.to_delay_map()
                    h = pr.first_bug_schedule.content_hash()
                    if h not in found_hashes:
                        found_hashes.add(h)
                        found_schedules.append(delays)
                        print(f"      → Bug found! Schedule {h[:12]} "
                              f"({len(delays)} delays)")

                # Also check pool for additional schedules
                if pr.pool:
                    for rec in pr.pool.list(bug_only=True):
                        h = rec.schedule.content_hash()
                        if h not in found_hashes:
                            found_hashes.add(h)
                            found_schedules.append(rec.schedule.to_delay_map())
                            if len(found_schedules) >= N_SCHEDULES_TARGET:
                                break
            except Exception as e:
                print(f"      [WARN] {e}")

        if not found_schedules:
            print(f"    No bug schedules found for {db}/{case_name}")
            rows.append(CaseReplayRow(
                db=db, case_name=case_name, schedule_idx=0,
                delay_count=0, total_delay_ms=0, schedule_json="{}",
                buggy_replays=0, buggy_hits=0, buggy_rate=0.0,
                control_replays=0, control_hits=0, control_rate=0.0,
                stable=False,
            ))
            continue

        print(f"    Found {len(found_schedules)} distinct bug schedule(s)")

        for idx, delays in enumerate(found_schedules):
            delay_count = len(delays)
            total_ms = sum(v * 1000 for v in delays.values())
            sched_json = json.dumps(delays, sort_keys=True)

            print(f"\n    [{idx+1}/{len(found_schedules)}] "
                  f"{delay_count} delays, {total_ms:.1f}ms total")

            # Replay on buggy
            if case.get("setup_fn"):
                try:
                    case["setup_fn"]()
                except Exception:
                    pass

            buggy_hits = 0
            print(f"      Buggy replay ({N_REPLAYS}x)...", end=" ", flush=True)
            for _ in range(N_REPLAYS):
                if case.get("setup_fn"):
                    try:
                        case["setup_fn"]()
                    except Exception:
                        pass
                try:
                    r = case["buggy_trial_fn"](delays)
                    if r.get("bug_found"):
                        buggy_hits += 1
                except Exception:
                    pass
            print(f"→ {buggy_hits}/{N_REPLAYS}")

            # Control replay
            control_hits = 0
            control_total = 0
            if case.get("control_trial_fn"):
                if case.get("setup_control_fn"):
                    try:
                        case["setup_control_fn"]()
                    except Exception:
                        pass
                print(f"      Control replay ({N_REPLAYS}x)...", end=" ", flush=True)
                for _ in range(N_REPLAYS):
                    if case.get("setup_control_fn"):
                        try:
                            case["setup_control_fn"]()
                        except Exception:
                            pass
                    try:
                        r = case["control_trial_fn"](delays)
                        if r.get("bug_found"):
                            control_hits += 1
                        control_total += 1
                    except Exception:
                        control_total += 1
                print(f"→ {control_hits}/{control_total}")
            else:
                print(f"      No control variant available for {db}")
                control_total = 0

            buggy_rate = buggy_hits / N_REPLAYS if N_REPLAYS > 0 else 0
            control_rate = control_hits / control_total if control_total > 0 else 0
            stable = (buggy_hits == N_REPLAYS and control_hits == 0
                      and control_total > 0)

            rows.append(CaseReplayRow(
                db=db, case_name=case_name, schedule_idx=idx,
                delay_count=delay_count, total_delay_ms=round(total_ms, 2),
                schedule_json=sched_json,
                buggy_replays=N_REPLAYS,
                buggy_hits=buggy_hits,
                buggy_rate=round(buggy_rate, 4),
                control_replays=control_total,
                control_hits=control_hits,
                control_rate=round(control_rate, 4),
                stable=stable,
            ))

    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_results(
    output_dir: Path,
    micro_rows: List[ReplayRow],
    case_rows: List[CaseReplayRow],
    elapsed_s: float,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # Micro matrix CSV
    micro_csv = output_dir / "determinism_matrix.csv"
    if micro_rows:
        with micro_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(asdict(micro_rows[0]).keys()))
            w.writeheader()
            for r in micro_rows:
                w.writerow(asdict(r))

    # Case study CSV
    case_csv = output_dir / "case_study_replay.csv"
    if case_rows:
        with case_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(asdict(case_rows[0]).keys()))
            w.writeheader()
            for r in case_rows:
                w.writerow(asdict(r))

    # Summary
    summary_path = output_dir / "summary.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("  Systematic Replay Determinism Evaluation\n")
        f.write(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"  Total elapsed: {elapsed_s:.0f}s\n")
        f.write("=" * 70 + "\n\n")

        # Micro workload summary
        f.write("MICRO WORKLOAD MATRIX (3 DBs × 3 workloads)\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'DB':<12} {'Workload':<20} {'Schedules':<10} "
                f"{'Best Buggy':<12} {'Best Control':<14} {'Stable'}\n")
        f.write("-" * 70 + "\n")

        for db in ["postgres", "mysql", "sqlite"]:
            for wl in WORKLOADS:
                cell_rows = [r for r in micro_rows
                             if r.db == db and r.workload == wl
                             and r.schedule_id != "none"]
                n_sched = len(cell_rows)
                if n_sched == 0:
                    f.write(f"{db:<12} {wl:<20} {'0 found':<10} "
                            f"{'—':<12} {'—':<14} {'—'}\n")
                    continue

                best_buggy = max(r.buggy_rate for r in cell_rows)
                worst_control = max(r.control_rate for r in cell_rows)
                n_stable = sum(1 for r in cell_rows if r.stable)
                f.write(f"{db:<12} {wl:<20} {n_sched:<10} "
                        f"{best_buggy:<12.2f} {worst_control:<14.2f} "
                        f"{n_stable}/{n_sched}\n")

        f.write("\n\n")

        # Case study summary
        f.write("CASE STUDY REPLAYS\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'DB':<12} {'Case':<25} {'Schedules':<10} "
                f"{'Best Buggy':<12} {'Control':<14} {'Stable'}\n")
        f.write("-" * 70 + "\n")

        for r in case_rows:
            if r.schedule_json == "{}":
                f.write(f"{r.db:<12} {r.case_name:<25} {'0 found':<10} "
                        f"{'—':<12} {'—':<14} {'—'}\n")
            else:
                ctl = f"{r.control_hits}/{r.control_replays}" if r.control_replays > 0 else "N/A"
                f.write(f"{r.db:<12} {r.case_name:<25} "
                        f"{'1':<10} "
                        f"{r.buggy_rate:<12.2f} {ctl:<14} "
                        f"{'Yes' if r.stable else 'No'}\n")

        f.write("\n")

        # Aggregate stats
        all_micro = [r for r in micro_rows if r.schedule_id != "none"]
        n_total = len(all_micro)
        n_stable = sum(1 for r in all_micro if r.stable)
        n_high = sum(1 for r in all_micro if r.buggy_rate >= 0.9)
        n_zero_ctl = sum(1 for r in all_micro if r.control_rate == 0)

        f.write("\nAGGREGATE (micro workloads only)\n")
        f.write(f"  Total schedules tested: {n_total}\n")
        f.write(f"  Perfectly stable (20/20 buggy, 0/20 control): "
                f"{n_stable}/{n_total}\n")
        f.write(f"  High replay (≥90% buggy): {n_high}/{n_total}\n")
        f.write(f"  Zero control hits: {n_zero_ctl}/{n_total}\n")

    print(f"\nResults written to {output_dir}/")
    print(f"  determinism_matrix.csv  ({len(micro_rows)} rows)")
    print(f"  case_study_replay.csv   ({len(case_rows)} rows)")
    print(f"  summary.txt")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = ROOT / "results" / f"replay_systematic_{ts}"

    print("=" * 70)
    print("  Systematic Replay Determinism Evaluation")
    print(f"  Output: {output_dir}")
    print(f"  Config: {N_SCHEDULES_TARGET} schedules/cell, "
          f"{N_REPLAYS} replays, {DISCOVERY_BUDGET_S}s discovery")
    print("=" * 70)

    t0 = time.monotonic()

    print("\n\n>>> PHASE 1: Micro Workload Matrix <<<\n")
    micro_rows = run_micro_matrix(output_dir)

    print("\n\n>>> PHASE 2: Case Study Replays <<<\n")
    case_rows = run_case_study_replays(output_dir)

    elapsed = time.monotonic() - t0

    write_results(output_dir, micro_rows, case_rows, elapsed)

    # Print summary to stdout
    print(f"\n{'='*70}")
    print(f"  DONE — total elapsed: {elapsed:.0f}s")
    print(f"{'='*70}")
    summary = (output_dir / "summary.txt").read_text()
    print(summary)


if __name__ == "__main__":
    main()
