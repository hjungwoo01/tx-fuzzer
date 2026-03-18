"""
Long-horizon experiment suite for schedule-pool evaluation.

Stage 1: 3600s micro coverage × 1 rep × 4 schedulers × 3 DBs × 3 workloads
Stage 2: 7200s headline case studies × 1 rep × 4 schedulers × 3 DBs
Stage 3: Comparison against short-budget baseline
Stage 4: Report generation

Usage:
  python scripts/experiments/run_long_horizon.py
  python scripts/experiments/run_long_horizon.py --resume
  python scripts/experiments/run_long_horizon.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[2]
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
    write_coverage_csv,
    plot_coverage,
)
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
from scripts.schedule_pool import (
    Schedule,
    ScheduleRecord,
    SchedulePool,
    ScheduleMutator,
    MutationConfig,
    RetentionPolicy,
    RetentionReason,
)
from scripts.schedule_pool.pool import PoolConfig
from scripts.schedule_pool.runner import run_pool_search

# Case study functions
from scripts.experiments.case_studies import (
    CaseResult,
    run_pg_case_study,
    run_mysql_rr_nonexistent_case,
    run_sqlite_narrow_dirty_case,
    MYSQL_RR_NONEXISTENT_SCHEMA,
    SQLITE_NARROW_DIRTY_SCHEMA,
)

from scripts.experiments.run_final_eval import (
    _pg_uco_trial_with_delays,
    _mysql_rr_trial_with_delays,
    _sqlite_dirty_trial_with_delays,
    _pg_uco_trial_with_delays_hard,
    _mysql_rr_trial_with_delays_hard,
    _sqlite_dirty_trial_with_delays_hard,
    run_case_pool_search,
    CaseStudyRow,
    CasePoolResult,
    PG_UCO_EVENTS,
    MYSQL_RR_EVENTS,
    SQLITE_DIRTY_EVENTS,
    PG_UCO_EVENTS_HARD,
    MYSQL_RR_EVENTS_HARD,
    SQLITE_DIRTY_EVENTS_HARD,
    CASE_DSNS,
    _db_available,
    _pg_dsn_available,
)

try:
    from scripts.discover_update_conflict_out import (
        setup_schema as _uco_setup_schema,
        reset_tables as _uco_reset_tables,
    )
    _HAS_UCO = True
except ImportError:
    _HAS_UCO = False


STRATEGIES = ["stress", "random", "heuristic", "schedule_pool"]
WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]
DBS = ["postgres", "mysql", "sqlite"]
SEEDS = [42]
REPS = 1

MICRO_DURATION_S = 3600.0   # 1 hour
CASE_DURATION_S = 7200.0     # 2 hours
REPLAY_COUNT = 20
N_WORKERS = 4

SHORT_BASELINE_DIR = ROOT / "results" / "schedule_pool_main_quick"

DSNS = {
    "postgres": DEFAULT_DSNS.get("postgres", "postgres://postgres:postgres@localhost:5432/postgres"),
    "mysql": "mysql://root:root@localhost:33080/test",
    "sqlite": "file:lh_cov_{wl}_{seed}?mode=memory&cache=shared",
}

CASE_PG_BUGGY_DSN = CASE_DSNS["pg_12_3"]
CASE_PG_CONTROL_DSN = CASE_DSNS["pg_12_4"]
CASE_MYSQL_DSN = CASE_DSNS.get("mysql_8", CASE_DSNS.get("mysql"))
CASE_SQLITE_DSN = CASE_DSNS["sqlite"]

CASE_MAP = {
    "postgres": "update_conflict_out",
    "mysql": "rr_nonexistent_row",
    "sqlite": "narrow_dirty_read",
}


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _log_cmd(cmd_log: Path, desc: str, rc: int, elapsed: float) -> None:
    with cmd_log.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {desc} | rc={rc} | {elapsed:.1f}s\n")


@dataclass
class CoverageJob:
    db: str
    workload: str
    strategy: str
    rep: int
    seed: int
    duration_s: float
    marker_file: str  # relative to output_dir


@dataclass
class CaseJob:
    db: str
    case_name: str
    strategy: str
    rep: int
    seed: int
    duration_s: float
    marker_file: str


def build_coverage_jobs(db: str) -> List[CoverageJob]:
    jobs = []
    for wl in WORKLOADS:
        for strat in STRATEGIES:
            for rep_idx in range(REPS):
                seed = SEEDS[rep_idx]
                marker = f"micro_long/{db}/{wl}/{strat}/rep{rep_idx}.done"
                jobs.append(CoverageJob(db, wl, strat, rep_idx, seed, MICRO_DURATION_S, marker))
    return jobs


def build_case_jobs(db: str) -> List[CaseJob]:
    case_name = CASE_MAP[db]
    jobs = []
    for strat in STRATEGIES:
        for rep_idx in range(REPS):
            seed = SEEDS[rep_idx]
            marker = f"cases_long/{db}/{case_name}/{strat}/rep{rep_idx}.done"
            jobs.append(CaseJob(db, case_name, strat, rep_idx, seed, CASE_DURATION_S, marker))
    return jobs


def write_inventory(output_dir: Path) -> None:
    lines = []
    lines.append(f"date: {datetime.now().isoformat()}")
    try:
        lines.append(f"git_commit: {subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], cwd=ROOT, text=True).strip()}")
    except Exception:
        lines.append("git_commit: unknown")
    lines.append(f"python: {sys.version}")
    lines.append(f"cpus: {os.cpu_count()}")
    lines.append(f"compose_file: docker-compose-repro.yml")
    lines.append(f"micro_duration_s: {MICRO_DURATION_S}")
    lines.append(f"case_duration_s: {CASE_DURATION_S}")
    lines.append(f"reps: {REPS}")
    lines.append(f"seeds: {SEEDS}")
    lines.append(f"strategies: {STRATEGIES}")
    lines.append(f"workloads: {WORKLOADS}")
    lines.append(f"dbs: {DBS}")
    for db, dsn in DSNS.items():
        lines.append(f"dsn_{db}: {dsn}")
    lines.append(f"case_pg_buggy_dsn: {CASE_PG_BUGGY_DSN}")
    lines.append(f"case_pg_control_dsn: {CASE_PG_CONTROL_DSN}")
    lines.append(f"case_mysql_dsn: {CASE_MYSQL_DSN}")
    lines.append(f"case_sqlite_dsn: {CASE_SQLITE_DSN}")
    lines.append(f"short_baseline: {SHORT_BASELINE_DIR}")
    (output_dir / "inventory.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_run_matrix(output_dir: Path) -> None:
    fields = ["type", "db", "workload_or_case", "strategy", "rep", "seed", "duration_s"]
    rows = []
    for db in DBS:
        for job in build_coverage_jobs(db):
            rows.append({"type": "coverage", "db": job.db, "workload_or_case": job.workload,
                         "strategy": job.strategy, "rep": job.rep, "seed": job.seed,
                         "duration_s": job.duration_s})
        for job in build_case_jobs(db):
            rows.append({"type": "case", "db": job.db, "workload_or_case": job.case_name,
                         "strategy": job.strategy, "rep": job.rep, "seed": job.seed,
                         "duration_s": job.duration_s})
    csv_path = output_dir / "run_matrix.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def run_one_coverage(job: CoverageJob, output_dir: Path, cmd_log: Path) -> Dict[str, Any]:
    marker = output_dir / job.marker_file
    if marker.exists():
        return {"status": "skipped", "db": job.db, "workload": job.workload,
                "strategy": job.strategy, "rep": job.rep}

    label = f"{job.db}/{job.workload}/{job.strategy}/rep{job.rep}"
    print(f"  [COV] {label} (seed={job.seed}, {job.duration_s}s)", flush=True)
    t0 = time.monotonic()

    dsn = DSNS[job.db]
    if "{wl}" in dsn or "{seed}" in dsn:
        dsn = dsn.replace("{wl}", job.workload).replace("{seed}", str(job.seed))

    try:
        pool = None
        if job.strategy == "schedule_pool":
            pool = SchedulePool(config=PoolConfig())
            points, trials, pool = run_pool_search(
                db=job.db, dsn=dsn,
                workload_name=job.workload,
                variant="buggy",
                duration_s=job.duration_s,
                n_workers=N_WORKERS,
                seed=job.seed,
                pool=pool,
                rep=job.rep,
                sample_interval_s=5.0,  # every 5s for 1h runs
            )
            # Save pool state & stats
            pool_dir = _ensure(output_dir / "micro_long" / job.db / job.workload / job.strategy)
            pool.save(pool_dir / f"pool_rep{job.rep}.jsonl")
            _write_pool_stats(pool, pool_dir / f"pool_stats_rep{job.rep}.json")
        else:
            points, trials = run_coverage_session(
                db=job.db, dsn=dsn,
                workload_name=job.workload,
                strategy=job.strategy,
                variant="buggy",
                duration_s=job.duration_s,
                n_workers=N_WORKERS,
                delay_ms=5.0,
                rep=job.rep,
                sample_interval_s=5.0,
            )

        elapsed = time.monotonic() - t0
        final = points[-1] if points else None
        states = final.unique_states if final else 0
        txns = final.txns_done if final else 0

        # Save timeseries CSV
        job_dir = _ensure(output_dir / "micro_long" / job.db / job.workload / job.strategy)
        ts_path = job_dir / f"timeseries_rep{job.rep}.csv"
        write_coverage_csv(points, ts_path)

        # Mark done
        _ensure(marker.parent)
        marker.write_text(f"states={states} trials={txns} elapsed={elapsed:.1f}s\n")
        _log_cmd(cmd_log, f"coverage {label}", 0, elapsed)

        print(f"  [COV] {label} → {states} states, {txns} trials ({elapsed:.1f}s)", flush=True)
        return {
            "status": "ok", "db": job.db, "workload": job.workload,
            "strategy": job.strategy, "rep": job.rep, "seed": job.seed,
            "final_states": states, "total_trials": txns,
            "duration_s": round(elapsed, 1),
            "pool_size": pool.size if pool else None,
            "points": points,
        }
    except Exception as e:
        elapsed = time.monotonic() - t0
        _log_cmd(cmd_log, f"coverage {label}", 1, elapsed)
        print(f"  [COV] {label} → ERROR: {e}", flush=True)
        traceback.print_exc()
        return {"status": "error", "db": job.db, "workload": job.workload,
                "strategy": job.strategy, "rep": job.rep, "error": str(e)}


def _write_pool_stats(pool: SchedulePool, path: Path) -> None:
    """Write detailed pool statistics to JSON."""
    records = pool.records
    retention_reasons: Dict[str, int] = defaultdict(int)
    mutation_sources = 0
    for r in records:
        if hasattr(r, "retention_reason") and r.retention_reason:
            retention_reasons[str(r.retention_reason)] += 1
        if r.parent_schedule_id is not None:
            mutation_sources += 1

    total_delays = [r.schedule.total_delay_ms for r in records if r.schedule]
    delay_counts = [r.schedule.delay_count for r in records if r.schedule]

    stats = {
        "pool_size": pool.size,
        "phase": str(pool.phase),
        "has_bugs": pool.has_bugs,
        "retention_reasons": dict(retention_reasons),
        "from_mutation": mutation_sources,
        "from_empty": pool.size - mutation_sources,
        "mean_total_delay_ms": round(statistics.mean(total_delays), 2) if total_delays else 0,
        "mean_delay_count": round(statistics.mean(delay_counts), 2) if delay_counts else 0,
        "min_total_delay_ms": round(min(total_delays), 2) if total_delays else 0,
        "max_total_delay_ms": round(max(total_delays), 2) if total_delays else 0,
        "bug_records": sum(1 for r in records if r.bug_found),
    }
    path.write_text(json.dumps(stats, indent=2), encoding="utf-8")


def run_one_case(job: CaseJob, output_dir: Path, cmd_log: Path) -> Dict[str, Any]:
    marker = output_dir / job.marker_file
    if marker.exists():
        return {"status": "skipped", "db": job.db, "case_name": job.case_name,
                "strategy": job.strategy, "rep": job.rep}

    label = f"{job.db}/{job.case_name}/{job.strategy}/rep{job.rep}"
    print(f"  [CASE] {label} (seed={job.seed}, {job.duration_s}s)", flush=True)
    t0 = time.monotonic()

    try:
        row, pool_result = _run_case_inner(job, output_dir)
        elapsed = time.monotonic() - t0

        # Save result JSON
        job_dir = _ensure(output_dir / "cases_long" / job.db / job.case_name / job.strategy)
        result_data = asdict(row)
        result_path = job_dir / f"result_rep{job.rep}.json"
        result_path.write_text(json.dumps(result_data, default=str, indent=2))

        # If schedule_pool found bug, save pool and do replay follow-up
        replay_data = None
        if pool_result and pool_result.bug_found and pool_result.first_bug_schedule:
            pool_result.pool.save(job_dir / f"pool_rep{job.rep}.jsonl")
            _write_pool_stats(pool_result.pool, job_dir / f"pool_stats_rep{job.rep}.json")
            replay_data = _run_replay_followup(job, pool_result, output_dir)

        # Mark done
        _ensure(marker.parent)
        bug_str = f"BUG at t={row.ttfb_s:.2f}s" if row.bug_found else "no bug"
        marker.write_text(f"{bug_str} elapsed={elapsed:.1f}s\n")
        _log_cmd(cmd_log, f"case {label}", 0, elapsed)

        print(f"  [CASE] {label} → {bug_str} ({elapsed:.1f}s, {row.total_trials} trials)", flush=True)
        result = {"status": "ok", **result_data}
        if replay_data:
            result["replay"] = replay_data
        return result
    except Exception as e:
        elapsed = time.monotonic() - t0
        _log_cmd(cmd_log, f"case {label}", 1, elapsed)
        print(f"  [CASE] {label} → ERROR: {e}", flush=True)
        traceback.print_exc()
        return {"status": "error", "db": job.db, "case_name": job.case_name,
                "strategy": job.strategy, "rep": job.rep, "error": str(e)}


def _gen_strategy_delays(strategy: str, event_ids: List[str], rng: random.Random) -> Dict[str, float]:
    """Generate delay dict for non-schedule_pool strategies."""
    if strategy == "stress":
        return {}
    elif strategy == "random":
        return {eid: rng.uniform(0, 0.100) for eid in event_ids}
    elif strategy == "heuristic":
        # Apply moderate delays to a few randomly chosen events
        k = min(4, len(event_ids))
        chosen = rng.sample(event_ids, k)
        return {eid: rng.uniform(0.005, 0.050) for eid in chosen}
    return {}


def _run_case_inner(job: CaseJob, output_dir: Path) -> Tuple[CaseStudyRow, Optional[CasePoolResult]]:
    cs_dir = _ensure(output_dir / "cases_long")
    pool_result = None

    if job.db == "postgres":
        if not _HAS_UCO:
            raise RuntimeError("UCO module not available")
        _uco_setup_schema(CASE_PG_BUGGY_DSN)
        if job.strategy == "schedule_pool":
            pr = run_case_pool_search(
                trial_fn=lambda d: _pg_uco_trial_with_delays_hard(CASE_PG_BUGGY_DSN, d),
                event_ids=PG_UCO_EVENTS_HARD, db="postgres",
                case_name="update_conflict_out",
                duration_s=job.duration_s, seed=job.seed,
                hot_events=["foo_commit", "bar_commit", "foo_select",
                            "bar_select", "trouble_update", "trouble_abort"],
            )
            pool_result = pr
            return CaseStudyRow(
                db="postgres", case_name="update_conflict_out",
                strategy=job.strategy, rep=job.rep,
                bug_found=pr.bug_found, ttfb_s=pr.ttfb_s,
                trials_to_first=pr.trials_to_first,
                total_trials=pr.total_trials, total_time_s=pr.total_time_s,
                pool_size=pr.pool_size,
                replay_rate_buggy=(pr.first_bug_record.replay_rate_buggy
                                   if pr.first_bug_record else None),
            ), pool_result
        else:
            # For non-schedule_pool strategies, use delay-based trial with
            # strategy-specific delay generation against the hardened workload.
            rng = random.Random(job.seed + job.rep)
            start = time.monotonic()
            trial = 0
            max_trials = 10000
            for trial in range(1, max_trials + 1):
                if (time.monotonic() - start) > job.duration_s:
                    break
                delays = _gen_strategy_delays(job.strategy, PG_UCO_EVENTS_HARD, rng)
                _uco_reset_tables(CASE_PG_BUGGY_DSN)
                r = _pg_uco_trial_with_delays_hard(CASE_PG_BUGGY_DSN, delays)
                if r["bug_found"]:
                    ttf = time.monotonic() - start
                    return CaseStudyRow(
                        db="postgres", case_name="update_conflict_out",
                        strategy=job.strategy, rep=job.rep,
                        bug_found=True, ttfb_s=ttf,
                        trials_to_first=trial,
                        total_trials=trial, total_time_s=ttf,
                    ), None
            total = time.monotonic() - start
            return CaseStudyRow(
                db="postgres", case_name="update_conflict_out",
                strategy=job.strategy, rep=job.rep,
                bug_found=False, ttfb_s=None,
                trials_to_first=None,
                total_trials=trial, total_time_s=total,
            ), None

    elif job.db == "mysql":
        setup = make_adapter("mysql", CASE_MYSQL_DSN)
        try:
            setup.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
        finally:
            setup.close()

        if job.strategy == "schedule_pool":
            pr = run_case_pool_search(
                trial_fn=lambda d: _mysql_rr_trial_with_delays_hard(CASE_MYSQL_DSN, d),
                event_ids=MYSQL_RR_EVENTS_HARD, db="mysql",
                case_name="rr_nonexistent_row",
                duration_s=job.duration_s, seed=job.seed,
                hot_events=["t1_commit", "t2_commit", "t3_mid", "t3_update"],
            )
            pool_result = pr
            return CaseStudyRow(
                db="mysql", case_name="rr_nonexistent_row",
                strategy=job.strategy, rep=job.rep,
                bug_found=pr.bug_found, ttfb_s=pr.ttfb_s,
                trials_to_first=pr.trials_to_first,
                total_trials=pr.total_trials, total_time_s=pr.total_time_s,
                pool_size=pr.pool_size,
                replay_rate_buggy=(pr.first_bug_record.replay_rate_buggy
                                   if pr.first_bug_record else None),
            ), pool_result
        else:
            rng = random.Random(job.seed + job.rep)
            start = time.monotonic()
            trial = 0
            max_trials = 10000
            for trial in range(1, max_trials + 1):
                if (time.monotonic() - start) > job.duration_s:
                    break
                delays = _gen_strategy_delays(job.strategy, MYSQL_RR_EVENTS_HARD, rng)
                r = _mysql_rr_trial_with_delays_hard(CASE_MYSQL_DSN, delays)
                if r["bug_found"]:
                    ttf = time.monotonic() - start
                    return CaseStudyRow(
                        db="mysql", case_name="rr_nonexistent_row",
                        strategy=job.strategy, rep=job.rep,
                        bug_found=True, ttfb_s=ttf,
                        trials_to_first=trial,
                        total_trials=trial, total_time_s=ttf,
                    ), None
            total = time.monotonic() - start
            return CaseStudyRow(
                db="mysql", case_name="rr_nonexistent_row",
                strategy=job.strategy, rep=job.rep,
                bug_found=False, ttfb_s=None,
                trials_to_first=None,
                total_trials=trial, total_time_s=total,
            ), None

    elif job.db == "sqlite":
        keeper = make_adapter("sqlite", CASE_SQLITE_DSN,
                               sqlite_shared_cache=True, sqlite_read_uncommitted=True)
        try:
            keeper.setup(SQLITE_NARROW_DIRTY_SCHEMA)

            if job.strategy == "schedule_pool":
                pr = run_case_pool_search(
                    trial_fn=lambda d: _sqlite_dirty_trial_with_delays_hard(
                        CASE_SQLITE_DSN, d, shared_cache=True, read_uncommitted=True),
                    event_ids=SQLITE_DIRTY_EVENTS_HARD, db="sqlite",
                    case_name="narrow_dirty_read",
                    duration_s=job.duration_s, seed=job.seed,
                    hot_events=["writer_hold", "reader_pre", "writer_pre"],
                )
                pool_result = pr
                return CaseStudyRow(
                    db="sqlite", case_name="narrow_dirty_read",
                    strategy=job.strategy, rep=job.rep,
                    bug_found=pr.bug_found, ttfb_s=pr.ttfb_s,
                    trials_to_first=pr.trials_to_first,
                    total_trials=pr.total_trials, total_time_s=pr.total_time_s,
                    pool_size=pr.pool_size,
                    replay_rate_buggy=(pr.first_bug_record.replay_rate_buggy
                                       if pr.first_bug_record else None),
                ), pool_result
            else:
                rng = random.Random(job.seed + job.rep)
                start = time.monotonic()
                trial = 0
                max_trials = 10000
                for trial in range(1, max_trials + 1):
                    if (time.monotonic() - start) > job.duration_s:
                        break
                    delays = _gen_strategy_delays(job.strategy, SQLITE_DIRTY_EVENTS_HARD, rng)
                    r = _sqlite_dirty_trial_with_delays_hard(
                        CASE_SQLITE_DSN, delays, shared_cache=True, read_uncommitted=True)
                    if r["bug_found"]:
                        ttf = time.monotonic() - start
                        return CaseStudyRow(
                            db="sqlite", case_name="narrow_dirty_read",
                            strategy=job.strategy, rep=job.rep,
                            bug_found=True, ttfb_s=ttf,
                            trials_to_first=trial,
                            total_trials=trial, total_time_s=ttf,
                        ), None
                total = time.monotonic() - start
                return CaseStudyRow(
                    db="sqlite", case_name="narrow_dirty_read",
                    strategy=job.strategy, rep=job.rep,
                    bug_found=False, ttfb_s=None,
                    trials_to_first=None,
                    total_trials=trial, total_time_s=total,
                ), None
        finally:
            keeper.close()
    else:
        raise ValueError(f"Unknown DB: {job.db}")


def _run_replay_followup(
    job: CaseJob,
    pool_result: CasePoolResult,
    output_dir: Path,
) -> Dict[str, Any]:
    """Run 20 buggy + 20 control replays for the first bug-triggering schedule."""
    sched = pool_result.first_bug_schedule
    if not sched:
        return {"error": "no bug schedule"}

    delays = sched.to_delay_map()
    buggy_hits = 0
    control_hits = 0

    replay_dir = _ensure(output_dir / "cases_long" / job.db / job.case_name / job.strategy)

    # Buggy replays (hardened trial functions)
    for i in range(REPLAY_COUNT):
        try:
            if job.db == "postgres":
                _uco_setup_schema(CASE_PG_BUGGY_DSN)
                r = _pg_uco_trial_with_delays_hard(CASE_PG_BUGGY_DSN, delays)
            elif job.db == "mysql":
                setup = make_adapter("mysql", CASE_MYSQL_DSN)
                try:
                    setup.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
                finally:
                    setup.close()
                r = _mysql_rr_trial_with_delays_hard(CASE_MYSQL_DSN, delays)
            elif job.db == "sqlite":
                r = _sqlite_dirty_trial_with_delays_hard(
                    CASE_SQLITE_DSN, delays,
                    shared_cache=True, read_uncommitted=True)
            else:
                break
            if r.get("bug_found"):
                buggy_hits += 1
        except Exception:
            pass

    # Control replays (postgres only — uses pg_12_4)
    if job.db == "postgres":
        for i in range(REPLAY_COUNT):
            try:
                _uco_setup_schema(CASE_PG_CONTROL_DSN)
                r = _pg_uco_trial_with_delays_hard(CASE_PG_CONTROL_DSN, delays)
                if r.get("bug_found"):
                    control_hits += 1
            except Exception:
                pass

    replay_data = {
        "schedule_delays": delays,
        "buggy_hits": buggy_hits,
        "buggy_total": REPLAY_COUNT,
        "buggy_rate": round(buggy_hits / REPLAY_COUNT, 2) if REPLAY_COUNT > 0 else 0,
        "control_hits": control_hits,
        "control_total": REPLAY_COUNT if job.db == "postgres" else 0,
        "control_rate": round(control_hits / REPLAY_COUNT, 2) if job.db == "postgres" and REPLAY_COUNT > 0 else None,
    }
    (replay_dir / f"replay_rep{job.rep}.json").write_text(
        json.dumps(replay_data, indent=2, default=str), encoding="utf-8")
    return replay_data


def run_db_worker(db: str, output_dir: Path, cmd_log: Path) -> Dict[str, Any]:
    print(f"\n{'='*60}\n  DB Worker: {db}\n{'='*60}", flush=True)
    results: Dict[str, Any] = {"db": db, "coverage": [], "cases": []}

    for job in build_coverage_jobs(db):
        r = run_one_coverage(job, output_dir, cmd_log)
        results["coverage"].append(r)

    for job in build_case_jobs(db):
        r = run_one_case(job, output_dir, cmd_log)
        results["cases"].append(r)

    print(f"\n  DB Worker {db} complete.", flush=True)
    return results


def aggregate_coverage_csvs(output_dir: Path) -> Tuple[List[CoveragePoint], Path]:
    all_points: List[CoveragePoint] = []
    cov_dir = output_dir / "micro_long"
    for ts_file in sorted(cov_dir.rglob("timeseries_rep*.csv")):
        with ts_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_points.append(CoveragePoint(
                    time_s=float(row["time_s"]),
                    db=row["db"],
                    workload=row["workload"],
                    strategy=row["strategy"],
                    rep=int(row["rep"]),
                    unique_states=int(row["unique_states"]),
                    txns_done=int(row["txns_done"]),
                    bugs_found=int(row["bugs_found"]),
                ))

    merged_path = cov_dir / "coverage_timeseries.csv"
    if all_points:
        write_coverage_csv(all_points, merged_path)
    return all_points, merged_path


def aggregate_case_results(output_dir: Path) -> Tuple[List[Dict], Path]:
    rows = []
    cases_dir = output_dir / "cases_long"
    for result_file in sorted(cases_dir.rglob("result_rep*.json")):
        with result_file.open("r", encoding="utf-8") as f:
            rows.append(json.load(f))

    csv_path = cases_dir / "case_results.csv"
    if rows:
        all_keys = set()
        for r in rows:
            all_keys.update(r.keys())
        fields = sorted(all_keys)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fields})
    return rows, csv_path


def aggregate_replay_results(output_dir: Path) -> Tuple[List[Dict], Path]:
    rows = []
    cases_dir = output_dir / "cases_long"
    for rfile in sorted(cases_dir.rglob("replay_rep*.json")):
        parts = rfile.relative_to(cases_dir).parts
        if len(parts) >= 4:
            db, case_name, strategy = parts[0], parts[1], parts[2]
        else:
            db, case_name, strategy = "?", "?", "?"
        with rfile.open("r", encoding="utf-8") as f:
            data = json.load(f)
        data["db"] = db
        data["case_name"] = case_name
        data["strategy"] = strategy
        rows.append(data)

    csv_path = cases_dir / "replay_results.csv"
    if rows:
        fields = ["db", "case_name", "strategy", "buggy_hits", "buggy_total",
                   "buggy_rate", "control_hits", "control_total", "control_rate"]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
    return rows, csv_path


def write_coverage_summary(all_points: List[CoveragePoint], output_dir: Path) -> Path:
    csv_path = output_dir / "micro_long" / "coverage_summary.csv"
    groups: Dict[Tuple[str, str, str], List[CoveragePoint]] = defaultdict(list)
    for p in all_points:
        groups[(p.db, p.workload, p.strategy)].append(p)

    rows = []
    for (db, wl, strat), pts in sorted(groups.items()):
        by_rep: Dict[int, List[CoveragePoint]] = defaultdict(list)
        for p in pts:
            by_rep[p.rep].append(p)

        final_states = []
        final_trials = []
        durations = []
        aucs = []
        for rep, rpts in by_rep.items():
            rpts_sorted = sorted(rpts, key=lambda x: x.time_s)
            if rpts_sorted:
                final_states.append(rpts_sorted[-1].unique_states)
                final_trials.append(rpts_sorted[-1].txns_done)
                durations.append(rpts_sorted[-1].time_s)
            if len(rpts_sorted) >= 2:
                auc = 0.0
                for i in range(1, len(rpts_sorted)):
                    dt = rpts_sorted[i].time_s - rpts_sorted[i - 1].time_s
                    avg_s = (rpts_sorted[i].unique_states + rpts_sorted[i - 1].unique_states) / 2.0
                    auc += dt * avg_s
                aucs.append(auc)

        mean_states = statistics.mean(final_states) if final_states else 0
        std_states = statistics.stdev(final_states) if len(final_states) > 1 else 0
        mean_trials = statistics.mean(final_trials) if final_trials else 0
        duration = statistics.mean(durations) if durations else 0
        auc = statistics.mean(aucs) if aucs else 0
        normalized_auc = auc / duration if duration > 0 else 0
        states_per_100 = (mean_states / mean_trials * 100) if mean_trials > 0 else 0

        # Extract states at 60s for comparison with short baseline
        states_at_60 = []
        for rep, rpts in by_rep.items():
            rpts_sorted = sorted(rpts, key=lambda x: x.time_s)
            for p in rpts_sorted:
                if p.time_s >= 60.0:
                    states_at_60.append(p.unique_states)
                    break

        rows.append({
            "db": db, "workload": wl, "strategy": strat,
            "reps": len(final_states),
            "mean_final_states": round(mean_states, 2),
            "std_final_states": round(std_states, 2),
            "mean_final_trials": round(mean_trials, 1),
            "mean_auc": round(auc, 2),
            "normalized_auc": round(normalized_auc, 4),
            "states_per_100_trials": round(states_per_100, 2),
            "duration_s": round(duration, 1),
            "states_at_60": round(statistics.mean(states_at_60), 2) if states_at_60 else "",
        })

    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
    return csv_path


def load_short_baseline() -> Tuple[Optional[Path], Optional[Path], Optional[List[Dict]], Optional[List[Dict]]]:
    """Load existing quick-eval baseline results. Returns (cov_ts_path, case_path, cov_summary_rows, case_rows)."""
    cov_ts_path = SHORT_BASELINE_DIR / "micro_coverage" / "coverage_timeseries.csv"
    cov_summary_path = SHORT_BASELINE_DIR / "micro_coverage" / "coverage_summary.csv"
    case_path = SHORT_BASELINE_DIR / "case_studies" / "case_study_results.csv"

    cov_rows = None
    if cov_summary_path.exists():
        with cov_summary_path.open("r", encoding="utf-8") as f:
            cov_rows = list(csv.DictReader(f))

    case_rows = None
    if case_path.exists():
        with case_path.open("r", encoding="utf-8") as f:
            case_rows = list(csv.DictReader(f))

    return cov_ts_path, case_path, cov_rows, case_rows


def build_micro_comparison(long_summary_path: Path, short_cov_rows: Optional[List[Dict]], output_dir: Path) -> Path:
    compare_dir = _ensure(output_dir / "compare")
    csv_path = compare_dir / "micro_compare.csv"

    long_rows = []
    if long_summary_path.exists():
        with long_summary_path.open("r", encoding="utf-8") as f:
            long_rows = list(csv.DictReader(f))

    short_idx = {}
    if short_cov_rows:
        for r in short_cov_rows:
            key = (r.get("db", ""), r.get("workload", ""), r.get("strategy", ""))
            short_idx[key] = r

    out_rows = []
    for lr in long_rows:
        key = (lr["db"], lr["workload"], lr["strategy"])
        sr = short_idx.get(key)

        short_dur = 15.0
        short_states = float(sr["mean_final_states"]) if sr else ""
        short_auc = float(sr["mean_auc"]) if sr and sr.get("mean_auc") else ""
        short_norm_auc = round(float(sr["mean_auc"]) / short_dur, 4) if sr and sr.get("mean_auc") and float(sr.get("mean_auc", 0)) > 0 else ""
        short_sp100 = float(sr["states_per_100_trials"]) if sr and sr.get("states_per_100_trials") else ""

        out_rows.append({
            "db": lr["db"], "workload": lr["workload"], "strategy": lr["strategy"],
            "short_states_at_15": short_states,
            "long_states_at_60": lr.get("states_at_60", ""),
            "long_states_at_3600": lr["mean_final_states"],
            "short_auc_15": short_auc,
            "long_auc_3600": lr["mean_auc"],
            "short_norm_auc": short_norm_auc,
            "long_norm_auc": lr.get("normalized_auc", ""),
            "short_states_per_100": short_sp100,
            "long_states_per_100": lr["states_per_100_trials"],
            "short_duration_s": short_dur,
            "long_duration_s": lr.get("duration_s", MICRO_DURATION_S),
        })

    if out_rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=out_rows[0].keys())
            w.writeheader()
            w.writerows(out_rows)
    return csv_path


def build_case_comparison(long_case_rows: List[Dict], short_case_rows: Optional[List[Dict]], output_dir: Path) -> Path:
    compare_dir = _ensure(output_dir / "compare")
    csv_path = compare_dir / "case_compare.csv"

    short_idx: Dict[Tuple, List[Dict]] = defaultdict(list)
    if short_case_rows:
        for r in short_case_rows:
            key = (r["db"], r["case_name"], r["strategy"])
            short_idx[key].append(r)

    long_idx: Dict[Tuple, List[Dict]] = defaultdict(list)
    for r in long_case_rows:
        key = (r["db"], r["case_name"], r["strategy"])
        long_idx[key].append(r)

    out_rows = []
    all_keys = sorted(set(list(short_idx.keys()) + list(long_idx.keys())))
    for key in all_keys:
        db, case, strat = key
        sr_list = short_idx.get(key, [])
        lr_list = long_idx.get(key, [])

        short_success = sum(1 for r in sr_list if str(r.get("bug_found", "")).lower() == "true")
        short_total = len(sr_list)
        long_success = sum(1 for r in lr_list if str(r.get("bug_found", "")).lower() == "true")
        long_total = len(lr_list)

        short_ttfbs = [float(r["ttfb_s"]) for r in sr_list if r.get("ttfb_s") and str(r["ttfb_s"]) not in ("None", "")]
        long_ttfbs = [float(r["ttfb_s"]) for r in lr_list if r.get("ttfb_s") and str(r["ttfb_s"]) not in ("None", "")]

        long_pool = ""
        for r in lr_list:
            if r.get("pool_size") and str(r["pool_size"]) not in ("None", ""):
                long_pool = r["pool_size"]

        out_rows.append({
            "db": db, "case_name": case, "strategy": strat,
            "short_success_rate": f"{short_success}/{short_total}" if short_total else "",
            "long_success_rate": f"{long_success}/{long_total}" if long_total else "",
            "short_median_ttfb": round(statistics.median(short_ttfbs), 2) if short_ttfbs else "",
            "long_median_ttfb": round(statistics.median(long_ttfbs), 2) if long_ttfbs else "",
            "long_pool_size": long_pool,
            "short_duration_s": "15.0",
            "long_duration_s": str(CASE_DURATION_S),
        })

    if out_rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=out_rows[0].keys())
            w.writeheader()
            w.writerows(out_rows)
    return csv_path


def generate_plots(all_points: List[CoveragePoint], output_dir: Path) -> List[str]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[WARN] matplotlib not available, skipping plots", file=sys.stderr)
        return []

    plot_dir = _ensure(output_dir / "plots")
    generated = []
    colors = {"stress": "tab:blue", "random": "tab:orange",
              "heuristic": "tab:green", "schedule_pool": "tab:red"}

    # 1. Coverage curves per DB/workload (full 3600s)
    for db in DBS:
        for wl in WORKLOADS:
            fig, ax = plt.subplots(figsize=(12, 6))
            for strat in STRATEGIES:
                strat_pts = sorted(
                    [p for p in all_points if p.db == db and p.workload == wl and p.strategy == strat],
                    key=lambda p: p.time_s)
                if not strat_pts:
                    continue
                ax.plot([p.time_s for p in strat_pts],
                        [p.unique_states for p in strat_pts],
                        label=strat, color=colors.get(strat), linewidth=2)

            ax.set_title(f"Coverage: {wl} ({db}) — 3600s")
            ax.set_xlabel("Time (s)")
            ax.set_ylabel("Unique Canonical States")
            ax.legend()
            ax.grid(True, alpha=0.3)
            out = plot_dir / f"coverage_{db}_{wl}_3600s.png"
            fig.tight_layout()
            fig.savefig(out, dpi=140)
            plt.close(fig)
            generated.append(str(out))

    # 2. Log-scale X axis variant (to show early + late behavior together)
    for db in DBS:
        for wl in WORKLOADS:
            fig, ax = plt.subplots(figsize=(12, 6))
            for strat in STRATEGIES:
                strat_pts = sorted(
                    [p for p in all_points if p.db == db and p.workload == wl and p.strategy == strat],
                    key=lambda p: p.time_s)
                if not strat_pts:
                    continue
                times = [max(p.time_s, 0.1) for p in strat_pts]
                ax.plot(times, [p.unique_states for p in strat_pts],
                        label=strat, color=colors.get(strat), linewidth=2)

            ax.set_xscale("log")
            ax.set_title(f"Coverage (log-time): {wl} ({db})")
            ax.set_xlabel("Time (s, log scale)")
            ax.set_ylabel("Unique Canonical States")
            ax.legend()
            ax.grid(True, alpha=0.3, which="both")
            out = plot_dir / f"coverage_{db}_{wl}_logtime.png"
            fig.tight_layout()
            fig.savefig(out, dpi=140)
            plt.close(fig)
            generated.append(str(out))

    # 3. Short vs long overlay (load short baseline timeseries)
    short_ts_path = SHORT_BASELINE_DIR / "micro_coverage" / "coverage_timeseries.csv"
    short_points = []
    if short_ts_path.exists():
        with short_ts_path.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                short_points.append(CoveragePoint(
                    time_s=float(row["time_s"]),
                    db=row["db"], workload=row["workload"],
                    strategy=row["strategy"], rep=int(row["rep"]),
                    unique_states=int(row["unique_states"]),
                    txns_done=int(row["txns_done"]),
                    bugs_found=int(row["bugs_found"]),
                ))

    if short_points:
        for db in DBS:
            for wl in WORKLOADS:
                fig, ax = plt.subplots(figsize=(12, 6))
                for strat in STRATEGIES:
                    # Short runs (dotted, averaged across reps)
                    s_pts = sorted(
                        [p for p in short_points if p.db == db and p.workload == wl and p.strategy == strat],
                        key=lambda p: p.time_s)
                    if s_pts:
                        by_bucket: Dict[int, List[int]] = defaultdict(list)
                        for p in s_pts:
                            by_bucket[int(p.time_s)].append(p.unique_states)
                        times_s = sorted(by_bucket.keys())
                        avgs_s = [statistics.mean(by_bucket[t]) for t in times_s]
                        ax.plot(times_s, avgs_s, color=colors.get(strat), linewidth=1.5,
                                linestyle="--", alpha=0.6, label=f"{strat} (15s)")

                    # Long runs (solid)
                    l_pts = sorted(
                        [p for p in all_points if p.db == db and p.workload == wl and p.strategy == strat],
                        key=lambda p: p.time_s)
                    if l_pts:
                        ax.plot([p.time_s for p in l_pts],
                                [p.unique_states for p in l_pts],
                                color=colors.get(strat), linewidth=2, label=f"{strat} (3600s)")

                ax.set_title(f"Short vs Long: {wl} ({db})")
                ax.set_xlabel("Time (s)")
                ax.set_ylabel("Unique Canonical States")
                ax.legend(fontsize=8, ncol=2)
                ax.grid(True, alpha=0.3)
                out = plot_dir / f"overlay_{db}_{wl}.png"
                fig.tight_layout()
                fig.savefig(out, dpi=140)
                plt.close(fig)
                generated.append(str(out))

    # 4. Pool growth over time (schedule_pool only)
    for db in DBS:
        pool_stats_files = sorted((output_dir / "micro_long").rglob(f"{db}/**/schedule_pool/pool_stats_rep*.json"))
        if not pool_stats_files:
            continue
        # From timeseries: bugs_found over time as proxy for pool growth
        for wl in WORKLOADS:
            pts = sorted(
                [p for p in all_points if p.db == db and p.workload == wl and p.strategy == "schedule_pool"],
                key=lambda p: p.time_s)
            if not pts:
                continue
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            ax1.plot([p.time_s for p in pts], [p.unique_states for p in pts], color="tab:red", linewidth=2)
            ax1.set_title(f"Pool Coverage: {wl} ({db})")
            ax1.set_xlabel("Time (s)")
            ax1.set_ylabel("Unique Canonical States")
            ax1.grid(True, alpha=0.3)

            ax2.plot([p.time_s for p in pts], [p.bugs_found for p in pts], color="tab:purple", linewidth=2)
            ax2.set_title(f"Bugs Found: {wl} ({db})")
            ax2.set_xlabel("Time (s)")
            ax2.set_ylabel("Cumulative Bugs")
            ax2.grid(True, alpha=0.3)

            out = plot_dir / f"pool_growth_{db}_{wl}.png"
            fig.tight_layout()
            fig.savefig(out, dpi=140)
            plt.close(fig)
            generated.append(str(out))

    return generated


def generate_report(
    output_dir: Path,
    all_points: List[CoveragePoint],
    case_rows: List[Dict],
    replay_rows: List[Dict],
    micro_compare_path: Path,
    case_compare_path: Path,
    wall_elapsed: float,
) -> Path:
    report_path = output_dir / "LONG_HORIZON_REPORT.md"
    lines = []

    lines.append("# Long-Horizon Experiment Report")
    lines.append(f"\nGenerated: {datetime.now().isoformat()}")
    lines.append(f"Wall-clock: {wall_elapsed/3600:.1f}h ({wall_elapsed:.0f}s)")
    lines.append(f"Micro-coverage: {MICRO_DURATION_S}s × {REPS} reps")
    lines.append(f"Case studies: {CASE_DURATION_S}s × {REPS} reps")
    lines.append("")

    # Section 1: Why
    lines.append("## 1. Why a Long-Horizon Suite Was Needed")
    lines.append("")
    lines.append("Short runs (15s × 3 reps) are useful for variance and quick comparison,")
    lines.append("but cannot answer whether coverage separates asymptotically or whether")
    lines.append("hard-case bugs require extended search. Long runs reveal:")
    lines.append("- Whether scheduler rankings hold under extended time horizons")
    lines.append("- Whether pool growth continues or plateaus")
    lines.append("- Whether longer search time materially helps on difficult bugs like PG UCO")
    lines.append("")

    # Section 2: Micro coverage
    lines.append("## 2. Long Micro Coverage")
    lines.append("")

    # Summary table
    summary_path = output_dir / "micro_long" / "coverage_summary.csv"
    if summary_path.exists():
        lines.append("### Coverage Summary (3600s)")
        lines.append("")
        with summary_path.open("r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        lines.append("| DB | Workload | Strategy | States@3600 | Trials | Norm AUC | States/100 |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in rows:
            lines.append(f"| {r['db']} | {r['workload']} | {r['strategy']} "
                         f"| {r['mean_final_states']} | {r['mean_final_trials']} "
                         f"| {r['normalized_auc']} | {r['states_per_100_trials']} |")
        lines.append("")

    # Pool stats
    pool_stats_files = sorted((output_dir / "micro_long").rglob("pool_stats_rep*.json"))
    if pool_stats_files:
        lines.append("### Schedule Pool Statistics")
        lines.append("")
        lines.append("| DB | Workload | Pool Size | Phase | Bugs | From Mutation | Mean Delay (ms) |")
        lines.append("|---|---|---|---|---|---|---|")
        for pf in pool_stats_files:
            parts = pf.relative_to(output_dir / "micro_long").parts
            db = parts[0] if len(parts) > 0 else "?"
            wl = parts[1] if len(parts) > 1 else "?"
            with pf.open("r", encoding="utf-8") as f:
                ps = json.load(f)
            lines.append(f"| {db} | {wl} | {ps['pool_size']} | {ps['phase']} "
                         f"| {ps['bug_records']} | {ps['from_mutation']} "
                         f"| {ps['mean_total_delay_ms']} |")
        lines.append("")

    # Section 3: Case studies
    lines.append("## 3. Long Case Studies")
    lines.append("")

    if case_rows:
        lines.append("### Case Study Results (7200s)")
        lines.append("")
        lines.append("| DB | Case | Strategy | Bug Found | TTFB (s) | Trials | Pool Size |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in case_rows:
            ttfb = round(float(r["ttfb_s"]), 2) if r.get("ttfb_s") and str(r["ttfb_s"]) not in ("None", "") else "—"
            pool_sz = r.get("pool_size", "—") if r.get("pool_size") and str(r.get("pool_size", "")) not in ("None", "") else "—"
            lines.append(f"| {r['db']} | {r['case_name']} | {r['strategy']} "
                         f"| {r['bug_found']} | {ttfb} | {r['total_trials']} | {pool_sz} |")
        lines.append("")

    if replay_rows:
        lines.append("### Replay Follow-ups")
        lines.append("")
        lines.append("| DB | Case | Strategy | Buggy Rate | Control Rate |")
        lines.append("|---|---|---|---|---|")
        for r in replay_rows:
            ctrl = f"{r.get('control_rate', '—')}" if r.get("control_rate") is not None else "N/A"
            lines.append(f"| {r['db']} | {r['case_name']} | {r['strategy']} "
                         f"| {r['buggy_rate']} ({r['buggy_hits']}/{r['buggy_total']}) "
                         f"| {ctrl} |")
        lines.append("")

    # Section 4: Comparison
    lines.append("## 4. Comparison to Short-Budget Results")
    lines.append("")

    if micro_compare_path.exists():
        lines.append("### Micro Coverage: Short (15s) vs Long (3600s)")
        lines.append("")
        with micro_compare_path.open("r", encoding="utf-8") as f:
            cmp_rows = list(csv.DictReader(f))
        lines.append("| DB | Workload | Strategy | States@15 | States@60 | States@3600 | ShortNormAUC | LongNormAUC | Short S/100 | Long S/100 |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for r in cmp_rows:
            lines.append(f"| {r['db']} | {r['workload']} | {r['strategy']} "
                         f"| {r.get('short_states_at_15', '—')} "
                         f"| {r.get('long_states_at_60', '—')} "
                         f"| {r.get('long_states_at_3600', '—')} "
                         f"| {r.get('short_norm_auc', '—')} "
                         f"| {r.get('long_norm_auc', '—')} "
                         f"| {r.get('short_states_per_100', '—')} "
                         f"| {r.get('long_states_per_100', '—')} |")
        lines.append("")

    if case_compare_path.exists():
        lines.append("### Case Studies: Short (15s) vs Long (7200s)")
        lines.append("")
        with case_compare_path.open("r", encoding="utf-8") as f:
            cmp_rows = list(csv.DictReader(f))
        lines.append("| DB | Case | Strategy | Short Succ | Long Succ | Short TTFB | Long TTFB | Long Pool |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for r in cmp_rows:
            lines.append(f"| {r['db']} | {r['case_name']} | {r['strategy']} "
                         f"| {r.get('short_success_rate', '—')} "
                         f"| {r.get('long_success_rate', '—')} "
                         f"| {r.get('short_median_ttfb', '—')} "
                         f"| {r.get('long_median_ttfb', '—')} "
                         f"| {r.get('long_pool_size', '—')} |")
        lines.append("")

    # Section 5: Recommendation
    lines.append("## 5. Recommendation")
    lines.append("")
    lines.append("*To be filled after reviewing the numerical evidence above.*")
    lines.append("")
    lines.append("Questions to answer:")
    lines.append("- Did scheduler rankings change over longer time?")
    lines.append("- Did schedule_pool get closer to random in coverage?")
    lines.append("- Did pool growth continue or plateau?")
    lines.append("- Did stress still miss the hard case-study bugs?")
    lines.append("- Did longer time help schedule_pool on PostgreSQL UCO?")
    lines.append("- Should the long-run evidence be cited in main paper text, appendix, or artifact notes?")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def main():
    parser = argparse.ArgumentParser(description="Long-horizon experiment suite")
    parser.add_argument("--resume", action="store_true",
                        help="Resume previous run (skip completed jobs)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print job matrix without running")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Override output directory")
    parser.add_argument("--sequential", action="store_true",
                        help="Run DBs sequentially instead of in parallel")
    args = parser.parse_args()

    # Output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif args.resume:
        candidates = sorted(ROOT.glob("results/long_horizon_*"))
        if not candidates:
            print("No previous long_horizon results found to resume.")
            sys.exit(1)
        output_dir = candidates[-1]
        print(f"Resuming from: {output_dir}")
    else:
        output_dir = ROOT / "results" / f"long_horizon_{_ts()}"

    _ensure(output_dir)
    cmd_log = output_dir / "commands.log"

    # PID-file guard
    pid_file = output_dir / ".pid"
    if not args.dry_run:
        if pid_file.exists():
            old_pid = pid_file.read_text().strip()
            try:
                os.kill(int(old_pid), 0)
                print(f"ERROR: Another instance is already running (PID {old_pid}).")
                print(f"Remove {pid_file} if the process is stale.")
                sys.exit(1)
            except (OSError, ValueError):
                pass
        pid_file.write_text(str(os.getpid()))

    # Write setup files
    write_inventory(output_dir)
    write_run_matrix(output_dir)

    n_cov = len(WORKLOADS) * len(STRATEGIES) * REPS * len(DBS)
    n_case = len(DBS) * len(STRATEGIES) * REPS

    if args.dry_run:
        print("DRY RUN — Job matrix written to:")
        print(f"  {output_dir / 'run_matrix.csv'}")
        print(f"\nTotal coverage jobs: {n_cov}")
        print(f"Total case jobs: {n_case}")
        print(f"Expected wall-clock: ~12h (coverage) + ~8h (cases) with parallel DBs")
        return

    print(f"\n{'='*60}")
    print(f"  Long-Horizon Experiment Suite")
    print(f"  Output: {output_dir}")
    print(f"  Micro: {MICRO_DURATION_S}s × {REPS} reps")
    print(f"  Cases: {CASE_DURATION_S}s × {REPS} reps")
    print(f"  DBs: {', '.join(DBS)} ({'parallel' if not args.sequential else 'sequential'})")
    print(f"  Expected: ~12h coverage + ~8h cases (parallel)")
    print(f"{'='*60}\n")

    wall_start = time.monotonic()

    if args.sequential:
        all_results = []
        for db in DBS:
            r = run_db_worker(db, output_dir, cmd_log)
            all_results.append(r)
    else:
        import concurrent.futures
        all_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(DBS)) as executor:
            futures = {
                executor.submit(run_db_worker, db, output_dir, cmd_log): db
                for db in DBS
            }
            for future in concurrent.futures.as_completed(futures):
                db = futures[future]
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    print(f"\n  DB Worker {db} FAILED: {e}")
                    traceback.print_exc()
                    all_results.append({"db": db, "coverage": [], "cases": [], "error": str(e)})

    wall_elapsed = time.monotonic() - wall_start


    print(f"\n{'='*60}")
    print(f"  Post-processing")
    print(f"{'='*60}\n")

    all_points, _ = aggregate_coverage_csvs(output_dir)
    case_rows, _ = aggregate_case_results(output_dir)
    replay_rows, _ = aggregate_replay_results(output_dir)

    cov_summary_path = write_coverage_summary(all_points, output_dir)
    print(f"  Coverage summary: {cov_summary_path}")

    _, _, short_cov_rows, short_case_rows = load_short_baseline()

    micro_compare_path = build_micro_comparison(cov_summary_path, short_cov_rows, output_dir)
    case_compare_path = build_case_comparison(case_rows, short_case_rows, output_dir)
    print(f"  Micro comparison: {micro_compare_path}")
    print(f"  Case comparison: {case_compare_path}")

    print("  Generating plots...")
    plot_files = generate_plots(all_points, output_dir)
    print(f"  Generated {len(plot_files)} plots")

    report_path = generate_report(output_dir, all_points, case_rows, replay_rows,
                                   micro_compare_path, case_compare_path, wall_elapsed)
    print(f"  Report: {report_path}")


    print(f"\n{'='*60}")
    print(f"  DONE")
    print(f"  Wall-clock: {wall_elapsed/3600:.1f}h ({wall_elapsed:.0f}s)")
    print(f"  Output: {output_dir}")
    print(f"  Report: {report_path}")

    # Count successes/failures
    ok_cov = sum(1 for r in all_results for c in r.get("coverage", []) if c.get("status") == "ok")
    err_cov = sum(1 for r in all_results for c in r.get("coverage", []) if c.get("status") == "error")
    ok_case = sum(1 for r in all_results for c in r.get("cases", []) if c.get("status") == "ok")
    err_case = sum(1 for r in all_results for c in r.get("cases", []) if c.get("status") == "error")

    print(f"  Coverage: {ok_cov} ok, {err_cov} errors (of {n_cov})")
    print(f"  Cases: {ok_case} ok, {err_case} errors (of {n_case})")
    if replay_rows:
        print(f"  Replays: {len(replay_rows)} completed")
    print(f"{'='*60}")

    # Clean up PID file
    pid_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
