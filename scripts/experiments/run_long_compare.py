"""
Long-run comparison experiment for schedule-pool evaluation.

Runs 900s micro coverage + 600s case studies across 3 DBs in parallel,
then compares against existing short-run (15s/quick) baseline.

Usage:
  python scripts/experiments/run_long_compare.py
  python scripts/experiments/run_long_compare.py --resume
  python scripts/experiments/run_long_compare.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import multiprocessing
import os
import statistics
import sys
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

# Case study trial-with-delays from run_final_eval
from scripts.experiments.run_final_eval import (
    _pg_uco_trial_with_delays,
    _mysql_rr_trial_with_delays,
    _sqlite_dirty_trial_with_delays,
    run_case_pool_search,
    CaseStudyRow,
    CasePoolResult,
    PG_UCO_EVENTS,
    MYSQL_RR_EVENTS,
    SQLITE_DIRTY_EVENTS,
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
SEEDS = [42, 10042, 20042]
REPS = 3
MICRO_DURATION_S = 900.0   # 15 min
CASE_DURATION_S = 600.0     # 10 min
REPLAY_COUNT = 20
N_WORKERS = 4

SHORT_BASELINE_DIR = ROOT / "results" / "schedule_pool_main_quick"

DSNS = {
    "postgres": DEFAULT_DSNS.get("postgres", "postgres://postgres:postgres@localhost:5432/postgres"),
    "mysql": "mysql://root:root@localhost:33080/test",
    "sqlite": "file:long_cov_{wl}_{seed}?mode=memory&cache=shared",
}

CASE_PG_BUGGY_DSN = CASE_DSNS["pg_12_3"]
CASE_PG_CONTROL_DSN = CASE_DSNS["pg_12_4"]
CASE_MYSQL_DSN = CASE_DSNS.get("mysql_8", CASE_DSNS.get("mysql"))
CASE_SQLITE_DSN = CASE_DSNS["sqlite"]


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _log_cmd(log_path: Path, cmd: str, exit_code: int, runtime_s: float):
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()}  exit={exit_code}  {runtime_s:.1f}s  {cmd}\n")


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
                marker = f"coverage/{db}/{wl}/{strat}/rep{rep_idx}.done"
                jobs.append(CoverageJob(db, wl, strat, rep_idx, seed, MICRO_DURATION_S, marker))
    return jobs


def build_case_jobs(db: str) -> List[CaseJob]:
    cases = {"postgres": "update_conflict_out", "mysql": "rr_nonexistent_row", "sqlite": "narrow_dirty_read"}
    case_name = cases.get(db)
    if not case_name:
        return []
    jobs = []
    for strat in STRATEGIES:
        for rep_idx in range(REPS):
            seed = SEEDS[rep_idx]
            marker = f"cases/{db}/{case_name}/{strat}/rep{rep_idx}.done"
            jobs.append(CaseJob(db, case_name, strat, rep_idx, seed, CASE_DURATION_S, marker))
    return jobs


def run_one_coverage(job: CoverageJob, output_dir: Path, cmd_log: Path) -> Dict[str, Any]:
    """Run a single coverage job. Returns result dict."""
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
            )
            # Save pool state
            pool_dir = _ensure(output_dir / "coverage" / job.db / job.workload / job.strategy)
            pool.save(pool_dir / f"pool_rep{job.rep}.jsonl")
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
            )

        elapsed = time.monotonic() - t0
        final = points[-1] if points else None
        states = final.unique_states if final else 0
        txns = final.txns_done if final else 0

        # Save timeseries CSV for this job
        job_dir = _ensure(output_dir / "coverage" / job.db / job.workload / job.strategy)
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
            "points": points,
        }
    except Exception as e:
        elapsed = time.monotonic() - t0
        _log_cmd(cmd_log, f"coverage {label}", 1, elapsed)
        print(f"  [COV] {label} → ERROR: {e}", flush=True)
        traceback.print_exc()
        return {"status": "error", "db": job.db, "workload": job.workload,
                "strategy": job.strategy, "rep": job.rep, "error": str(e)}


def run_one_case(job: CaseJob, output_dir: Path, cmd_log: Path) -> Dict[str, Any]:
    """Run a single case study job. Returns result dict."""
    marker = output_dir / job.marker_file
    if marker.exists():
        return {"status": "skipped", "db": job.db, "case_name": job.case_name,
                "strategy": job.strategy, "rep": job.rep}

    label = f"{job.db}/{job.case_name}/{job.strategy}/rep{job.rep}"
    print(f"  [CASE] {label} (seed={job.seed}, {job.duration_s}s)", flush=True)
    t0 = time.monotonic()

    try:
        row = _run_case_inner(job, output_dir)
        elapsed = time.monotonic() - t0

        # Save result
        job_dir = _ensure(output_dir / "cases" / job.db / job.case_name / job.strategy)
        result_path = job_dir / f"result_rep{job.rep}.json"
        result_path.write_text(json.dumps(asdict(row), default=str, indent=2))

        # Mark done
        _ensure(marker.parent)
        marker.write_text(f"bug={row.bug_found} ttfb={row.ttfb_s} elapsed={elapsed:.1f}s\n")
        _log_cmd(cmd_log, f"case {label}", 0, elapsed)

        bug_str = f"BUG at trial {row.trials_to_first} ({row.ttfb_s:.2f}s)" if row.bug_found else f"no bug ({elapsed:.1f}s, {row.total_trials} trials)"
        print(f"  [CASE] {label} → {bug_str}", flush=True)
        return {"status": "ok", **asdict(row)}
    except Exception as e:
        elapsed = time.monotonic() - t0
        _log_cmd(cmd_log, f"case {label}", 1, elapsed)
        print(f"  [CASE] {label} → ERROR: {e}", flush=True)
        traceback.print_exc()
        return {"status": "error", "db": job.db, "case_name": job.case_name,
                "strategy": job.strategy, "rep": job.rep, "error": str(e)}


def _run_case_inner(job: CaseJob, output_dir: Path) -> CaseStudyRow:
    """Dispatch to the appropriate case study runner."""
    cs_dir = _ensure(output_dir / "cases")

    if job.db == "postgres":
        if not _HAS_UCO:
            raise RuntimeError("UCO module not available")
        _uco_setup_schema(CASE_PG_BUGGY_DSN)
        if job.strategy == "schedule_pool":
            pr = run_case_pool_search(
                trial_fn=lambda d: _pg_uco_trial_with_delays(CASE_PG_BUGGY_DSN, d),
                event_ids=PG_UCO_EVENTS, db="postgres",
                case_name="update_conflict_out",
                duration_s=job.duration_s, seed=job.seed,
                hot_events=["foo_commit", "bar_commit", "foo_select",
                            "bar_select", "trouble_update", "trouble_abort"],
            )
            return CaseStudyRow(
                db="postgres", case_name="update_conflict_out",
                strategy=job.strategy, rep=job.rep,
                bug_found=pr.bug_found, ttfb_s=pr.ttfb_s,
                trials_to_first=pr.trials_to_first,
                total_trials=pr.total_trials, total_time_s=pr.total_time_s,
                pool_size=pr.pool_size,
                replay_rate_buggy=(pr.first_bug_record.replay_rate_buggy
                                   if pr.first_bug_record else None),
            )
        else:
            cr = run_pg_case_study(
                dsn=CASE_PG_BUGGY_DSN, strategy=job.strategy,
                duration_s=job.duration_s, rep=job.rep,
            )
            return CaseStudyRow(
                db="postgres", case_name="update_conflict_out",
                strategy=job.strategy, rep=job.rep,
                bug_found=cr.bug_found, ttfb_s=cr.time_to_first_s,
                trials_to_first=cr.trials_to_first,
                total_trials=cr.total_trials, total_time_s=cr.total_time_s,
            )

    elif job.db == "mysql":
        setup = make_adapter("mysql", CASE_MYSQL_DSN)
        try:
            setup.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
        finally:
            setup.close()

        if job.strategy == "schedule_pool":
            pr = run_case_pool_search(
                trial_fn=lambda d: _mysql_rr_trial_with_delays(CASE_MYSQL_DSN, d),
                event_ids=MYSQL_RR_EVENTS, db="mysql",
                case_name="rr_nonexistent_row",
                duration_s=job.duration_s, seed=job.seed,
                hot_events=["t1_commit", "t2_commit", "t3_mid", "t3_update"],
            )
            return CaseStudyRow(
                db="mysql", case_name="rr_nonexistent_row",
                strategy=job.strategy, rep=job.rep,
                bug_found=pr.bug_found, ttfb_s=pr.ttfb_s,
                trials_to_first=pr.trials_to_first,
                total_trials=pr.total_trials, total_time_s=pr.total_time_s,
                pool_size=pr.pool_size,
                replay_rate_buggy=(pr.first_bug_record.replay_rate_buggy
                                   if pr.first_bug_record else None),
            )
        else:
            cr = run_mysql_rr_nonexistent_case(
                dsn=CASE_MYSQL_DSN, strategy=job.strategy,
                duration_s=job.duration_s, rep=job.rep,
                artifact_dir=cs_dir / "mysql_artifacts",
            )
            return CaseStudyRow(
                db="mysql", case_name="rr_nonexistent_row",
                strategy=job.strategy, rep=job.rep,
                bug_found=cr.bug_found, ttfb_s=cr.time_to_first_s,
                trials_to_first=cr.trials_to_first,
                total_trials=cr.total_trials, total_time_s=cr.total_time_s,
            )

    elif job.db == "sqlite":
        keeper = make_adapter("sqlite", CASE_SQLITE_DSN,
                               sqlite_shared_cache=True, sqlite_read_uncommitted=True)
        try:
            keeper.setup(SQLITE_NARROW_DIRTY_SCHEMA)

            if job.strategy == "schedule_pool":
                pr = run_case_pool_search(
                    trial_fn=lambda d: _sqlite_dirty_trial_with_delays(
                        CASE_SQLITE_DSN, d, shared_cache=True, read_uncommitted=True),
                    event_ids=SQLITE_DIRTY_EVENTS, db="sqlite",
                    case_name="narrow_dirty_read",
                    duration_s=job.duration_s, seed=job.seed,
                    hot_events=["writer_hold", "reader_pre", "writer_pre"],
                )
                return CaseStudyRow(
                    db="sqlite", case_name="narrow_dirty_read",
                    strategy=job.strategy, rep=job.rep,
                    bug_found=pr.bug_found, ttfb_s=pr.ttfb_s,
                    trials_to_first=pr.trials_to_first,
                    total_trials=pr.total_trials, total_time_s=pr.total_time_s,
                    pool_size=pr.pool_size,
                    replay_rate_buggy=(pr.first_bug_record.replay_rate_buggy
                                       if pr.first_bug_record else None),
                )
            else:
                cr = run_sqlite_narrow_dirty_case(
                    dsn=CASE_SQLITE_DSN, strategy=job.strategy,
                    duration_s=job.duration_s, rep=job.rep,
                    artifact_dir=cs_dir / "sqlite_artifacts",
                )
                return CaseStudyRow(
                    db="sqlite", case_name="narrow_dirty_read",
                    strategy=job.strategy, rep=job.rep,
                    bug_found=cr.bug_found, ttfb_s=cr.time_to_first_s,
                    trials_to_first=cr.trials_to_first,
                    total_trials=cr.total_trials, total_time_s=cr.total_time_s,
                )
        finally:
            keeper.close()
    else:
        raise ValueError(f"Unknown DB: {job.db}")


def run_db_worker(db: str, output_dir: Path, cmd_log: Path) -> Dict[str, Any]:
    """Run all coverage + case jobs for one DB, sequentially."""
    print(f"\n{'='*60}\n  DB Worker: {db}\n{'='*60}", flush=True)
    results = {"db": db, "coverage": [], "cases": []}

    # Coverage jobs
    for job in build_coverage_jobs(db):
        r = run_one_coverage(job, output_dir, cmd_log)
        results["coverage"].append(r)

    # Case study jobs
    for job in build_case_jobs(db):
        r = run_one_case(job, output_dir, cmd_log)
        results["cases"].append(r)

    print(f"\n  DB Worker {db} complete.", flush=True)
    return results


def _db_worker_wrapper(args):
    """Wrapper for multiprocessing."""
    db, output_dir_str, cmd_log_str = args
    return run_db_worker(db, Path(output_dir_str), Path(cmd_log_str))


def aggregate_coverage_csvs(output_dir: Path) -> Tuple[List[CoveragePoint], Path]:
    """Read all per-job timeseries CSVs and merge into one."""
    all_points: List[CoveragePoint] = []
    cov_dir = output_dir / "coverage"
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

    # Write merged timeseries
    merged_path = cov_dir / "coverage_timeseries.csv"
    if all_points:
        write_coverage_csv(all_points, merged_path)

    return all_points, merged_path


def aggregate_case_results(output_dir: Path) -> Tuple[List[Dict], Path]:
    """Read all per-job case result JSONs and merge."""
    rows = []
    cases_dir = output_dir / "cases"
    for result_file in sorted(cases_dir.rglob("result_rep*.json")):
        with result_file.open("r", encoding="utf-8") as f:
            rows.append(json.load(f))

    csv_path = cases_dir / "case_results.csv"
    if rows:
        fields = ["db", "case_name", "strategy", "rep", "bug_found",
                   "ttfb_s", "trials_to_first", "total_trials", "total_time_s",
                   "replay_rate_buggy", "replay_rate_control", "stable", "pool_size"]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    return rows, csv_path


def write_coverage_summary(points: List[CoveragePoint], output_dir: Path) -> Path:
    """Write coverage summary CSV."""
    cov_dir = output_dir / "coverage"
    csv_path = cov_dir / "coverage_summary.csv"
    rows = []

    for db in DBS:
        for wl in WORKLOADS:
            for strat in STRATEGIES:
                filtered = [p for p in points
                            if p.db == db and p.workload == wl and p.strategy == strat]
                if not filtered:
                    continue
                # Final point per rep
                by_rep: Dict[int, CoveragePoint] = {}
                for p in filtered:
                    if p.rep not in by_rep or p.time_s > by_rep[p.rep].time_s:
                        by_rep[p.rep] = p
                final_states = [by_rep[r].unique_states for r in sorted(by_rep)]
                final_trials = [by_rep[r].txns_done for r in sorted(by_rep)]

                auc = _compute_mean_auc(points, db, wl, strat)
                duration = max(by_rep[r].time_s for r in by_rep) if by_rep else 0
                mean_states = statistics.mean(final_states) if final_states else 0
                std_states = statistics.stdev(final_states) if len(final_states) > 1 else 0
                mean_trials = statistics.mean(final_trials) if final_trials else 0
                normalized_auc = auc / duration if duration > 0 else 0
                states_per_100 = (mean_states / mean_trials * 100) if mean_trials > 0 else 0

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
                })

    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
    return csv_path


def _compute_mean_auc(points: List[CoveragePoint], db: str, wl: str, strat: str) -> float:
    reps_data: Dict[int, List[CoveragePoint]] = defaultdict(list)
    for p in points:
        if p.db == db and p.workload == wl and p.strategy == strat:
            reps_data[p.rep].append(p)
    aucs = []
    for rep, pts in reps_data.items():
        pts_sorted = sorted(pts, key=lambda x: x.time_s)
        if len(pts_sorted) < 2:
            continue
        auc = 0.0
        for i in range(1, len(pts_sorted)):
            dt = pts_sorted[i].time_s - pts_sorted[i - 1].time_s
            avg_s = (pts_sorted[i].unique_states + pts_sorted[i - 1].unique_states) / 2.0
            auc += dt * avg_s
        aucs.append(auc)
    return statistics.mean(aucs) if aucs else 0.0


def load_short_baseline() -> Tuple[Optional[List[Dict]], Optional[List[Dict]]]:
    """Load existing quick-eval baseline results."""
    cov_rows = None
    case_rows = None

    cov_path = SHORT_BASELINE_DIR / "micro_coverage" / "coverage_summary.csv"
    if cov_path.exists():
        with cov_path.open("r", encoding="utf-8") as f:
            cov_rows = list(csv.DictReader(f))

    case_path = SHORT_BASELINE_DIR / "case_studies" / "case_study_results.csv"
    if case_path.exists():
        with case_path.open("r", encoding="utf-8") as f:
            case_rows = list(csv.DictReader(f))

    return cov_rows, case_rows


def build_coverage_comparison(
    long_summary_path: Path,
    short_cov_rows: Optional[List[Dict]],
    output_dir: Path,
) -> Path:
    """Build side-by-side coverage comparison CSV."""
    compare_dir = _ensure(output_dir / "compare")
    csv_path = compare_dir / "coverage_compare.csv"

    # Load long results
    long_rows = []
    if long_summary_path.exists():
        with long_summary_path.open("r", encoding="utf-8") as f:
            long_rows = list(csv.DictReader(f))

    # Index short by (db, workload, strategy)
    short_idx = {}
    if short_cov_rows:
        for r in short_cov_rows:
            key = (r["db"], r["workload"], r["strategy"])
            short_idx[key] = r

    # Build comparison
    out_rows = []
    for lr in long_rows:
        key = (lr["db"], lr["workload"], lr["strategy"])
        sr = short_idx.get(key)
        out_rows.append({
            "db": lr["db"],
            "workload": lr["workload"],
            "strategy": lr["strategy"],
            "short_states": float(sr["mean_final_states"]) if sr else "",
            "long_states": lr["mean_final_states"],
            "short_auc": float(sr["mean_auc"]) if sr else "",
            "long_auc": lr["mean_auc"],
            "short_norm_auc": round(float(sr["mean_auc"]) / 15.0, 4) if sr and float(sr.get("mean_auc", 0)) > 0 else "",
            "long_norm_auc": lr.get("normalized_auc", ""),
            "short_states_per_100": float(sr["states_per_100_trials"]) if sr else "",
            "long_states_per_100": lr["states_per_100_trials"],
            "short_duration_s": "15.0",
            "long_duration_s": lr.get("duration_s", "900"),
        })

    if out_rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=out_rows[0].keys())
            w.writeheader()
            w.writerows(out_rows)

    return csv_path


def build_case_comparison(
    long_case_rows: List[Dict],
    short_case_rows: Optional[List[Dict]],
    output_dir: Path,
) -> Path:
    """Build side-by-side case study comparison CSV."""
    compare_dir = _ensure(output_dir / "compare")
    csv_path = compare_dir / "case_compare.csv"

    # Index short by (db, case_name, strategy)
    short_idx: Dict[Tuple, List[Dict]] = defaultdict(list)
    if short_case_rows:
        for r in short_case_rows:
            key = (r["db"], r["case_name"], r["strategy"])
            short_idx[key].append(r)

    # Index long by same key
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

        short_ttfbs = [float(r["ttfb_s"]) for r in sr_list if r.get("ttfb_s") and r["ttfb_s"] != "None"]
        long_ttfbs = [float(r["ttfb_s"]) for r in lr_list if r.get("ttfb_s") and r["ttfb_s"] != "None"]

        out_rows.append({
            "db": db, "case_name": case, "strategy": strat,
            "short_success_rate": f"{short_success}/{short_total}" if short_total else "",
            "long_success_rate": f"{long_success}/{long_total}" if long_total else "",
            "short_median_ttfb": round(statistics.median(short_ttfbs), 2) if short_ttfbs else "",
            "long_median_ttfb": round(statistics.median(long_ttfbs), 2) if long_ttfbs else "",
            "short_duration_s": "15.0",
            "long_duration_s": "600",
        })

    if out_rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=out_rows[0].keys())
            w.writeheader()
            w.writerows(out_rows)

    return csv_path


def generate_plots(all_points: List[CoveragePoint], output_dir: Path) -> List[str]:
    """Generate all plots."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[WARN] matplotlib not available, skipping plots", file=sys.stderr)
        return []

    plot_dir = _ensure(output_dir / "plots")
    generated = []

    # 1. Coverage curves for each DB × workload
    for db in DBS:
        for wl in WORKLOADS:
            fig, ax = plt.subplots(figsize=(10, 6))
            colors = {"stress": "tab:blue", "random": "tab:orange",
                      "heuristic": "tab:green", "schedule_pool": "tab:red"}
            for strat in STRATEGIES:
                strat_pts = [p for p in all_points
                             if p.db == db and p.workload == wl and p.strategy == strat]
                if not strat_pts:
                    continue
                # Average across reps: bucket by ~1s intervals
                by_time: Dict[int, List[int]] = defaultdict(list)
                for p in strat_pts:
                    bucket = int(p.time_s)
                    by_time[bucket].append(p.unique_states)
                times = sorted(by_time.keys())
                avgs = [statistics.mean(by_time[t]) for t in times]
                ax.plot(times, avgs, label=strat, color=colors.get(strat), linewidth=2)

            ax.set_title(f"Coverage: {wl} ({db}) — 900s")
            ax.set_xlabel("Time (s)")
            ax.set_ylabel("Unique Canonical States")
            ax.legend()
            ax.grid(True, alpha=0.3)
            out = plot_dir / f"coverage_{db}_{wl}.png"
            fig.tight_layout()
            fig.savefig(out, dpi=140)
            plt.close(fig)
            generated.append(str(out))

    # 2. Short vs long overlay (load short baseline timeseries if available)
    short_ts_path = SHORT_BASELINE_DIR / "micro_coverage" / "coverage_timeseries.csv"
    short_points = []
    if short_ts_path.exists():
        with short_ts_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                short_points.append(CoveragePoint(
                    time_s=float(row["time_s"]),
                    db=row["db"],
                    workload=row["workload"],
                    strategy=row["strategy"],
                    rep=int(row["rep"]),
                    unique_states=int(row["unique_states"]),
                    txns_done=int(row["txns_done"]),
                    bugs_found=int(row["bugs_found"]),
                ))

    if short_points:
        for db in DBS:
            for wl in WORKLOADS:
                fig, ax = plt.subplots(figsize=(10, 6))
                colors = {"stress": "tab:blue", "random": "tab:orange",
                          "heuristic": "tab:green", "schedule_pool": "tab:red"}
                for strat in STRATEGIES:
                    # Long (solid)
                    lpts = [p for p in all_points
                            if p.db == db and p.workload == wl and p.strategy == strat]
                    if lpts:
                        by_t: Dict[int, List[int]] = defaultdict(list)
                        for p in lpts:
                            by_t[int(p.time_s)].append(p.unique_states)
                        ts = sorted(by_t.keys())
                        avgs = [statistics.mean(by_t[t]) for t in ts]
                        ax.plot(ts, avgs, label=f"{strat} (900s)",
                                color=colors.get(strat), linewidth=2, linestyle="-")

                    # Short (dashed)
                    spts = [p for p in short_points
                            if p.db == db and p.workload == wl and p.strategy == strat]
                    if spts:
                        by_t2: Dict[int, List[int]] = defaultdict(list)
                        for p in spts:
                            by_t2[int(p.time_s)].append(p.unique_states)
                        ts2 = sorted(by_t2.keys())
                        avgs2 = [statistics.mean(by_t2[t]) for t in ts2]
                        ax.plot(ts2, avgs2, label=f"{strat} (15s)",
                                color=colors.get(strat), linewidth=1.5, linestyle="--", alpha=0.6)

                ax.set_title(f"Short vs Long: {wl} ({db})")
                ax.set_xlabel("Time (s)")
                ax.set_ylabel("Unique Canonical States")
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)
                out = plot_dir / f"short_vs_long_{db}_{wl}.png"
                fig.tight_layout()
                fig.savefig(out, dpi=140)
                plt.close(fig)
                generated.append(str(out))

    # 3. Pool growth: schedule_pool only
    for db in DBS:
        for wl in WORKLOADS:
            pool_pts = [p for p in all_points
                        if p.db == db and p.workload == wl and p.strategy == "schedule_pool"]
            if not pool_pts:
                continue
            fig, axes = plt.subplots(1, 2, figsize=(14, 5))
            # Left: states over time
            by_rep: Dict[int, List[CoveragePoint]] = defaultdict(list)
            for p in pool_pts:
                by_rep[p.rep].append(p)
            for rep, pts in sorted(by_rep.items()):
                pts_s = sorted(pts, key=lambda x: x.time_s)
                axes[0].plot([p.time_s for p in pts_s],
                             [p.unique_states for p in pts_s],
                             label=f"rep {rep}", linewidth=1.5)
            axes[0].set_title(f"Pool Coverage: {wl} ({db})")
            axes[0].set_xlabel("Time (s)")
            axes[0].set_ylabel("Unique States")
            axes[0].legend()
            axes[0].grid(True, alpha=0.3)

            # Right: trials over time
            for rep, pts in sorted(by_rep.items()):
                pts_s = sorted(pts, key=lambda x: x.time_s)
                axes[1].plot([p.time_s for p in pts_s],
                             [p.txns_done for p in pts_s],
                             label=f"rep {rep}", linewidth=1.5)
            axes[1].set_title(f"Pool Trials: {wl} ({db})")
            axes[1].set_xlabel("Time (s)")
            axes[1].set_ylabel("Total Trials")
            axes[1].legend()
            axes[1].grid(True, alpha=0.3)

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
    cov_compare_path: Path,
    case_compare_path: Path,
    wall_clock_s: float,
) -> Path:
    """Generate LONG_COMPARE_REPORT.md."""
    report_path = output_dir / "LONG_COMPARE_REPORT.md"

    lines = [
        "# Long-Run Comparison Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        f"Configuration: micro={MICRO_DURATION_S:.0f}s, cases={CASE_DURATION_S:.0f}s, "
        f"reps={REPS}, seeds={SEEDS}",
        f"Short baseline: {SHORT_BASELINE_DIR.name} (15s, 3 reps)",
        f"Total wall-clock: {wall_clock_s/3600:.1f}h",
        "",
        "---",
        "",
    ]


    lines.append("## 1. Coverage Growth (900s)\n")
    lines.append("| DB | Workload | Strategy | Final States | AUC | Norm AUC | States/100 |")
    lines.append("|---|---|---|---|---|---|---|")

    cov_summary_path = output_dir / "coverage" / "coverage_summary.csv"
    if cov_summary_path.exists():
        with cov_summary_path.open("r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                lines.append(
                    f"| {r['db']} | {r['workload']} | {r['strategy']} "
                    f"| {r['mean_final_states']}±{r['std_final_states']} "
                    f"| {r['mean_auc']} | {r.get('normalized_auc', '')} "
                    f"| {r['states_per_100_trials']} |"
                )
    lines.append("")


    lines.append("## 2. Case Studies (600s)\n")
    cases_by_key: Dict[Tuple, List[Dict]] = defaultdict(list)
    for r in case_rows:
        key = (r["db"], r["case_name"], r["strategy"])
        cases_by_key[key].append(r)

    lines.append("| DB | Case | Strategy | Success Rate | Median TTFB | Mean Trials |")
    lines.append("|---|---|---|---|---|---|")
    for key in sorted(cases_by_key.keys()):
        db, case, strat = key
        rlist = cases_by_key[key]
        n_total = len(rlist)
        n_success = sum(1 for r in rlist if str(r.get("bug_found", "")).lower() == "true")
        ttfbs = [float(r["ttfb_s"]) for r in rlist if r.get("ttfb_s") and str(r["ttfb_s"]) != "None"]
        trials = [int(r["total_trials"]) for r in rlist]
        med_ttfb = f"{statistics.median(ttfbs):.2f}s" if ttfbs else "—"
        mean_t = round(statistics.mean(trials), 1) if trials else "—"
        lines.append(f"| {db} | {case} | {strat} | {n_success}/{n_total} | {med_ttfb} | {mean_t} |")
    lines.append("")


    lines.append("## 3. Short vs Long Coverage Comparison\n")
    if cov_compare_path.exists():
        lines.append("| DB | Workload | Strategy | Short States (15s) | Long States (900s) | Short S/100 | Long S/100 |")
        lines.append("|---|---|---|---|---|---|---|")
        with cov_compare_path.open("r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                lines.append(
                    f"| {r['db']} | {r['workload']} | {r['strategy']} "
                    f"| {r.get('short_states', '')} | {r['long_states']} "
                    f"| {r.get('short_states_per_100', '')} | {r['long_states_per_100']} |"
                )
    lines.append("")


    lines.append("## 4. Short vs Long Case Comparison\n")
    if case_compare_path.exists():
        lines.append("| DB | Case | Strategy | Short Success | Long Success | Short TTFB | Long TTFB |")
        lines.append("|---|---|---|---|---|---|---|")
        with case_compare_path.open("r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                lines.append(
                    f"| {r['db']} | {r['case_name']} | {r['strategy']} "
                    f"| {r.get('short_success_rate', '')} | {r['long_success_rate']} "
                    f"| {r.get('short_median_ttfb', '')} | {r.get('long_median_ttfb', '')} |"
                )
    lines.append("")


    lines.append("## 5. Decision Questions\n")
    lines.append("### Q1: Does longer runtime change scheduler ranking for coverage?")
    lines.append("")
    lines.append("*Answer populated after data analysis — see coverage_compare.csv*")
    lines.append("")
    lines.append("### Q2: Does schedule_pool become more competitive with random?")
    lines.append("")
    lines.append("*Answer populated after data analysis — see coverage_compare.csv*")
    lines.append("")
    lines.append("### Q3: Does longer runtime help PG update_conflict_out?")
    lines.append("")
    lines.append("*Answer populated after data analysis — see case_compare.csv*")
    lines.append("")
    lines.append("### Q4: Are differences stronger than in 60s suite?")
    lines.append("")
    lines.append("*Answer populated after data analysis*")
    lines.append("")
    lines.append("### Q5: How should long-run results be used in the paper?")
    lines.append("")
    lines.append("*Answer populated after data analysis*")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def write_methodology(output_dir: Path):
    """Write docs/notes/long_compare_methodology.md."""
    doc_dir = _ensure(ROOT / "docs" / "notes")
    doc_path = doc_dir / "long_compare_methodology.md"

    git_hash = os.popen("git -C " + str(ROOT) + " log --oneline -1 2>/dev/null").read().strip()
    py_ver = sys.version.split()[0]
    cpu_count = os.cpu_count() or "unknown"

    content = f"""# Long-Run Comparison Methodology

## Experiment Design
- **Micro coverage**: {MICRO_DURATION_S:.0f}s per run, {REPS} reps
- **Case studies**: {CASE_DURATION_S:.0f}s per run, {REPS} reps
- **Seeds**: {SEEDS}
- **Strategies**: {', '.join(STRATEGIES)}
- **Workloads**: {', '.join(WORKLOADS)}
- **DBs**: {', '.join(DBS)}
- **n_workers**: {N_WORKERS}

## Parallelization
- One process per DB (3 in parallel)
- Sequential within each DB: coverage first, then case studies
- No concurrent jobs against the same DB

## Short Baseline
- Directory: {SHORT_BASELINE_DIR.name}
- Duration: 15s, 3 reps

## System Info
- Git: {git_hash}
- Python: {py_ver}
- CPUs: {cpu_count}
- Docker compose: docker-compose-repro.yml

## DB Endpoints
- PostgreSQL 12.3: localhost:54323 (buggy case studies)
- PostgreSQL 12.4: localhost:54324 (control)
- MySQL 8.0.34: localhost:33080
- SQLite: in-memory shared cache

## Output Directory
- {output_dir}

## Time Budget
- Per DB: ~{MICRO_DURATION_S * len(WORKLOADS) * len(STRATEGIES) * REPS / 3600 + CASE_DURATION_S * len(STRATEGIES) * REPS / 3600:.1f}h
- Total wall-clock (parallel): ~{max(MICRO_DURATION_S * len(WORKLOADS) * len(STRATEGIES) * REPS / 3600, CASE_DURATION_S * len(STRATEGIES) * REPS / 3600) + CASE_DURATION_S * len(STRATEGIES) * REPS / 3600:.1f}h

## Metric Definitions
- **AUC**: Area under coverage curve (trapezoidal rule)
- **Normalized AUC**: AUC / duration_s (comparable across different durations)
- **States/100 Trials**: coverage efficiency
- **TTFB**: Time to first bug (wall-clock)

## Fairness
- Same workload logic, DB setup, n_workers=4, seeds across all strategies
- Fresh DB state for each run
- Same oracle and canonical state definitions as short suite
"""

    doc_path.write_text(content, encoding="utf-8")
    # Also copy to output dir
    (output_dir / "methodology.md").write_text(content, encoding="utf-8")
    return doc_path


def write_inventory(output_dir: Path):
    """Write inventory.txt."""
    git_hash = os.popen("git -C " + str(ROOT) + " log --oneline -1 2>/dev/null").read().strip()
    py_ver = sys.version.split()[0]
    cpu_count = os.cpu_count() or "unknown"

    lines = [
        f"timestamp: {datetime.now().isoformat()}",
        f"git: {git_hash}",
        f"python: {py_ver}",
        f"cpus: {cpu_count}",
        f"docker_compose: docker-compose-repro.yml",
        f"short_baseline: {SHORT_BASELINE_DIR}",
        "",
        "DB DSNs:",
        f"  postgres (micro): {DSNS['postgres']}",
        f"  mysql (micro): {DSNS['mysql']}",
        f"  sqlite (micro): {DSNS['sqlite']}",
        f"  pg_buggy (case): {CASE_PG_BUGGY_DSN}",
        f"  pg_control (case): {CASE_PG_CONTROL_DSN}",
        f"  mysql (case): {CASE_MYSQL_DSN}",
        f"  sqlite (case): {CASE_SQLITE_DSN}",
        "",
        "Config:",
        f"  micro_duration_s: {MICRO_DURATION_S}",
        f"  case_duration_s: {CASE_DURATION_S}",
        f"  reps: {REPS}",
        f"  seeds: {SEEDS}",
        f"  strategies: {STRATEGIES}",
        f"  workloads: {WORKLOADS}",
        f"  dbs: {DBS}",
        "",
        "Expected jobs:",
        f"  coverage: {len(WORKLOADS)} wl × {len(STRATEGIES)} strat × {REPS} reps × {len(DBS)} dbs = {len(WORKLOADS)*len(STRATEGIES)*REPS*len(DBS)}",
        f"  cases: {len(DBS)} dbs × {len(STRATEGIES)} strat × {REPS} reps = {len(DBS)*len(STRATEGIES)*REPS}",
    ]
    (output_dir / "inventory.txt").write_text("\n".join(lines), encoding="utf-8")


def write_run_matrix(output_dir: Path):
    """Write run_matrix.csv listing every job."""
    fields = ["type", "db", "workload", "case_name", "strategy", "rep", "seed",
              "duration_s", "marker"]
    rows = []
    for db in DBS:
        for job in build_coverage_jobs(db):
            rows.append({
                "type": "coverage", "db": job.db, "workload": job.workload,
                "case_name": "", "strategy": job.strategy, "rep": job.rep,
                "seed": job.seed, "duration_s": job.duration_s, "marker": job.marker_file,
            })
        for job in build_case_jobs(db):
            rows.append({
                "type": "case", "db": job.db, "workload": "",
                "case_name": job.case_name, "strategy": job.strategy,
                "rep": job.rep, "seed": job.seed, "duration_s": job.duration_s,
                "marker": job.marker_file,
            })

    csv_path = output_dir / "run_matrix.csv"
    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Long-run comparison experiment")
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
        # Find most recent long_compare dir
        candidates = sorted(ROOT.glob("results/long_compare_*"))
        if not candidates:
            print("No previous long_compare results found to resume.")
            sys.exit(1)
        output_dir = candidates[-1]
        print(f"Resuming from: {output_dir}")
    else:
        output_dir = ROOT / "results" / f"long_compare_{_ts()}"

    _ensure(output_dir)
    cmd_log = output_dir / "commands.log"

    # PID-file guard: prevent duplicate launches
    pid_file = output_dir / ".pid"
    if not args.dry_run:
        if pid_file.exists():
            old_pid = pid_file.read_text().strip()
            try:
                os.kill(int(old_pid), 0)
                print(f"ERROR: Another instance is already running (PID {old_pid}).")
                print("Remove {pid_file} if the process is stale.")
                sys.exit(1)
            except (OSError, ValueError):
                pass  # stale PID file, safe to overwrite
        pid_file.write_text(str(os.getpid()))

    # Write setup files
    write_inventory(output_dir)
    write_run_matrix(output_dir)
    write_methodology(output_dir)

    if args.dry_run:
        print("DRY RUN — Job matrix written to:")
        print(f"  {output_dir / 'run_matrix.csv'}")
        print(f"\nTotal coverage jobs: {len(WORKLOADS)*len(STRATEGIES)*REPS*len(DBS)}")
        print(f"Total case jobs: {len(DBS)*len(STRATEGIES)*REPS}")
        return

    print(f"\n{'='*60}")
    print(f"  Long-Run Comparison Experiment")
    print(f"  Output: {output_dir}")
    print(f"  Micro: {MICRO_DURATION_S}s × {REPS} reps")
    print(f"  Cases: {CASE_DURATION_S}s × {REPS} reps")
    print(f"  DBs: {', '.join(DBS)} ({'parallel' if not args.sequential else 'sequential'})")
    print(f"{'='*60}\n")

    wall_start = time.monotonic()

    if args.sequential:
        all_results = []
        for db in DBS:
            r = run_db_worker(db, output_dir, cmd_log)
            all_results.append(r)
    else:
        # Parallel: one process per DB
        # Use fork-safe approach: run in threads since we share memory objects
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

    # Aggregate CSVs
    all_points, _ = aggregate_coverage_csvs(output_dir)
    case_rows, _ = aggregate_case_results(output_dir)

    # Write summary
    cov_summary_path = write_coverage_summary(all_points, output_dir)
    print(f"  Coverage summary: {cov_summary_path}")

    # Load short baseline
    short_cov, short_cases = load_short_baseline()

    # Build comparisons
    cov_compare_path = build_coverage_comparison(cov_summary_path, short_cov, output_dir)
    case_compare_path = build_case_comparison(case_rows, short_cases, output_dir)
    print(f"  Coverage comparison: {cov_compare_path}")
    print(f"  Case comparison: {case_compare_path}")

    # Plots
    print("  Generating plots...")
    plot_files = generate_plots(all_points, output_dir)
    print(f"  Generated {len(plot_files)} plots")

    # Report
    report_path = generate_report(output_dir, all_points, case_rows,
                                   cov_compare_path, case_compare_path, wall_elapsed)
    print(f"  Report: {report_path}")


    print(f"\n{'='*60}")
    print(f"  DONE")
    print(f"  Wall-clock: {wall_elapsed/3600:.1f}h ({wall_elapsed:.0f}s)")
    print(f"  Output: {output_dir}")
    print(f"  Report: {report_path}")
    print(f"{'='*60}")

    # Clean up PID file
    pid_file = output_dir / ".pid"
    pid_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
