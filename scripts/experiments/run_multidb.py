"""
Multi-DB evaluation runner — orchestrates all experiments.

Usage:
  python scripts/experiments/run_multidb.py --suite quick
  python scripts/experiments/run_multidb.py --suite long --smoke-only
  python scripts/experiments/run_multidb.py --suite quick --db sqlite
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.error_taxonomy import ErrorCategory, fisher_exact_test, rate_ratio_ci
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
from scripts.experiments.micro_runner import (
    CoveragePoint, TrialResult, run_coverage_session,
    write_coverage_csv, plot_coverage,
)
from scripts.experiments.case_studies import (
    CaseResult, run_pg_case_study, run_mysql_case_study, run_sqlite_case_study,
    run_mysql_rr_nonexistent_case, run_sqlite_narrow_dirty_case,
)
from scripts.experiments.overhead_bench import (
    OverheadRow, run_overhead_benchmark, write_overhead_csv, write_overhead_md,
)
from scripts.repro_pipeline.bug_pack import (
    list_packs, SMOKE_CONFIG as REPRO_SMOKE_CONFIG,
    QUICK_CONFIG as REPRO_QUICK_CONFIG,
)
import scripts.repro_pipeline.packs  # noqa: F401 — registers packs


QUICK_CONFIG = {
    "duration_s": 20,
    "reps": 5,
    "n_workers": 4,
    "delay_ms": 5.0,
    "heuristic_k": 3,
    "case_duration_s": 30,
    "case_max_trials": 100,
    "overhead_duration_s": 10,
}

LONG_CONFIG = {
    "duration_s": 300,  # 5 minutes
    "reps": 5,
    "n_workers": 4,
    "delay_ms": 5.0,
    "heuristic_k": 3,
    "case_duration_s": 600,   # 10 minutes
    "case_max_trials": 1000,
    "overhead_duration_s": 120,
}

# DB-specific DSN overrides
PG_12_3_DSN = os.getenv("PG_12_3_DSN", "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable")
PG_12_4_DSN = os.getenv("PG_12_4_DSN", "postgres://postgres:postgres@localhost:54324/postgres?sslmode=disable")
MYSQL_DSN = os.getenv("MYSQL_DSN", DEFAULT_DSNS["mysql"])
MYSQL_8034_DSN = os.getenv("MYSQL_8034_DSN", "mysql://root:root@localhost:33080/test")
SQLITE_DSN = os.getenv("SQLITE_DSN", "/tmp/tx_fuzzer_multidb.sqlite")


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_available(db: str, dsn: str) -> bool:
    """Check if a DB is connectable."""
    try:
        a = make_adapter(db, dsn)
        if db == "postgres":
            a.exec("SELECT 1")
        elif db == "mysql":
            a.exec("SELECT 1")
        elif db == "sqlite":
            a.exec("SELECT 1")
        a.close()
        return True
    except Exception as e:
        print(f"  [{db}] not available: {e}", file=sys.stderr)
        return False


def run_micro_suite(
    *,
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    cfg: Dict[str, Any],
    smoke_only: bool = False,
) -> List[CoveragePoint]:
    """Run all micro workloads on all DBs with all strategies."""
    print("\n=== MICRO WORKLOAD COVERAGE SUITE ===")
    all_points: List[CoveragePoint] = []
    strategies = ["stress", "random", "heuristic"]
    workload_names = list(MICRO_WORKLOADS.keys())
    reps = 1 if smoke_only else cfg["reps"]
    dur = 5 if smoke_only else cfg["duration_s"]

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            print(f"  SKIP {db} (not available)")
            continue
        for wl_name in workload_names:
            for strat in strategies:
                for rep in range(reps):
                    variant = "buggy"
                    print(f"  [{db}] {wl_name} / {strat} / rep={rep+1}/{reps} ({variant})")
                    try:
                        points, trials = run_coverage_session(
                            db=db, dsn=dsn, workload_name=wl_name,
                            strategy=strat, variant=variant,
                            duration_s=dur, n_workers=cfg["n_workers"],
                            delay_ms=cfg["delay_ms"], heuristic_k=cfg["heuristic_k"],
                            rep=rep,
                        )
                        all_points.extend(points)
                    except Exception as e:
                        print(f"    ERROR: {e}", file=sys.stderr)

                # Also run fixed variant (control) once
                print(f"  [{db}] {wl_name} / {strat} / FIXED (control)")
                try:
                    points, trials = run_coverage_session(
                        db=db, dsn=dsn, workload_name=wl_name,
                        strategy=strat, variant="fixed",
                        duration_s=min(dur, 10), n_workers=cfg["n_workers"],
                        delay_ms=cfg["delay_ms"], heuristic_k=cfg["heuristic_k"],
                        rep=0,
                    )
                    # Control: verify no bugs
                    bugs_in_fixed = sum(1 for p in points if p.bugs_found > 0)
                    if bugs_in_fixed > 0:
                        print(f"    WARNING: Fixed variant found {bugs_in_fixed} bugs (false positive!)")
                except Exception as e:
                    print(f"    ERROR in control: {e}", file=sys.stderr)

    # Write CSV
    csv_path = output_dir / "coverage_timeseries.csv"
    write_coverage_csv(all_points, csv_path)
    print(f"  Wrote {csv_path}")

    # Generate plots
    plots_dir = _ensure_dir(output_dir / "plots")
    for db in dbs:
        db_points = [p for p in all_points if p.db == db]
        if db_points:
            generated = plot_coverage(db_points, plots_dir, db)
            for g in generated:
                print(f"  Plot: {g}")

    return all_points


def run_case_studies(
    *,
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    cfg: Dict[str, Any],
    smoke_only: bool = False,
) -> List[CaseResult]:
    """Run case studies per DB."""
    print("\n=== CASE STUDIES ===")
    results: List[CaseResult] = []
    strategies = ["stress", "random", "heuristic"]
    reps = 1 if smoke_only else cfg["reps"]
    dur = 10 if smoke_only else cfg["case_duration_s"]
    max_trials = 20 if smoke_only else cfg["case_max_trials"]

    # Postgres: update_conflict_out
    if "postgres" in dbs:
        for dsn_label, dsn, is_control in [
            ("pg12.3", PG_12_3_DSN, False),
            ("pg12.4", PG_12_4_DSN, True),
        ]:
            if not _db_available("postgres", dsn):
                print(f"  SKIP postgres ({dsn_label})")
                continue
            for strat in strategies:
                for rep in range(reps):
                    print(f"  [postgres/{dsn_label}] update_conflict_out / {strat} / rep={rep+1}")
                    try:
                        cr = run_pg_case_study(
                            dsn=dsn, strategy=strat,
                            duration_s=dur, max_trials=max_trials,
                            delay_ms=cfg["delay_ms"], rep=rep,
                            control=is_control,
                        )
                        cr.case_name = f"update_conflict_out_{dsn_label}"
                        results.append(cr)
                        status = "BUG" if cr.bug_found else "clean"
                        print(f"    {status} (ttf={cr.time_to_first_s})")
                    except Exception as e:
                        print(f"    ERROR: {e}", file=sys.stderr)

    # MySQL: non-repeatable read
    if "mysql" in dbs:
        mysql_dsn = dsns.get("mysql", MYSQL_DSN)
        if _db_available("mysql", mysql_dsn):
            for for_update, label, is_control in [
                (False, "rr_buggy", False),
                (True, "rr_for_update", True),
            ]:
                for strat in strategies:
                    for rep in range(reps):
                        print(f"  [mysql] {label} / {strat} / rep={rep+1}")
                        try:
                            cr = run_mysql_case_study(
                                dsn=mysql_dsn, strategy=strat,
                                duration_s=dur, max_trials=max_trials,
                                delay_ms=cfg["delay_ms"], rep=rep,
                                control=is_control, use_for_update=for_update,
                            )
                            cr.case_name = f"nonrepeatable_read_{label}"
                            results.append(cr)
                            status = "BUG" if cr.bug_found else "clean"
                            print(f"    {status} (ttf={cr.time_to_first_s})")
                        except Exception as e:
                            print(f"    ERROR: {e}", file=sys.stderr)

    # SQLite: dirty read
    if "sqlite" in dbs:
        sqlite_dsn = dsns.get("sqlite", SQLITE_DSN)
        for sc, ru, label, is_control in [
            (True, True, "shared_cache_dirty", False),
            (False, False, "no_shared_cache", True),
        ]:
            for strat in strategies:
                for rep in range(reps):
                    print(f"  [sqlite] {label} / {strat} / rep={rep+1}")
                    try:
                        cr = run_sqlite_case_study(
                            dsn=sqlite_dsn, strategy=strat,
                            duration_s=dur, max_trials=max_trials,
                            delay_ms=10.0, rep=rep,
                            control=is_control,
                            shared_cache=sc, read_uncommitted=ru,
                        )
                        cr.case_name = f"dirty_read_{label}"
                        results.append(cr)
                        status = "BUG" if cr.bug_found else "clean"
                        print(f"    {status} (ttf={cr.time_to_first_s})")
                    except Exception as e:
                        print(f"    ERROR: {e}", file=sys.stderr)


    # MySQL headline: RR non-existent row (Bug #114389, requires MySQL 8.0.34)
    if "mysql" in dbs:
        mysql_8034_dsn = dsns.get("mysql_8034", MYSQL_8034_DSN)
        if _db_available("mysql", mysql_8034_dsn):
            for use_rc, label, is_control in [
                (False, "rr_nonexistent_buggy", False),
                (True, "rr_nonexistent_fixed_rc", True),
            ]:
                for strat in strategies:
                    for rep in range(reps):
                        print(f"  [mysql/headline] {label} / {strat} / rep={rep+1}")
                        try:
                            cr = run_mysql_rr_nonexistent_case(
                                dsn=mysql_8034_dsn, strategy=strat,
                                duration_s=dur, max_trials=max_trials,
                                delay_ms=cfg["delay_ms"], rep=rep,
                                control=is_control,
                                use_rc_for_t3=use_rc,
                                artifact_dir=output_dir / "artifacts" / "mysql_rr_nonexistent",
                            )
                            cr.case_name = f"rr_nonexistent_{label}"
                            results.append(cr)
                            status = "BUG" if cr.bug_found else "clean"
                            print(f"    {status} (ttf={cr.time_to_first_s})")
                        except Exception as e:
                            print(f"    ERROR: {e}", file=sys.stderr)

    # SQLite headline: Narrow dirty read (shared-cache + read_uncommitted)
    # Uses in-memory shared-cache URI (different from default file-based SQLite)
    if "sqlite" in dbs:
        sqlite_headline_dsn = "file:multidb_headline?mode=memory&cache=shared"
        for sc, ru, label, is_control in [
            (True, True, "narrow_dirty_buggy", False),
            (True, False, "narrow_dirty_fixed_no_ru", True),
        ]:
            for strat in strategies:
                for rep in range(reps):
                    print(f"  [sqlite/headline] {label} / {strat} / rep={rep+1}")
                    try:
                        cr = run_sqlite_narrow_dirty_case(
                            dsn=sqlite_headline_dsn, strategy=strat,
                            duration_s=dur, max_trials=max_trials,
                            delay_ms=10.0, rep=rep,
                            control=is_control,
                            shared_cache=sc, read_uncommitted=ru,
                            artifact_dir=output_dir / "artifacts" / "sqlite_narrow_dirty",
                        )
                        cr.case_name = f"dirty_read_narrow_{label}"
                        results.append(cr)
                        status = "BUG" if cr.bug_found else "clean"
                        print(f"    {status} (ttf={cr.time_to_first_s})")
                    except Exception as e:
                        print(f"    ERROR: {e}", file=sys.stderr)

    # Write case study CSV
    csv_path = output_dir / "case_studies.csv"
    _ensure_dir(csv_path.parent)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["db", "case_name", "strategy", "rep", "bug_found",
                     "time_to_first_s", "trials_to_first", "total_trials",
                     "total_time_s", "control"])
        for cr in results:
            w.writerow([
                cr.db, cr.case_name, cr.strategy, cr.rep, cr.bug_found,
                cr.time_to_first_s, cr.trials_to_first, cr.total_trials,
                cr.total_time_s, cr.control,
            ])
    print(f"  Wrote {csv_path}")
    return results


def run_overhead_suite(
    *,
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    cfg: Dict[str, Any],
    smoke_only: bool = False,
) -> List[OverheadRow]:
    """Run overhead benchmark per DB."""
    print("\n=== OVERHEAD BENCHMARK ===")
    rows: List[OverheadRow] = []
    modes = ["baseline", "analysis", "full"]
    dur = 5 if smoke_only else cfg["overhead_duration_s"]
    workload_names = list(MICRO_WORKLOADS.keys())

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            continue
        for wl_name in workload_names:
            for mode in modes:
                print(f"  [{db}] {wl_name} / {mode}")
                try:
                    r = run_overhead_benchmark(
                        db=db, dsn=dsn, workload_name=wl_name,
                        mode=mode, duration_s=dur,
                        n_workers=cfg["n_workers"], delay_ms=cfg["delay_ms"],
                    )
                    rows.append(r)
                    print(f"    {r.txns_per_sec} txn/s, median={r.median_latency_ms}ms")
                except Exception as e:
                    print(f"    ERROR: {e}", file=sys.stderr)

    csv_path = output_dir / "overhead.csv"
    write_overhead_csv(rows, csv_path)
    md_path = output_dir / "overhead.md"
    write_overhead_md(rows, md_path)
    print(f"  Wrote {csv_path} and {md_path}")
    return rows


@dataclass
class ReproPackResult:
    """Lightweight result for a repro pack run within multidb suite."""
    pack_id: str = ""
    db: str = ""
    strategy: str = ""
    bug_found: bool = False
    time_to_first_s: Optional[float] = None
    total_trials: int = 0
    replay_buggy: int = 0
    replay_total: int = 0
    control_buggy: int = 0
    control_total: int = 0


def run_repro_packs(
    *,
    dbs: List[str],
    output_dir: Path,
    smoke_only: bool = False,
) -> List[ReproPackResult]:
    """Run registered repro pipeline packs as part of multidb suite."""
    print("\n=== REPRO PIPELINE PACKS ===")
    results: List[ReproPackResult] = []
    cfg = REPRO_SMOKE_CONFIG if smoke_only else REPRO_QUICK_CONFIG

    all_packs = list_packs()
    for pack in all_packs:
        if pack.db not in dbs:
            continue
        dsn = pack.buggy_dsn()
        if not pack.is_available(dsn):
            print(f"  SKIP {pack.id} (not available)")
            continue

        try:
            pack.setup(dsn)
        except Exception as e:
            print(f"  SKIP {pack.id} (setup failed: {e})")
            continue

        # Run heuristic discovery only (fastest to confirm)
        print(f"  [{pack.db}] {pack.id} / heuristic")
        disc = pack.run_discovery(dsn, "heuristic", cfg)
        status = "BUG" if disc["bug_found"] else "clean"
        ttf = disc.get("time_to_first_s")
        print(f"    {status} (ttf={ttf})")

        r = ReproPackResult(
            pack_id=pack.id, db=pack.db, strategy="heuristic",
            bug_found=disc["bug_found"],
            time_to_first_s=ttf,
            total_trials=disc["total_trials"],
        )

        # Quick replay if bug was found
        if disc["bug_found"]:
            replay_n = cfg.replay_count
            buggy_replay = pack.run_replay(dsn, replay_n)
            r.replay_buggy = buggy_replay.bug_count
            r.replay_total = buggy_replay.total
            print(f"    replay: {buggy_replay.bug_count}/{buggy_replay.total}")

            if hasattr(pack, "run_control_replay"):
                ctrl = pack.run_control_replay(pack.control_dsn(), replay_n)
            else:
                ctrl_dsn = pack.control_dsn()
                if pack.is_available(ctrl_dsn):
                    pack.setup(ctrl_dsn)
                    ctrl = pack.run_replay(ctrl_dsn, replay_n)
                else:
                    ctrl = None
            if ctrl:
                r.control_buggy = ctrl.bug_count
                r.control_total = ctrl.total
                print(f"    control: {ctrl.bug_count}/{ctrl.total}")

        results.append(r)

    # Write CSV
    csv_path = output_dir / "repro_packs.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pack_id", "db", "strategy", "bug_found",
                     "time_to_first_s", "total_trials",
                     "replay_buggy", "replay_total",
                     "control_buggy", "control_total"])
        for rr in results:
            w.writerow([
                rr.pack_id, rr.db, rr.strategy, rr.bug_found,
                rr.time_to_first_s, rr.total_trials,
                rr.replay_buggy, rr.replay_total,
                rr.control_buggy, rr.control_total,
            ])
    print(f"  Wrote {csv_path}")
    return results


def generate_summary(
    *,
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    case_results: List[CaseResult],
    overhead_rows: List[OverheadRow],
    repro_results: Optional[List[ReproPackResult]] = None,
) -> None:
    """Generate summary.md and report.md."""
    summary_path = output_dir / "summary.md"
    report_path = output_dir / "report.md"

    lines = ["# Multi-DB Evaluation Summary", ""]
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    lines.append("")

    # Coverage summary
    lines.append("## Coverage (Micro Workloads)")
    lines.append("")
    if coverage_points:
        # Final coverage by DB × workload × strategy
        final_points: Dict[str, CoveragePoint] = {}
        for p in coverage_points:
            key = f"{p.db}|{p.workload}|{p.strategy}|{p.rep}"
            if key not in final_points or p.time_s > final_points[key].time_s:
                final_points[key] = p

        lines.append("| DB | Workload | Strategy | Rep | States | Txns | Bugs |")
        lines.append("|---|---|---|---:|---:|---:|---:|")
        for key in sorted(final_points.keys()):
            p = final_points[key]
            lines.append(f"| {p.db} | {p.workload} | {p.strategy} | {p.rep} | {p.unique_states} | {p.txns_done} | {p.bugs_found} |")
    lines.append("")

    # Case study summary
    lines.append("## Case Studies")
    lines.append("")
    if case_results:
        lines.append("| DB | Case | Strategy | Control | Rep | Bug | TTF (s) | Trials |")
        lines.append("|---|---|---|---|---:|---|---:|---:|")
        for cr in case_results:
            ttf_str = f"{cr.time_to_first_s:.2f}" if cr.time_to_first_s is not None else "-"
            trials_str = str(cr.trials_to_first) if cr.trials_to_first is not None else "-"
            lines.append(
                f"| {cr.db} | {cr.case_name} | {cr.strategy} | "
                f"{'yes' if cr.control else 'no'} | {cr.rep} | "
                f"{'YES' if cr.bug_found else 'no'} | {ttf_str} | {trials_str} |"
            )

        # Statistical comparison: heuristic vs stress
        lines.append("")
        lines.append("### Statistical Comparisons")
        lines.append("")
        cases = sorted(set(cr.case_name for cr in case_results))
        for case in cases:
            heuristic_bugs = sum(1 for cr in case_results
                                  if cr.case_name == case and cr.strategy == "heuristic" and not cr.control)
            heuristic_total = sum(1 for cr in case_results
                                   if cr.case_name == case and cr.strategy == "heuristic" and not cr.control)
            stress_bugs = sum(1 for cr in case_results
                               if cr.case_name == case and cr.strategy == "stress" and not cr.control and cr.bug_found)
            stress_total = sum(1 for cr in case_results
                                if cr.case_name == case and cr.strategy == "stress" and not cr.control)
            heuristic_found = sum(1 for cr in case_results
                                    if cr.case_name == case and cr.strategy == "heuristic" and not cr.control and cr.bug_found)
            if heuristic_total > 0 and stress_total > 0:
                p_val = fisher_exact_test(heuristic_found, heuristic_total - heuristic_found,
                                           stress_bugs, stress_total - stress_bugs)
                lines.append(f"- **{case}**: heuristic {heuristic_found}/{heuristic_total} vs stress {stress_bugs}/{stress_total}, Fisher p={p_val:.4f}")
                if heuristic_found > 0 and stress_bugs > 0:
                    rr, lo, hi = rate_ratio_ci(heuristic_found, heuristic_total, stress_bugs, stress_total)
                    lines.append(f"  Rate ratio: {rr:.2f} (95% CI: {lo:.2f}–{hi:.2f})")
    lines.append("")

    # Controls check
    lines.append("## Controls (0 False Positives Check)")
    lines.append("")
    control_results = [cr for cr in case_results if cr.control]
    false_positives = [cr for cr in control_results if cr.bug_found]
    if false_positives:
        lines.append(f"**WARNING**: {len(false_positives)} false positive(s) in controls!")
        for fp in false_positives:
            lines.append(f"  - {fp.db}/{fp.case_name}/{fp.strategy}/rep{fp.rep}")
    else:
        lines.append(f"All {len(control_results)} control runs: 0 false positives.")
    lines.append("")

    # Overhead summary
    lines.append("## Overhead")
    lines.append("")
    if overhead_rows:
        lines.append("| DB | Workload | Mode | Txns/sec | Median (ms) | P95 (ms) |")
        lines.append("|---|---|---|---:|---:|---:|")
        for r in overhead_rows:
            lines.append(f"| {r.db} | {r.workload} | {r.mode} | {r.txns_per_sec} | {r.median_latency_ms} | {r.p95_latency_ms} |")
    lines.append("")

    # Repro pipeline packs
    if repro_results:
        lines.append("## Repro Pipeline Packs")
        lines.append("")
        lines.append("| Pack | DB | Bug Found | TTF (s) | Replay (buggy) | Replay (control) |")
        lines.append("|------|-----|-----------|---------|----------------|------------------|")
        for rr in repro_results:
            ttf_str = f"{rr.time_to_first_s:.2f}" if rr.time_to_first_s is not None else "-"
            buggy_str = f"{rr.replay_buggy}/{rr.replay_total}" if rr.replay_total else "-"
            ctrl_str = f"{rr.control_buggy}/{rr.control_total}" if rr.control_total else "-"
            lines.append(
                f"| {rr.pack_id} | {rr.db} | "
                f"{'YES' if rr.bug_found else 'no'} | {ttf_str} | "
                f"{buggy_str} | {ctrl_str} |"
            )
        lines.append("")

    text = "\n".join(lines)
    summary_path.write_text(text, encoding="utf-8")
    report_path.write_text(text, encoding="utf-8")
    print(f"\nWrote {summary_path}")
    print(f"Wrote {report_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Multi-DB evaluation runner")
    parser.add_argument("--suite", choices=["quick", "long"], default="quick")
    parser.add_argument("--smoke-only", action="store_true", help="Smoke test only (minimal reps/duration)")
    parser.add_argument("--db", nargs="*", default=None, help="Specific DB(s) to run (default: all available)")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--pg-12-3-dsn", default=None)
    parser.add_argument("--pg-12-4-dsn", default=None)
    parser.add_argument("--mysql-dsn", default=None)
    parser.add_argument("--sqlite-dsn", default=None)
    parser.add_argument("--skip-micro", action="store_true")
    parser.add_argument("--skip-cases", action="store_true")
    parser.add_argument("--skip-overhead", action="store_true")
    parser.add_argument("--skip-repro", action="store_true",
                        help="Skip repro pipeline pack checks")
    args = parser.parse_args()

    cfg = QUICK_CONFIG if args.suite == "quick" else LONG_CONFIG
    output_dir = args.output_dir or (ROOT / "results" / f"multidb_{args.suite}")
    _ensure_dir(output_dir)

    # Override DSNs
    global PG_12_3_DSN, PG_12_4_DSN, MYSQL_DSN, SQLITE_DSN
    if args.pg_12_3_dsn:
        PG_12_3_DSN = args.pg_12_3_dsn
    if args.pg_12_4_dsn:
        PG_12_4_DSN = args.pg_12_4_dsn
    if args.mysql_dsn:
        MYSQL_DSN = args.mysql_dsn
    if args.sqlite_dsn:
        SQLITE_DSN = args.sqlite_dsn

    # Determine which DBs to run
    all_dbs = args.db or ["postgres", "mysql", "sqlite"]
    dsns: Dict[str, str] = {
        "postgres": PG_12_3_DSN,  # default to 12.3 for micro workloads
        "mysql": MYSQL_DSN,
        "sqlite": SQLITE_DSN,
    }

    print(f"Multi-DB Evaluation: suite={args.suite}, smoke={args.smoke_only}")
    print(f"DBs: {all_dbs}")
    print(f"Output: {output_dir}")

    coverage_points: List[CoveragePoint] = []
    case_results: List[CaseResult] = []
    overhead_rows: List[OverheadRow] = []
    repro_results: List[ReproPackResult] = []

    if not args.skip_micro:
        coverage_points = run_micro_suite(
            dbs=all_dbs, dsns=dsns, output_dir=output_dir,
            cfg=cfg, smoke_only=args.smoke_only,
        )

    if not args.skip_cases:
        case_results = run_case_studies(
            dbs=all_dbs, dsns=dsns, output_dir=output_dir,
            cfg=cfg, smoke_only=args.smoke_only,
        )

    if not args.skip_overhead:
        overhead_rows = run_overhead_suite(
            dbs=all_dbs, dsns=dsns, output_dir=output_dir,
            cfg=cfg, smoke_only=args.smoke_only,
        )

    if not args.skip_repro:
        repro_results = run_repro_packs(
            dbs=all_dbs, output_dir=output_dir,
            smoke_only=args.smoke_only,
        )

    generate_summary(
        output_dir=output_dir,
        coverage_points=coverage_points,
        case_results=case_results,
        overhead_rows=overhead_rows,
        repro_results=repro_results,
    )

    print("\n=== DONE ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
