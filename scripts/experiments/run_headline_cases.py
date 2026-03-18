"""
Headline case study experiment runner.

Runs the three paper headline cases:
  - PostgreSQL `pg_update_conflict_out`
  - MySQL `mysql_rr_phantom`
  - SQLite `sqlite_narrow_dirty`

Each case is exercised with stress, random, and heuristic strategies
(5 reps each), plus a 20x replay proof on buggy and control variants.

Usage:
  python scripts/experiments/run_headline_cases.py
  python scripts/experiments/run_headline_cases.py --case all
  python scripts/experiments/run_headline_cases.py --case postgres
  python scripts/experiments/run_headline_cases.py --case mysql
  python scripts/experiments/run_headline_cases.py --case sqlite
  python scripts/experiments/run_headline_cases.py --replay-only
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.case_studies import (
    CaseResult,
    run_pg_case_study,
    run_mysql_rr_nonexistent_case,
    replay_mysql_rr_nonexistent,
    run_sqlite_narrow_dirty_case,
    replay_sqlite_narrow_dirty,
)


STRATEGIES = ["stress", "random", "heuristic"]
REPS = 5
DURATION_S = 20
MAX_TRIALS = 100
PG_DELAY_MS = 5.0
MYSQL_DELAY_MS = 5.0
SQLITE_DELAY_MS = 10.0
REPLAY_COUNT = 20

PG_BUGGY_DSN_DEFAULT = os.getenv(
    "PG_BUGGY_DSN",
    "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable",
)
PG_CONTROL_DSN_DEFAULT = os.getenv(
    "PG_CONTROL_DSN",
    "postgres://postgres:postgres@localhost:54324/postgres?sslmode=disable",
)

CASE_ALIASES = {
    "all": {"postgres", "mysql", "sqlite"},
    "both": {"mysql", "sqlite"},
    "postgres": {"postgres"},
    "pg": {"postgres"},
    "pg_update_conflict_out": {"postgres"},
    "mysql": {"mysql"},
    "mysql_rr_phantom": {"mysql"},
    "sqlite": {"sqlite"},
    "sqlite_narrow_dirty": {"sqlite"},
}


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_available(db: str, dsn: str) -> bool:
    try:
        a = make_adapter(db, dsn)
        a.exec("SELECT 1")
        a.close()
        return True
    except Exception as e:
        print(f"  [{db}] not available: {e}", file=sys.stderr)
        return False


def _normalize_case_selection(value: str) -> set[str]:
    key = value.strip().lower()
    if key not in CASE_ALIASES:
        accepted = ", ".join(sorted(CASE_ALIASES))
        raise ValueError(f"unknown --case value {value!r}; expected one of: {accepted}")
    return CASE_ALIASES[key]


def run_postgres_headline(
    buggy_dsn: str,
    control_dsn: str,
    output_dir: Path,
) -> List[CaseResult]:
    """Run PostgreSQL update_conflict_out: buggy + fixed × strategies × reps."""
    print("\n=== PostgreSQL Headline: update_conflict_out (PG 12.3 vs 12.4) ===")
    results: List[CaseResult] = []

    for dsn, label, is_control in [
        (buggy_dsn, "buggy_pg12_3", False),
        (control_dsn, "control_pg12_4", True),
    ]:
        if not _db_available("postgres", dsn):
            print(f"  [{label}] SKIP (not available)")
            continue
        for strat in STRATEGIES:
            for rep in range(REPS):
                print(f"  [{label}] {strat} / rep={rep+1}/{REPS}", end=" ", flush=True)
                try:
                    cr = run_pg_case_study(
                        dsn=dsn,
                        strategy=strat,
                        duration_s=DURATION_S,
                        max_trials=MAX_TRIALS,
                        delay_ms=PG_DELAY_MS,
                        rep=rep,
                        control=is_control,
                    )
                    cr.case_name = f"pg_update_conflict_out_{label}"
                    results.append(cr)
                    status = "BUG" if cr.bug_found else "clean"
                    ttf = f"{cr.time_to_first_s:.2f}s" if cr.time_to_first_s else "-"
                    print(f"→ {status} (ttf={ttf}, trials={cr.total_trials})")
                except Exception as e:
                    print(f"→ ERROR: {e}")
    return results


def run_mysql_headline(
    dsn: str, output_dir: Path
) -> List[CaseResult]:
    """Run MySQL RR non-existent row: buggy + fixed × strategies × reps."""
    print("\n=== MySQL Headline: RR Non-existent Row (Bug #114389) ===")
    results: List[CaseResult] = []
    artifact_dir = _ensure_dir(output_dir / "artifacts" / "mysql_rr_nonexistent")

    for use_rc, label, is_control in [
        (False, "buggy_rr", False),
        (True, "fixed_rc", True),
    ]:
        for strat in STRATEGIES:
            for rep in range(REPS):
                print(f"  [{label}] {strat} / rep={rep+1}/{REPS}", end=" ", flush=True)
                try:
                    cr = run_mysql_rr_nonexistent_case(
                        dsn=dsn,
                        strategy=strat,
                        duration_s=DURATION_S,
                        max_trials=MAX_TRIALS,
                        delay_ms=MYSQL_DELAY_MS,
                        rep=rep,
                        control=is_control,
                        use_rc_for_t3=use_rc,
                        artifact_dir=artifact_dir,
                    )
                    cr.case_name = f"mysql_rr_phantom_{label}"
                    results.append(cr)
                    status = "BUG" if cr.bug_found else "clean"
                    ttf = f"{cr.time_to_first_s:.2f}s" if cr.time_to_first_s else "-"
                    print(f"→ {status} (ttf={ttf}, trials={cr.total_trials})")
                except Exception as e:
                    print(f"→ ERROR: {e}")
    return results


def run_sqlite_headline(
    dsn: str, output_dir: Path
) -> List[CaseResult]:
    """Run SQLite narrow dirty read: buggy + fixed × strategies × reps."""
    print("\n=== SQLite Headline: Narrow Dirty Read (shared-cache) ===")
    results: List[CaseResult] = []
    artifact_dir = _ensure_dir(output_dir / "artifacts" / "sqlite_narrow_dirty")

    for sc, ru, label, is_control in [
        (True, True, "buggy_shared", False),
        (True, False, "fixed_no_ru", True),
    ]:
        for strat in STRATEGIES:
            for rep in range(REPS):
                print(f"  [{label}] {strat} / rep={rep+1}/{REPS}", end=" ", flush=True)
                try:
                    cr = run_sqlite_narrow_dirty_case(
                        dsn=dsn,
                        strategy=strat,
                        duration_s=DURATION_S,
                        max_trials=MAX_TRIALS,
                        delay_ms=SQLITE_DELAY_MS,
                        rep=rep,
                        control=is_control,
                        shared_cache=sc,
                        read_uncommitted=ru,
                        artifact_dir=artifact_dir,
                    )
                    cr.case_name = f"sqlite_narrow_dirty_{label}"
                    results.append(cr)
                    status = "BUG" if cr.bug_found else "clean"
                    ttf = f"{cr.time_to_first_s:.2f}s" if cr.time_to_first_s else "-"
                    print(f"→ {status} (ttf={ttf}, trials={cr.total_trials})")
                except Exception as e:
                    print(f"→ ERROR: {e}")
    return results


def run_replay_proof(
    pg_buggy_dsn: Optional[str],
    pg_control_dsn: Optional[str],
    mysql_dsn: Optional[str],
    sqlite_dsn: Optional[str],
    output_dir: Path,
) -> Dict[str, Any]:
    """Run 20x replay on the selected buggy/control headline cases."""
    print("\n=== REPLAY PROOF (20x each) ===")
    replay_results: Dict[str, Any] = {}
    replay_dir = _ensure_dir(output_dir / "replay")

    if pg_buggy_dsn and pg_control_dsn:
        print("\n  PostgreSQL buggy (PG 12.3) replay 20x...")
        buggy_hits = 0
        for rep in range(REPLAY_COUNT):
            cr = run_pg_case_study(
                dsn=pg_buggy_dsn,
                strategy="heuristic",
                duration_s=DURATION_S,
                max_trials=MAX_TRIALS,
                delay_ms=PG_DELAY_MS,
                rep=rep,
                control=False,
            )
            if cr.bug_found:
                buggy_hits += 1
        print(f"    Buggy: {buggy_hits}/{REPLAY_COUNT}")

        print("  PostgreSQL control (PG 12.4) replay 20x...")
        control_hits = 0
        for rep in range(REPLAY_COUNT):
            cr = run_pg_case_study(
                dsn=pg_control_dsn,
                strategy="heuristic",
                duration_s=DURATION_S,
                max_trials=MAX_TRIALS,
                delay_ms=PG_DELAY_MS,
                rep=rep,
                control=True,
            )
            if cr.bug_found:
                control_hits += 1
        print(f"    Control: {control_hits}/{REPLAY_COUNT}")

        replay_results["pg_update_conflict_out"] = {
            "buggy": {"bug_count": buggy_hits, "total": REPLAY_COUNT},
            "control": {"bug_count": control_hits, "total": REPLAY_COUNT},
        }
        rpath = replay_dir / "pg_update_conflict_out_replay.json"
        rpath.write_text(
            json.dumps(replay_results["pg_update_conflict_out"], indent=2),
            encoding="utf-8",
        )

    if mysql_dsn:
        print("\n  MySQL buggy (RR) replay 20x...")
        buggy = replay_mysql_rr_nonexistent(
            mysql_dsn, n_replays=REPLAY_COUNT,
            delay_ms=MYSQL_DELAY_MS, use_rc_for_t3=False,
        )
        print(f"    Buggy: {buggy['bug_count']}/{buggy['total']}")

        print("  MySQL fixed (RC) replay 20x...")
        fixed = replay_mysql_rr_nonexistent(
            mysql_dsn, n_replays=REPLAY_COUNT,
            delay_ms=MYSQL_DELAY_MS, use_rc_for_t3=True,
        )
        print(f"    Fixed: {fixed['bug_count']}/{fixed['total']}")

        replay_results["mysql_rr_nonexistent"] = {
            "buggy": buggy,
            "fixed": fixed,
        }
        rpath = replay_dir / "mysql_rr_nonexistent_replay.json"
        rpath.write_text(json.dumps(replay_results["mysql_rr_nonexistent"], indent=2), encoding="utf-8")

    if sqlite_dsn:
        print("\n  SQLite buggy (shared-cache + RU) replay 20x...")
        buggy = replay_sqlite_narrow_dirty(
            sqlite_dsn, n_replays=REPLAY_COUNT,
            delay_ms=SQLITE_DELAY_MS,
            shared_cache=True, read_uncommitted=True,
        )
        print(f"    Buggy: {buggy['bug_count']}/{buggy['total']}")

        print("  SQLite fixed (shared-cache, no RU) replay 20x...")
        fixed = replay_sqlite_narrow_dirty(
            sqlite_dsn, n_replays=REPLAY_COUNT,
            delay_ms=SQLITE_DELAY_MS,
            shared_cache=True, read_uncommitted=False,
        )
        print(f"    Fixed: {fixed['bug_count']}/{fixed['total']}")

        replay_results["sqlite_narrow_dirty"] = {
            "buggy": buggy,
            "fixed": fixed,
        }
        rpath = replay_dir / "sqlite_narrow_dirty_replay.json"
        rpath.write_text(json.dumps(replay_results["sqlite_narrow_dirty"], indent=2), encoding="utf-8")

    return replay_results


def write_results_csv(results: List[CaseResult], path: Path) -> None:
    _ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "case_name", "strategy", "rep", "bug_found",
            "time_to_first_s", "trials_to_first", "total_trials",
            "total_time_s", "control",
        ])
        for cr in results:
            w.writerow([
                cr.db, cr.case_name, cr.strategy, cr.rep, cr.bug_found,
                cr.time_to_first_s, cr.trials_to_first, cr.total_trials,
                cr.total_time_s, cr.control,
            ])


def generate_report(
    results: List[CaseResult],
    replay_results: Dict[str, Any],
    output_dir: Path,
) -> None:
    """Generate RUN_REPORT.md with strategy comparison and replay stats."""
    lines = [
        "# Headline Case Studies — Run Report",
        "",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
        f"Budget: {DURATION_S}s / {MAX_TRIALS} trials per rep, {REPS} reps",
        "",
    ]

    # Group by case name
    cases = sorted(set(cr.case_name for cr in results))
    for case in cases:
        case_results = [cr for cr in results if cr.case_name == case]
        is_control = case_results[0].control if case_results else False
        db = case_results[0].db if case_results else "?"

        lines.append(f"## {db.upper()}: {case} {'(CONTROL)' if is_control else ''}")
        lines.append("")
        lines.append("| Strategy | Reps with Bug | Success Rate | Median TTF (s) | Pooled Bug Rate |")
        lines.append("|----------|---------------|-------------|----------------|-----------------|")

        for strat in STRATEGIES:
            strat_results = [cr for cr in case_results if cr.strategy == strat]
            n_bug = sum(1 for cr in strat_results if cr.bug_found)
            n_total = len(strat_results)
            success_rate = f"{n_bug}/{n_total}" if n_total else "-"

            ttfs = sorted(cr.time_to_first_s for cr in strat_results
                          if cr.bug_found and cr.time_to_first_s is not None)
            median_ttf = f"{ttfs[len(ttfs)//2]:.2f}" if ttfs else "-"

            total_trials_all = sum(cr.total_trials for cr in strat_results)
            total_bugs = sum(1 for cr in strat_results if cr.bug_found)
            bug_rate = f"{total_bugs}/{total_trials_all}" if total_trials_all else "-"

            lines.append(f"| {strat} | {success_rate} | {n_bug/n_total*100:.0f}% | {median_ttf} | {bug_rate} |")

        lines.append("")

    # Replay section
    if replay_results:
        lines.append("## Replay Proof (20x)")
        lines.append("")
        for case_key, data in replay_results.items():
            lines.append(f"### {case_key}")
            if "buggy" in data:
                b = data["buggy"]
                lines.append(f"- **Buggy**: {b['bug_count']}/{b['total']} bugs")
            if "fixed" in data:
                f = data["fixed"]
                lines.append(f"- **Fixed**: {f['bug_count']}/{f['total']} bugs")
            if "control" in data:
                c = data["control"]
                lines.append(f"- **Control**: {c['bug_count']}/{c['total']} bugs")
            lines.append("")

    # Quality gates
    lines.append("## Quality Gates")
    lines.append("")
    control_results = [cr for cr in results if cr.control]
    fp = [cr for cr in control_results if cr.bug_found]
    lines.append(f"- Fixed variants false positives: {len(fp)} (expect 0)")

    for case in cases:
        if any(cr.case_name == case and cr.control for cr in results):
            continue
        h_bugs = sum(1 for cr in results if cr.case_name == case and cr.strategy == "heuristic" and cr.bug_found)
        s_bugs = sum(1 for cr in results if cr.case_name == case and cr.strategy == "stress" and cr.bug_found)
        lines.append(f"- {case}: heuristic {h_bugs}/{REPS} vs stress {s_bugs}/{REPS}")

    lines.append("")
    text = "\n".join(lines)
    report_path = output_dir / "RUN_REPORT.md"
    report_path.write_text(text, encoding="utf-8")
    print(f"\nWrote {report_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Headline case study runner for the paper headline packs")
    parser.add_argument("--pg-buggy-dsn", default=PG_BUGGY_DSN_DEFAULT)
    parser.add_argument("--pg-control-dsn", default=PG_CONTROL_DSN_DEFAULT)
    parser.add_argument("--mysql-dsn", default=os.getenv(
        "MYSQL_DSN", "mysql://root:root@127.0.0.1:33080/test"))
    parser.add_argument("--sqlite-dsn", default=os.getenv(
        "SQLITE_DSN", "file:headline_dirty?mode=memory&cache=shared"))
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument(
        "--case",
        default="all",
        help="Selection alias: all, both, postgres, mysql, sqlite, pg_update_conflict_out, mysql_rr_phantom, sqlite_narrow_dirty",
    )
    parser.add_argument("--replay-only", action="store_true")
    parser.add_argument("--skip-replay", action="store_true")
    args = parser.parse_args()
    selected_cases = _normalize_case_selection(args.case)

    ts = time.strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (ROOT / "results" / f"headline_{ts}")
    _ensure_dir(output_dir)

    print(f"Headline Cases Experiment")
    print(f"  Output: {output_dir}")
    print(f"  PG buggy DSN: {args.pg_buggy_dsn}")
    print(f"  PG control DSN: {args.pg_control_dsn}")
    print(f"  MySQL DSN: {args.mysql_dsn}")
    print(f"  SQLite DSN: {args.sqlite_dsn}")

    all_results: List[CaseResult] = []

    if not args.replay_only:
        if "postgres" in selected_cases:
            all_results.extend(
                run_postgres_headline(args.pg_buggy_dsn, args.pg_control_dsn, output_dir)
            )

        if "mysql" in selected_cases:
            if _db_available("mysql", args.mysql_dsn):
                all_results.extend(run_mysql_headline(args.mysql_dsn, output_dir))
            else:
                print("  SKIP MySQL (not available)")

        if "sqlite" in selected_cases:
            all_results.extend(run_sqlite_headline(args.sqlite_dsn, output_dir))

        # Write CSV
        csv_path = output_dir / "headline_case_studies.csv"
        write_results_csv(all_results, csv_path)
        print(f"\nWrote {csv_path}")

    # Replay proof
    replay_results: Dict[str, Any] = {}
    if not args.skip_replay:
        pg_buggy_dsn = args.pg_buggy_dsn if "postgres" in selected_cases else None
        pg_control_dsn = args.pg_control_dsn if "postgres" in selected_cases else None
        mysql_dsn = args.mysql_dsn if "mysql" in selected_cases else None
        sqlite_dsn = args.sqlite_dsn if "sqlite" in selected_cases else None
        if pg_buggy_dsn and not _db_available("postgres", pg_buggy_dsn):
            pg_buggy_dsn = None
        if pg_control_dsn and not _db_available("postgres", pg_control_dsn):
            pg_control_dsn = None
        if mysql_dsn and not _db_available("mysql", mysql_dsn):
            mysql_dsn = None
        replay_results = run_replay_proof(
            pg_buggy_dsn, pg_control_dsn, mysql_dsn, sqlite_dsn, output_dir
        )

    # Generate report
    generate_report(all_results, replay_results, output_dir)

    print("\n=== DONE ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
