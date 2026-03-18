"""
Deterministic reproducer for the PostgreSQL SSI "update-conflict-out" bug.

This module implements:
  - A step-by-step deterministic schedule executor (no time.sleep)
  - Both canonical permutations (UPDATE variant + DELETE variant)
  - K-bounded schedule enumeration around the canonical ordering
  - Version-comparison harness (postgres:12.3 vulnerable vs postgres:12.4 fixed)

Bug references:
  - Jepsen report (PG 12.3): https://www.postgresql.org/message-id/db7b729d-0226-d162-a126-8a8ab2dc4443@jepsen.io
  - Fix commit: https://postgrespro.ru/list/id/E1jjQjL-00011k-3w%40gemulon.postgresql.org
  - PG 12.4 release notes: https://www.postgresql.org/docs/release/12.4/
  - Canonical spec: https://github.com/postgres/postgres/blob/master/src/test/isolation/specs/update-conflict-out.spec

Usage:
  # Run against local postgres on default port:
  python scripts/repro_update_conflict_out.py --dsn "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

  # Run against specific PG version containers:
  python scripts/repro_update_conflict_out.py --pg 12.3
  python scripts/repro_update_conflict_out.py --pg 12.4

  # Run both versions comparison:
  python scripts/repro_update_conflict_out.py --compare

  # K-bounded schedule search:
  python scripts/repro_update_conflict_out.py --dsn <dsn> --k-bounded 2
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.errors
except ImportError:
    print("ERROR: psycopg2 is required. Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]


DEFAULT_PORTS = {"12.3": 54323, "12.4": 54324}


def dsn_for_pg_version(version: str) -> str:
    port = DEFAULT_PORTS.get(version, 5432)
    return f"postgres://postgres:postgres@localhost:{port}/postgres?sslmode=disable"


SETUP_SQL = [
    "DROP TABLE IF EXISTS txn0 CASCADE",
    "DROP TABLE IF EXISTS txn1 CASCADE",
    "CREATE TABLE txn0 (id int4 PRIMARY KEY, val TEXT)",
    "CREATE TABLE txn1 (id int4 PRIMARY KEY, val TEXT)",
]


# Each session is a list of (event_id, sql) tuples.
# The global schedule is an ordered list of event_ids.

SESSION_FOO = [
    ("foo_begin",  "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    ("foo_select", "SELECT * FROM txn0 WHERE id = 42"),
    ("foo_insert", "INSERT INTO txn1 SELECT 7, 'foo_insert'"),
    ("foo_commit", "COMMIT"),
]

SESSION_BAR = [
    ("bar_begin",  "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    ("bar_insert", "INSERT INTO txn0 SELECT 42, 'bar_insert'"),
    ("bar_select", "SELECT * FROM txn1 WHERE id = 7"),
    ("bar_commit", "COMMIT"),
]

SESSION_TROUBLE_UPDATE = [
    ("trouble_begin",  "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    ("trouble_update", "UPDATE txn1 SET val = 'add physical version for bar_select' WHERE id = 7"),
    ("trouble_abort",  "ABORT"),
]

SESSION_TROUBLE_DELETE = [
    ("trouble_begin",  "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    ("trouble_delete", "DELETE FROM txn1 WHERE id = 7"),
    ("trouble_abort",  "ABORT"),
]

# Canonical schedules from the Postgres isolation test spec.
# BEGINs happen before the first scheduled step for each session.
CANONICAL_UPDATE = [
    "foo_begin", "bar_begin", "trouble_begin",
    "foo_select",
    "bar_insert",
    "foo_insert",
    "foo_commit",
    "trouble_update",
    "bar_select",
    "bar_commit",
    "trouble_abort",
]

CANONICAL_DELETE = [
    "foo_begin", "bar_begin", "trouble_begin",
    "foo_select",
    "bar_insert",
    "foo_insert",
    "foo_commit",
    "trouble_delete",
    "bar_select",
    "bar_commit",
    "trouble_abort",
]


@dataclass
class StepResult:
    event_id: str
    session: str
    sql: str
    rows: Optional[List[Any]] = None
    error: Optional[str] = None
    sqlstate: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class ScheduleResult:
    variant: str  # "update" or "delete"
    schedule: List[str]
    steps: List[StepResult] = field(default_factory=list)
    bug_reproduced: bool = False
    bar_select_error: Optional[str] = None
    bar_commit_error: Optional[str] = None
    summary: str = ""


class DeterministicExecutor:
    """
    Execute a global schedule of event_ids against three named sessions on
    separate PostgreSQL connections.  Steps are executed one at a time in the
    exact order specified — no threads, no sleeps.
    """

    def __init__(self, dsn: str, variant: str = "update"):
        self.dsn = dsn
        self.variant = variant
        self.conns: Dict[str, Any] = {}       # session_name -> connection
        self.cursors: Dict[str, Any] = {}     # session_name -> cursor

        if variant == "update":
            self.sessions = {
                "foo": SESSION_FOO,
                "bar": SESSION_BAR,
                "trouble": SESSION_TROUBLE_UPDATE,
            }
        elif variant == "delete":
            self.sessions = {
                "foo": SESSION_FOO,
                "bar": SESSION_BAR,
                "trouble": SESSION_TROUBLE_DELETE,
            }
        else:
            raise ValueError(f"Unknown variant: {variant!r}")

        # Build event_id -> (session, sql) lookup.
        self.event_map: Dict[str, Tuple[str, str]] = {}
        for sess_name, steps in self.sessions.items():
            for eid, sql in steps:
                self.event_map[eid] = (sess_name, sql)

    def _connect(self) -> None:
        """Open one connection per session."""
        for name in self.sessions:
            conn = psycopg2.connect(self.dsn)
            conn.autocommit = False
            self.conns[name] = conn
            self.cursors[name] = conn.cursor()

    def _close(self) -> None:
        for name in list(self.conns):
            try:
                self.conns[name].close()
            except Exception:
                pass
        self.conns.clear()
        self.cursors.clear()

    def _setup_schema(self) -> None:
        """Create fresh tables (idempotent)."""
        conn = psycopg2.connect(self.dsn)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for sql in SETUP_SQL:
                    cur.execute(sql)
        finally:
            conn.close()

    def _execute_step(self, event_id: str) -> StepResult:
        session, sql = self.event_map[event_id]
        conn = self.conns[session]
        cur = self.cursors[session]
        result = StepResult(event_id=event_id, session=session, sql=sql)
        t0 = time.monotonic()
        try:
            upper = sql.strip().upper()
            if upper.startswith("BEGIN"):
                cur.execute(sql)
            elif upper.startswith("COMMIT"):
                conn.commit()
            elif upper in ("ABORT", "ROLLBACK"):
                conn.rollback()
            elif upper.startswith("SELECT"):
                cur.execute(sql)
                result.rows = cur.fetchall()
            else:
                cur.execute(sql)
        except Exception as e:
            result.error = str(e)
            # Extract SQLSTATE if available.
            pgcode = getattr(e, "pgcode", None)
            if pgcode:
                result.sqlstate = pgcode
            # After error, the transaction is aborted; need rollback to reuse connection.
            try:
                conn.rollback()
            except Exception:
                pass
        result.duration_ms = (time.monotonic() - t0) * 1000
        return result

    def run(self, schedule: List[str]) -> ScheduleResult:
        """Execute the full schedule and return structured results."""
        sr = ScheduleResult(variant=self.variant, schedule=list(schedule))
        self._setup_schema()
        self._connect()
        try:
            for eid in schedule:
                step = self._execute_step(eid)
                sr.steps.append(step)

                # Track bar_select and bar_commit outcomes specifically.
                if eid == "bar_select":
                    sr.bar_select_error = step.error
                elif eid == "bar_commit":
                    sr.bar_commit_error = step.error
        finally:
            self._close()

        # Determine if the bug is reproduced:
        # On a vulnerable version, bar_select succeeds (no 40001) AND bar_commit succeeds.
        # On a fixed version, either bar_select or bar_commit raises 40001.
        bar_got_40001 = False
        for step in sr.steps:
            if step.event_id in ("bar_select", "bar_commit") and step.sqlstate == "40001":
                bar_got_40001 = True
                break

        if bar_got_40001:
            sr.bug_reproduced = False
            sr.summary = "CORRECT: serialization failure (40001) raised — SSI working properly."
        else:
            # Check if bar_commit actually succeeded
            bar_commit_step = next((s for s in sr.steps if s.event_id == "bar_commit"), None)
            if bar_commit_step and bar_commit_step.error is None:
                sr.bug_reproduced = True
                sr.summary = "BUG REPRODUCED: bar transaction committed without 40001 — SSI conflict-out check failed!"
            else:
                sr.bug_reproduced = False
                sr.summary = f"INCONCLUSIVE: bar_commit had error but not 40001."

        return sr


def _build_session_queues(sessions: Dict[str, List[Tuple[str, str]]]) -> Dict[str, List[str]]:
    """Return per-session FIFO queues of event_ids."""
    return {name: [eid for eid, _ in steps] for name, steps in sessions.items()}


def _enumerate_schedules_k_bounded(
    sessions: Dict[str, List[Tuple[str, str]]],
    baseline_schedule: List[str],
    max_k: int = 2,
) -> List[List[str]]:
    """
    Generate all valid global schedules that are at most K "delays" away from the
    baseline. A delay counts when at a step we pick a different runnable session
    than the baseline would have chosen.

    Respects per-session FIFO ordering.  Returns a deduplicated list.
    """
    queues = _build_session_queues(sessions)

    # Map event_id -> session
    eid_to_session: Dict[str, str] = {}
    for name, eids in queues.items():
        for eid in eids:
            eid_to_session[eid] = name

    # Build baseline session-order (which session the baseline picks at each step)
    baseline_sessions = [eid_to_session[eid] for eid in baseline_schedule]

    results: List[List[str]] = []
    seen: set = set()

    def _runnable(heads: Dict[str, int]) -> List[str]:
        """Return sessions that have a next event available."""
        out = []
        for name, eids in queues.items():
            idx = heads[name]
            if idx < len(eids):
                out.append(name)
        return out

    def _dfs(step: int, heads: Dict[str, int], path: List[str], delays: int):
        if step == len(baseline_schedule):
            key = tuple(path)
            if key not in seen:
                seen.add(key)
                results.append(list(path))
            return

        runnable = _runnable(heads)
        if not runnable:
            return

        baseline_sess = baseline_sessions[step] if step < len(baseline_sessions) else None

        for sess in runnable:
            is_delay = (sess != baseline_sess) if baseline_sess in runnable else False
            new_delays = delays + (1 if is_delay else 0)
            if new_delays > max_k:
                continue

            eid = queues[sess][heads[sess]]
            new_heads = dict(heads)
            new_heads[sess] = heads[sess] + 1
            path.append(eid)
            _dfs(step + 1, new_heads, path, new_delays)
            path.pop()

    init_heads = {name: 0 for name in queues}
    _dfs(0, init_heads, [], 0)
    return results


def print_result(sr: ScheduleResult, verbose: bool = True) -> None:
    variant_label = sr.variant.upper()
    print(f"\n{'='*70}")
    print(f"  Variant: {variant_label}")
    print(f"  Schedule: {' -> '.join(sr.schedule)}")
    print(f"{'='*70}")

    if verbose:
        for step in sr.steps:
            status = "OK"
            extra = ""
            if step.error:
                status = f"ERROR [{step.sqlstate or '?'}]"
                extra = f"  -- {step.error.splitlines()[0]}"
            elif step.rows is not None:
                extra = f"  -> {step.rows}"
            print(f"  [{step.session:>8}] {step.event_id:<20} {status}{extra}")

    print()
    if sr.bug_reproduced:
        print(f"  *** {sr.summary} ***")
    else:
        print(f"  {sr.summary}")
    print()


def result_to_json(sr: ScheduleResult) -> Dict[str, Any]:
    return {
        "variant": sr.variant,
        "schedule": sr.schedule,
        "bug_reproduced": sr.bug_reproduced,
        "summary": sr.summary,
        "bar_select_error": sr.bar_select_error,
        "bar_commit_error": sr.bar_commit_error,
        "steps": [
            {
                "event_id": s.event_id,
                "session": s.session,
                "sql": s.sql,
                "rows": s.rows,
                "error": s.error,
                "sqlstate": s.sqlstate,
                "duration_ms": round(s.duration_ms, 3),
            }
            for s in sr.steps
        ],
    }


def run_canonical(dsn: str, variant: str = "update", verbose: bool = True) -> ScheduleResult:
    """Run the canonical permutation for a variant."""
    schedule = CANONICAL_UPDATE if variant == "update" else CANONICAL_DELETE
    executor = DeterministicExecutor(dsn, variant)
    sr = executor.run(schedule)
    if verbose:
        print_result(sr)
    return sr


def run_k_bounded_search(
    dsn: str,
    variant: str = "update",
    max_k: int = 2,
    verbose: bool = True,
    stop_on_bug: bool = True,
) -> List[ScheduleResult]:
    """
    Enumerate K-bounded schedules around the canonical ordering and run each.
    Returns all results; stops early if a bug is found and stop_on_bug=True.
    """
    if variant == "update":
        sessions = {"foo": SESSION_FOO, "bar": SESSION_BAR, "trouble": SESSION_TROUBLE_UPDATE}
        baseline = CANONICAL_UPDATE
    else:
        sessions = {"foo": SESSION_FOO, "bar": SESSION_BAR, "trouble": SESSION_TROUBLE_DELETE}
        baseline = CANONICAL_DELETE

    schedules = _enumerate_schedules_k_bounded(sessions, baseline, max_k)
    print(f"\n[k-bounded] Variant={variant}, K={max_k}: {len(schedules)} schedules to explore")

    results = []
    for i, sched in enumerate(schedules):
        executor = DeterministicExecutor(dsn, variant)
        sr = executor.run(sched)
        results.append(sr)
        label = "BUG!" if sr.bug_reproduced else "ok"
        if verbose:
            print(f"  Schedule {i+1}/{len(schedules)}: {label}")
        if sr.bug_reproduced and stop_on_bug:
            if verbose:
                print_result(sr)
            break

    return results


def run_comparison(verbose: bool = True) -> Dict[str, Any]:
    """
    Run canonical permutations against both postgres:12.3 and postgres:12.4
    and compare behavior.
    """
    report: Dict[str, Any] = {"versions": {}}

    for version in ("12.3", "12.4"):
        dsn = dsn_for_pg_version(version)
        print(f"\n{'#'*70}")
        print(f"#  PostgreSQL {version} (port {DEFAULT_PORTS[version]})")
        print(f"{'#'*70}")

        version_results = []
        for variant in ("update", "delete"):
            sr = run_canonical(dsn, variant, verbose=verbose)
            version_results.append(result_to_json(sr))

        report["versions"][version] = version_results

    # Summary
    print("\n" + "=" * 70)
    print("  COMPARISON SUMMARY")
    print("=" * 70)

    exit_code = 0
    for version, results in report["versions"].items():
        for r in results:
            variant = r["variant"]
            bug = r["bug_reproduced"]
            if version == "12.3":
                if bug:
                    print(f"  PG {version} [{variant}]: BUG REPRODUCED (expected on vulnerable version)")
                else:
                    print(f"  PG {version} [{variant}]: No bug (SSI may have been fixed in this build)")
            elif version == "12.4":
                if bug:
                    print(f"  PG {version} [{variant}]: BUG REPRODUCED — UNEXPECTED on fixed version!")
                    exit_code = 1
                else:
                    print(f"  PG {version} [{variant}]: CORRECT (40001 raised as expected)")
    print()
    report["exit_code"] = exit_code
    return report


def wait_for_postgres(dsn: str, timeout: float = 30.0, poll: float = 0.5) -> bool:
    """Poll until a connection succeeds or timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return True
        except Exception:
            time.sleep(poll)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deterministic reproducer for PostgreSQL SSI update-conflict-out bug.",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL connection DSN. Overrides --pg.",
    )
    parser.add_argument(
        "--pg",
        default=None,
        choices=["12.3", "12.4"],
        help="Run against a specific PG version container (uses default ports 54323/54324).",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Run against both 12.3 and 12.4 and compare results.",
    )
    parser.add_argument(
        "--variant",
        default="both",
        choices=["update", "delete", "both"],
        help="Which permutation variant to run (default: both).",
    )
    parser.add_argument(
        "--k-bounded",
        type=int,
        default=None,
        metavar="K",
        help="Run K-bounded schedule enumeration (K=0,1,2 recommended).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-step output.",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=30.0,
        help="Seconds to wait for Postgres readiness (default: 30).",
    )
    args = parser.parse_args()

    verbose = not args.quiet

    # Determine mode
    if args.compare:
        # Wait for both
        for version in ("12.3", "12.4"):
            d = dsn_for_pg_version(version)
            print(f"Waiting for PostgreSQL {version} on port {DEFAULT_PORTS[version]}...", end=" ", flush=True)
            if wait_for_postgres(d, timeout=args.wait):
                print("ready.")
            else:
                print("TIMEOUT — skipping or will fail.")
        report = run_comparison(verbose=verbose)
        if args.json:
            print(json.dumps(report, indent=2))
        return report.get("exit_code", 0)

    # Single-version mode
    if args.dsn:
        dsn = args.dsn
    elif args.pg:
        dsn = dsn_for_pg_version(args.pg)
    else:
        dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

    print(f"Waiting for Postgres readiness...", end=" ", flush=True)
    if not wait_for_postgres(dsn, timeout=args.wait):
        print("TIMEOUT — cannot connect.")
        return 2
    print("ready.")

    variants = ["update", "delete"] if args.variant == "both" else [args.variant]
    all_results = []
    exit_code = 0

    if args.k_bounded is not None:
        for v in variants:
            results = run_k_bounded_search(dsn, v, max_k=args.k_bounded, verbose=verbose)
            all_results.extend(results)
            for sr in results:
                if sr.bug_reproduced:
                    exit_code = max(exit_code, 0)  # Bug found is success for repro
    else:
        for v in variants:
            sr = run_canonical(dsn, v, verbose=verbose)
            all_results.append(sr)

    if args.json:
        print(json.dumps([result_to_json(sr) for sr in all_results], indent=2))

    # Exit code: 0 = ran successfully.
    # If any result had an unexpected error (not 40001, not clean success), exit 1.
    for sr in all_results:
        for step in sr.steps:
            if step.error and step.sqlstate and step.sqlstate != "40001":
                print(f"WARNING: unexpected error at {step.event_id}: {step.error}")
                exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
