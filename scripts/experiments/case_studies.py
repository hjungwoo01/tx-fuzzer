"""
Case studies for time-to-exposure benchmarking per DB.

Each case study compares stress vs random vs heuristic strategies
and includes a control that must show 0 false positives.
"""

from __future__ import annotations

import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.experiments.db_adapter import DBAdapter, ExecResult, make_adapter
from scripts.experiments.error_taxonomy import ErrorCategory


@dataclass
class CaseResult:
    db: str
    case_name: str
    strategy: str
    rep: int
    bug_found: bool
    time_to_first_s: Optional[float]
    trials_to_first: Optional[int]
    total_trials: int
    total_time_s: float
    control: bool  # True if this is a control run (expect 0 bugs)


# ---------------------------------------------------------------------------
# Postgres: update_conflict_out (PG 12.3 vs 12.4)
# ---------------------------------------------------------------------------
# Real PG SSI bug: 3 sessions (foo/bar/trouble).
# trouble creates an aborted physical version that confuses PG 12.3's
# conflict-out tracking, allowing a rw-antidependency cycle (foo/bar)
# to go undetected.  Fixed in PG 12.4.
#
# We delegate to the existing proven implementation in
# scripts/discover_update_conflict_out.py for the trial logic.
# ---------------------------------------------------------------------------

# Import the existing well-tested trial runner and discovery helpers
try:
    from scripts.discover_update_conflict_out import (
        run_concurrent_trial as _uco_trial,
        setup_schema as _uco_setup_schema,
        reset_tables as _uco_reset_tables,
        build_natural_trace as _uco_build_trace,
        analyze_antideps as _uco_analyze_antideps,
        build_delay_candidates as _uco_build_candidates,
        select_delays_lottery as _uco_select_delays,
    )
    _HAS_UCO = True
except ImportError:
    _HAS_UCO = False

# Cached heuristic analysis (computed once per process)
_UCO_CANDIDATES: Optional[list] = None


def _get_uco_candidates() -> list:
    """Build and cache the anti-dependency delay candidates."""
    global _UCO_CANDIDATES
    if _UCO_CANDIDATES is None:
        trace = _uco_build_trace("update")
        antideps = _uco_analyze_antideps(trace)
        _UCO_CANDIDATES = _uco_build_candidates(trace, antideps)
    return _UCO_CANDIDATES


def _pg_uco_trial_adapter(dsn: str, strategy: str, rng: random.Random, k: int = 3) -> Tuple[bool, str]:
    """
    Run a single update_conflict_out trial using the existing 3-session
    runner, mapping our strategy names to delay configurations.

    - stress: no delays (baseline)
    - random: uniform random delays (0-100ms) on all events
    - heuristic: anti-dependency analysis + weighted lottery (K delay points)
    """
    delays: Dict[str, float] = {}

    if strategy == "stress":
        # No delays — pure OS scheduling (baseline)
        pass
    elif strategy == "random":
        # Random delays on all session events (uniform 0-100ms)
        all_eids = [
            "foo_begin", "foo_select", "foo_insert", "foo_commit",
            "bar_begin", "bar_insert", "bar_select", "bar_commit",
            "trouble_begin", "trouble_update", "trouble_abort",
        ]
        for eid in all_eids:
            delays[eid] = rng.uniform(0, 0.100)
    elif strategy == "heuristic":
        # Use the real anti-dependency analysis and weighted lottery
        candidates = _get_uco_candidates()
        delays = _uco_select_delays(candidates, rng, k=k)
    else:
        pass

    _uco_reset_tables(dsn)
    result = _uco_trial(dsn, variant="update", delays=delays)
    if result.bug_found:
        return True, "SSI conflict-out miss: rw-cycle undetected"
    return False, f"got_40001={result.got_40001}"


def run_pg_case_study(
    *,
    dsn: str,
    strategy: str,
    duration_s: float = 60.0,
    max_trials: int = 200,
    delay_ms: float = 5.0,
    rep: int = 0,
    control: bool = False,
) -> CaseResult:
    """Run PG update_conflict_out case study using the real 3-session workload."""
    if not _HAS_UCO:
        raise RuntimeError(
            "Cannot import discover_update_conflict_out. "
            "Ensure psycopg2 is installed and PYTHONPATH includes project root."
        )
    rng = random.Random()  # non-deterministic per rep
    _uco_setup_schema(dsn)
    start = time.monotonic()
    trial = 0
    for trial in range(1, max_trials + 1):
        if (time.monotonic() - start) > duration_s:
            break
        bug, reason = _pg_uco_trial_adapter(dsn, strategy, rng)
        if bug:
            ttf = time.monotonic() - start
            return CaseResult(
                db="postgres", case_name="update_conflict_out",
                strategy=strategy, rep=rep, bug_found=True,
                time_to_first_s=ttf, trials_to_first=trial,
                total_trials=trial, total_time_s=ttf, control=control,
            )
    total = time.monotonic() - start
    return CaseResult(
        db="postgres", case_name="update_conflict_out",
        strategy=strategy, rep=rep, bug_found=False,
        time_to_first_s=None, trials_to_first=None,
        total_trials=trial, total_time_s=total, control=control,
    )


# ---------------------------------------------------------------------------
# MySQL: Non-repeatable read at REPEATABLE READ
# ---------------------------------------------------------------------------
MYSQL_CASE_SCHEMA = [
    "DROP TABLE IF EXISTS people",
    "CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(100), gender VARCHAR(10))",
    "INSERT INTO people VALUES (1, 'Alice', 'F')",
]


def _mysql_nrr_trial(
    dsn: str, strategy: str, delay_ms: float = 5.0, use_for_update: bool = False,
) -> Tuple[bool, str]:
    """
    Non-repeatable read under MySQL RR:
      Txn R: BEGIN; SELECT name; UPDATE gender; SELECT name; COMMIT
      Txn W: UPDATE name; COMMIT

    Bug: Txn R sees different name between its two SELECTs.
    Control: SELECT ... FOR UPDATE or SERIALIZABLE eliminates this.
    """
    r_adapter = make_adapter("mysql", dsn)
    w_adapter = make_adapter("mysql", dsn)
    try:
        # Reset
        reset = make_adapter("mysql", dsn)
        reset.begin()
        reset.exec("UPDATE people SET name = 'Alice', gender = 'F' WHERE id = 1")
        reset.commit()
        reset.close()

        lock_suffix = " FOR UPDATE" if use_for_update else ""
        iso = "SERIALIZABLE" if use_for_update else "REPEATABLE READ"

        read_done = threading.Event()
        results: Dict[str, Any] = {}
        barrier = threading.Barrier(2, timeout=10)

        def txn_r() -> None:
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                pass
            r_adapter.begin(iso)
            # First SELECT
            r1 = r_adapter.exec(f"SELECT name FROM people WHERE id = 1{lock_suffix}")
            name1 = r1.rows[0][0] if r1.rows else None

            if strategy == "heuristic":
                time.sleep(delay_ms / 1000)  # Give writer time to sneak in

            # UPDATE gender (touches the row, forces a new snapshot in some engines)
            r_adapter.exec("UPDATE people SET gender = 'M' WHERE id = 1")

            # Second SELECT
            r2 = r_adapter.exec(f"SELECT name FROM people WHERE id = 1{lock_suffix}")
            name2 = r2.rows[0][0] if r2.rows else None

            cr = r_adapter.commit()
            results["name1"] = name1
            results["name2"] = name2
            results["r_error"] = cr.error_category.value if cr.error_category != ErrorCategory.OK else None

        def txn_w() -> None:
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                pass
            if strategy == "random":
                time.sleep(random.uniform(0, delay_ms / 1000))
            w_adapter.begin("REPEATABLE READ")
            w_adapter.exec("UPDATE people SET name = 'Bob' WHERE id = 1")
            w_adapter.commit()

        tr = threading.Thread(target=txn_r)
        tw = threading.Thread(target=txn_w)
        tr.start()
        tw.start()
        tr.join(timeout=15)
        tw.join(timeout=15)

        name1 = results.get("name1")
        name2 = results.get("name2")
        if name1 is not None and name2 is not None and name1 != name2:
            return True, f"Non-repeatable read: name changed from {name1!r} to {name2!r}"
        return False, f"name1={name1!r}, name2={name2!r}"
    finally:
        r_adapter.close()
        w_adapter.close()


def run_mysql_case_study(
    *,
    dsn: str,
    strategy: str,
    duration_s: float = 60.0,
    max_trials: int = 200,
    delay_ms: float = 5.0,
    rep: int = 0,
    control: bool = False,
    use_for_update: bool = False,
) -> CaseResult:
    """Run MySQL NRR case study."""
    setup_adapter = make_adapter("mysql", dsn)
    try:
        setup_adapter.setup(MYSQL_CASE_SCHEMA)
    finally:
        setup_adapter.close()

    start = time.monotonic()
    trial = 0
    for trial in range(1, max_trials + 1):
        if (time.monotonic() - start) > duration_s:
            break
        bug, reason = _mysql_nrr_trial(dsn, strategy, delay_ms, use_for_update)
        if bug:
            ttf = time.monotonic() - start
            return CaseResult(
                db="mysql", case_name="nonrepeatable_read_rr",
                strategy=strategy, rep=rep, bug_found=True,
                time_to_first_s=ttf, trials_to_first=trial,
                total_trials=trial, total_time_s=ttf, control=control,
            )
    total = time.monotonic() - start
    return CaseResult(
        db="mysql", case_name="nonrepeatable_read_rr",
        strategy=strategy, rep=rep, bug_found=False,
        time_to_first_s=None, trials_to_first=None,
        total_trials=trial if trial else max_trials,
        total_time_s=total, control=control,
    )


# ---------------------------------------------------------------------------
# SQLite: Dirty read with shared-cache + read_uncommitted
# ---------------------------------------------------------------------------
SQLITE_CASE_SCHEMA = [
    "DROP TABLE IF EXISTS shared_test",
    "CREATE TABLE shared_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
    "INSERT INTO shared_test VALUES (1, 'clean')",
]


def _sqlite_dirty_read_trial(
    dsn: str, strategy: str, delay_ms: float = 5.0,
    shared_cache: bool = True, read_uncommitted: bool = True,
) -> Tuple[bool, str]:
    """
    Dirty read case study:
      Writer: BEGIN; UPDATE value='dirty'; sleep; ROLLBACK
      Reader: BEGIN; SELECT value; COMMIT
      Bug: reader sees 'dirty' before rollback.
    """
    w_adapter = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted,
    )
    r_adapter = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted,
    )
    try:
        # Reset
        w_adapter.begin()
        w_adapter.exec("UPDATE shared_test SET value = 'clean' WHERE id = 1")
        w_adapter.commit()

        barrier = threading.Barrier(2, timeout=10)
        results: Dict[str, Any] = {}

        def writer() -> None:
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                pass
            w_adapter.begin("IMMEDIATE")
            w_adapter.exec("UPDATE shared_test SET value = 'dirty' WHERE id = 1")
            # Hold dirty state
            time.sleep(delay_ms / 1000 * 2)
            w_adapter.rollback()

        def reader() -> None:
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                pass
            if strategy == "heuristic":
                # Delay reader so it reads during writer's hold
                time.sleep(delay_ms / 1000)
            elif strategy == "random":
                time.sleep(random.uniform(0, delay_ms / 1000))
            r_adapter.begin("DEFERRED")
            r = r_adapter.exec("SELECT value FROM shared_test WHERE id = 1")
            val = r.rows[0][0] if r.rows else None
            r_adapter.commit()
            results["value"] = val

        tw = threading.Thread(target=writer)
        tr = threading.Thread(target=reader)
        tw.start()
        tr.start()
        tw.join(timeout=15)
        tr.join(timeout=15)

        val = results.get("value")
        if val == "dirty":
            return True, "Dirty read: reader saw uncommitted 'dirty' value"
        return False, f"value={val!r}"
    finally:
        w_adapter.close()
        r_adapter.close()


def run_sqlite_case_study(
    *,
    dsn: str,
    strategy: str,
    duration_s: float = 60.0,
    max_trials: int = 200,
    delay_ms: float = 10.0,
    rep: int = 0,
    control: bool = False,
    shared_cache: bool = True,
    read_uncommitted: bool = True,
) -> CaseResult:
    """Run SQLite dirty read case study."""
    setup_adapter = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted,
    )
    try:
        setup_adapter.setup(SQLITE_CASE_SCHEMA)
    finally:
        setup_adapter.close()

    start = time.monotonic()
    trial = 0
    for trial in range(1, max_trials + 1):
        if (time.monotonic() - start) > duration_s:
            break
        bug, reason = _sqlite_dirty_read_trial(
            dsn, strategy, delay_ms,
            shared_cache=shared_cache,
            read_uncommitted=read_uncommitted,
        )
        if bug:
            ttf = time.monotonic() - start
            return CaseResult(
                db="sqlite", case_name="dirty_read_shared_cache",
                strategy=strategy, rep=rep, bug_found=True,
                time_to_first_s=ttf, trials_to_first=trial,
                total_trials=trial, total_time_s=ttf, control=control,
            )
    total = time.monotonic() - start
    return CaseResult(
        db="sqlite", case_name="dirty_read_shared_cache",
        strategy=strategy, rep=rep, bug_found=False,
        time_to_first_s=None, trials_to_first=None,
        total_trials=trial if trial else max_trials,
        total_time_s=total, control=control,
    )
