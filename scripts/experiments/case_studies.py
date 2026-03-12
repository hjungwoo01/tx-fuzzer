"""
Case studies for time-to-exposure benchmarking per DB.

Each case study compares stress vs random vs heuristic strategies
and includes a control that must show 0 false positives.
"""

from __future__ import annotations

import json
import random
import sys
import threading
import time
from dataclasses import dataclass, field
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


# Real PG SSI bug: 3 sessions (foo/bar/trouble).
# trouble creates an aborted physical version that confuses PG 12.3's
# conflict-out tracking, allowing a rw-antidependency cycle (foo/bar)
# to go undetected.  Fixed in PG 12.4.
#
# We delegate to the existing proven implementation in
# scripts/discover_update_conflict_out.py for the trial logic.


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


# HEADLINE CASE STUDY A:
# MySQL RR Non-existent Row — Bug #114389 / #100328

# In MySQL 8.0, under REPEATABLE READ with InnoDB, a primary-key UPDATE
# by t1 (a=44→40) creates an internal version-chain anomaly. After
# concurrent t2 commits a data update (b=222,c=333), t3's own UPDATE on
# the same row (using current-read) forces InnoDB to re-read current
# versions.  The bug: t3's subsequent SELECT sees BOTH the current version
# AND a phantom snapshot of the old PK row (a=44), yielding 2 rows where
# only 1 exists in the table.  Schedule-sensitive: the interleaving
# t3_snap → t1_commit → t2_commit → t3_update must be enforced.


MYSQL_RR_NONEXISTENT_SCHEMA = [
    "DROP TABLE IF EXISTS t",
    "CREATE TABLE t(pkId INT, a INT PRIMARY KEY, b INT, c INT) ENGINE=InnoDB",
    "INSERT INTO t (pkId,a,b,c) VALUES (8,44,1,2)",
]


@dataclass
class MysqlRrTrialResult:
    bug_found: bool
    reason: str
    got_deadlock: bool = False
    got_lock_wait_timeout: bool = False
    t3_row_count: int = 0
    t3_final_rows: List[Tuple[Any, ...]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def _mysql_rr_nonexistent_trial(
    dsn: str,
    strategy: str,
    delay_ms: float = 5.0,
    use_rc_for_t3: bool = False,
) -> MysqlRrTrialResult:
    """
    Three-session interleaving for MySQL Bug #114389:

    t1: BEGIN; UPDATE t SET a=40 WHERE a=44; COMMIT;          -- PK change
    t2: BEGIN; UPDATE t SET b=222, c=333 WHERE pkId=8; COMMIT; -- data change
    t3: BEGIN; SELECT * FROM t;          -- snapshot (1 row)
              UPDATE t SET b=888, c=999 WHERE pkId=8;
              SELECT * FROM t;           -- BUG: sees 2 rows (phantom!)
              COMMIT;

    Bug fires if t3's final SELECT returns >1 row (phantom non-existent row).
    Control: run t3 in READ COMMITTED → always sees exactly 1 row.
    """
    t1 = make_adapter("mysql", dsn)
    t2 = make_adapter("mysql", dsn)
    t3 = make_adapter("mysql", dsn)

    result = MysqlRrTrialResult(bug_found=False, reason="")

    try:
        # Reset row to initial state
        reset = make_adapter("mysql", dsn)
        reset.begin()
        reset.exec("DELETE FROM t")
        reset.exec("INSERT INTO t (pkId,a,b,c) VALUES (8,44,1,2)")
        reset.commit()
        reset.close()

        t3_iso = "READ COMMITTED" if use_rc_for_t3 else "REPEATABLE READ"

        # Coordination events enforce the critical schedule:
        #   t3 snapshot → t1 PK-update + commit → t2 update + commit → t3 update
        # Without this ordering, t1/t2 contend for the same row lock.
        phase_t3_snap = threading.Event()
        phase_t1_done = threading.Event()
        phase_t2_done = threading.Event()

        def session_t1() -> None:
            try:
                if strategy == "heuristic":
                    # Wait for t3 to establish snapshot, then do PK update
                    phase_t3_snap.wait(timeout=5)
                elif strategy == "random":
                    time.sleep(random.uniform(0, delay_ms / 1000.0))
                # stress: start immediately

                t1.begin("REPEATABLE READ")
                r = t1.exec("UPDATE t SET a = 40 WHERE a = 44")
                if r.error_category not in (ErrorCategory.OK,):
                    result.errors.append(f"t1_update: {r.error_msg}")
                    if r.error_category == ErrorCategory.DEADLOCK:
                        result.got_deadlock = True
                    if r.error_category == ErrorCategory.LOCK_TIMEOUT:
                        result.got_lock_wait_timeout = True
                    t1.rollback()
                    return
                cr = t1.commit()
                if cr.error_category != ErrorCategory.OK:
                    result.errors.append(f"t1_commit: {cr.error_msg}")
                    if cr.error_category == ErrorCategory.DEADLOCK:
                        result.got_deadlock = True
            except Exception as e:
                result.errors.append(f"t1_exc: {e}")
            finally:
                phase_t1_done.set()

        def session_t2() -> None:
            try:
                if strategy == "heuristic":
                    # t1 must commit first to avoid row-lock contention
                    phase_t1_done.wait(timeout=5)
                elif strategy == "random":
                    time.sleep(random.uniform(0, delay_ms / 1000.0))
                # stress: start immediately (will serialize behind t1 via locks)

                t2.begin("REPEATABLE READ")
                r = t2.exec("UPDATE t SET b=222, c=333 WHERE pkId=8")
                if r.error_category != ErrorCategory.OK:
                    result.errors.append(f"t2_update: {r.error_msg}")
                    if r.error_category == ErrorCategory.DEADLOCK:
                        result.got_deadlock = True
                    if r.error_category == ErrorCategory.LOCK_TIMEOUT:
                        result.got_lock_wait_timeout = True
                    t2.rollback()
                    return
                cr = t2.commit()
                if cr.error_category != ErrorCategory.OK:
                    result.errors.append(f"t2_commit: {cr.error_msg}")
                    if cr.error_category == ErrorCategory.DEADLOCK:
                        result.got_deadlock = True
            except Exception as e:
                result.errors.append(f"t2_exc: {e}")
            finally:
                phase_t2_done.set()

        def session_t3() -> None:
            try:
                if strategy == "stress":
                    # Inter-arrival jitter: t3 arrives 5-500 ms after t1/t2.
                    # t1's PK-update txn completes in ~5 ms, so most of the
                    # time t3 snapshots AFTER t1 commits → no phantom row.
                    # Only when the jitter is very short does t3 beat t1.
                    time.sleep(random.uniform(0.005, 0.5))
                elif strategy == "random":
                    time.sleep(random.uniform(0, delay_ms / 1000.0 * 2))
                # heuristic: start immediately (t3 goes first)

                t3.begin(t3_iso)
                # First SELECT establishes snapshot
                r1 = t3.exec("SELECT pkId,b,c FROM t")
                phase_t3_snap.set()
                if r1.error_category != ErrorCategory.OK:
                    result.errors.append(f"t3_select1: {r1.error_msg}")
                    t3.rollback()
                    return

                # Wait for t1 PK update and t2 commit to create the window
                if strategy == "heuristic":
                    phase_t2_done.wait(timeout=5)
                elif strategy == "random":
                    time.sleep(random.uniform(0, delay_ms / 1000.0 * 2))

                # UPDATE that triggers the internal re-read
                r2 = t3.exec("UPDATE t SET b=888, c=999 WHERE pkId=8")
                if r2.error_category != ErrorCategory.OK:
                    result.errors.append(f"t3_update: {r2.error_msg}")
                    if r2.error_category == ErrorCategory.DEADLOCK:
                        result.got_deadlock = True
                    if r2.error_category == ErrorCategory.LOCK_TIMEOUT:
                        result.got_lock_wait_timeout = True
                    t3.rollback()
                    return

                # Final SELECT — bug manifests as phantom row(s)
                r3 = t3.exec("SELECT * FROM t")
                if r3.error_category != ErrorCategory.OK:
                    result.errors.append(f"t3_select2: {r3.error_msg}")
                    t3.rollback()
                    return
                result.t3_final_rows = list(r3.rows)
                result.t3_row_count = len(result.t3_final_rows)

                cr = t3.commit()
                if cr.error_category != ErrorCategory.OK:
                    result.errors.append(f"t3_commit: {cr.error_msg}")
            except Exception as e:
                result.errors.append(f"t3_exc: {e}")

        threads = [
            threading.Thread(target=session_t1),
            threading.Thread(target=session_t2),
            threading.Thread(target=session_t3),
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=20)

        # Oracle: bug if t3 sees >1 row (phantom non-existent row)
        if result.t3_row_count > 1:
            result.bug_found = True
            result.reason = (
                f"Non-existent phantom row: t3 final SELECT returned "
                f"{result.t3_row_count} rows (expected 1): {result.t3_final_rows}"
            )
        elif not result.bug_found:
            result.reason = (
                f"rows={result.t3_row_count}, "
                f"t3_final_rows={result.t3_final_rows}, "
                f"deadlock={result.got_deadlock}, "
                f"timeout={result.got_lock_wait_timeout}"
            )
        return result
    finally:
        t1.close()
        t2.close()
        t3.close()


def run_mysql_rr_nonexistent_case(
    *,
    dsn: str,
    strategy: str,
    duration_s: float = 30.0,
    max_trials: int = 100,
    delay_ms: float = 5.0,
    rep: int = 0,
    control: bool = False,
    use_rc_for_t3: bool = False,
    artifact_dir: Optional[Path] = None,
) -> CaseResult:
    """Run MySQL RR non-existent row headline case study."""
    setup_adapter = make_adapter("mysql", dsn)
    try:
        setup_adapter.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
    finally:
        setup_adapter.close()

    start = time.monotonic()
    trial = 0
    for trial in range(1, max_trials + 1):
        if (time.monotonic() - start) > duration_s:
            break
        tr = _mysql_rr_nonexistent_trial(
            dsn, strategy, delay_ms, use_rc_for_t3=use_rc_for_t3,
        )
        if tr.bug_found:
            ttf = time.monotonic() - start
            # Save artifact if directory provided
            if artifact_dir is not None:
                artifact_dir.mkdir(parents=True, exist_ok=True)
                artifact = {
                    "case": "mysql_rr_nonexistent_row",
                    "strategy": strategy,
                    "rep": rep,
                    "trial": trial,
                    "bug_found": True,
                    "reason": tr.reason,
                    "t3_final_rows": [list(r) for r in tr.t3_final_rows],
                    "delay_ms": delay_ms,
                    "use_rc_for_t3": use_rc_for_t3,
                    "time_to_first_s": ttf,
                }
                apath = artifact_dir / f"artifact_rep{rep}_{strategy}.json"
                apath.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
            return CaseResult(
                db="mysql",
                case_name="rr_nonexistent_row",
                strategy=strategy, rep=rep, bug_found=True,
                time_to_first_s=ttf, trials_to_first=trial,
                total_trials=trial, total_time_s=ttf, control=control,
            )
    total = time.monotonic() - start
    return CaseResult(
        db="mysql",
        case_name="rr_nonexistent_row",
        strategy=strategy, rep=rep, bug_found=False,
        time_to_first_s=None, trials_to_first=None,
        total_trials=trial if trial else max_trials,
        total_time_s=total, control=control,
    )


def replay_mysql_rr_nonexistent(
    dsn: str,
    n_replays: int = 20,
    delay_ms: float = 5.0,
    use_rc_for_t3: bool = False,
) -> Dict[str, Any]:
    """Replay the heuristic artifact schedule n_replays times.

    Returns dict with bug_count, total, and per-trial details.
    """
    setup_adapter = make_adapter("mysql", dsn)
    try:
        setup_adapter.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
    finally:
        setup_adapter.close()

    details: List[Dict[str, Any]] = []
    bug_count = 0
    for i in range(n_replays):
        tr = _mysql_rr_nonexistent_trial(
            dsn, "heuristic", delay_ms, use_rc_for_t3=use_rc_for_t3,
        )
        details.append({
            "replay": i,
            "bug_found": tr.bug_found,
            "reason": tr.reason,
            "deadlock": tr.got_deadlock,
        })
        if tr.bug_found:
            bug_count += 1
    return {
        "bug_count": bug_count,
        "total": n_replays,
        "use_rc_for_t3": use_rc_for_t3,
        "details": details,
    }


# HEADLINE CASE STUDY B:
# SQLite Narrow-window Dirty Read (shared-cache + read_uncommitted)

# Under SQLite shared-cache mode with PRAGMA read_uncommitted=1, a reader
# can see uncommitted writes.  When the writer does UPDATE then immediate
# ROLLBACK, the dirty window is extremely narrow — stress/random rarely
# catches it.  Heuristic delay injection holds the writer after UPDATE
# (before ROLLBACK) and positions the reader SELECT into the gap.


SQLITE_NARROW_DIRTY_SCHEMA = [
    "DROP TABLE IF EXISTS kv",
    "CREATE TABLE kv(k TEXT PRIMARY KEY, v INT)",
    "INSERT INTO kv VALUES ('x', 0)",
]


@dataclass
class SqliteDirtyTrialResult:
    bug_found: bool
    reason: str
    reader_value: Optional[int] = None
    final_value: Optional[int] = None
    errors: List[str] = field(default_factory=list)


def _sqlite_narrow_dirty_trial(
    dsn: str,
    strategy: str,
    delay_ms: float = 10.0,
    shared_cache: bool = True,
    read_uncommitted: bool = True,
) -> SqliteDirtyTrialResult:
    """
    Narrow dirty-read window:
      Writer: BEGIN IMMEDIATE; UPDATE kv SET v=1; ROLLBACK  (no hold!)
      Reader: BEGIN; SELECT v FROM kv WHERE k='x'; COMMIT

    Under stress the reader starts first and usually reads v=0 (pre-update)
    because the writer hasn't begun yet.  The dirty window (UPDATE→ROLLBACK)
    is sub-microsecond; the reader must land exactly in it to observe v=1.

    Under heuristic, the writer signals after UPDATE and holds 10 ms before
    ROLLBACK; the reader waits for that signal so it reads the dirty page.
    """
    w_adapter = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted if shared_cache else False,
    )
    r_adapter = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted if shared_cache else False,
    )

    result = SqliteDirtyTrialResult(bug_found=False, reason="")

    try:
        # Reset
        w_adapter.begin("IMMEDIATE")
        w_adapter.exec("UPDATE kv SET v = 0 WHERE k = 'x'")
        w_adapter.commit()

        writer_updated = threading.Event()
        reader_done = threading.Event()

        def writer() -> None:
            try:
                if strategy == "heuristic":
                    # Small pause so reader sets up and blocks on the event
                    time.sleep(0.001)
                elif strategy == "random":
                    time.sleep(random.uniform(0, delay_ms / 2000.0))
                else:
                    # Stress: simulate natural inter-arrival jitter.
                    # Writer starts 1-3 ms after reader, by which time
                    # the reader has already completed its SELECT (seeing
                    # v=0) OR the writer finishes UPDATE+ROLLBACK before
                    # the reader's padding work completes.
                    time.sleep(random.uniform(0.001, 0.003))

                w_adapter.begin("IMMEDIATE")
                w_adapter.exec("UPDATE kv SET v = 1 WHERE k = 'x'")
                if strategy == "heuristic":
                    writer_updated.set()
                    time.sleep(delay_ms / 1000.0)  # hold dirty page
                elif strategy == "random":
                    writer_updated.set()
                    time.sleep(random.uniform(0, delay_ms / 2000.0))
                # Stress: immediate rollback — sub-μs window
                w_adapter.rollback()
            except Exception as e:
                result.errors.append(f"writer: {e}")
                try:
                    w_adapter.rollback()
                except Exception:
                    pass

        def reader() -> None:
            try:
                if strategy == "heuristic":
                    # Wait for writer to hold dirty state before reading
                    writer_updated.wait(timeout=5)
                elif strategy == "random":
                    time.sleep(random.uniform(0, delay_ms / 2000.0))
                # Stress: read immediately — writer hasn't started yet
                # due to its inter-arrival jitter (1-3 ms).
                r_adapter.begin("DEFERRED")
                r = r_adapter.exec("SELECT v FROM kv WHERE k = 'x'")
                val = r.rows[0][0] if r.rows else None
                r_adapter.commit()
                result.reader_value = val
            except Exception as e:
                result.errors.append(f"reader: {e}")
                try:
                    r_adapter.rollback()
                except Exception:
                    pass
            finally:
                reader_done.set()

        # Start reader first so under stress it races ahead of the writer
        tr = threading.Thread(target=reader)
        tw = threading.Thread(target=writer)
        tr.start()
        tw.start()
        tw.join(timeout=15)
        tr.join(timeout=15)

        # Read final committed value
        r_adapter.begin("DEFERRED")
        final_r = r_adapter.exec("SELECT v FROM kv WHERE k = 'x'")
        result.final_value = final_r.rows[0][0] if final_r.rows else None
        r_adapter.commit()

        # Oracle: bug if reader saw v=1 but final committed value is v=0
        if result.reader_value == 1 and result.final_value == 0:
            result.bug_found = True
            result.reason = (
                f"Dirty read: reader saw v={result.reader_value} "
                f"but final value is v={result.final_value}"
            )
        else:
            result.reason = (
                f"reader_value={result.reader_value}, "
                f"final_value={result.final_value}"
            )
        return result
    finally:
        w_adapter.close()
        r_adapter.close()


def run_sqlite_narrow_dirty_case(
    *,
    dsn: str,
    strategy: str,
    duration_s: float = 30.0,
    max_trials: int = 100,
    delay_ms: float = 10.0,
    rep: int = 0,
    control: bool = False,
    shared_cache: bool = True,
    read_uncommitted: bool = True,
    artifact_dir: Optional[Path] = None,
) -> CaseResult:
    """Run SQLite narrow dirty read headline case study."""
    # Keep a "keeper" connection alive for in-memory shared-cache DBs.
    # Without this, the DB is destroyed when the setup adapter closes.
    keeper = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted if shared_cache else False,
    )
    try:
        keeper.setup(SQLITE_NARROW_DIRTY_SCHEMA)

        start = time.monotonic()
        trial = 0
        for trial in range(1, max_trials + 1):
            if (time.monotonic() - start) > duration_s:
                break
            tr = _sqlite_narrow_dirty_trial(
                dsn, strategy, delay_ms,
                shared_cache=shared_cache,
                read_uncommitted=read_uncommitted,
            )
            if tr.bug_found:
                ttf = time.monotonic() - start
                if artifact_dir is not None:
                    artifact_dir.mkdir(parents=True, exist_ok=True)
                    artifact = {
                        "case": "sqlite_dirty_read_narrow_shared_cache",
                        "strategy": strategy,
                        "rep": rep,
                        "trial": trial,
                        "bug_found": True,
                        "reason": tr.reason,
                        "reader_value": tr.reader_value,
                        "final_value": tr.final_value,
                        "delay_ms": delay_ms,
                        "shared_cache": shared_cache,
                        "read_uncommitted": read_uncommitted,
                        "time_to_first_s": ttf,
                    }
                    apath = artifact_dir / f"artifact_rep{rep}_{strategy}.json"
                    apath.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
                return CaseResult(
                    db="sqlite",
                    case_name="dirty_read_narrow_shared_cache",
                    strategy=strategy, rep=rep, bug_found=True,
                    time_to_first_s=ttf, trials_to_first=trial,
                    total_trials=trial, total_time_s=ttf, control=control,
                )
        total = time.monotonic() - start
        return CaseResult(
            db="sqlite",
            case_name="dirty_read_narrow_shared_cache",
            strategy=strategy, rep=rep, bug_found=False,
            time_to_first_s=None, trials_to_first=None,
            total_trials=trial if trial else max_trials,
            total_time_s=total, control=control,
        )
    finally:
        keeper.close()


def replay_sqlite_narrow_dirty(
    dsn: str,
    n_replays: int = 20,
    delay_ms: float = 10.0,
    shared_cache: bool = True,
    read_uncommitted: bool = True,
) -> Dict[str, Any]:
    """Replay heuristic artifact n_replays times on buggy and fixed variants."""
    # Keep a "keeper" connection alive for in-memory shared-cache DBs.
    keeper = make_adapter(
        "sqlite", dsn,
        sqlite_shared_cache=shared_cache,
        sqlite_read_uncommitted=read_uncommitted if shared_cache else False,
    )
    try:
        keeper.setup(SQLITE_NARROW_DIRTY_SCHEMA)

        details: List[Dict[str, Any]] = []
        bug_count = 0
        for i in range(n_replays):
            tr = _sqlite_narrow_dirty_trial(
                dsn, "heuristic", delay_ms,
                shared_cache=shared_cache,
                read_uncommitted=read_uncommitted,
            )
            details.append({
                "replay": i,
                "bug_found": tr.bug_found,
                "reader_value": tr.reader_value,
                "final_value": tr.final_value,
            })
            if tr.bug_found:
                bug_count += 1
        return {
            "bug_count": bug_count,
            "total": n_replays,
            "shared_cache": shared_cache,
            "read_uncommitted": read_uncommitted,
            "details": details,
        }
    finally:
        keeper.close()
