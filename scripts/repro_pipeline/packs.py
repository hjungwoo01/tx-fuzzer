"""
Headline BugPack implementations — one per DB engine.

- pg_update_conflict_out  — PostgreSQL SSI bug (PG 12.3 vs 12.4)
- mysql_rr_phantom        — MySQL Bug #114389 phantom non-existent row
- sqlite_narrow_dirty     — SQLite narrow dirty read (shared-cache)
"""

from __future__ import annotations

import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.experiments.db_adapter import make_adapter, DEFAULT_DSNS, ExecResult
from scripts.experiments.error_taxonomy import ErrorCategory
from scripts.repro_pipeline.bug_pack import (
    BugPack, TrialResult, ReplayResult, register_pack,
)


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

_UCO_CANDIDATES: Optional[list] = None


def _get_uco_candidates() -> list:
    global _UCO_CANDIDATES
    if _UCO_CANDIDATES is None:
        trace = _uco_build_trace("update")
        antideps = _uco_analyze_antideps(trace)
        _UCO_CANDIDATES = _uco_build_candidates(trace, antideps)
    return _UCO_CANDIDATES


class PgUpdateConflictOutPack(BugPack):
    """PostgreSQL SSI conflict-out miss — PG 12.3 bug, fixed in 12.4."""

    @property
    def id(self) -> str:
        return "pg_update_conflict_out"

    @property
    def db(self) -> str:
        return "postgres"

    @property
    def description(self) -> str:
        return (
            "SSI conflict-out check misses an rw-antidependency cycle "
            "when trouble session's aborted UPDATE creates a dead physical "
            "tuple version (PG 12.3 bug, fixed in 12.4)"
        )

    def buggy_dsn(self) -> str:
        return os.getenv(
            "PG_12_3_DSN",
            "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable",
        )

    def control_dsn(self) -> str:
        return os.getenv(
            "PG_12_4_DSN",
            "postgres://postgres:postgres@localhost:54324/postgres?sslmode=disable",
        )

    def setup(self, dsn: str) -> None:
        if not _HAS_UCO:
            raise RuntimeError("Cannot import discover_update_conflict_out")
        _uco_setup_schema(dsn)

    def run_trial(
        self,
        dsn: str,
        strategy: str,
        delay_ms: float,
        rng: random.Random,
    ) -> TrialResult:
        if not _HAS_UCO:
            raise RuntimeError("Cannot import discover_update_conflict_out")

        delays: Dict[str, float] = {}
        if strategy == "stress":
            pass
        elif strategy == "random":
            all_eids = [
                "foo_begin", "foo_select", "foo_insert", "foo_commit",
                "bar_begin", "bar_insert", "bar_select", "bar_commit",
                "trouble_begin", "trouble_update", "trouble_abort",
            ]
            for eid in all_eids:
                delays[eid] = rng.uniform(0, 0.100)
        elif strategy == "heuristic":
            candidates = _get_uco_candidates()
            delays = _uco_select_delays(candidates, rng, k=3)

        _uco_reset_tables(dsn)
        result = _uco_trial(dsn, variant="update", delays=delays)

        tr = TrialResult(delays_used=delays)
        if result.bug_found:
            tr.bug_found = True
            tr.reason = "SSI conflict-out miss: rw-cycle undetected"
        else:
            tr.reason = f"got_40001={result.got_40001}"
        return tr

    def is_available(self, dsn: str) -> bool:
        if not _HAS_UCO:
            return False
        try:
            a = make_adapter("postgres", dsn)
            a.exec("SELECT 1")
            a.close()
            return True
        except Exception:
            return False

    def default_delay_ms(self) -> float:
        return 5.0


MYSQL_RR_PHANTOM_SCHEMA = [
    "DROP TABLE IF EXISTS t",
    "CREATE TABLE t(pkId INT, a INT PRIMARY KEY, b INT, c INT) ENGINE=InnoDB",
    "INSERT INTO t (pkId,a,b,c) VALUES (8,44,1,2)",
]


class MysqlRrPhantomPack(BugPack):
    """MySQL Bug #114389 — phantom non-existent row under RR."""

    def __init__(self) -> None:
        self._use_rc_for_control = True

    @property
    def id(self) -> str:
        return "mysql_rr_phantom"

    @property
    def db(self) -> str:
        return "mysql"

    @property
    def description(self) -> str:
        return (
            "MySQL 8.0 InnoDB Bug #114389: under REPEATABLE READ, a PK update "
            "by t1 + concurrent data update by t2 causes t3 to see a phantom "
            "non-existent row after its own UPDATE triggers internal re-read"
        )

    def buggy_dsn(self) -> str:
        return os.getenv(
            "MYSQL_8034_DSN",
            "mysql://root:root@localhost:33080/test",
        )

    def control_dsn(self) -> str:
        # Same DSN, different isolation level (READ COMMITTED for t3)
        return self.buggy_dsn()

    def setup(self, dsn: str) -> None:
        a = make_adapter("mysql", dsn)
        try:
            a.setup(MYSQL_RR_PHANTOM_SCHEMA)
        finally:
            a.close()

    def run_trial(
        self,
        dsn: str,
        strategy: str,
        delay_ms: float,
        rng: random.Random,
        use_rc_for_t3: bool = False,
    ) -> TrialResult:
        t1 = make_adapter("mysql", dsn)
        t2 = make_adapter("mysql", dsn)
        t3 = make_adapter("mysql", dsn)
        tr = TrialResult()

        try:
            # Reset
            reset = make_adapter("mysql", dsn)
            reset.begin()
            reset.exec("DELETE FROM t")
            reset.exec("INSERT INTO t (pkId,a,b,c) VALUES (8,44,1,2)")
            reset.commit()
            reset.close()

            t3_iso = "READ COMMITTED" if use_rc_for_t3 else "REPEATABLE READ"
            errors: List[str] = []
            t3_rows: List[tuple] = []
            got_deadlock = False
            got_timeout = False

            phase_t3_snap = threading.Event()
            phase_t1_done = threading.Event()
            phase_t2_done = threading.Event()

            def session_t1() -> None:
                nonlocal got_deadlock, got_timeout
                try:
                    if strategy == "heuristic":
                        phase_t3_snap.wait(timeout=5)
                    elif strategy == "random":
                        time.sleep(rng.uniform(0, delay_ms / 1000.0))

                    t1.begin("REPEATABLE READ")
                    r = t1.exec("UPDATE t SET a = 40 WHERE a = 44")
                    if r.error_category != ErrorCategory.OK:
                        errors.append(f"t1_update: {r.error_msg}")
                        if r.error_category == ErrorCategory.DEADLOCK:
                            got_deadlock = True
                        if r.error_category == ErrorCategory.LOCK_TIMEOUT:
                            got_timeout = True
                        t1.rollback()
                        return
                    cr = t1.commit()
                    if cr.error_category != ErrorCategory.OK:
                        errors.append(f"t1_commit: {cr.error_msg}")
                        if cr.error_category == ErrorCategory.DEADLOCK:
                            got_deadlock = True
                except Exception as e:
                    errors.append(f"t1_exc: {e}")
                finally:
                    phase_t1_done.set()

            def session_t2() -> None:
                nonlocal got_deadlock, got_timeout
                try:
                    if strategy == "heuristic":
                        phase_t1_done.wait(timeout=5)
                    elif strategy == "random":
                        time.sleep(rng.uniform(0, delay_ms / 1000.0))

                    t2.begin("REPEATABLE READ")
                    r = t2.exec("UPDATE t SET b=222, c=333 WHERE pkId=8")
                    if r.error_category != ErrorCategory.OK:
                        errors.append(f"t2_update: {r.error_msg}")
                        if r.error_category == ErrorCategory.DEADLOCK:
                            got_deadlock = True
                        if r.error_category == ErrorCategory.LOCK_TIMEOUT:
                            got_timeout = True
                        t2.rollback()
                        return
                    cr = t2.commit()
                    if cr.error_category != ErrorCategory.OK:
                        errors.append(f"t2_commit: {cr.error_msg}")
                except Exception as e:
                    errors.append(f"t2_exc: {e}")
                finally:
                    phase_t2_done.set()

            def session_t3() -> None:
                nonlocal got_deadlock, got_timeout
                try:
                    if strategy == "stress":
                        time.sleep(rng.uniform(0.005, 0.5))
                    elif strategy == "random":
                        time.sleep(rng.uniform(0, delay_ms / 1000.0 * 2))

                    t3.begin(t3_iso)
                    r1 = t3.exec("SELECT pkId,b,c FROM t")
                    phase_t3_snap.set()
                    if r1.error_category != ErrorCategory.OK:
                        errors.append(f"t3_select1: {r1.error_msg}")
                        t3.rollback()
                        return

                    if strategy == "heuristic":
                        phase_t2_done.wait(timeout=5)
                    elif strategy == "random":
                        time.sleep(rng.uniform(0, delay_ms / 1000.0 * 2))

                    r2 = t3.exec("UPDATE t SET b=888, c=999 WHERE pkId=8")
                    if r2.error_category != ErrorCategory.OK:
                        errors.append(f"t3_update: {r2.error_msg}")
                        if r2.error_category == ErrorCategory.DEADLOCK:
                            got_deadlock = True
                        if r2.error_category == ErrorCategory.LOCK_TIMEOUT:
                            got_timeout = True
                        t3.rollback()
                        return

                    r3 = t3.exec("SELECT * FROM t")
                    if r3.error_category != ErrorCategory.OK:
                        errors.append(f"t3_select2: {r3.error_msg}")
                        t3.rollback()
                        return
                    t3_rows.extend(r3.rows)
                    t3.commit()
                except Exception as e:
                    errors.append(f"t3_exc: {e}")

            threads = [
                threading.Thread(target=session_t1),
                threading.Thread(target=session_t2),
                threading.Thread(target=session_t3),
            ]
            for th in threads:
                th.start()
            for th in threads:
                th.join(timeout=20)

            tr.extra = {
                "t3_row_count": len(t3_rows),
                "t3_rows": [list(r) for r in t3_rows],
                "errors": errors,
                "got_deadlock": got_deadlock,
                "got_timeout": got_timeout,
            }

            if len(t3_rows) > 1:
                tr.bug_found = True
                tr.reason = (
                    f"Phantom row: t3 final SELECT returned "
                    f"{len(t3_rows)} rows (expected 1): {t3_rows}"
                )
            else:
                tr.reason = (
                    f"rows={len(t3_rows)}, deadlock={got_deadlock}, "
                    f"timeout={got_timeout}"
                )
            return tr
        finally:
            t1.close()
            t2.close()
            t3.close()

    def is_available(self, dsn: str) -> bool:
        try:
            a = make_adapter("mysql", dsn)
            a.exec("SELECT 1")
            a.close()
            return True
        except Exception:
            return False

    def default_delay_ms(self) -> float:
        return 5.0

    def run_replay(self, dsn: str, n: int = 20) -> ReplayResult:
        """Replay with heuristic on buggy variant."""
        self.setup(dsn)
        rng = random.Random(42)
        details: List[Dict[str, Any]] = []
        bug_count = 0
        for i in range(n):
            tr = self.run_trial(dsn, "heuristic", self.default_delay_ms(), rng)
            details.append({"replay": i, "bug_found": tr.bug_found, "reason": tr.reason})
            if tr.bug_found:
                bug_count += 1
        return ReplayResult(bug_count=bug_count, total=n, details=details)

    def run_control_replay(self, dsn: str, n: int = 20) -> ReplayResult:
        """Replay with READ COMMITTED (control) — should produce 0 bugs."""
        self.setup(dsn)
        rng = random.Random(42)
        details: List[Dict[str, Any]] = []
        bug_count = 0
        for i in range(n):
            tr = self.run_trial(dsn, "heuristic", self.default_delay_ms(), rng, use_rc_for_t3=True)
            details.append({"replay": i, "bug_found": tr.bug_found, "reason": tr.reason})
            if tr.bug_found:
                bug_count += 1
        return ReplayResult(bug_count=bug_count, total=n, details=details)


SQLITE_NARROW_SCHEMA = [
    "DROP TABLE IF EXISTS kv",
    "CREATE TABLE kv(k TEXT PRIMARY KEY, v INT)",
    "INSERT INTO kv VALUES ('x', 0)",
]


class SqliteNarrowDirtyPack(BugPack):
    """SQLite dirty read in narrow shared-cache window."""

    @property
    def id(self) -> str:
        return "sqlite_narrow_dirty"

    @property
    def db(self) -> str:
        return "sqlite"

    @property
    def description(self) -> str:
        return (
            "Under shared_cache + read_uncommitted=ON, a reader can see "
            "uncommitted writes. When the writer does UPDATE then immediate "
            "ROLLBACK, the dirty window is sub-microsecond under stress. "
            "Heuristic delay holds the writer to widen the window."
        )

    def buggy_dsn(self) -> str:
        return os.getenv(
            "SQLITE_HEADLINE_DSN",
            "file:repro_headline?mode=memory&cache=shared",
        )

    def control_dsn(self) -> str:
        return self.buggy_dsn()

    def __init__(self) -> None:
        self._keeper = None  # Keep alive for in-memory shared-cache DBs

    def setup(self, dsn: str) -> None:
        # Close any existing keeper first
        if self._keeper is not None:
            try:
                self._keeper.close()
            except Exception:
                pass
        self._keeper = make_adapter(
            "sqlite", dsn,
            sqlite_shared_cache=True,
            sqlite_read_uncommitted=True,
        )
        self._keeper.setup(SQLITE_NARROW_SCHEMA)

    def teardown(self) -> None:
        if self._keeper is not None:
            try:
                self._keeper.close()
            except Exception:
                pass
            self._keeper = None

    def _make_adapter(self, dsn: str, shared_cache: bool = True, read_uncommitted: bool = True):
        return make_adapter(
            "sqlite", dsn,
            sqlite_shared_cache=shared_cache,
            sqlite_read_uncommitted=read_uncommitted if shared_cache else False,
        )

    def run_trial(
        self,
        dsn: str,
        strategy: str,
        delay_ms: float,
        rng: random.Random,
        shared_cache: bool = True,
        read_uncommitted: bool = True,
    ) -> TrialResult:
        w = self._make_adapter(dsn, shared_cache, read_uncommitted)
        r = self._make_adapter(dsn, shared_cache, read_uncommitted)
        tr = TrialResult()

        try:
            # Reset
            w.begin("IMMEDIATE")
            w.exec("UPDATE kv SET v = 0 WHERE k = 'x'")
            w.commit()

            writer_updated = threading.Event()
            reader_value = [None]
            errors: List[str] = []

            def writer_fn() -> None:
                try:
                    if strategy == "heuristic":
                        time.sleep(0.001)
                    elif strategy == "random":
                        time.sleep(rng.uniform(0, delay_ms / 2000.0))
                    else:
                        time.sleep(rng.uniform(0.001, 0.003))

                    w.begin("IMMEDIATE")
                    w.exec("UPDATE kv SET v = 1 WHERE k = 'x'")
                    if strategy == "heuristic":
                        writer_updated.set()
                        time.sleep(delay_ms / 1000.0)
                    elif strategy == "random":
                        writer_updated.set()
                        time.sleep(rng.uniform(0, delay_ms / 2000.0))
                    w.rollback()
                except Exception as e:
                    errors.append(f"writer: {e}")
                    try:
                        w.rollback()
                    except Exception:
                        pass

            def reader_fn() -> None:
                try:
                    if strategy == "heuristic":
                        writer_updated.wait(timeout=5)
                    elif strategy == "random":
                        time.sleep(rng.uniform(0, delay_ms / 2000.0))
                    r.begin("DEFERRED")
                    res = r.exec("SELECT v FROM kv WHERE k = 'x'")
                    reader_value[0] = res.rows[0][0] if res.rows else None
                    r.commit()
                except Exception as e:
                    errors.append(f"reader: {e}")
                    try:
                        r.rollback()
                    except Exception:
                        pass

            t_r = threading.Thread(target=reader_fn)
            t_w = threading.Thread(target=writer_fn)
            t_r.start()
            t_w.start()
            t_w.join(timeout=15)
            t_r.join(timeout=15)

            # Read final committed value
            r.begin("DEFERRED")
            final_res = r.exec("SELECT v FROM kv WHERE k = 'x'")
            final_val = final_res.rows[0][0] if final_res.rows else None
            r.commit()

            tr.extra = {
                "reader_value": reader_value[0],
                "final_value": final_val,
                "errors": errors,
            }

            if reader_value[0] == 1 and final_val == 0:
                tr.bug_found = True
                tr.reason = (
                    f"Dirty read: reader saw v={reader_value[0]} "
                    f"but final value is v={final_val}"
                )
            else:
                tr.reason = (
                    f"reader_value={reader_value[0]}, "
                    f"final_value={final_val}"
                )
            return tr
        finally:
            w.close()
            r.close()

    def is_available(self, dsn: str) -> bool:
        try:
            a = self._make_adapter(dsn)
            a.exec("SELECT 1")
            a.close()
            return True
        except Exception:
            return False

    def default_delay_ms(self) -> float:
        return 10.0

    def run_replay(self, dsn: str, n: int = 20) -> ReplayResult:
        """Replay buggy variant (shared_cache + read_uncommitted ON)."""
        self.setup(dsn)  # Sets up keeper
        rng = random.Random(42)
        details: List[Dict[str, Any]] = []
        bug_count = 0
        for i in range(n):
            tr = self.run_trial(dsn, "heuristic", self.default_delay_ms(), rng, True, True)
            details.append({
                "replay": i,
                "bug_found": tr.bug_found,
                "reader_value": tr.extra.get("reader_value"),
            })
            if tr.bug_found:
                bug_count += 1
        return ReplayResult(bug_count=bug_count, total=n, details=details)

    def run_control_replay(self, dsn: str, n: int = 20) -> ReplayResult:
        """Replay control (read_uncommitted=OFF) — should be 0 bugs."""
        # Need a separate keeper with read_uncommitted=OFF for control
        ctrl_keeper = self._make_adapter(dsn, True, False)
        try:
            ctrl_keeper.setup(SQLITE_NARROW_SCHEMA)
            rng = random.Random(42)
            details: List[Dict[str, Any]] = []
            bug_count = 0
            for i in range(n):
                tr = self.run_trial(dsn, "heuristic", self.default_delay_ms(), rng, True, False)
                details.append({
                    "replay": i,
                    "bug_found": tr.bug_found,
                    "reader_value": tr.extra.get("reader_value"),
                })
                if tr.bug_found:
                    bug_count += 1
            return ReplayResult(bug_count=bug_count, total=n, details=details)
        finally:
            ctrl_keeper.close()


register_pack(PgUpdateConflictOutPack())
register_pack(MysqlRrPhantomPack())
register_pack(SqliteNarrowDirtyPack())
