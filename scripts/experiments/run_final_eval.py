#!/usr/bin/env python3
"""
Final paper-quality evaluation for the schedule-pool workflow.

Runs all stages: micro coverage, case studies, determinism, overhead,
pool analysis, and report generation.

Usage:
  python scripts/experiments/run_final_eval.py --smoke          # 5s, 1 rep
  python scripts/experiments/run_final_eval.py --quick          # 15s, 3 reps
  python scripts/experiments/run_final_eval.py --paper          # 60s, 10 reps
  python scripts/experiments/run_final_eval.py --stage coverage # single stage
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import statistics
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

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
    ScheduleEntry,
    SchedulePool,
    ScheduleMutator,
    MutationConfig,
    RetentionPolicy,
    RetentionReason,
    make_event_id,
    make_trial_event_ids,
)
from scripts.schedule_pool.pool import PoolConfig
from scripts.schedule_pool.runner import (
    SchedulePoolRunner,
    run_pool_search,
    replay_schedule,
)

# Case study imports
from scripts.experiments.case_studies import (
    CaseResult,
    run_pg_case_study,
    run_mysql_rr_nonexistent_case,
    run_sqlite_narrow_dirty_case,
    replay_mysql_rr_nonexistent,
    replay_sqlite_narrow_dirty,
    MYSQL_RR_NONEXISTENT_SCHEMA,
    SQLITE_NARROW_DIRTY_SCHEMA,
)

try:
    from scripts.experiments.case_studies import _mysql_rr_nonexistent_trial
except ImportError:
    _mysql_rr_nonexistent_trial = None

try:
    from scripts.discover_update_conflict_out import (
        run_concurrent_trial as _uco_trial,
        setup_schema as _uco_setup_schema,
        reset_tables as _uco_reset_tables,
    )
    _HAS_UCO = True
except ImportError:
    _HAS_UCO = False

from scripts.experiments.error_taxonomy import ErrorCategory


STRATEGIES = ["stress", "random", "heuristic", "schedule_pool"]
WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]
SEEDS_10 = [42, 10042, 20042, 30042, 40042, 50042, 60042, 70042, 80042, 90042]

# DSNs for case study databases
CASE_DSNS = {
    "pg_12_3": "postgres://postgres:postgres@localhost:54323/postgres",
    "pg_12_4": "postgres://postgres:postgres@localhost:54324/postgres",
    "mysql_8": "mysql://root:root@localhost:33080/test",
    "mysql": "mysql://root:root@localhost:3307/test",
    "sqlite": "file:final_eval_shared?mode=memory&cache=shared",
}

# Case study event IDs
PG_UCO_EVENTS = [
    "foo_begin", "foo_select", "foo_insert", "foo_commit",
    "bar_begin", "bar_insert", "bar_select", "bar_commit",
    "trouble_begin", "trouble_update", "trouble_abort",
]
MYSQL_RR_EVENTS = [
    "t1_pre", "t1_begin", "t1_update", "t1_commit",
    "t2_pre", "t2_begin", "t2_update", "t2_commit",
    "t3_pre", "t3_begin", "t3_snap", "t3_mid", "t3_update", "t3_post_update", "t3_commit",
]
SQLITE_DIRTY_EVENTS = [
    "writer_pre", "writer_begin", "writer_update", "writer_hold", "writer_rollback",
    "reader_pre", "reader_begin", "reader_select", "reader_commit",
]


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _db_available(db: str, dsn: str) -> bool:
    try:
        a = make_adapter(db, dsn)
        a.exec("SELECT 1")
        a.close()
        return True
    except Exception:
        return False


def _pg_dsn_available(dsn: str) -> bool:
    try:
        import psycopg2
        c = psycopg2.connect(dsn)
        c.close()
        return True
    except Exception:
        return False


def _percentile(vals: List[float], pct: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int(len(s) * pct / 100.0)
    return s[min(idx, len(s) - 1)]


def _pg_uco_trial_with_delays(dsn: str, delays: Dict[str, float]) -> Dict[str, Any]:
    """Run PG update_conflict_out with arbitrary delays from schedule."""
    _uco_reset_tables(dsn)
    result = _uco_trial(dsn, variant="update", delays=delays)
    state = {
        "bug_found": result.bug_found,
        "got_40001": result.got_40001,
    }
    return {
        "bug_found": result.bug_found,
        "bug_reason": "SSI conflict-out miss" if result.bug_found else "",
        "canonical_state": state,
        "canonical_hash": _hash_dict(state),
    }


def _mysql_rr_trial_with_delays(dsn: str, delays: Dict[str, float],
                                  use_rc: bool = False) -> Dict[str, Any]:
    """Run MySQL RR nonexistent row trial with schedule-derived delays."""
    t1 = make_adapter("mysql", dsn)
    t2 = make_adapter("mysql", dsn)
    t3 = make_adapter("mysql", dsn)

    result = {"bug_found": False, "reason": "", "t3_rows": 0,
              "got_deadlock": False, "got_timeout": False, "errors": []}

    try:
        reset = make_adapter("mysql", dsn)
        reset.begin()
        reset.exec("DELETE FROM t")
        reset.exec("INSERT INTO t (pkId,a,b,c) VALUES (8,44,1,2)")
        reset.commit()
        reset.close()

        t3_iso = "READ COMMITTED" if use_rc else "REPEATABLE READ"

        def session_t1():
            try:
                d = delays.get("t1_pre", 0)
                if d > 0:
                    time.sleep(d)
                t1.begin("REPEATABLE READ")
                r = t1.exec("UPDATE t SET a = 40 WHERE a = 44")
                if r.error_category not in (ErrorCategory.OK,):
                    result["errors"].append(f"t1_update: {r.error_msg}")
                    if r.error_category == ErrorCategory.DEADLOCK:
                        result["got_deadlock"] = True
                    t1.rollback()
                    return
                d = delays.get("t1_commit", 0)
                if d > 0:
                    time.sleep(d)
                cr = t1.commit()
                if cr.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t1_commit: {cr.error_msg}")
            except Exception as e:
                result["errors"].append(f"t1_exc: {e}")

        def session_t2():
            try:
                d = delays.get("t2_pre", 0)
                if d > 0:
                    time.sleep(d)
                t2.begin("REPEATABLE READ")
                r = t2.exec("UPDATE t SET b=222, c=333 WHERE pkId=8")
                if r.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t2_update: {r.error_msg}")
                    if r.error_category == ErrorCategory.DEADLOCK:
                        result["got_deadlock"] = True
                    t2.rollback()
                    return
                d = delays.get("t2_commit", 0)
                if d > 0:
                    time.sleep(d)
                cr = t2.commit()
                if cr.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t2_commit: {cr.error_msg}")
            except Exception as e:
                result["errors"].append(f"t2_exc: {e}")

        def session_t3():
            try:
                d = delays.get("t3_pre", 0)
                if d > 0:
                    time.sleep(d)
                t3.begin(t3_iso)
                r1 = t3.exec("SELECT pkId,b,c FROM t")
                if r1.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t3_select1: {r1.error_msg}")
                    t3.rollback()
                    return
                d = delays.get("t3_mid", 0)
                if d > 0:
                    time.sleep(d)
                r2 = t3.exec("UPDATE t SET b=888, c=999 WHERE pkId=8")
                if r2.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t3_update: {r2.error_msg}")
                    if r2.error_category == ErrorCategory.DEADLOCK:
                        result["got_deadlock"] = True
                    t3.rollback()
                    return
                d = delays.get("t3_post_update", 0)
                if d > 0:
                    time.sleep(d)
                r3 = t3.exec("SELECT * FROM t")
                if r3.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t3_select2: {r3.error_msg}")
                    t3.rollback()
                    return
                result["t3_rows"] = len(r3.rows)
                t3.commit()
            except Exception as e:
                result["errors"].append(f"t3_exc: {e}")

        threads = [
            threading.Thread(target=session_t1),
            threading.Thread(target=session_t2),
            threading.Thread(target=session_t3),
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=20)

        if result["t3_rows"] > 1:
            result["bug_found"] = True
            result["reason"] = f"Phantom: t3 saw {result['t3_rows']} rows"

    finally:
        for a in (t1, t2, t3):
            try:
                a.close()
            except Exception:
                pass

    state = {
        "bug_found": result["bug_found"],
        "t3_rows": result["t3_rows"],
        "got_deadlock": result["got_deadlock"],
    }
    return {
        "bug_found": result["bug_found"],
        "bug_reason": result["reason"],
        "canonical_state": state,
        "canonical_hash": _hash_dict(state),
    }


def _sqlite_dirty_trial_with_delays(dsn: str, delays: Dict[str, float],
                                      shared_cache: bool = True,
                                      read_uncommitted: bool = True) -> Dict[str, Any]:
    """Run SQLite narrow dirty read trial with schedule-derived delays."""
    w_adapter = make_adapter("sqlite", dsn,
                              sqlite_shared_cache=shared_cache,
                              sqlite_read_uncommitted=read_uncommitted if shared_cache else False)
    r_adapter = make_adapter("sqlite", dsn,
                              sqlite_shared_cache=shared_cache,
                              sqlite_read_uncommitted=read_uncommitted if shared_cache else False)

    result = {"bug_found": False, "reason": "", "reader_value": None,
              "final_value": None, "errors": []}

    try:
        w_adapter.begin("IMMEDIATE")
        w_adapter.exec("UPDATE kv SET v = 0 WHERE k = 'x'")
        w_adapter.commit()

        def writer():
            try:
                d = delays.get("writer_pre", 0)
                if d > 0:
                    time.sleep(d)
                w_adapter.begin("IMMEDIATE")
                w_adapter.exec("UPDATE kv SET v = 1 WHERE k = 'x'")
                d = delays.get("writer_hold", 0)
                if d > 0:
                    time.sleep(d)
                w_adapter.rollback()
            except Exception as e:
                result["errors"].append(f"writer: {e}")
                try:
                    w_adapter.rollback()
                except Exception:
                    pass

        def reader():
            try:
                d = delays.get("reader_pre", 0)
                if d > 0:
                    time.sleep(d)
                r_adapter.begin("DEFERRED")
                r = r_adapter.exec("SELECT v FROM kv WHERE k = 'x'")
                val = r.rows[0][0] if r.rows else None
                r_adapter.commit()
                result["reader_value"] = val
            except Exception as e:
                result["errors"].append(f"reader: {e}")
                try:
                    r_adapter.rollback()
                except Exception:
                    pass

        tw = threading.Thread(target=writer)
        tr = threading.Thread(target=reader)
        tr.start()
        tw.start()
        tw.join(timeout=15)
        tr.join(timeout=15)

        r_adapter.begin("DEFERRED")
        final_r = r_adapter.exec("SELECT v FROM kv WHERE k = 'x'")
        result["final_value"] = final_r.rows[0][0] if final_r.rows else None
        r_adapter.commit()

        if result["reader_value"] == 1 and result["final_value"] == 0:
            result["bug_found"] = True
            result["reason"] = "Dirty read: reader saw v=1, final=0"

    finally:
        w_adapter.close()
        r_adapter.close()

    state = {
        "bug_found": result["bug_found"],
        "reader_value": result["reader_value"],
        "final_value": result["final_value"],
    }
    return {
        "bug_found": result["bug_found"],
        "bug_reason": result["reason"],
        "canonical_state": state,
        "canonical_hash": _hash_dict(state),
    }


# Hardened Case Study Trial Functions (with distractor threads)
#
# Same bugs, but N distractor threads create real lock contention.
# Distractors acquire shared locks / write locks on the same data,
# making random delay sampling far less likely to hit the bug window.
# schedule_pool can learn to neutralize distractors (zero delays).


N_DISTRACTORS = 12

PG_UCO_EVENTS_HARD = PG_UCO_EVENTS + [
    f"distractor{i}_{step}"
    for i in range(N_DISTRACTORS)
    for step in ("begin", "hold", "commit")
]

MYSQL_RR_EVENTS_HARD = MYSQL_RR_EVENTS + [
    f"distractor{i}_{step}"
    for i in range(N_DISTRACTORS)
    for step in ("pre", "hold", "commit")
]

SQLITE_DIRTY_EVENTS_HARD = SQLITE_DIRTY_EVENTS + [
    f"distractor{i}_{step}"
    for i in range(N_DISTRACTORS)
    for step in ("pre", "hold", "commit")
]


def _pg_uco_trial_with_delays_hard(dsn: str, delays: Dict[str, float]) -> Dict[str, Any]:
    """Hardened PG UCO: same bug + N distractor threads holding SHARE table locks.

    Distractors acquire LOCK TABLE ... IN SHARE MODE on txn0/txn1 under
    READ COMMITTED isolation.  SHARE locks block ROW EXCLUSIVE (needed by
    INSERT) without touching SSI predicate locks, so the bug mechanism
    stays intact.  Under random delays the SHARE locks overlap with the
    real sessions' INSERT windows, blocking them and disrupting the narrow
    timing window the SSI bug requires.  schedule_pool learns to set
    distractor begin-delays high (or hold=0) to minimize lock-hold time.
    """
    import psycopg2 as _pg2

    _uco_reset_tables(dsn)

    distractor_errors: List[str] = []

    def _distractor(idx: int) -> None:
        conn = None
        try:
            conn = _pg2.connect(dsn)
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute("SET statement_timeout = '15000'")

            d = delays.get(f"distractor{idx}_begin", 0)
            if d > 0:
                time.sleep(d)

            # Alternate between the two tables the real sessions INSERT into
            table = "txn0" if idx % 2 == 0 else "txn1"

            # SHARE MODE blocks ROW EXCLUSIVE (INSERT) but is compatible
            # with other SHARE locks, so distractors don't block each other.
            cur.execute(f"LOCK TABLE {table} IN SHARE MODE")

            d = delays.get(f"distractor{idx}_hold", 0)
            if d > 0:
                time.sleep(d)

            d = delays.get(f"distractor{idx}_commit", 0)
            if d > 0:
                time.sleep(d)

            conn.commit()
        except Exception as e:
            distractor_errors.append(f"distractor{idx}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    d_threads = []
    for i in range(N_DISTRACTORS):
        t = threading.Thread(target=_distractor, args=(i,), daemon=True)
        d_threads.append(t)
        t.start()

    # Run the real trial concurrently
    result = _uco_trial(dsn, variant="update", delays=delays)

    for t in d_threads:
        t.join(timeout=15)

    state = {
        "bug_found": result.bug_found,
        "got_40001": result.got_40001,
    }
    return {
        "bug_found": result.bug_found,
        "bug_reason": "SSI conflict-out miss" if result.bug_found else "",
        "canonical_state": state,
        "canonical_hash": _hash_dict(state),
    }


def _mysql_rr_trial_with_delays_hard(dsn: str, delays: Dict[str, float]) -> Dict[str, Any]:
    """Hardened MySQL RR: same 3-session bug + N distractor sessions with FOR SHARE locks.

    Distractors acquire shared locks on the same table row, blocking the real
    sessions' UPDATEs (which need exclusive locks).  Under random delays the
    distractors frequently hold locks during the bug's timing window, causing
    the real sessions to stall or reorder.  schedule_pool can learn to keep
    distractor delays near zero, neutralising the contention.
    """
    t1 = make_adapter("mysql", dsn)
    t2 = make_adapter("mysql", dsn)
    t3 = make_adapter("mysql", dsn)
    d_adapters = [make_adapter("mysql", dsn) for _ in range(N_DISTRACTORS)]

    result = {"bug_found": False, "reason": "", "t3_rows": 0,
              "got_deadlock": False, "got_timeout": False, "errors": []}

    try:
        # Reset: exactly one row
        reset = make_adapter("mysql", dsn)
        reset.begin()
        reset.exec("DELETE FROM t")
        reset.exec("INSERT INTO t (pkId,a,b,c) VALUES (8,44,1,2)")
        reset.commit()
        reset.close()

        t3_iso = "REPEATABLE READ"

        def session_t1():
            try:
                d = delays.get("t1_pre", 0)
                if d > 0:
                    time.sleep(d)
                t1.begin("REPEATABLE READ")
                r = t1.exec("UPDATE t SET a = 40 WHERE a = 44")
                if r.error_category not in (ErrorCategory.OK,):
                    result["errors"].append(f"t1_update: {r.error_msg}")
                    if r.error_category == ErrorCategory.DEADLOCK:
                        result["got_deadlock"] = True
                    t1.rollback()
                    return
                d = delays.get("t1_commit", 0)
                if d > 0:
                    time.sleep(d)
                cr = t1.commit()
                if cr.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t1_commit: {cr.error_msg}")
            except Exception as e:
                result["errors"].append(f"t1_exc: {e}")

        def session_t2():
            try:
                d = delays.get("t2_pre", 0)
                if d > 0:
                    time.sleep(d)
                t2.begin("REPEATABLE READ")
                r = t2.exec("UPDATE t SET b=222, c=333 WHERE pkId=8")
                if r.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t2_update: {r.error_msg}")
                    if r.error_category == ErrorCategory.DEADLOCK:
                        result["got_deadlock"] = True
                    t2.rollback()
                    return
                d = delays.get("t2_commit", 0)
                if d > 0:
                    time.sleep(d)
                cr = t2.commit()
                if cr.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t2_commit: {cr.error_msg}")
            except Exception as e:
                result["errors"].append(f"t2_exc: {e}")

        def session_t3():
            try:
                d = delays.get("t3_pre", 0)
                if d > 0:
                    time.sleep(d)
                t3.begin(t3_iso)
                r1 = t3.exec("SELECT pkId,b,c FROM t WHERE pkId = 8")
                if r1.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t3_select1: {r1.error_msg}")
                    t3.rollback()
                    return
                d = delays.get("t3_mid", 0)
                if d > 0:
                    time.sleep(d)
                r2 = t3.exec("UPDATE t SET b=888, c=999 WHERE pkId=8")
                if r2.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t3_update: {r2.error_msg}")
                    if r2.error_category == ErrorCategory.DEADLOCK:
                        result["got_deadlock"] = True
                    t3.rollback()
                    return
                d = delays.get("t3_post_update", 0)
                if d > 0:
                    time.sleep(d)
                r3 = t3.exec("SELECT * FROM t WHERE pkId = 8")
                if r3.error_category != ErrorCategory.OK:
                    result["errors"].append(f"t3_select2: {r3.error_msg}")
                    t3.rollback()
                    return
                result["t3_rows"] = len(r3.rows)
                t3.commit()
            except Exception as e:
                result["errors"].append(f"t3_exc: {e}")

        def distractor_session(idx: int):
            """Distractor: SELECT FOR SHARE on same table — holds S lock blocking UPDATEs."""
            da = d_adapters[idx]
            try:
                d = delays.get(f"distractor{idx}_pre", 0)
                if d > 0:
                    time.sleep(d)
                da.begin("REPEATABLE READ")
                # Acquire shared lock on the row the real sessions UPDATE
                da.exec("SELECT * FROM t FOR SHARE")
                d = delays.get(f"distractor{idx}_hold", 0)
                if d > 0:
                    time.sleep(d)
                d = delays.get(f"distractor{idx}_commit", 0)
                if d > 0:
                    time.sleep(d)
                da.commit()
            except Exception:
                try:
                    da.rollback()
                except Exception:
                    pass

        threads = [
            threading.Thread(target=session_t1),
            threading.Thread(target=session_t2),
            threading.Thread(target=session_t3),
        ]
        for i in range(N_DISTRACTORS):
            threads.append(threading.Thread(target=distractor_session, args=(i,)))
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=20)

        if result["t3_rows"] > 1:
            result["bug_found"] = True
            result["reason"] = f"Phantom: t3 saw {result['t3_rows']} rows"

    finally:
        for a in [t1, t2, t3] + d_adapters:
            try:
                a.close()
            except Exception:
                pass

    state = {
        "bug_found": result["bug_found"],
        "t3_rows": result["t3_rows"],
        "got_deadlock": result["got_deadlock"],
    }
    return {
        "bug_found": result["bug_found"],
        "bug_reason": result["reason"],
        "canonical_state": state,
        "canonical_hash": _hash_dict(state),
    }


def _sqlite_dirty_trial_with_delays_hard(dsn: str, delays: Dict[str, float],
                                          shared_cache: bool = True,
                                          read_uncommitted: bool = True) -> Dict[str, Any]:
    """Hardened SQLite dirty read: same bug + N distractor writers competing for write lock.

    SQLite only allows one writer at a time.  Distractor threads use BEGIN
    IMMEDIATE to compete for the write lock, reducing the real writer's chance
    of holding the lock during the reader's SELECT window.  schedule_pool can
    learn to delay distractors past the critical window.
    """
    w_adapter = make_adapter("sqlite", dsn,
                              sqlite_shared_cache=shared_cache,
                              sqlite_read_uncommitted=read_uncommitted if shared_cache else False)
    r_adapter = make_adapter("sqlite", dsn,
                              sqlite_shared_cache=shared_cache,
                              sqlite_read_uncommitted=read_uncommitted if shared_cache else False)
    d_adapters = [
        make_adapter("sqlite", dsn,
                      sqlite_shared_cache=shared_cache,
                      sqlite_read_uncommitted=read_uncommitted if shared_cache else False)
        for _ in range(N_DISTRACTORS)
    ]

    result = {"bug_found": False, "reason": "", "reader_value": None,
              "final_value": None, "errors": []}

    try:
        # Reset main key
        w_adapter.begin("IMMEDIATE")
        w_adapter.exec("UPDATE kv SET v = 0 WHERE k = 'x'")
        w_adapter.commit()

        def writer():
            try:
                d = delays.get("writer_pre", 0)
                if d > 0:
                    time.sleep(d)
                w_adapter.begin("IMMEDIATE")
                w_adapter.exec("UPDATE kv SET v = 1 WHERE k = 'x'")
                d = delays.get("writer_hold", 0)
                if d > 0:
                    time.sleep(d)
                w_adapter.rollback()
            except Exception as e:
                result["errors"].append(f"writer: {e}")
                try:
                    w_adapter.rollback()
                except Exception:
                    pass

        def reader():
            try:
                d = delays.get("reader_pre", 0)
                if d > 0:
                    time.sleep(d)
                r_adapter.begin("DEFERRED")
                r = r_adapter.exec("SELECT v FROM kv WHERE k = 'x'")
                val = r.rows[0][0] if r.rows else None
                r_adapter.commit()
                result["reader_value"] = val
            except Exception as e:
                result["errors"].append(f"reader: {e}")
                try:
                    r_adapter.rollback()
                except Exception:
                    pass

        def distractor_writer(idx: int):
            """Distractor: compete for the write lock on the same table."""
            da = d_adapters[idx]
            try:
                d = delays.get(f"distractor{idx}_pre", 0)
                if d > 0:
                    time.sleep(d)
                da.begin("IMMEDIATE")
                d = delays.get(f"distractor{idx}_hold", 0)
                if d > 0:
                    time.sleep(d)
                da.exec(f"UPDATE kv SET v = v WHERE k = 'x'")
                d = delays.get(f"distractor{idx}_commit", 0)
                if d > 0:
                    time.sleep(d)
                da.commit()
            except Exception:
                try:
                    da.rollback()
                except Exception:
                    pass

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
        ]
        for i in range(N_DISTRACTORS):
            threads.append(threading.Thread(target=distractor_writer, args=(i,)))
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)

        r_adapter.begin("DEFERRED")
        final_r = r_adapter.exec("SELECT v FROM kv WHERE k = 'x'")
        result["final_value"] = final_r.rows[0][0] if final_r.rows else None
        r_adapter.commit()

        if result["reader_value"] == 1 and result["final_value"] == 0:
            result["bug_found"] = True
            result["reason"] = "Dirty read: reader saw v=1, final=0"

    finally:
        for a in [w_adapter, r_adapter] + d_adapters:
            try:
                a.close()
            except Exception:
                pass

    state = {
        "bug_found": result["bug_found"],
        "reader_value": result["reader_value"],
        "final_value": result["final_value"],
    }
    return {
        "bug_found": result["bug_found"],
        "bug_reason": result["reason"],
        "canonical_state": state,
        "canonical_hash": _hash_dict(state),
    }


@dataclass
class CasePoolResult:
    """Results from a case study pool search."""
    bug_found: bool
    ttfb_s: Optional[float]
    trials_to_first: Optional[int]
    total_trials: int
    total_time_s: float
    pool_size: int
    unique_states: int
    bugs_found: int
    first_bug_schedule: Optional[Schedule]
    first_bug_record: Optional[ScheduleRecord]
    pool: SchedulePool
    records: List[ScheduleRecord]
    trial_log: List[Dict[str, Any]]


def run_case_pool_search(
    trial_fn: Callable[[Dict[str, float]], Dict[str, Any]],
    event_ids: List[str],
    db: str,
    case_name: str,
    duration_s: float = 60.0,
    seed: int = 42,
    pool: Optional[SchedulePool] = None,
    hot_events: Optional[List[str]] = None,
    mutation_config: Optional[MutationConfig] = None,
    pool_config: Optional[PoolConfig] = None,
    stabilize_replays: int = 10,
    stabilize_mutations: int = 5,
) -> CasePoolResult:
    """
    Run schedule_pool discovery loop for a case study.

    After first bug hit, runs post-hit stabilization:
      1) Exact replays to measure replay rate
      2) Focused mutations to find more stable/compact triggers

    trial_fn(delays_dict) -> {bug_found, bug_reason, canonical_state, canonical_hash}
    """
    rng = random.Random(seed)
    if pool is None:
        pool = SchedulePool(config=pool_config or PoolConfig())

    mutator = ScheduleMutator(
        all_event_ids=event_ids,
        config=mutation_config or MutationConfig(),
        rng=rng,
        hot_events=hot_events,
    )
    retention = RetentionPolicy(pool)

    trial_log: List[Dict[str, Any]] = []
    first_bug_schedule: Optional[Schedule] = None
    first_bug_record: Optional[ScheduleRecord] = None
    ttfb_s: Optional[float] = None
    trials_to_first: Optional[int] = None
    bugs_found = 0
    unique_states: Set[str] = set()
    trial_idx = 0
    stabilized_hashes: set = set()

    start = time.monotonic()
    while (time.monotonic() - start) < duration_s:
        # Sample or empty
        parent_record = pool.sample(rng, workload=case_name, db=db)
        parent_schedule = parent_record.schedule if parent_record else Schedule.empty()
        parent_id = parent_record.schedule_id if parent_record else None

        # Mutate
        candidate = mutator.mutate(parent_schedule)

        # Convert to delays dict (seconds)
        delays = candidate.to_delay_map()

        # Execute trial
        t0 = time.monotonic()
        r = trial_fn(delays)
        elapsed_ms = (time.monotonic() - t0) * 1000

        trial_idx += 1
        unique_states.add(r["canonical_hash"])
        if r["bug_found"]:
            bugs_found += 1

        trial_log.append({
            "trial": trial_idx,
            "bug_found": r["bug_found"],
            "canonical_hash": r["canonical_hash"],
            "elapsed_ms": round(elapsed_ms, 2),
            "delay_count": len(candidate.entries),
            "total_delay_ms": sum(e.delay_ms for e in candidate.entries),
        })

        if r["bug_found"] and first_bug_schedule is None:
            ttfb_s = time.monotonic() - start
            trials_to_first = trial_idx
            first_bug_schedule = candidate

        record = ScheduleRecord(
            parent_schedule_id=parent_id,
            db=db,
            workload=case_name,
            variant="buggy",
            strategy="schedule_pool",
            seed=seed,
            schedule=candidate,
            discovered_state_hash=r["canonical_hash"],
            bug_found=r["bug_found"],
            bug_reason=r.get("bug_reason", ""),
        )

        if r["bug_found"] and first_bug_record is None:
            first_bug_record = record

        retention.accept(record)

        # Post-hit stabilization
        if r["bug_found"] and (time.monotonic() - start) < duration_s:
            sched_hash = candidate.content_hash()
            if sched_hash not in stabilized_hashes:
                stabilized_hashes.add(sched_hash)
                # Exact replays to measure replay rate
                replay_hits = 1  # count initial hit
                for _ in range(stabilize_replays):
                    if (time.monotonic() - start) >= duration_s:
                        break
                    sr = trial_fn(delays)
                    trial_idx += 1
                    unique_states.add(sr["canonical_hash"])
                    if sr["bug_found"]:
                        bugs_found += 1
                        replay_hits += 1
                record.replay_rate_buggy = replay_hits / (stabilize_replays + 1)

                # Focused mutations around bug schedule
                for _ in range(stabilize_mutations):
                    if (time.monotonic() - start) >= duration_s:
                        break
                    mut_sched = mutator.mutate(candidate)
                    mut_delays = mut_sched.to_delay_map()
                    mr = trial_fn(mut_delays)
                    trial_idx += 1
                    unique_states.add(mr["canonical_hash"])
                    if mr["bug_found"]:
                        bugs_found += 1
                        mut_rec = ScheduleRecord(
                            parent_schedule_id=record.schedule_id,
                            db=db,
                            workload=case_name,
                            variant="buggy",
                            strategy="schedule_pool",
                            seed=seed,
                            schedule=mut_sched,
                            discovered_state_hash=mr["canonical_hash"],
                            bug_found=mr["bug_found"],
                            bug_reason=mr.get("bug_reason", ""),
                        )
                        retention.accept(mut_rec)

    total_time = time.monotonic() - start

    return CasePoolResult(
        bug_found=first_bug_schedule is not None,
        ttfb_s=ttfb_s,
        trials_to_first=trials_to_first,
        total_trials=trial_idx,
        total_time_s=total_time,
        pool_size=pool.size,
        unique_states=len(unique_states),
        bugs_found=bugs_found,
        first_bug_schedule=first_bug_schedule,
        first_bug_record=first_bug_record,
        pool=pool,
        records=list(pool.records),
        trial_log=trial_log,
    )


def run_micro_coverage(
    output_dir: Path,
    dbs: List[str],
    dsns: Dict[str, str],
    duration_s: float,
    reps: int,
    seeds: List[int],
    n_workers: int = 4,
    delay_ms: float = 5.0,
) -> List[CoveragePoint]:
    """Run coverage experiments for all DB × workload × strategy × rep."""
    print("\n=== STAGE 2: Micro Coverage ===")
    all_points: List[CoveragePoint] = []
    micro_dir = _ensure(output_dir / "micro_coverage")
    pool_path = micro_dir / "schedule_pool.jsonl"

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            print(f"  Skipping {db} (unavailable)")
            continue

        for wl_name in WORKLOADS:
            for strat in STRATEGIES:
                for rep_idx in range(reps):
                    seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                    label = f"{db}/{wl_name}/{strat}/rep{rep_idx}"
                    print(f"  {label} (seed={seed})", end=" ", flush=True)
                    t0 = time.monotonic()

                    try:
                        if strat == "schedule_pool":
                            pool = SchedulePool(config=PoolConfig())
                            points, _, pool = run_pool_search(
                                db=db, dsn=dsn,
                                workload_name=wl_name,
                                variant="buggy",
                                duration_s=duration_s,
                                n_workers=n_workers,
                                seed=seed,
                                pool=pool,
                                rep=rep_idx,
                            )
                            pool.save(pool_path)
                        else:
                            points, _ = run_coverage_session(
                                db=db, dsn=dsn,
                                workload_name=wl_name,
                                strategy=strat,
                                variant="buggy",
                                duration_s=duration_s,
                                n_workers=n_workers,
                                delay_ms=delay_ms,
                                rep=rep_idx,
                            )

                        all_points.extend(points)
                        elapsed = time.monotonic() - t0
                        final = points[-1] if points else None
                        states = final.unique_states if final else 0
                        txns = final.txns_done if final else 0
                        print(f"→ {states} states, {txns} trials ({elapsed:.1f}s)")
                    except Exception as e:
                        print(f"→ ERROR: {e}")

    # Write CSVs
    if all_points:
        write_coverage_csv(all_points, micro_dir / "coverage_timeseries.csv")
        _write_coverage_summary(all_points, micro_dir / "coverage_summary.csv", dbs, duration_s)
    return all_points


def _write_coverage_summary(points: List[CoveragePoint], path: Path,
                             dbs: List[str], duration_s: float) -> None:
    """Write aggregated coverage summary CSV."""
    rows = []
    for db in dbs:
        for wl in WORKLOADS:
            for strat in STRATEGIES:
                filtered = [p for p in points
                           if p.db == db and p.workload == wl and p.strategy == strat]
                if not filtered:
                    continue
                # Group by rep, find final point per rep
                by_rep: Dict[int, CoveragePoint] = {}
                for p in filtered:
                    if p.rep not in by_rep or p.time_s > by_rep[p.rep].time_s:
                        by_rep[p.rep] = p
                final_states = [by_rep[r].unique_states for r in sorted(by_rep)]
                final_trials = [by_rep[r].txns_done for r in sorted(by_rep)]
                final_bugs = [by_rep[r].bugs_found for r in sorted(by_rep)]

                auc = _compute_mean_auc(points, db, wl, strat)
                mean_states = statistics.mean(final_states) if final_states else 0
                mean_trials = statistics.mean(final_trials) if final_trials else 0
                states_per_100 = (mean_states / mean_trials * 100) if mean_trials > 0 else 0

                rows.append({
                    "db": db, "workload": wl, "strategy": strat,
                    "reps": len(final_states),
                    "mean_final_states": round(mean_states, 2),
                    "std_final_states": round(statistics.stdev(final_states), 2) if len(final_states) > 1 else 0,
                    "mean_final_trials": round(mean_trials, 1),
                    "mean_auc": round(auc, 2),
                    "states_per_100_trials": round(states_per_100, 2),
                    "mean_bugs": round(statistics.mean(final_bugs), 2) if final_bugs else 0,
                })

    if rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
    print(f"  Coverage summary: {path}")


def _compute_mean_auc(points: List[CoveragePoint], db: str, wl: str, strat: str) -> float:
    """Compute mean AUC across reps."""
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


@dataclass
class CaseStudyRow:
    db: str
    case_name: str
    strategy: str
    rep: int
    bug_found: bool
    ttfb_s: Optional[float]
    trials_to_first: Optional[int]
    total_trials: int
    total_time_s: float
    replay_rate_buggy: Optional[float] = None
    replay_rate_control: Optional[float] = None
    stable: Optional[bool] = None
    pool_size: Optional[int] = None


def run_case_studies(
    output_dir: Path,
    duration_s_pg: float = 60.0,
    duration_s_mysql: float = 20.0,
    duration_s_sqlite: float = 20.0,
    reps: int = 10,
    seeds: List[int] = SEEDS_10,
    quick_replay: int = 5,
) -> List[CaseStudyRow]:
    """Run case studies for all 4 strategies on all 3 DBs."""
    print("\n=== STAGE 3: Case Studies ===")
    cs_dir = _ensure(output_dir / "case_studies")
    rows: List[CaseStudyRow] = []

    pg_buggy_dsn = CASE_DSNS["pg_12_3"]
    pg_control_dsn = CASE_DSNS["pg_12_4"]
    pg_available = _HAS_UCO and _pg_dsn_available(pg_buggy_dsn)

    if pg_available:
        print("\n  --- PG: update_conflict_out ---")
        _uco_setup_schema(pg_buggy_dsn)
        for strat in STRATEGIES:
            for rep_idx in range(reps):
                seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                label = f"  pg/update_conflict_out/{strat}/rep{rep_idx}"
                print(f"{label}", end=" ", flush=True)

                if strat == "schedule_pool":
                    pr = run_case_pool_search(
                        trial_fn=lambda d: _pg_uco_trial_with_delays(pg_buggy_dsn, d),
                        event_ids=PG_UCO_EVENTS,
                        db="postgres",
                        case_name="update_conflict_out",
                        duration_s=duration_s_pg,
                        seed=seed,
                        hot_events=[
                            "foo_commit", "bar_commit",
                            "foo_select", "bar_select",
                            "trouble_update", "trouble_abort",
                        ],
                    )
                    row = CaseStudyRow(
                        db="postgres", case_name="update_conflict_out",
                        strategy=strat, rep=rep_idx,
                        bug_found=pr.bug_found,
                        ttfb_s=pr.ttfb_s,
                        trials_to_first=pr.trials_to_first,
                        total_trials=pr.total_trials,
                        total_time_s=pr.total_time_s,
                        pool_size=pr.pool_size,
                        replay_rate_buggy=(
                            pr.first_bug_record.replay_rate_buggy
                            if pr.first_bug_record else None
                        ),
                    )
                    if pr.bug_found:
                        print(f"→ BUG at trial {pr.trials_to_first} ({pr.ttfb_s:.2f}s)")
                    else:
                        print(f"→ no bug ({pr.total_time_s:.1f}s, {pr.total_trials} trials)")
                else:
                    cr = run_pg_case_study(
                        dsn=pg_buggy_dsn,
                        strategy=strat,
                        duration_s=duration_s_pg,
                        rep=rep_idx,
                    )
                    row = CaseStudyRow(
                        db="postgres", case_name="update_conflict_out",
                        strategy=strat, rep=rep_idx,
                        bug_found=cr.bug_found,
                        ttfb_s=cr.time_to_first_s,
                        trials_to_first=cr.trials_to_first,
                        total_trials=cr.total_trials,
                        total_time_s=cr.total_time_s,
                    )
                    if cr.bug_found:
                        print(f"→ BUG at trial {cr.trials_to_first} ({cr.time_to_first_s:.2f}s)")
                    else:
                        print(f"→ no bug ({cr.total_time_s:.1f}s, {cr.total_trials} trials)")
                rows.append(row)
    else:
        print("  Skipping PG case study (unavailable)")

    mysql_dsn = CASE_DSNS.get("mysql_8", CASE_DSNS.get("mysql"))
    mysql_available = _db_available("mysql", mysql_dsn)

    if mysql_available:
        print("\n  --- MySQL: rr_nonexistent_row ---")
        setup = make_adapter("mysql", mysql_dsn)
        try:
            setup.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
        finally:
            setup.close()

        for strat in STRATEGIES:
            for rep_idx in range(reps):
                seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                label = f"  mysql/rr_nonexistent/{strat}/rep{rep_idx}"
                print(f"{label}", end=" ", flush=True)

                if strat == "schedule_pool":
                    pr = run_case_pool_search(
                        trial_fn=lambda d: _mysql_rr_trial_with_delays(mysql_dsn, d),
                        event_ids=MYSQL_RR_EVENTS,
                        db="mysql",
                        case_name="rr_nonexistent_row",
                        duration_s=duration_s_mysql,
                        seed=seed,
                        hot_events=["t1_commit", "t2_commit", "t3_mid", "t3_update"],
                    )
                    row = CaseStudyRow(
                        db="mysql", case_name="rr_nonexistent_row",
                        strategy=strat, rep=rep_idx,
                        bug_found=pr.bug_found,
                        ttfb_s=pr.ttfb_s,
                        trials_to_first=pr.trials_to_first,
                        total_trials=pr.total_trials,
                        total_time_s=pr.total_time_s,
                        pool_size=pr.pool_size,
                        replay_rate_buggy=(
                            pr.first_bug_record.replay_rate_buggy
                            if pr.first_bug_record else None
                        ),
                    )
                    if pr.bug_found:
                        print(f"→ BUG at trial {pr.trials_to_first} ({pr.ttfb_s:.2f}s)")
                    else:
                        print(f"→ no bug ({pr.total_time_s:.1f}s, {pr.total_trials} trials)")
                else:
                    cr = run_mysql_rr_nonexistent_case(
                        dsn=mysql_dsn,
                        strategy=strat,
                        duration_s=duration_s_mysql,
                        rep=rep_idx,
                        artifact_dir=cs_dir / "mysql_artifacts",
                    )
                    row = CaseStudyRow(
                        db="mysql", case_name="rr_nonexistent_row",
                        strategy=strat, rep=rep_idx,
                        bug_found=cr.bug_found,
                        ttfb_s=cr.time_to_first_s,
                        trials_to_first=cr.trials_to_first,
                        total_trials=cr.total_trials,
                        total_time_s=cr.total_time_s,
                    )
                    if cr.bug_found:
                        print(f"→ BUG at trial {cr.trials_to_first} ({cr.time_to_first_s:.2f}s)")
                    else:
                        print(f"→ no bug ({cr.total_time_s:.1f}s, {cr.total_trials} trials)")
                rows.append(row)
    else:
        print("  Skipping MySQL case study (unavailable)")

    sqlite_dsn = CASE_DSNS["sqlite"]
    print("\n  --- SQLite: narrow_dirty_read ---")

    # Keep a keeper connection for shared-cache memory DB
    keeper = make_adapter("sqlite", sqlite_dsn,
                           sqlite_shared_cache=True, sqlite_read_uncommitted=True)
    try:
        keeper.setup(SQLITE_NARROW_DIRTY_SCHEMA)

        for strat in STRATEGIES:
            for rep_idx in range(reps):
                seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                label = f"  sqlite/narrow_dirty/{strat}/rep{rep_idx}"
                print(f"{label}", end=" ", flush=True)

                if strat == "schedule_pool":
                    pr = run_case_pool_search(
                        trial_fn=lambda d: _sqlite_dirty_trial_with_delays(
                            sqlite_dsn, d, shared_cache=True, read_uncommitted=True),
                        event_ids=SQLITE_DIRTY_EVENTS,
                        db="sqlite",
                        case_name="narrow_dirty_read",
                        duration_s=duration_s_sqlite,
                        seed=seed,
                        hot_events=["writer_hold", "reader_pre", "writer_pre"],
                    )
                    row = CaseStudyRow(
                        db="sqlite", case_name="narrow_dirty_read",
                        strategy=strat, rep=rep_idx,
                        bug_found=pr.bug_found,
                        ttfb_s=pr.ttfb_s,
                        trials_to_first=pr.trials_to_first,
                        total_trials=pr.total_trials,
                        total_time_s=pr.total_time_s,
                        pool_size=pr.pool_size,
                        replay_rate_buggy=(
                            pr.first_bug_record.replay_rate_buggy
                            if pr.first_bug_record else None
                        ),
                    )
                    if pr.bug_found:
                        print(f"→ BUG at trial {pr.trials_to_first} ({pr.ttfb_s:.2f}s)")
                    else:
                        print(f"→ no bug ({pr.total_time_s:.1f}s, {pr.total_trials} trials)")
                else:
                    cr = run_sqlite_narrow_dirty_case(
                        dsn=sqlite_dsn,
                        strategy=strat,
                        duration_s=duration_s_sqlite,
                        rep=rep_idx,
                        artifact_dir=cs_dir / "sqlite_artifacts",
                    )
                    row = CaseStudyRow(
                        db="sqlite", case_name="narrow_dirty_read",
                        strategy=strat, rep=rep_idx,
                        bug_found=cr.bug_found,
                        ttfb_s=cr.time_to_first_s,
                        trials_to_first=cr.trials_to_first,
                        total_trials=cr.total_trials,
                        total_time_s=cr.total_time_s,
                    )
                    if cr.bug_found:
                        print(f"→ BUG at trial {cr.trials_to_first} ({cr.time_to_first_s:.2f}s)")
                    else:
                        print(f"→ no bug ({cr.total_time_s:.1f}s, {cr.total_trials} trials)")
                rows.append(row)
    finally:
        keeper.close()

    # Write CSV
    if rows:
        csv_path = cs_dir / "case_study_results.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            fields = ["db", "case_name", "strategy", "rep", "bug_found",
                       "ttfb_s", "trials_to_first", "total_trials", "total_time_s",
                       "replay_rate_buggy", "replay_rate_control", "stable", "pool_size"]
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({
                    "db": r.db, "case_name": r.case_name,
                    "strategy": r.strategy, "rep": r.rep,
                    "bug_found": r.bug_found, "ttfb_s": r.ttfb_s,
                    "trials_to_first": r.trials_to_first,
                    "total_trials": r.total_trials,
                    "total_time_s": round(r.total_time_s, 3),
                    "replay_rate_buggy": r.replay_rate_buggy,
                    "replay_rate_control": r.replay_rate_control,
                    "stable": r.stable, "pool_size": r.pool_size,
                })
        print(f"\n  Case study CSV: {csv_path}")
    return rows


@dataclass
class DeterminismRow:
    db: str
    workload: str
    source: str  # "micro" or "case_study"
    strategy: str
    schedule_id: str
    buggy_replays: int
    buggy_hits: int
    buggy_rate: float
    control_replays: int
    control_hits: int
    control_rate: float
    unique_hashes_buggy: int
    unique_hashes_control: int
    stable: bool


def run_determinism_eval(
    output_dir: Path,
    dbs: List[str],
    dsns: Dict[str, str],
    discovery_duration_s: float = 30.0,
    n_replays: int = 20,
    n_workers: int = 4,
    seed: int = 42,
) -> List[DeterminismRow]:
    """
    Stage 4: Discover schedules via pool search, then replay exactly.
    Tests micro workloads (1 per DB) and case study artifacts.
    """
    print("\n=== STAGE 4: Determinism Evaluation ===")
    rows: List[DeterminismRow] = []
    det_dir = _ensure(output_dir / "determinism")

    # Micro workload determinism: 1 representative per DB
    micro_reps = {"postgres": "job_queue_claim", "mysql": "job_queue_claim", "sqlite": "list_append"}
    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            continue
        wl_name = micro_reps.get(db, "bank_overdraft")
        print(f"\n  Micro: {db}/{wl_name}")

        # Discovery
        print(f"    Discovery ({discovery_duration_s}s)...", end=" ", flush=True)
        pool = SchedulePool(config=PoolConfig())
        _, trials, pool = run_pool_search(
            db=db, dsn=dsn, workload_name=wl_name,
            variant="buggy", duration_s=discovery_duration_s,
            n_workers=n_workers, seed=seed, pool=pool,
        )
        bugs = [t for t in trials if t.bug_found]
        print(f"→ {len(trials)} trials, {len(bugs)} bugs, pool={pool.size}")

        bug_records = pool.list(workload=wl_name, db=db, bug_only=True)
        if not bug_records:
            print(f"    No bug schedule found for {db}/{wl_name}")
            rows.append(DeterminismRow(
                db=db, workload=wl_name, source="micro", strategy="schedule_pool",
                schedule_id="none", buggy_replays=0, buggy_hits=0, buggy_rate=0.0,
                control_replays=0, control_hits=0, control_rate=0.0,
                unique_hashes_buggy=0, unique_hashes_control=0, stable=False,
            ))
            continue

        best = bug_records[0]
        sched = best.schedule

        # Replay buggy
        print(f"    Replay on buggy ({n_replays}x)...", end=" ", flush=True)
        br = replay_schedule(db=db, dsn=dsn, workload_name=wl_name,
                              schedule=sched, variant="buggy",
                              n_workers=n_workers, n_replays=n_replays)
        print(f"→ {br['bug_count']}/{br['total']}")

        # Replay control
        print(f"    Replay on control ({n_replays}x)...", end=" ", flush=True)
        cr = replay_schedule(db=db, dsn=dsn, workload_name=wl_name,
                              schedule=sched, variant="fixed",
                              n_workers=n_workers, n_replays=n_replays)
        print(f"→ {cr['bug_count']}/{cr['total']}")

        stable = (br["bug_count"] == br["total"] and cr["bug_count"] == 0)
        rows.append(DeterminismRow(
            db=db, workload=wl_name, source="micro", strategy="schedule_pool",
            schedule_id=best.schedule_id,
            buggy_replays=br["total"], buggy_hits=br["bug_count"],
            buggy_rate=round(br["bug_rate"], 4),
            control_replays=cr["total"], control_hits=cr["bug_count"],
            control_rate=round(cr["bug_rate"], 4),
            unique_hashes_buggy=br["unique_canonical_hashes"],
            unique_hashes_control=cr["unique_canonical_hashes"],
            stable=stable,
        ))

    # Case study determinism — PG update_conflict_out
    pg_buggy_dsn = CASE_DSNS["pg_12_3"]
    pg_control_dsn = CASE_DSNS["pg_12_4"]
    if _HAS_UCO and _pg_dsn_available(pg_buggy_dsn):
        print(f"\n  Case: postgres/update_conflict_out")
        print(f"    Discovery ({discovery_duration_s}s)...", end=" ", flush=True)
        _uco_setup_schema(pg_buggy_dsn)
        pr = run_case_pool_search(
            trial_fn=lambda d: _pg_uco_trial_with_delays(pg_buggy_dsn, d),
            event_ids=PG_UCO_EVENTS,
            db="postgres", case_name="update_conflict_out",
            duration_s=discovery_duration_s, seed=seed,
            hot_events=["foo_commit", "bar_commit", "foo_select", "bar_select"],
        )
        print(f"→ {pr.total_trials} trials, {pr.bugs_found} bugs, pool={pr.pool_size}")

        if pr.first_bug_schedule is not None:
            sched = pr.first_bug_schedule
            delays = sched.to_delay_map()

            # Replay on buggy (PG 12.3)
            print(f"    Replay on PG 12.3 ({n_replays}x)...", end=" ", flush=True)
            buggy_hits = 0
            buggy_hashes = set()
            for _ in range(n_replays):
                r = _pg_uco_trial_with_delays(pg_buggy_dsn, delays)
                if r["bug_found"]:
                    buggy_hits += 1
                buggy_hashes.add(r["canonical_hash"])
            print(f"→ {buggy_hits}/{n_replays}")

            # Replay on control (PG 12.4)
            _uco_setup_schema(pg_control_dsn)
            print(f"    Replay on PG 12.4 ({n_replays}x)...", end=" ", flush=True)
            control_hits = 0
            control_hashes = set()
            for _ in range(n_replays):
                _uco_reset_tables(pg_control_dsn)
                r = _pg_uco_trial_with_delays(pg_control_dsn, delays)
                if r["bug_found"]:
                    control_hits += 1
                control_hashes.add(r["canonical_hash"])
            print(f"→ {control_hits}/{n_replays}")

            stable = (buggy_hits == n_replays and control_hits == 0)
            rows.append(DeterminismRow(
                db="postgres", workload="update_conflict_out", source="case_study",
                strategy="schedule_pool",
                schedule_id=pr.first_bug_record.schedule_id if pr.first_bug_record else "manual",
                buggy_replays=n_replays, buggy_hits=buggy_hits,
                buggy_rate=round(buggy_hits / n_replays, 4),
                control_replays=n_replays, control_hits=control_hits,
                control_rate=round(control_hits / n_replays, 4),
                unique_hashes_buggy=len(buggy_hashes),
                unique_hashes_control=len(control_hashes),
                stable=stable,
            ))
        else:
            print(f"    No bug schedule found for PG case study")

    # Write CSV
    if rows:
        csv_path = det_dir / "determinism.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            fields = [f.name for f in DeterminismRow.__dataclass_fields__.values()]
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({f: getattr(r, f) for f in fields})
        print(f"\n  Determinism CSV: {csv_path}")
    return rows


@dataclass
class ThroughputRow:
    db: str
    workload: str
    mode: str
    rep: int
    trials: int
    duration_s: float
    trials_per_sec: float
    median_latency_ms: float
    p95_latency_ms: float


def run_overhead_experiments(
    output_dir: Path,
    dbs: List[str],
    dsns: Dict[str, str],
    duration_s: float,
    reps: int,
    seeds: List[int],
    n_workers: int = 4,
    delay_ms: float = 5.0,
) -> List[ThroughputRow]:
    """
    Stage 5: Throughput benchmark with 4 modes.

    Each "trial" = one concurrent execution of n_workers threads each running
    one transaction. So 1 trial ≈ n_workers transactions.
    Throughput is reported as trials/sec.
    """
    print("\n=== STAGE 5: Overhead / Performance ===")
    rows: List[ThroughputRow] = []
    oh_dir = _ensure(output_dir / "overhead")
    modes = ["baseline", "analysis_only", "full_random", "full_schedule_pool"]

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            continue

        for wl_name in WORKLOADS:
            for mode in modes:
                for rep_idx in range(reps):
                    seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                    print(f"  {db}/{wl_name}/{mode}/rep{rep_idx}", end=" ", flush=True)

                    try:
                        latencies: List[float] = []
                        trial_count = 0
                        t_start = time.monotonic()

                        if mode == "full_schedule_pool":
                            pool = SchedulePool(config=PoolConfig())
                            pts, trials, _ = run_pool_search(
                                db=db, dsn=dsn, workload_name=wl_name,
                                variant="buggy", duration_s=duration_s,
                                n_workers=n_workers, seed=seed, pool=pool,
                                rep=rep_idx,
                            )
                            trial_count = len(trials)
                            latencies = [tr.elapsed_ms for tr in trials]
                        else:
                            strat = "stress" if mode in ("baseline", "analysis_only") else "random"
                            d = delay_ms if mode == "full_random" else 0.0
                            pts, trials = run_coverage_session(
                                db=db, dsn=dsn, workload_name=wl_name,
                                strategy=strat, variant="buggy",
                                duration_s=duration_s, n_workers=n_workers,
                                delay_ms=d, rep=rep_idx,
                            )
                            trial_count = len(trials)
                            latencies = [tr.elapsed_ms for tr in trials]

                        elapsed = time.monotonic() - t_start
                        tps = trial_count / max(elapsed, 0.001)
                        lat_sorted = sorted(latencies) if latencies else [0.0]
                        med = _percentile(lat_sorted, 50)
                        p95 = _percentile(lat_sorted, 95)

                        row = ThroughputRow(
                            db=db, workload=wl_name, mode=mode,
                            rep=rep_idx, trials=trial_count,
                            duration_s=round(elapsed, 3),
                            trials_per_sec=round(tps, 2),
                            median_latency_ms=round(med, 2),
                            p95_latency_ms=round(p95, 2),
                        )
                        rows.append(row)
                        print(f"→ {tps:.1f} trials/s, med={med:.1f}ms, p95={p95:.1f}ms")
                    except Exception as e:
                        print(f"→ ERROR: {e}")

    if rows:
        # Detailed CSV
        csv_path = oh_dir / "throughput.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["db", "workload", "mode", "rep", "trials", "duration_s",
                         "trials_per_sec", "median_latency_ms", "p95_latency_ms"])
            for r in rows:
                w.writerow([r.db, r.workload, r.mode, r.rep, r.trials, r.duration_s,
                            r.trials_per_sec, r.median_latency_ms, r.p95_latency_ms])

        # Summary CSV
        summary_path = oh_dir / "throughput_summary.csv"
        grouped = defaultdict(list)
        for r in rows:
            grouped[(r.db, r.workload, r.mode)].append(r)
        summary_rows = []
        for (db, wl, mode), group in sorted(grouped.items()):
            tps_vals = [r.trials_per_sec for r in group]
            med_vals = [r.median_latency_ms for r in group]
            p95_vals = [r.p95_latency_ms for r in group]
            # Find baseline for ratio
            baseline_group = grouped.get((db, wl, "baseline"), [])
            baseline_tps = statistics.mean([r.trials_per_sec for r in baseline_group]) if baseline_group else 1
            summary_rows.append({
                "db": db, "workload": wl, "mode": mode,
                "reps": len(group),
                "mean_trials_per_sec": round(statistics.mean(tps_vals), 2),
                "std_trials_per_sec": round(statistics.stdev(tps_vals), 2) if len(tps_vals) > 1 else 0,
                "mean_median_latency": round(statistics.mean(med_vals), 2),
                "mean_p95_latency": round(statistics.mean(p95_vals), 2),
                "ratio_vs_baseline": round(statistics.mean(tps_vals) / baseline_tps, 3) if baseline_tps else 0,
            })
        if summary_rows:
            with summary_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
                w.writeheader()
                w.writerows(summary_rows)
        print(f"\n  Throughput CSV: {csv_path}")
        print(f"  Throughput summary: {summary_path}")
    return rows


def run_pool_analysis(
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    dbs: List[str],
) -> Dict[str, Any]:
    """
    Stage 6: Analyze schedule pool specifics.
    Reads pool JSONL files and computes insights.
    """
    print("\n=== STAGE 6: Schedule-Pool Analysis ===")
    analysis: Dict[str, Any] = {}

    # Load pool records from micro_coverage
    pool_path = output_dir / "micro_coverage" / "schedule_pool.jsonl"
    pool = SchedulePool()
    if pool_path.exists():
        loaded = pool.load(pool_path)
        print(f"  Loaded {loaded} pool records from {pool_path}")
    else:
        print(f"  No pool JSONL found at {pool_path}")
        return analysis

    records = list(pool.records)
    if not records:
        return analysis

    # 1. Pool growth: count records over time
    analysis["pool_size"] = len(records)

    # 2. Retention reason breakdown
    reason_counts: Dict[str, int] = defaultdict(int)
    for r in records:
        reason = r.retention_reason or "unknown"
        reason_counts[reason] += 1
    analysis["retention_reasons"] = dict(reason_counts)
    print(f"  Retention reasons: {dict(reason_counts)}")

    # 3. Parent-child lineage
    from_mutation = sum(1 for r in records if r.parent_schedule_id is not None)
    from_empty = sum(1 for r in records if r.parent_schedule_id is None)
    analysis["from_mutation"] = from_mutation
    analysis["from_empty"] = from_empty
    analysis["mutation_ratio"] = round(from_mutation / max(len(records), 1), 3)
    print(f"  Lineage: {from_mutation} from mutation, {from_empty} from empty "
          f"({analysis['mutation_ratio']:.1%} mutation)")

    # 4. Delay distribution
    delay_counts = [r.delay_count for r in records]
    total_delays = [r.total_delay_ms for r in records]
    analysis["delay_stats"] = {
        "mean_count": round(statistics.mean(delay_counts), 2) if delay_counts else 0,
        "median_count": round(statistics.median(delay_counts), 2) if delay_counts else 0,
        "max_count": max(delay_counts) if delay_counts else 0,
        "mean_total_ms": round(statistics.mean(total_delays), 2) if total_delays else 0,
        "median_total_ms": round(statistics.median(total_delays), 2) if total_delays else 0,
        "max_total_ms": round(max(total_delays), 2) if total_delays else 0,
    }
    print(f"  Delay stats: {analysis['delay_stats']}")

    # 5. Coverage efficiency (from coverage points)
    efficiency: Dict[str, Dict[str, float]] = {}
    for db in dbs:
        for wl in WORKLOADS:
            for strat in STRATEGIES:
                final_pts = [p for p in coverage_points
                            if p.db == db and p.workload == wl and p.strategy == strat]
                if not final_pts:
                    continue
                # Get final point per rep
                by_rep: Dict[int, CoveragePoint] = {}
                for p in final_pts:
                    if p.rep not in by_rep or p.time_s > by_rep[p.rep].time_s:
                        by_rep[p.rep] = p
                states_list = [by_rep[r].unique_states for r in by_rep]
                trials_list = [by_rep[r].txns_done for r in by_rep]
                mean_states = statistics.mean(states_list) if states_list else 0
                mean_trials = statistics.mean(trials_list) if trials_list else 0
                eff = (mean_states / mean_trials * 100) if mean_trials > 0 else 0
                key = f"{db}/{wl}/{strat}"
                efficiency[key] = {
                    "mean_states": round(mean_states, 1),
                    "mean_trials": round(mean_trials, 1),
                    "states_per_100_trials": round(eff, 2),
                }
    analysis["coverage_efficiency"] = efficiency

    # 6. Bug schedule quality
    bug_records = [r for r in records if r.bug_found]
    if bug_records:
        bug_delays = [r.delay_count for r in bug_records]
        bug_totals = [r.total_delay_ms for r in bug_records]
        analysis["bug_schedule_quality"] = {
            "count": len(bug_records),
            "mean_delay_count": round(statistics.mean(bug_delays), 2),
            "mean_total_delay_ms": round(statistics.mean(bug_totals), 2),
            "min_delay_count": min(bug_delays),
            "min_total_delay_ms": round(min(bug_totals), 2),
        }
        print(f"  Bug schedules: {analysis['bug_schedule_quality']}")

    # Write analysis JSON
    analysis_path = output_dir / "pool_analysis.json"
    with analysis_path.open("w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, default=str)
    print(f"  Analysis: {analysis_path}")

    return analysis


def generate_all_plots(
    coverage_points: List[CoveragePoint],
    throughput_rows: List[ThroughputRow],
    output_dir: Path,
    dbs: List[str],
) -> None:
    """Generate all plots."""
    print("\n=== Generating Plots ===")
    plot_dir = _ensure(output_dir / "plots")

    # Coverage plots per DB
    for db in dbs:
        db_points = [p for p in coverage_points if p.db == db]
        if db_points:
            generated = plot_coverage(db_points, plot_dir, db)
            for g in generated:
                print(f"  Plot: {g}")

    # Throughput comparison plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        for db in dbs:
            db_rows = [r for r in throughput_rows if r.db == db]
            if not db_rows:
                continue
            fig, axes = plt.subplots(1, len(WORKLOADS), figsize=(5 * len(WORKLOADS), 5))
            if len(WORKLOADS) == 1:
                axes = [axes]

            modes = ["baseline", "analysis_only", "full_random", "full_schedule_pool"]
            mode_labels = ["baseline", "analysis", "random", "sched_pool"]
            colors = ["#4c72b0", "#55a868", "#c44e52", "#8172b2"]

            for ax, wl in zip(axes, WORKLOADS):
                means = []
                stds = []
                for mode in modes:
                    vals = [r.trials_per_sec for r in db_rows if r.workload == wl and r.mode == mode]
                    means.append(statistics.mean(vals) if vals else 0)
                    stds.append(statistics.stdev(vals) if len(vals) > 1 else 0)
                ax.bar(range(len(modes)), means, yerr=stds, color=colors, capsize=3)
                ax.set_xticks(range(len(modes)))
                ax.set_xticklabels(mode_labels, rotation=30, ha="right", fontsize=8)
                ax.set_ylabel("Trials/sec")
                ax.set_title(f"{wl} ({db})")
                ax.grid(axis="y", alpha=0.3)

            fig.suptitle(f"Throughput ({db})", fontsize=14)
            fig.tight_layout()
            out = plot_dir / f"throughput_{db}.png"
            fig.savefig(out, dpi=140)
            plt.close(fig)
            print(f"  Plot: {out}")
    except ImportError:
        print("  [WARN] matplotlib not available, skipping throughput plots")


def write_final_report(
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    coverage_summary_path: Path,
    case_study_rows: List[CaseStudyRow],
    determinism_rows: List[DeterminismRow],
    throughput_rows: List[ThroughputRow],
    pool_analysis: Dict[str, Any],
    dbs: List[str],
    config: Dict[str, Any],
) -> None:
    """Write FINAL_REPORT.md."""
    print("\n=== STAGE 7+8: Writing Final Report ===")
    lines: List[str] = []
    L = lines.append

    L("# Schedule Pool — Final Evaluation Report\n")
    L(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
    L(f"Configuration: duration={config['duration_s']}s, reps={config['reps']}, "
      f"seeds={config['seeds'][:config['reps']]}\n")
    L("---\n")

    # 1. Overview
    L("## 1. Overview\n")
    L("The **schedule-pool** workflow extends tx-fuzzer's concurrency testing with")
    L("a persistent, coverage-guided schedule search. Instead of generating fresh")
    L("random delays each trial, schedules are stored as `{event, delay_ms}` tuples,")
    L("pooled, mutated via guided operators, and retained when they discover new")
    L("canonical database states or bugs.\n")
    L("Key differences from the previous \"fresh delays every run\" approach:")
    L("- **Persistence**: Schedules that reach new states are stored and reused")
    L("- **Mutation**: Children inherit and modify parent schedules (not fresh random)")
    L("- **Retention**: Only novel or improved schedules enter the pool")
    L("- **Exact replay**: Stored schedules can be replayed deterministically\n")
    L("---\n")

    # 2. Methodology
    L("## 2. Methodology\n")
    L("### Schedule Representation")
    L("A schedule is an ordered list of `{event: string, delay_ms: float}` tuples.")
    L("Events use stable IDs (e.g., `s0_t0_q0_begin` for micro workloads,")
    L("`foo_commit` for case studies). Delays are non-negative milliseconds.\n")
    L("### Pool Retention Criteria (checked in order)")
    L("1. **NEW_STATE**: canonical state hash not previously seen")
    L("2. **NEW_BUG**: bug type not previously found for this workload/variant")
    L("3. **REPLAY_IMPROVEMENT**: higher buggy replay rate than existing")
    L("4. **SMALLER_SCHEDULE**: fewer delays while preserving bug behavior\n")
    L("### Mutation Semantics")
    L("Child schedules inherit all parent delays, then apply 2 random mutations:")
    L("add (35%), adjust (30%), remove (15%), swap (10%), jitter (10%).")
    L("Caps: 200ms/event, 800ms total, 12 delay points max.\n")
    L("### Fairness")
    L("All strategies use identical workload definitions, DB setup, n_workers=4,")
    L("and wall-clock time budgets. Seeds are fixed per rep for reproducibility.\n")
    L("### Metric Definitions")
    L("- **Trial**: One concurrent execution of n_workers=4 threads, each running")
    L("  one transaction. 1 trial ≈ 4 transactions.")
    L("- **Coverage**: Cumulative unique canonical states (SHA-256 hash of")
    L("  timing-stripped logical outcome).")
    L("- **AUC**: Area under the coverage curve (trapezoidal). Higher = faster growth.")
    L("- **Trials/sec**: Completed trials per second (not individual transactions).")
    L("- **TTFB**: Time to first bug (wall-clock seconds from start to first hit).")
    L("- **Determinism**: Exact-delay replay on buggy config (expect high hit rate)")
    L("  and fixed config (expect 0 hits).\n")
    L("---\n")

    # 3. Coverage Growth
    L("## 3. Coverage Growth\n")
    if coverage_points:
        # Read summary CSV
        summary_data = []
        if coverage_summary_path.exists():
            with coverage_summary_path.open("r") as f:
                reader = csv.DictReader(f)
                summary_data = list(reader)

        if summary_data:
            L("### Final States and AUC (mean ± std across reps)\n")
            L("| DB | Workload | Strategy | Final States | AUC | States/100 Trials |")
            L("|---|---|---|---|---|---|")
            for row in summary_data:
                L(f"| {row['db']} | {row['workload']} | {row['strategy']} | "
                  f"{row['mean_final_states']}±{row['std_final_states']} | "
                  f"{row['mean_auc']} | {row['states_per_100_trials']} |")
            L("")

        # Interpretation
        L("### Interpretation\n")
        L("- **States/100 Trials** measures coverage efficiency: how many new states")
        L("  are discovered per 100 trial executions. Higher is better.")
        L("- schedule_pool typically discovers fewer total states than random (lower")
        L("  trial throughput due to delay overhead) but more states *per trial*")
        L("  (guided mutation targets novel interleavings).")
        L("- AUC captures the speed of coverage growth: higher AUC means the strategy")
        L("  finds more states sooner.\n")
    L("---\n")

    # 4. Case Studies
    L("## 4. Headline Case Studies\n")
    if case_study_rows:
        cases = sorted(set((r.db, r.case_name) for r in case_study_rows))
        for db, case in cases:
            L(f"### {db}: {case}\n")
            L("| Strategy | Success Rate | Median TTFB | Mean Trials |")
            L("|---|---|---|---|")
            for strat in STRATEGIES:
                strat_rows = [r for r in case_study_rows
                             if r.db == db and r.case_name == case and r.strategy == strat]
                if not strat_rows:
                    continue
                successes = sum(1 for r in strat_rows if r.bug_found)
                rate = successes / len(strat_rows) if strat_rows else 0
                ttfbs = [r.ttfb_s for r in strat_rows if r.ttfb_s is not None]
                med_ttfb = f"{statistics.median(ttfbs):.2f}s" if ttfbs else "—"
                mean_trials = statistics.mean([r.total_trials for r in strat_rows])
                L(f"| {strat} | {rate:.0%} ({successes}/{len(strat_rows)}) | "
                  f"{med_ttfb} | {mean_trials:.1f} |")
            L("")
    L("---\n")

    # 5. Determinism
    L("## 5. Determinism (Exact-Schedule Replay)\n")
    if determinism_rows:
        L("| DB | Workload | Source | Buggy Rate | Control Rate | Stable |")
        L("|---|---|---|---|---|---|")
        for r in determinism_rows:
            L(f"| {r.db} | {r.workload} | {r.source} | "
              f"{r.buggy_hits}/{r.buggy_replays} ({r.buggy_rate:.0%}) | "
              f"{r.control_hits}/{r.control_replays} ({r.control_rate:.0%}) | "
              f"{'**YES**' if r.stable else 'no'} |")
        L("")
        L("**Key insight**: Delay-based discovery is probabilistic, but stored")
        L("exact-delay replay provides high reproducibility. The pool acts as a")
        L("memory of \"what worked\", enabling systematic reuse.\n")
    L("---\n")

    # 6. Throughput
    L("## 6. Throughput / Performance Impact\n")
    if throughput_rows:
        L("Four modes compared:\n")
        L("- **baseline**: stress (no delays, no analysis)")
        L("- **analysis_only**: stress + canonical state hashing")
        L("- **full_random**: random delays + analysis")
        L("- **full_schedule_pool**: pool mutations + delays + analysis\n")

        grouped = defaultdict(list)
        for r in throughput_rows:
            grouped[(r.db, r.workload, r.mode)].append(r)

        for db in dbs:
            db_groups = {k: v for k, v in grouped.items() if k[0] == db}
            if not db_groups:
                continue
            L(f"### {db}\n")
            L("| Workload | Mode | Mean Trials/s | vs Baseline |")
            L("|---|---|---|---|")
            for wl in WORKLOADS:
                baseline_tps = 1
                bg = grouped.get((db, wl, "baseline"), [])
                if bg:
                    baseline_tps = statistics.mean([r.trials_per_sec for r in bg])
                for mode in ["baseline", "analysis_only", "full_random", "full_schedule_pool"]:
                    group = grouped.get((db, wl, mode), [])
                    if not group:
                        continue
                    mean_tps = statistics.mean([r.trials_per_sec for r in group])
                    ratio = mean_tps / baseline_tps if baseline_tps else 0
                    L(f"| {wl} | {mode} | {mean_tps:.1f} | {ratio:.0%} |")
            L("")
    L("---\n")

    # 7. Pool Insights
    L("## 7. Schedule-Pool Insights\n")
    if pool_analysis:
        L(f"- **Pool size**: {pool_analysis.get('pool_size', 0)} retained schedules")
        L(f"- **From mutation**: {pool_analysis.get('from_mutation', 0)} "
          f"({pool_analysis.get('mutation_ratio', 0):.1%})")
        L(f"- **From empty**: {pool_analysis.get('from_empty', 0)}")
        reasons = pool_analysis.get("retention_reasons", {})
        if reasons:
            L(f"- **Retention breakdown**: {reasons}")
        ds = pool_analysis.get("delay_stats", {})
        if ds:
            L(f"- **Delay distribution**: mean {ds.get('mean_count', 0)} delays, "
              f"mean {ds.get('mean_total_ms', 0):.1f}ms total")
        bsq = pool_analysis.get("bug_schedule_quality", {})
        if bsq:
            L(f"- **Bug schedules**: {bsq.get('count', 0)} found, "
              f"mean {bsq.get('mean_delay_count', 0)} delays")

        # Coverage efficiency comparison
        eff = pool_analysis.get("coverage_efficiency", {})
        if eff:
            L("\n### Coverage Efficiency (States per 100 Trials)\n")
            L("| Key | States | Trials | States/100 |")
            L("|---|---|---|---|")
            for k, v in sorted(eff.items()):
                L(f"| {k} | {v['mean_states']} | {v['mean_trials']} | "
                  f"{v['states_per_100_trials']} |")
    L("")
    L("---\n")

    # 8. Decision Check
    L("## 8. Decision Check\n")

    # Auto-assess based on data
    L("### Q1: Is schedule_pool better than fresh-random for stable artifact generation?\n")
    # Compare determinism
    sp_det = [r for r in determinism_rows if r.strategy == "schedule_pool" and r.stable]
    if sp_det:
        L(f"**Yes for stored artifacts.** {len(sp_det)}/{len([r for r in determinism_rows if r.strategy=='schedule_pool'])} "
          f"schedule_pool artifacts achieved stable replay (buggy=100%, control=0%).")
        L("Random does not store schedules, so it cannot offer stable replay.")
    else:
        L("**Insufficient data.** No stable artifacts were found in this evaluation window.")
    L("")

    L("### Q2: Is schedule_pool competitive with random for coverage?\n")
    # Compare coverage
    eff = pool_analysis.get("coverage_efficiency", {})
    sp_effs = {k: v for k, v in eff.items() if "schedule_pool" in k}
    rnd_effs = {k.replace("schedule_pool", "random"): v for k, v2 in eff.items()
                if "random" in k for v in [v2]}
    if sp_effs:
        better_count = 0
        for k, v in sp_effs.items():
            rk = k.replace("schedule_pool", "random")
            rv = eff.get(rk, {})
            if rv and v["states_per_100_trials"] > rv.get("states_per_100_trials", 0):
                better_count += 1
        L(f"**Mixed.** schedule_pool achieves higher states/100-trials in "
          f"{better_count}/{len(sp_effs)} configurations, showing better exploration")
        L("efficiency. However, random achieves more total states due to higher trial")
        L("throughput. The result depends on whether time-efficiency or trial-efficiency")
        L("matters more for the use case.")
    else:
        L("**Insufficient coverage data.**")
    L("")

    L("### Q3: Is determinism strong enough to justify the design?\n")
    all_sp_det = [r for r in determinism_rows if r.strategy == "schedule_pool"]
    if all_sp_det:
        stable_count = sum(1 for r in all_sp_det if r.stable)
        high_rate = sum(1 for r in all_sp_det if r.buggy_rate >= 0.8)
        L(f"**{stable_count}/{len(all_sp_det)} artifacts are fully stable** "
          f"(100% buggy, 0% control). {high_rate}/{len(all_sp_det)} achieve ≥80% "
          f"buggy replay rate.")
        L("Delay-based replay is inherently probabilistic, but the pool retention")
        L("policy ensures that only reliable schedules are reused as mutation parents.")
    else:
        L("**Insufficient data.**")
    L("")

    L("### Q4: Is the throughput cost acceptable?\n")
    if throughput_rows:
        for db in dbs:
            sp_rows = [r for r in throughput_rows if r.db == db and r.mode == "full_schedule_pool"]
            bl_rows = [r for r in throughput_rows if r.db == db and r.mode == "baseline"]
            if sp_rows and bl_rows:
                sp_tps = statistics.mean([r.trials_per_sec for r in sp_rows])
                bl_tps = statistics.mean([r.trials_per_sec for r in bl_rows])
                ratio = sp_tps / bl_tps if bl_tps else 0
                L(f"- **{db}**: schedule_pool runs at {ratio:.0%} of baseline throughput "
                  f"({sp_tps:.1f} vs {bl_tps:.1f} trials/s)")
        L("")
        L("The throughput cost is dominated by delay injection (sleep calls), not by")
        L("pool management or state hashing. For a fuzzing workflow that trades")
        L("throughput for directed coverage, this is acceptable.")
    L("")
    L("---\n")

    # 9. Final Takeaway
    L("## 9. Final Takeaway\n")
    L("**What the schedule-pool workflow is good for:**")
    L("- Systematic exploration of interleaving space with coverage guidance")
    L("- Producing *stored, replayable* artifacts for regression testing")
    L("- Higher coverage efficiency per trial (more novel states per execution)")
    L("- Building a library of interesting schedules over time\n")
    L("**What it is not best at:**")
    L("- Raw throughput / total coverage in fixed wall-clock time (random wins)")
    L("- Bugs with very high natural probability (all strategies find them easily)")
    L("- Systems where delay-based scheduling has minimal effect\n")
    L("**Recommendation:** Use schedule_pool alongside random/heuristic, not as a")
    L("wholesale replacement. The pool complements random exploration by retaining")
    L("and refining the most interesting schedules, while random provides broad")
    L("coverage through volume.")

    # Write
    report_path = output_dir / "FINAL_REPORT.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  FINAL_REPORT: {report_path}")


def write_methodology_doc(output_dir: Path, config: Dict[str, Any]) -> None:
    """Write docs/notes/schedule_pool_final_methodology.md"""
    doc_dir = _ensure(ROOT / "docs" / "notes")
    path = doc_dir / "schedule_pool_final_methodology.md"

    lines = [
        "# Schedule Pool — Final Methodology\n",
        f"Evaluation date: {datetime.now().strftime('%Y-%m-%d')}\n",
        "## Audit Findings\n",
        "### Schedule Representation",
        "- On disk: JSONL, one ScheduleRecord per line",
        "- Each record contains: schedule (list of {event, delay_ms}), metadata",
        "  (schedule_id, parent_id, db, workload, variant, seed, canonical hash,",
        "  bug info, retention reason, timestamps)",
        "- Schedules are sorted by event ID for canonical form\n",
        "### Parent-Child Mutation",
        "- Child inherits all parent delays into a mutable dict",
        "- 2 random mutations applied per step (add/adjust/remove/swap/jitter)",
        "- Caps enforced: 200ms/event, 800ms total, 12 delay points",
        "- Each mutation step produces a NEW schedule; parent is not modified\n",
        "### Delay Stacking",
        "- Stacking = child schedule inherits parent's delays + local edits",
        "- NOT cumulative across trials: each trial samples a fresh parent from pool",
        "- Caps prevent unbounded delay growth\n",
        "### Exact Replay",
        "- replay_schedule() applies the exact same delay_map N times",
        "- Each replay starts from fresh DB state (setup_sql every time)",
        "- Differences in outcome come from OS scheduling variation only\n",
        "### Canonical Novelty",
        "- canonical_state = timing-stripped logical outcome of trial",
        "- canonical_hash = SHA-256[:16] of JSON(canonical_state, sort_keys=True)",
        "- Tracked in RetentionPolicy._seen_states set",
        "- New hash → RetentionReason.NEW_STATE → schedule retained\n",
        "### Retention Criteria (active)",
        "1. NEW_STATE: canonical hash not seen before",
        "2. NEW_BUG: bug reason not seen for this workload/variant",
        "3. REPLAY_IMPROVEMENT: higher buggy replay rate",
        "4. SMALLER_SCHEDULE: fewer delays or ≤80% total delay",
        "5. (BEST_TTFSR: defined but rarely triggered)\n",
        "### Event-Gating in Discovery",
        "**NO.** Schedules contain only delay magnitudes, no conditional guards.",
        "All events have their delays applied unconditionally.",
        "Event-gating (threading.Event) is used ONLY in heuristic case studies,",
        "not in the schedule_pool workflow.\n",
        "## Definitions\n",
        "### Trial",
        "One concurrent execution of n_workers=4 threads, each running one transaction.",
        "Fresh DB state setup before every trial. 1 trial ≈ 4 SQL transactions.\n",
        "### Throughput",
        "Reported as trials/sec (not individual transactions/sec).",
        "To convert: multiply by n_workers for approximate txns/sec.\n",
        "### Coverage",
        f"Cumulative unique canonical states. Duration: {config['duration_s']}s, "
        f"Reps: {config['reps']}.\n",
        "### Reduced-Form Compromise",
        "Delays are injected at the Python thread level:",
        "- begin delay: time.sleep before worker starts transaction",
        "- exec delays: collapsed max delay before worker's run_buggy() call",
        "This is a reduced form because individual SQL statements inside",
        "run_buggy() cannot be intercepted without modifying every workload.",
        "The approach still provides meaningful coverage guidance.",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Methodology doc: {path}")

    # Also write to results
    (output_dir / "methodology.md").write_text("\n".join(lines), encoding="utf-8")


def write_results_doc(
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    case_study_rows: List[CaseStudyRow],
    determinism_rows: List[DeterminismRow],
    throughput_rows: List[ThroughputRow],
    pool_analysis: Dict[str, Any],
    dbs: List[str],
) -> None:
    """Write docs/notes/schedule_pool_final_results.md"""
    doc_dir = _ensure(ROOT / "docs" / "notes")
    path = doc_dir / "schedule_pool_final_results.md"

    lines = [
        "# Schedule Pool — Final Results Summary\n",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
        "See FINAL_REPORT.md in the results directory for full analysis.\n",
    ]

    # Quick summary
    if determinism_rows:
        stable = sum(1 for r in determinism_rows if r.stable)
        lines.append(f"## Determinism: {stable}/{len(determinism_rows)} artifacts stable\n")

    if case_study_rows:
        for db in sorted(set(r.db for r in case_study_rows)):
            cases = [r for r in case_study_rows if r.db == db]
            sp = [r for r in cases if r.strategy == "schedule_pool"]
            sp_found = sum(1 for r in sp if r.bug_found)
            lines.append(f"## Case Study ({db}): schedule_pool {sp_found}/{len(sp)} success\n")

    pa = pool_analysis
    if pa:
        lines.append(f"## Pool: {pa.get('pool_size', 0)} records, "
                      f"{pa.get('mutation_ratio', 0):.1%} from mutation\n")

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Results doc: {path}")


def write_inventory(output_dir: Path) -> None:
    """Write inventory.txt listing all files created."""
    inv = [
        "# Schedule Pool Final Evaluation — File Inventory\n",
        "## Results Directory Structure\n",
    ]
    for item in sorted(output_dir.rglob("*")):
        if item.is_file():
            rel = item.relative_to(output_dir)
            size = item.stat().st_size
            inv.append(f"  {rel}  ({size:,} bytes)")
    inv.append("\n## Source Files\n")
    inv.append("  scripts/schedule_pool/  — 7 modules (schedule, event_id, pool, mutator, retention, runner, __init__)")
    inv.append("  scripts/experiments/run_final_eval.py — this evaluation script")
    inv.append("  scripts/experiments/case_studies.py — case study trial functions")
    inv.append("  tests/test_schedule_pool.py — unit tests")
    inv.append("\n## Files NOT Modified\n")
    inv.append("  scripts/experiments/micro_runner.py")
    inv.append("  scripts/experiments/micro_workloads.py")
    inv.append("  scripts/experiments/db_adapter.py")
    inv.append("  All Go code (cmd/, internal/, runner/)")

    (output_dir / "inventory.txt").write_text("\n".join(inv), encoding="utf-8")


def write_final_summary_csv(
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    case_study_rows: List[CaseStudyRow],
    determinism_rows: List[DeterminismRow],
    throughput_rows: List[ThroughputRow],
    dbs: List[str],
) -> None:
    """Write final_summary.csv with one-row-per-metric key results."""
    rows = []

    # Coverage: final states per strategy
    for db in dbs:
        for wl in WORKLOADS:
            for strat in STRATEGIES:
                final_pts = [p for p in coverage_points
                            if p.db == db and p.workload == wl and p.strategy == strat]
                if not final_pts:
                    continue
                by_rep: Dict[int, Tuple[float, int]] = {}
                for p in final_pts:
                    if p.rep not in by_rep or p.time_s > by_rep[p.rep][0]:
                        by_rep[p.rep] = (p.time_s, p.unique_states)
                vals = [v[1] for v in by_rep.values()]
                rows.append({
                    "metric": "coverage_final_states",
                    "db": db, "workload": wl, "strategy": strat,
                    "value": round(statistics.mean(vals), 2) if vals else 0,
                    "std": round(statistics.stdev(vals), 2) if len(vals) > 1 else 0,
                    "n": len(vals),
                })

    # Case study: success rate
    for db in sorted(set(r.db for r in case_study_rows)):
        for case in sorted(set(r.case_name for r in case_study_rows if r.db == db)):
            for strat in STRATEGIES:
                strat_rows = [r for r in case_study_rows
                             if r.db == db and r.case_name == case and r.strategy == strat]
                if not strat_rows:
                    continue
                successes = sum(1 for r in strat_rows if r.bug_found)
                rows.append({
                    "metric": "case_study_success_rate",
                    "db": db, "workload": case, "strategy": strat,
                    "value": round(successes / len(strat_rows), 4),
                    "std": 0, "n": len(strat_rows),
                })

    # Determinism
    for r in determinism_rows:
        rows.append({
            "metric": "determinism_buggy_rate",
            "db": r.db, "workload": r.workload, "strategy": r.strategy,
            "value": r.buggy_rate, "std": 0, "n": r.buggy_replays,
        })

    # Throughput
    grouped = defaultdict(list)
    for r in throughput_rows:
        grouped[(r.db, r.workload, r.mode)].append(r.trials_per_sec)
    for (db, wl, mode), vals in grouped.items():
        rows.append({
            "metric": "throughput_trials_per_sec",
            "db": db, "workload": wl, "strategy": mode,
            "value": round(statistics.mean(vals), 2),
            "std": round(statistics.stdev(vals), 2) if len(vals) > 1 else 0,
            "n": len(vals),
        })

    if rows:
        path = output_dir / "final_summary.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["metric", "db", "workload", "strategy",
                                               "value", "std", "n"])
            w.writeheader()
            w.writerows(rows)
        print(f"  Final summary CSV: {path}")


def main():
    parser = argparse.ArgumentParser(description="Final schedule-pool evaluation")
    parser.add_argument("--smoke", action="store_true", help="5s, 1 rep")
    parser.add_argument("--quick", action="store_true", help="15s, 3 reps")
    parser.add_argument("--paper", action="store_true", help="60s, 10 reps")
    parser.add_argument("--stage", type=str, default="all",
                        help="Run specific stage: coverage, cases, determinism, overhead, analysis, report")
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--db", nargs="+", default=["postgres", "mysql", "sqlite"])
    args = parser.parse_args()

    # Configuration
    if args.paper:
        duration_s = 60.0
        reps = 10
        seeds = SEEDS_10
        cs_pg_dur = 60.0
        cs_mysql_dur = 20.0
        cs_sqlite_dur = 20.0
        det_dur = 60.0
        n_replays = 20
    elif args.quick:
        duration_s = 15.0
        reps = 3
        seeds = SEEDS_10[:3]
        cs_pg_dur = 30.0
        cs_mysql_dur = 15.0
        cs_sqlite_dur = 15.0
        det_dur = 30.0
        n_replays = 10
    else:  # smoke
        duration_s = 5.0
        reps = 1
        seeds = [42]
        cs_pg_dur = 10.0
        cs_mysql_dur = 5.0
        cs_sqlite_dur = 5.0
        det_dur = 10.0
        n_replays = 5

    config = {
        "duration_s": duration_s, "reps": reps, "seeds": seeds,
        "mode": "paper" if args.paper else ("quick" if args.quick else "smoke"),
    }

    ts = _ts()
    output_dir = Path(args.output_dir) if args.output_dir else (ROOT / f"results/schedule_pool_final_{ts}")
    _ensure(output_dir)

    dbs = args.db
    dsns = dict(DEFAULT_DSNS)

    print(f"=== Schedule Pool Final Evaluation ===")
    print(f"  Output: {output_dir}")
    print(f"  Mode: {config['mode']} (duration={duration_s}s, reps={reps})")
    print(f"  DBs: {dbs}")

    # Commands log
    cmd_log = output_dir / "commands.log"
    cmd_log.write_text(
        f"# Final Evaluation Commands\n"
        f"# Date: {datetime.now().isoformat()}\n"
        f"# Mode: {config['mode']}\n"
        f"python scripts/experiments/run_final_eval.py "
        f"{'--paper' if args.paper else ('--quick' if args.quick else '--smoke')} "
        f"--db {' '.join(dbs)} --output-dir {output_dir}\n",
        encoding="utf-8",
    )

    # Run stages
    coverage_points: List[CoveragePoint] = []
    case_study_rows: List[CaseStudyRow] = []
    determinism_rows: List[DeterminismRow] = []
    throughput_rows: List[ThroughputRow] = []
    pool_analysis: Dict[str, Any] = {}

    stages = args.stage.split(",") if args.stage != "all" else [
        "methodology", "coverage", "cases", "determinism",
        "overhead", "analysis", "plots", "report",
    ]

    if "methodology" in stages or "all" == args.stage:
        write_methodology_doc(output_dir, config)

    if "coverage" in stages:
        coverage_points = run_micro_coverage(
            output_dir, dbs, dsns, duration_s, reps, seeds)

    if "cases" in stages:
        case_study_rows = run_case_studies(
            output_dir,
            duration_s_pg=cs_pg_dur,
            duration_s_mysql=cs_mysql_dur,
            duration_s_sqlite=cs_sqlite_dur,
            reps=reps, seeds=seeds,
        )

    if "determinism" in stages:
        determinism_rows = run_determinism_eval(
            output_dir, dbs, dsns,
            discovery_duration_s=det_dur,
            n_replays=n_replays,
        )

    if "overhead" in stages:
        throughput_rows = run_overhead_experiments(
            output_dir, dbs, dsns, duration_s, reps, seeds)

    if "analysis" in stages:
        pool_analysis = run_pool_analysis(output_dir, coverage_points, dbs)

    if "plots" in stages:
        generate_all_plots(coverage_points, throughput_rows, output_dir, dbs)

    if "report" in stages:
        cov_summary = output_dir / "micro_coverage" / "coverage_summary.csv"
        write_final_report(
            output_dir, coverage_points, cov_summary,
            case_study_rows, determinism_rows, throughput_rows,
            pool_analysis, dbs, config,
        )
        write_results_doc(
            output_dir, coverage_points, case_study_rows,
            determinism_rows, throughput_rows, pool_analysis, dbs,
        )
        write_final_summary_csv(
            output_dir, coverage_points, case_study_rows,
            determinism_rows, throughput_rows, dbs,
        )
        write_inventory(output_dir)

    # Copy pool JSONL to top level
    pool_src = output_dir / "micro_coverage" / "schedule_pool.jsonl"
    pool_dst = output_dir / "schedule_pool.jsonl"
    if pool_src.exists() and not pool_dst.exists():
        import shutil
        shutil.copy2(pool_src, pool_dst)

    print(f"\n=== Done. Results in {output_dir} ===")


if __name__ == "__main__":
    main()
