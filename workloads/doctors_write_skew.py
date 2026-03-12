"""
Workload: doctors_write_skew — classic write-skew anomaly.

Two doctors are on-call. Each transaction checks if the OTHER doctor is
still on-call, and if so, sets itself off-call. Under snapshot isolation
(REPEATABLE READ), both can succeed → invariant violated (nobody on-call).
Under SERIALIZABLE, PostgreSQL raises 40001.

Schema: oncall(doctor_id int PK, on_call boolean)
Invariant: COUNT(*) WHERE on_call = true >= 1 at all times.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Any, Dict, List, Optional

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore

from workloads import OracleResult

WORKLOAD_META = {
    "name": "doctors_write_skew",
    "description": "Classic write-skew anomaly (doctor on-call constraint)",
    "invariants": "At least 1 doctor must remain on_call=true after all committed txns.",
    "isolation_levels": ["REPEATABLE READ", "SERIALIZABLE"],
    "target_versions": {"primary": "16.2"},
    "clients": 2,
    "variants": ["buggy", "fixed", "fixed_naive_for_update", "fixed_ordered_locks"],
    "negative_controls": [
        "Under SERIALIZABLE → 40001 prevents anomaly",
        "Fixed variant uses SELECT…FOR UPDATE → prevents under RR too",
        "fixed_ordered_locks acquires row locks in id order (1,2) to prevent deadlocks",
    ],
}


def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS oncall CASCADE",
        "CREATE TABLE oncall (doctor_id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)",
    ]


def truncate_sql() -> List[str]:
    return ["DELETE FROM oncall"]


def _init_sql() -> List[str]:
    return [
        "INSERT INTO oncall VALUES (1, true)",
        "INSERT INTO oncall VALUES (2, true)",
    ]


def seed_trace(
    clients: int = 2,
    isolation: str = "REPEATABLE READ",
    variant: str = "buggy",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    Two-client trace:
      Client 0 (doctor 1): BEGIN → SELECT on_call FROM oncall WHERE doctor_id=2
                            → UPDATE oncall SET on_call=false WHERE doctor_id=1 → COMMIT
      Client 1 (doctor 2): BEGIN → SELECT on_call FROM oncall WHERE doctor_id=1
                            → UPDATE oncall SET on_call=false WHERE doctor_id=2 → COMMIT
    """
    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"

    use_for_update = variant in ("fixed", "fixed_naive_for_update")
    use_ordered_locks = variant == "fixed_ordered_locks"

    if use_for_update:
        select_tpl = "SELECT on_call FROM oncall WHERE doctor_id = {other} FOR UPDATE"
    else:
        select_tpl = "SELECT on_call FROM oncall WHERE doctor_id = {other}"

    trace = []
    for doc_id, other_id in [(1, 2), (2, 1)]:
        cid = doc_id - 1
        sess = f"doctor_{doc_id}"
        trace.append({"event_id": f"d{doc_id}_begin", "client_id": cid, "session": sess,
                       "type": "begin", "sql": iso_sql})
        if use_ordered_locks:
            # Lock both rows in ascending id order to prevent deadlocks
            trace.append({"event_id": f"d{doc_id}_lock1", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": "SELECT doctor_id FROM oncall WHERE doctor_id = 1 FOR UPDATE"})
            trace.append({"event_id": f"d{doc_id}_lock2", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": "SELECT doctor_id FROM oncall WHERE doctor_id = 2 FOR UPDATE"})
            # Now read the other doctor's on_call status (already locked)
            trace.append({"event_id": f"d{doc_id}_select", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": f"SELECT on_call FROM oncall WHERE doctor_id = {other_id}"})
        else:
            trace.append({"event_id": f"d{doc_id}_select", "client_id": cid, "session": sess,
                           "type": "sql", "sql": select_tpl.format(other=other_id)})
        trace.append({"event_id": f"d{doc_id}_update", "client_id": cid, "session": sess,
                       "type": "sql",
                       "sql": f"UPDATE oncall SET on_call = false WHERE doctor_id = {doc_id}"})
        trace.append({"event_id": f"d{doc_id}_commit", "client_id": cid, "session": sess,
                       "type": "commit", "sql": "COMMIT"})
    return trace


def run_trial(
    dsn: str,
    isolation: str = "REPEATABLE READ",
    variant: str = "buggy",
    delays: Optional[Dict[str, float]] = None,
    seed: int = 42,
) -> Dict[str, Any]:
    """
    Run one concurrent trial of the write-skew workload.

    Application logic per doctor:
      1. BEGIN
      2. SELECT on_call FROM oncall WHERE doctor_id = <other>
      3. IF other is on_call → UPDATE oncall SET on_call=false WHERE doctor_id=<self>
      4. COMMIT

    Write skew occurs when both see the other on-call (snapshot) and both update.
    """
    delays = delays or {}
    result = {
        "bug_found": False, "got_40001": False,
        "execution_log": [], "errors": [], "final_state": [],
    }
    lock = threading.Lock()

    conn_setup = psycopg2.connect(dsn)
    conn_setup.autocommit = True
    cur_setup = conn_setup.cursor()
    for sql in setup_sql():
        cur_setup.execute(sql)
    for sql in _init_sql():
        cur_setup.execute(sql)
    conn_setup.close()

    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"
    barrier = threading.Barrier(2)

    use_for_update = variant in ("fixed", "fixed_naive_for_update")
    use_ordered_locks = variant == "fixed_ordered_locks"

    if use_for_update:
        select_tpl = "SELECT on_call FROM oncall WHERE doctor_id = {} FOR UPDATE"
    else:
        select_tpl = "SELECT on_call FROM oncall WHERE doctor_id = {}"

    def worker(doc_id: int, other_id: int):
        cid = doc_id - 1
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            barrier.wait(timeout=30.0)

            # BEGIN
            eid = f"d{doc_id}_begin"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            cur.execute(iso_sql)
            with lock:
                result["execution_log"].append({"event_id": eid, "sql": iso_sql})

            if use_ordered_locks:
                # Acquire locks on BOTH rows in ascending doctor_id order (1, 2)
                # to guarantee no deadlock (both sessions wait in the same order).
                for lock_id in [1, 2]:
                    eid_lock = f"d{doc_id}_lock{lock_id}"
                    d = delays.get(eid_lock, 0.0)
                    if d > 0: time.sleep(d)
                    lock_sql = f"SELECT doctor_id FROM oncall WHERE doctor_id = {lock_id} FOR UPDATE"
                    try:
                        cur.execute(lock_sql)
                        cur.fetchall()
                    except Exception as e:
                        error_str = str(e)
                        sqlstate = getattr(e, "pgcode", None)
                        with lock:
                            result["errors"].append(f"{eid_lock}: {error_str}")
                            if sqlstate == "40001": result["got_40001"] = True
                            if sqlstate == "40P01": result["errors"].append(f"{eid_lock}: DEADLOCK 40P01")
                        try: cur.execute("ROLLBACK")
                        except: pass
                        return
                    with lock:
                        result["execution_log"].append({"event_id": eid_lock, "sql": lock_sql})

                # Now read the other doctor's on_call (already locked, no FOR UPDATE needed)
                eid = f"d{doc_id}_select"
                d = delays.get(eid, 0.0)
                if d > 0: time.sleep(d)
                select_sql = f"SELECT on_call FROM oncall WHERE doctor_id = {other_id}"
                other_on_call = False
                try:
                    cur.execute(select_sql)
                    rows = cur.fetchall()
                    other_on_call = bool(rows and rows[0][0])
                except Exception as e:
                    error_str = str(e)
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {error_str}")
                        if sqlstate == "40001": result["got_40001"] = True
                    try: cur.execute("ROLLBACK")
                    except: pass
                    return
                with lock:
                    result["execution_log"].append({
                        "event_id": eid, "sql": select_sql,
                        "other_on_call": other_on_call,
                    })
            else:
                # SELECT other doctor's on_call status (buggy or fixed_naive_for_update)
                eid = f"d{doc_id}_select"
                d = delays.get(eid, 0.0)
                if d > 0: time.sleep(d)
                select_sql = select_tpl.format(other_id)
                other_on_call = False
                try:
                    cur.execute(select_sql)
                    rows = cur.fetchall()
                    other_on_call = bool(rows and rows[0][0])
                except Exception as e:
                    error_str = str(e)
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {error_str}")
                        if sqlstate == "40001": result["got_40001"] = True
                    try: cur.execute("ROLLBACK")
                    except: pass
                    return
                with lock:
                    result["execution_log"].append({
                        "event_id": eid, "sql": select_sql,
                        "other_on_call": other_on_call,
                    })

            # Conditional UPDATE: only go off-call if other is on-call
            if other_on_call:
                eid = f"d{doc_id}_update"
                d = delays.get(eid, 0.0)
                if d > 0: time.sleep(d)
                update_sql = f"UPDATE oncall SET on_call = false WHERE doctor_id = {doc_id}"
                try:
                    cur.execute(update_sql)
                except Exception as e:
                    error_str = str(e)
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {error_str}")
                        if sqlstate == "40001": result["got_40001"] = True
                    try: cur.execute("ROLLBACK")
                    except: pass
                    return
                with lock:
                    result["execution_log"].append({"event_id": eid, "sql": update_sql})

            # COMMIT
            eid = f"d{doc_id}_commit"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute("COMMIT")
            except Exception as e:
                error_str = str(e)
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {error_str}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
            with lock:
                result["execution_log"].append({"event_id": eid, "sql": "COMMIT"})
        finally:
            try: conn.close()
            except: pass

    threads = [
        threading.Thread(target=worker, args=(1, 2), daemon=True),
        threading.Thread(target=worker, args=(2, 1), daemon=True),
    ]
    for t in threads: t.start()
    for t in threads: t.join(timeout=30.0)

    # Read final state
    conn_check = psycopg2.connect(dsn)
    conn_check.autocommit = True
    cur_check = conn_check.cursor()
    cur_check.execute("SELECT doctor_id, on_call FROM oncall ORDER BY doctor_id")
    result["final_state"] = [list(r) for r in cur_check.fetchall()]
    conn_check.close()

    # Evaluate invariant
    on_call_count = sum(1 for row in result["final_state"] if row[1] is True)
    # Check for deadlock (40P01) in errors — not a bug, just a transient abort
    got_deadlock = any("40P01" in str(e) or "deadlock detected" in str(e).lower()
                       for e in result.get("errors", []))
    result["got_40P01"] = got_deadlock
    if on_call_count == 0 and not result["got_40001"] and not got_deadlock:
        result["bug_found"] = True

    return result


def build_delay_candidates(trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build tiered delay candidates for write-skew workload."""
    candidates = []
    seen = set()

    def _add(eid, sess, tier, weight, delay_range, justification):
        key = (eid, tier)
        if key in seen:
            return
        seen.add(key)
        candidates.append({"event_id": eid, "session": sess, "tier": tier,
                           "weight": weight, "delay_range": delay_range,
                           "justification": justification})

    # Key insight: both doctors read, both update. The write-skew occurs
    # when both reads complete before either update.
    # Tier 1: Delay UPDATEs so both SELECTs see on_call=true
    _add("d1_update", "doctor_1", "tier_rw_conflict", 10, (0.03, 0.15),
         "Delay d1 update so d2 reads on_call=true first")
    _add("d2_update", "doctor_2", "tier_rw_conflict", 10, (0.03, 0.15),
         "Delay d2 update so d1 reads on_call=true first")

    # Tier 2: Delay COMMITs to widen write-skew window
    _add("d1_commit", "doctor_1", "tier_commit_hold", 9, (0.04, 0.20),
         "Hold d1 commit to keep update in-flight")
    _add("d2_commit", "doctor_2", "tier_commit_hold", 9, (0.04, 0.20),
         "Hold d2 commit to keep update in-flight")

    # Tier 3: Delay SELECTs (less useful but still worth trying)
    _add("d1_select", "doctor_1", "tier_wr_conflict", 5, (0.02, 0.10),
         "Delay d1 read")
    _add("d2_select", "doctor_2", "tier_wr_conflict", 5, (0.02, 0.10),
         "Delay d2 read")

    # Fallback
    for ev_id in ["d1_begin", "d2_begin"]:
        _add(ev_id, ev_id.split("_")[0] + "_1" if "1" in ev_id else "doctor_2",
             "tier_fallback", 1, (0.01, 0.05), f"Generic delay on {ev_id}")

    return candidates


def select_delays(candidates: List[Dict[str, Any]], rng: random.Random, k: int = 2) -> Dict[str, float]:
    if not candidates or k <= 0:
        return {}
    pool = []
    for c in candidates:
        pool.extend([c] * c["weight"])
    selected = {}
    used = set()
    for _ in range(k * 20):
        if len(selected) >= k:
            break
        c = rng.choice(pool)
        if c["event_id"] in used:
            continue
        used.add(c["event_id"])
        selected[c["event_id"]] = round(rng.uniform(*c["delay_range"]), 3)
    return selected


def select_random_delays(trace: List[Dict[str, Any]], rng: random.Random, k: int = 2) -> Dict[str, float]:
    eids = [ev.get("event_id", f"step_{i}") for i, ev in enumerate(trace)
            if ev.get("type") in ("sql", "begin", "commit", "rollback")]
    if not eids:
        return {}
    selected = {}
    for _ in range(min(k, len(eids))):
        eid = rng.choice(eids)
        if eid not in selected:
            selected[eid] = round(rng.uniform(0.01, 0.20), 3)
    return selected


def oracle(
    execution_history: List[Dict[str, Any]],
    final_state: Any,
    trial_result: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> OracleResult:
    if trial_result is None:
        return OracleResult(ok=True, details="No trial result")

    if trial_result.get("got_40001"):
        return OracleResult(ok=True, details="40001 raised; SSI prevented write skew.",
                            metrics={"got_40001": True, "got_40P01": False})
    if trial_result.get("got_40P01"):
        return OracleResult(ok=True, details="40P01 deadlock detected; txn aborted (retriable, not a bug).",
                            metrics={"got_40001": False, "got_40P01": True})
    if trial_result.get("bug_found"):
        return OracleResult(
            ok=False, bug_type="write_skew",
            details=f"Write skew: both doctors off-call. final_state={trial_result.get('final_state')}",
            metrics={"got_40001": False, "got_40P01": False, "on_call_count": 0},
        )
    on_call = sum(1 for row in (trial_result.get("final_state") or []) if row[1] is True)
    return OracleResult(ok=True, details=f"No write skew; {on_call} doctor(s) on-call.",
                        metrics={"on_call_count": on_call})
