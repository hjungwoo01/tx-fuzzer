"""
Workload: bank_lost_update — realistic read-modify-write lost-update bug.

Two concurrent transfers read the same source balance, compute a new value
client-side, then UPDATE SET balance = <computed>. Under READ COMMITTED,
one update can overwrite the other → balance sum changes.

Schema: accounts(id int PK, balance int)
Invariant: SUM(balance) remains constant; no balance goes negative.
Fixed variant: uses atomic UPDATE balance = balance - X (server-side math).
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
    "name": "bank_lost_update",
    "description": "Lost update via client-side read-modify-write on bank accounts",
    "invariants": "SUM(balance) must remain constant (conservation of money).",
    "isolation_levels": ["READ COMMITTED", "SERIALIZABLE"],
    "target_versions": {"primary": "16.2"},
    "clients": 2,
    "variants": ["buggy", "fixed"],
    "negative_controls": [
        "Fixed variant uses atomic UPDATE SET balance = balance - X",
        "Under SERIALIZABLE, 40001 should prevent lost update",
    ],
}

INITIAL_BALANCE_A = 1000
INITIAL_BALANCE_B = 1000
TRANSFER_AMOUNT = 100


def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS accounts CASCADE",
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
    ]


def truncate_sql() -> List[str]:
    return ["DELETE FROM accounts"]


def _init_sql() -> List[str]:
    return [
        f"INSERT INTO accounts VALUES (1, {INITIAL_BALANCE_A})",
        f"INSERT INTO accounts VALUES (2, {INITIAL_BALANCE_B})",
    ]


def seed_trace(
    clients: int = 2,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    Two clients each: read account 1 balance → compute new → update account 1 → commit.
    Buggy: UPDATE accounts SET balance = <literal_computed_value>
    Fixed: UPDATE accounts SET balance = balance - 100
    """
    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"
    trace = []

    for cid in range(2):
        sess = f"transfer_{cid}"
        trace.append({"event_id": f"t{cid}_begin", "client_id": cid, "session": sess,
                       "type": "begin", "sql": iso_sql})
        trace.append({"event_id": f"t{cid}_read", "client_id": cid, "session": sess,
                       "type": "sql", "sql": "SELECT balance FROM accounts WHERE id = 1"})
        if variant == "fixed":
            trace.append({"event_id": f"t{cid}_update", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": f"UPDATE accounts SET balance = balance - {TRANSFER_AMOUNT} WHERE id = 1"})
        else:
            # Buggy: will be computed at runtime from SELECT result
            # We use a placeholder that the trial runner will fill in
            trace.append({"event_id": f"t{cid}_update", "client_id": cid, "session": sess,
                           "type": "sql", "sql": "__COMPUTED_UPDATE__"})
        trace.append({"event_id": f"t{cid}_credit", "client_id": cid, "session": sess,
                       "type": "sql",
                       "sql": f"UPDATE accounts SET balance = balance + {TRANSFER_AMOUNT} WHERE id = 2"})
        trace.append({"event_id": f"t{cid}_commit", "client_id": cid, "session": sess,
                       "type": "commit", "sql": "COMMIT"})
    return trace


def run_trial(
    dsn: str,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    delays: Optional[Dict[str, float]] = None,
    seed: int = 42,
) -> Dict[str, Any]:
    delays = delays or {}
    expected_sum = INITIAL_BALANCE_A + INITIAL_BALANCE_B
    result = {
        "bug_found": False, "got_40001": False,
        "execution_log": [], "errors": [], "final_state": [],
        "initial_sum": expected_sum,
    }
    lock = threading.Lock()

    conn_s = psycopg2.connect(dsn)
    conn_s.autocommit = True
    cur_s = conn_s.cursor()
    for sql in setup_sql():
        cur_s.execute(sql)
    for sql in _init_sql():
        cur_s.execute(sql)
    conn_s.close()

    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"
    barrier = threading.Barrier(2)

    def worker(cid: int):
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            barrier.wait(timeout=30.0)
            sess = f"transfer_{cid}"

            # BEGIN
            eid = f"t{cid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            cur.execute(iso_sql)
            with lock:
                result["execution_log"].append({"event_id": eid, "sql": iso_sql})

            # READ
            eid = f"t{cid}_read"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute("SELECT balance FROM accounts WHERE id = 1")
                rows = cur.fetchall()
                balance = rows[0][0] if rows else 0
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
                result["execution_log"].append({"event_id": eid, "balance_read": balance})

            # UPDATE (debit)
            eid = f"t{cid}_update"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            if variant == "fixed":
                update_sql = f"UPDATE accounts SET balance = balance - {TRANSFER_AMOUNT} WHERE id = 1"
            else:
                new_balance = balance - TRANSFER_AMOUNT
                update_sql = f"UPDATE accounts SET balance = {new_balance} WHERE id = 1"
            try:
                cur.execute(update_sql)
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return
            with lock:
                result["execution_log"].append({"event_id": eid, "sql": update_sql})

            # CREDIT account 2
            eid = f"t{cid}_credit"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            credit_sql = f"UPDATE accounts SET balance = balance + {TRANSFER_AMOUNT} WHERE id = 2"
            try:
                cur.execute(credit_sql)
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return
            with lock:
                result["execution_log"].append({"event_id": eid, "sql": credit_sql})

            # COMMIT
            eid = f"t{cid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute("COMMIT")
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return
            with lock:
                result["execution_log"].append({"event_id": eid, "sql": "COMMIT"})
        finally:
            try: conn.close()
            except: pass

    threads = [threading.Thread(target=worker, args=(cid,), daemon=True) for cid in range(2)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=30.0)

    # Check final state
    conn_c = psycopg2.connect(dsn)
    conn_c.autocommit = True
    cur_c = conn_c.cursor()
    cur_c.execute("SELECT id, balance FROM accounts ORDER BY id")
    result["final_state"] = [list(r) for r in cur_c.fetchall()]
    conn_c.close()

    actual_sum = sum(row[1] for row in result["final_state"])
    result["final_sum"] = actual_sum
    if actual_sum != expected_sum and not result["got_40001"]:
        result["bug_found"] = True
        result["sum_delta"] = actual_sum - expected_sum

    return result


def build_delay_candidates(trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = []
    seen = set()

    def _add(eid, sess, tier, weight, delay_range, justification):
        k = (eid, tier)
        if k in seen: return
        seen.add(k)
        candidates.append({"event_id": eid, "session": sess, "tier": tier,
                           "weight": weight, "delay_range": delay_range,
                           "justification": justification})

    # Key: delay updates so both reads complete first → both compute from stale value
    _add("t0_update", "transfer_0", "tier_rw_conflict", 10, (0.03, 0.15),
         "Delay t0 update so t1 reads stale balance first")
    _add("t1_update", "transfer_1", "tier_rw_conflict", 10, (0.03, 0.15),
         "Delay t1 update so t0 reads stale balance first")

    # Delay commits to widen overlap window
    _add("t0_commit", "transfer_0", "tier_commit_hold", 9, (0.04, 0.20),
         "Hold t0 commit to keep update invisible longer")
    _add("t1_commit", "transfer_1", "tier_commit_hold", 9, (0.04, 0.20),
         "Hold t1 commit to keep update invisible longer")

    # Delay reads for variability
    _add("t0_read", "transfer_0", "tier_wr_conflict", 5, (0.02, 0.10),
         "Delay t0 read")
    _add("t1_read", "transfer_1", "tier_wr_conflict", 5, (0.02, 0.10),
         "Delay t1 read")

    # Fallback
    for eid in ["t0_begin", "t1_begin"]:
        _add(eid, eid[:2], "tier_fallback", 1, (0.01, 0.05), f"Generic delay on {eid}")

    return candidates


def select_delays(candidates: List[Dict[str, Any]], rng: random.Random, k: int = 2) -> Dict[str, float]:
    if not candidates or k <= 0: return {}
    pool = []
    for c in candidates: pool.extend([c] * c["weight"])
    selected = {}
    used = set()
    for _ in range(k * 20):
        if len(selected) >= k: break
        c = rng.choice(pool)
        if c["event_id"] in used: continue
        used.add(c["event_id"])
        selected[c["event_id"]] = round(rng.uniform(*c["delay_range"]), 3)
    return selected


def select_random_delays(trace: List[Dict[str, Any]], rng: random.Random, k: int = 2) -> Dict[str, float]:
    eids = [ev.get("event_id", f"step_{i}") for i, ev in enumerate(trace)
            if ev.get("type") in ("sql", "begin", "commit", "rollback")]
    if not eids: return {}
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
        return OracleResult(ok=True, details="40001 raised; serialization prevented lost update.",
                            metrics={"got_40001": True})
    if trial_result.get("bug_found"):
        return OracleResult(
            ok=False, bug_type="lost_update",
            details=(f"Lost update: expected sum={trial_result.get('initial_sum')}, "
                     f"actual sum={trial_result.get('final_sum')}, "
                     f"delta={trial_result.get('sum_delta')}. "
                     f"final_state={trial_result.get('final_state')}"),
            metrics={"sum_delta": trial_result.get("sum_delta", 0)},
        )
    return OracleResult(ok=True, details="No lost update; sum preserved.",
                        metrics={"final_sum": trial_result.get("final_sum")})
