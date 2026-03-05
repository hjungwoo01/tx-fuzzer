"""
Workload: bank_overdraft — realistic "check-then-withdraw" race condition.

Two sessions concurrently check if an account has sufficient funds, then
withdraw. Under READ COMMITTED, both can read the same balance, both decide
to proceed, and both withdraw — overdrawing the account.

Schema:
    accounts(id INT PRIMARY KEY, balance INT NOT NULL)
    withdrawals(id SERIAL PRIMARY KEY, session_id TEXT, amount INT)

Initial state:
    accounts: (1, balance=100)
    withdrawals: empty

Buggy transaction (READ COMMITTED), per session:
    BEGIN;
    SELECT balance FROM accounts WHERE id=1;
    if balance >= 60:
        UPDATE accounts SET balance = balance - 60 WHERE id=1;
        INSERT INTO withdrawals(session_id, amount) VALUES (..., 60);
        COMMIT;
    else:
        ROLLBACK;

Bug oracle:
    final balance must be >= 0 AND total withdrawn must not exceed 100.
    If final balance < 0 OR more than one withdrawal succeeded => BUG.

Fixed variant:
    BEGIN;
    UPDATE accounts SET balance = balance - 60
      WHERE id=1 AND balance >= 60
      RETURNING balance;
    if row returned:
        INSERT INTO withdrawals(session_id, amount) VALUES (..., 60);
        COMMIT;
    else:
        ROLLBACK;

Design for low baseline bug rate:
    NO barrier: sessions start with a small random stagger (0-20ms).
    Under natural execution the first session's UPDATE commits before
    the second session's SELECT, so the second session sees the reduced
    balance and correctly decides not to withdraw.
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
    "name": "bank_overdraft",
    "description": "Check-then-withdraw race: TOCTOU overdraft under READ COMMITTED",
    "invariants": "balance >= 0 and total withdrawn <= initial balance",
    "isolation_levels": ["READ COMMITTED"],
    "target_versions": {"primary": "16.2"},
    "clients": 2,
    "variants": ["buggy", "fixed"],
    "negative_controls": [
        "Fixed variant uses atomic UPDATE WHERE balance >= amount",
    ],
}

INITIAL_BALANCE = 100
WITHDRAW_AMOUNT = 60
NUM_SESSIONS = 2


# ── Schema ──────────────────────────────────────────────────────────────────

def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS withdrawals CASCADE",
        "DROP TABLE IF EXISTS accounts CASCADE",
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
        "CREATE TABLE withdrawals (id SERIAL PRIMARY KEY, session_id TEXT, amount INT)",
    ]


def _init_sql() -> List[str]:
    return [
        f"INSERT INTO accounts VALUES (1, {INITIAL_BALANCE})",
    ]


def truncate_sql() -> List[str]:
    return [
        "DELETE FROM withdrawals",
        "DELETE FROM accounts",
    ]


# ── Seed trace ──────────────────────────────────────────────────────────────

def seed_trace(
    clients: int = NUM_SESSIONS,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """Build a seed trace: each session does BEGIN → SELECT → UPDATE → INSERT → COMMIT."""
    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"
    trace = []
    for sid in range(clients):
        sess = f"session_{sid}"
        trace.append({"event_id": f"s{sid}_begin", "client_id": sid, "session": sess,
                       "type": "begin", "sql": iso_sql})
        trace.append({"event_id": f"s{sid}_check", "client_id": sid, "session": sess,
                       "type": "sql",
                       "sql": "SELECT balance FROM accounts WHERE id=1"})
        if variant == "fixed":
            trace.append({"event_id": f"s{sid}_withdraw", "client_id": sid, "session": sess,
                           "type": "sql",
                           "sql": f"UPDATE accounts SET balance = balance - {WITHDRAW_AMOUNT} "
                                  f"WHERE id=1 AND balance >= {WITHDRAW_AMOUNT} RETURNING balance"})
        else:
            trace.append({"event_id": f"s{sid}_withdraw", "client_id": sid, "session": sess,
                           "type": "sql", "sql": "__CONDITIONAL_WITHDRAW__"})
        trace.append({"event_id": f"s{sid}_record", "client_id": sid, "session": sess,
                       "type": "sql",
                       "sql": f"INSERT INTO withdrawals(session_id, amount) VALUES ('session_{sid}', {WITHDRAW_AMOUNT})"})
        trace.append({"event_id": f"s{sid}_commit", "client_id": sid, "session": sess,
                       "type": "commit", "sql": "COMMIT"})
    return trace


# ── Canonical state ─────────────────────────────────────────────────────────

def canonical_state(trial_result: Dict[str, Any]) -> Dict[str, Any]:
    """Return timing-free logical snapshot for hashing."""
    fs = trial_result.get("final_state", {})
    return {
        "bug_found": trial_result.get("bug_found", False),
        "final_balance": fs.get("final_balance"),
        "withdrawal_count": fs.get("withdrawal_count", 0),
    }


# ── Concurrent trial runner ─────────────────────────────────────────────────

def run_trial(
    dsn: str,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    delays: Optional[Dict[str, float]] = None,
    seed: int = 42,
    clients: int = NUM_SESSIONS,
) -> Dict[str, Any]:
    """Run one concurrent trial with real threads.

    NO barrier: sessions start with a tiny random stagger (0-20ms)
    determined by seed, simulating realistic arrival jitter.
    """
    delays = delays or {}
    rng = random.Random(seed)
    result: Dict[str, Any] = {
        "bug_found": False, "got_40001": False,
        "execution_log": [], "errors": [], "final_state": {},
    }
    lock = threading.Lock()

    # Setup DB
    conn_s = psycopg2.connect(dsn)
    conn_s.autocommit = True
    cur_s = conn_s.cursor()
    for sql in setup_sql():
        cur_s.execute(sql)
    for sql in _init_sql():
        cur_s.execute(sql)
    conn_s.close()

    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"

    # Per-session stagger (0-20ms) — simulates real-world arrival jitter
    staggers = [rng.uniform(0.0, 0.020) for _ in range(clients)]

    def worker_buggy(sid: int):
        time.sleep(staggers[sid])

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            # BEGIN
            eid = f"s{sid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            cur.execute(iso_sql)

            # CHECK balance
            eid = f"s{sid}_check"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute("SELECT balance FROM accounts WHERE id=1")
                rows = cur.fetchall()
                balance = rows[0][0] if rows else 0
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return
            with lock:
                result["execution_log"].append({
                    "event_id": eid, "session": sid, "balance_read": balance,
                })

            # Check funds
            if balance < WITHDRAW_AMOUNT:
                # Not enough funds — rollback
                with lock:
                    result["execution_log"].append({
                        "event_id": f"s{sid}_insufficient", "session": sid,
                        "balance": balance,
                    })
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            # WITHDRAW (UPDATE)
            eid = f"s{sid}_withdraw"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute(
                    f"UPDATE accounts SET balance = balance - {WITHDRAW_AMOUNT} WHERE id=1"
                )
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            # RECORD withdrawal
            eid = f"s{sid}_record"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute(
                    "INSERT INTO withdrawals(session_id, amount) VALUES (%s, %s)",
                    (f"session_{sid}", WITHDRAW_AMOUNT),
                )
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            # COMMIT
            eid = f"s{sid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute("COMMIT")
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return
            with lock:
                result["execution_log"].append({
                    "event_id": eid, "session": sid, "committed": True,
                })
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def worker_fixed(sid: int):
        time.sleep(staggers[sid])

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            # BEGIN
            eid = f"s{sid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            cur.execute(iso_sql)

            # Atomic check-and-withdraw
            eid = f"s{sid}_withdraw"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute(
                    f"UPDATE accounts SET balance = balance - {WITHDRAW_AMOUNT} "
                    f"WHERE id=1 AND balance >= {WITHDRAW_AMOUNT} RETURNING balance"
                )
                rows = cur.fetchall()
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            if not rows:
                # No row returned → insufficient funds, rollback
                with lock:
                    result["execution_log"].append({
                        "event_id": f"s{sid}_insufficient_fixed", "session": sid,
                    })
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            with lock:
                result["execution_log"].append({
                    "event_id": eid, "session": sid, "new_balance": rows[0][0],
                })

            # RECORD
            eid = f"s{sid}_record"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute(
                    "INSERT INTO withdrawals(session_id, amount) VALUES (%s, %s)",
                    (f"session_{sid}", WITHDRAW_AMOUNT),
                )
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            # COMMIT
            eid = f"s{sid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute("COMMIT")
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return
            with lock:
                result["execution_log"].append({
                    "event_id": eid, "session": sid, "committed": True,
                })
        finally:
            try:
                conn.close()
            except Exception:
                pass

    worker_fn = worker_fixed if variant == "fixed" else worker_buggy
    threads = [threading.Thread(target=worker_fn, args=(sid,), daemon=True)
               for sid in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)

    # Check final state
    conn_c = psycopg2.connect(dsn)
    conn_c.autocommit = True
    cur_c = conn_c.cursor()
    cur_c.execute("SELECT balance FROM accounts WHERE id=1")
    final_balance = cur_c.fetchone()[0]
    cur_c.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM withdrawals")
    w_count, w_total = cur_c.fetchone()
    conn_c.close()

    result["final_state"] = {
        "final_balance": final_balance,
        "withdrawal_count": w_count,
        "total_withdrawn": w_total,
        "initial_balance": INITIAL_BALANCE,
    }

    # Bug detection: overdraft or more withdrawn than available
    if (final_balance < 0 or w_total > INITIAL_BALANCE) and not result["got_40001"]:
        result["bug_found"] = True

    return result


# ── Delay candidates ────────────────────────────────────────────────────────

def build_delay_candidates(
    trace: List[Dict[str, Any]],
    clients: int = NUM_SESSIONS,
) -> List[Dict[str, Any]]:
    """Build tiered delay candidates for the bank_overdraft workload.

    Key heuristic insight: delaying one session's UPDATE or COMMIT keeps
    its withdrawal invisible, so the second session's balance CHECK still
    sees the full amount and decides to withdraw too.
    """
    candidates = []
    seen = set()

    def _add(eid, sess, tier, weight, delay_range, justification):
        k = (eid, tier)
        if k in seen:
            return
        seen.add(k)
        candidates.append({
            "event_id": eid, "session": sess, "tier": tier,
            "weight": weight, "delay_range": delay_range,
            "justification": justification,
        })

    for sid in range(clients):
        sess = f"session_{sid}"
        # Delay WITHDRAW (UPDATE) so other session's CHECK sees full balance
        _add(f"s{sid}_withdraw", sess, "tier_rw_conflict", 10, (0.03, 0.15),
             f"Delay s{sid} withdraw so other session checks stale balance")
        # Hold COMMIT to keep withdrawal invisible
        _add(f"s{sid}_commit", sess, "tier_commit_hold", 9, (0.04, 0.20),
             f"Hold s{sid} commit so withdrawal is invisible to others")
        # Delay CHECK to let another session's CHECK run simultaneously
        _add(f"s{sid}_check", sess, "tier_wr_conflict", 7, (0.02, 0.12),
             f"Delay s{sid} check to overlap with other session")
        # Delay RECORD for extra overlap
        _add(f"s{sid}_record", sess, "tier_ww_conflict", 5, (0.02, 0.10),
             f"Delay s{sid} record insert")
        # Fallback
        _add(f"s{sid}_begin", sess, "tier_fallback", 1, (0.01, 0.05),
             f"Generic delay on s{sid} begin")

    return candidates


def select_delays(
    candidates: List[Dict[str, Any]],
    rng: random.Random,
    k: int = 3,
) -> Dict[str, float]:
    """Weighted lottery selection of up to K delay points."""
    if not candidates or k <= 0:
        return {}
    pool = []
    for c in candidates:
        pool.extend([c] * c["weight"])
    selected: Dict[str, float] = {}
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


def select_random_delays(
    trace: List[Dict[str, Any]],
    rng: random.Random,
    k: int = 3,
) -> Dict[str, float]:
    """Random baseline: pick K random events, random delay 10-200ms."""
    eids = [ev.get("event_id", f"step_{i}") for i, ev in enumerate(trace)
            if ev.get("type") in ("sql", "begin", "commit", "rollback")]
    if not eids:
        return {}
    selected: Dict[str, float] = {}
    for _ in range(min(k, len(eids))):
        eid = rng.choice(eids)
        if eid not in selected:
            selected[eid] = round(rng.uniform(0.01, 0.20), 3)
    return selected


# ── Oracle ──────────────────────────────────────────────────────────────────

def oracle(
    execution_history: List[Dict[str, Any]],
    final_state: Any,
    trial_result: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> OracleResult:
    """Check for the overdraft invariant violation."""
    if trial_result is None:
        return OracleResult(ok=True, details="No trial result provided.")

    if trial_result.get("got_40001"):
        return OracleResult(ok=True, details="40001 raised; serialization prevented race.",
                            metrics={"got_40001": True})

    if trial_result.get("bug_found"):
        fs = trial_result.get("final_state", {})
        return OracleResult(
            ok=False,
            bug_type="bank_overdraft",
            details=(
                f"Overdraft detected: final_balance={fs.get('final_balance')}, "
                f"withdrawal_count={fs.get('withdrawal_count')}, "
                f"total_withdrawn={fs.get('total_withdrawn')}, "
                f"initial_balance={fs.get('initial_balance')}"
            ),
            metrics={
                "final_balance": fs.get("final_balance"),
                "total_withdrawn": fs.get("total_withdrawn"),
            },
        )

    return OracleResult(ok=True, details="No overdraft; balance constraint holds.",
                        metrics=trial_result.get("final_state", {}))
