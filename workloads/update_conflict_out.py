"""
Workload: update_conflict_out — PostgreSQL SSI "update-conflict-out" anomaly.

Targets a real PostgreSQL 12.3 bug where SSI fails to detect an
rw-antidependency cycle across three SERIALIZABLE sessions.

Schema: txn0(id int PK, val text), txn1(id int PK, val text)
Sessions: foo, bar, trouble (3 clients).
Cycle: foo reads txn0 (empty) → bar writes txn0 (rw foo→bar)
       bar reads txn1 (empty) → foo writes txn1 (rw bar→foo)
       trouble writes txn1 then aborts (creates physical version
       that confuses SSI conflict-out check in 12.3).
Oracle:  bug = both selects empty + both committed + no 40001
Fixed:   12.4+ raises 40001 when cycle detected.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    import psycopg2.extensions
except ImportError:
    psycopg2 = None  # type: ignore

from workloads import OracleResult

WORKLOAD_META = {
    "name": "update_conflict_out",
    "description": "PostgreSQL 12.3 SSI update-conflict-out serializability violation",
    "invariants": (
        "Under SERIALIZABLE isolation, mutual invisibility between foo and bar "
        "(both selects return 0 rows) while both commit constitutes a "
        "non-serializable history (rw-antidependency cycle). "
        "A correct SSI implementation must raise error 40001."
    ),
    "isolation_levels": ["SERIALIZABLE"],
    "target_versions": {"vulnerable": "12.3", "fixed": "12.4"},
    "clients": 3,
    "variants": ["update", "delete"],
    "negative_controls": ["Run on 12.4 — should always get 40001 or non-cyclic ordering"],
}


def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS txn0 CASCADE",
        "DROP TABLE IF EXISTS txn1 CASCADE",
        "CREATE TABLE txn0 (id int4 PRIMARY KEY, val TEXT)",
        "CREATE TABLE txn1 (id int4 PRIMARY KEY, val TEXT)",
    ]


def truncate_sql() -> List[str]:
    return ["DELETE FROM txn1", "DELETE FROM txn0"]


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


def seed_trace(
    clients: int = 3,
    isolation: str = "SERIALIZABLE",
    variant: str = "update",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    Build a natural seed trace — per-session FIFO order, flat interleaving.
    The concurrent executor will run these with real thread nondeterminism.
    """
    trouble_session = SESSION_TROUBLE_UPDATE if variant == "update" else SESSION_TROUBLE_DELETE
    sessions = {"foo": SESSION_FOO, "bar": SESSION_BAR, "trouble": trouble_session}
    client_map = {"foo": 0, "bar": 1, "trouble": 2}

    trace: List[Dict[str, Any]] = []
    for sess_name, steps in sessions.items():
        cid = client_map[sess_name]
        for eid, sql in steps:
            upper = sql.strip().upper()
            if upper.startswith("BEGIN"):
                evt_type = "begin"
            elif upper.startswith("COMMIT"):
                evt_type = "commit"
            elif upper in ("ABORT", "ROLLBACK"):
                evt_type = "rollback"
            else:
                evt_type = "sql"
            trace.append({
                "event_id": eid,
                "client_id": cid,
                "session": sess_name,
                "type": evt_type,
                "sql": sql,
            })
    return trace


@dataclass
class TrialResult:
    variant: str = "update"
    bug_found: bool = False
    got_40001: bool = False
    foo_select_rows: Optional[List] = None
    bar_select_rows: Optional[List] = None
    foo_commit_error: Optional[str] = None
    bar_commit_error: Optional[str] = None
    trouble_write_rows_affected: Optional[int] = None
    all_errors: List[str] = field(default_factory=list)
    timestamps: Dict[str, float] = field(default_factory=dict)
    execution_log: List[Dict[str, Any]] = field(default_factory=list)


def _run_session(
    dsn: str,
    session_steps: List[Tuple[str, str]],
    session_name: str,
    barrier: threading.Barrier,
    result: TrialResult,
    delays: Dict[str, float],
    result_lock: threading.Lock,
) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        barrier.wait(timeout=30.0)
        for eid, sql in session_steps:
            delay = delays.get(eid, 0.0)
            if delay > 0:
                time.sleep(delay)
            t0 = time.monotonic()
            upper = sql.strip().upper()
            error_str = None
            rows = None
            rows_affected = None
            sqlstate = None
            try:
                cur.execute(sql)
                if upper.startswith("SELECT"):
                    rows = cur.fetchall()
                elif not upper.startswith(("BEGIN", "COMMIT", "ABORT", "ROLLBACK")):
                    rows_affected = cur.rowcount
            except Exception as e:
                error_str = str(e)
                sqlstate = getattr(e, "pgcode", None)
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
            elapsed = time.monotonic() - t0
            with result_lock:
                result.timestamps[eid] = t0
                log_entry = {"event_id": eid, "session": session_name, "sql": sql,
                             "duration_ms": round(elapsed * 1000, 3)}
                if rows is not None:
                    log_entry["rows"] = rows
                if rows_affected is not None:
                    log_entry["rows_affected"] = rows_affected
                if error_str:
                    log_entry["error"] = error_str
                    log_entry["sqlstate"] = sqlstate
                    result.all_errors.append(f"{eid}: {error_str}")
                result.execution_log.append(log_entry)
                if eid == "foo_select":
                    result.foo_select_rows = rows
                elif eid == "bar_select":
                    result.bar_select_rows = rows
                elif eid == "bar_commit":
                    result.bar_commit_error = error_str
                elif eid == "foo_commit":
                    result.foo_commit_error = error_str
                elif eid in ("trouble_update", "trouble_delete"):
                    result.trouble_write_rows_affected = rows_affected
                if sqlstate == "40001":
                    result.got_40001 = True
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_trial(dsn: str, variant: str = "update", delays: Optional[Dict[str, float]] = None,
              isolation: str = "SERIALIZABLE", seed: int = 42, **kwargs) -> TrialResult:
    """Run one concurrent trial, returns TrialResult."""
    delays = delays or {}
    result = TrialResult(variant=variant)
    result_lock = threading.Lock()

    # Reset tables before each trial — critical so selects can see "empty"
    setup_conn = psycopg2.connect(dsn)
    setup_conn.autocommit = True
    try:
        scur = setup_conn.cursor()
        for sql in setup_sql():
            scur.execute(sql)
        scur.close()
    finally:
        setup_conn.close()

    trouble = SESSION_TROUBLE_UPDATE if variant == "update" else SESSION_TROUBLE_DELETE
    sessions = [(SESSION_FOO, "foo"), (SESSION_BAR, "bar"), (trouble, "trouble")]
    barrier = threading.Barrier(len(sessions))
    threads = []
    for steps, name in sessions:
        t = threading.Thread(target=_run_session,
                             args=(dsn, steps, name, barrier, result, delays, result_lock),
                             daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=30.0)
    # Evaluate bug predicate
    foo_saw_empty = result.foo_select_rows is not None and len(result.foo_select_rows) == 0
    bar_saw_empty = result.bar_select_rows is not None and len(result.bar_select_rows) == 0
    foo_committed = result.foo_commit_error is None
    bar_committed = result.bar_commit_error is None
    if not result.got_40001 and foo_saw_empty and bar_saw_empty and foo_committed and bar_committed:
        result.bug_found = True
    return result


def canonical_state(trial_result) -> Dict[str, Any]:
    """Return a timing-free logical snapshot suitable for hashing.

    The canonical state captures only the logical outcome:
    - Whether each SELECT saw rows
    - Whether each session committed or errored
    - Whether 40001 was raised
    This avoids inflated unique-state counts from timestamp noise.
    """
    r = trial_result
    return {
        "bug_found": r.bug_found,
        "got_40001": r.got_40001,
        "foo_select_empty": r.foo_select_rows is not None and len(r.foo_select_rows) == 0,
        "bar_select_empty": r.bar_select_rows is not None and len(r.bar_select_rows) == 0,
        "foo_committed": r.foo_commit_error is None,
        "bar_committed": r.bar_commit_error is None,
        "variant": r.variant,
    }


def _extract_table_key(sql: str):
    import re
    upper = sql.upper().strip()
    for pat, grp in [
        (r'FROM\s+(\w+)\s+WHERE\s+ID\s*=\s*(\d+)', None),
        (r'INSERT\s+INTO\s+(\w+)\s+SELECT\s+(\d+)', None),
        (r'UPDATE\s+(\w+)\s+SET\s+.+WHERE\s+ID\s*=\s*(\d+)', None),
        (r'DELETE\s+FROM\s+(\w+)\s+WHERE\s+ID\s*=\s*(\d+)', None),
    ]:
        m = re.search(pat, upper)
        if m:
            return (m.group(1).lower(), int(m.group(2)))
    return None


def _classify_sql(sql: str) -> str:
    upper = sql.strip().upper()
    if upper.startswith("SELECT"):
        return "read"
    if upper.startswith(("INSERT", "UPDATE", "DELETE")):
        return "write"
    if upper.startswith("BEGIN"):
        return "begin"
    if upper.startswith("COMMIT"):
        return "commit"
    if upper in ("ABORT", "ROLLBACK"):
        return "rollback"
    return "other"


def build_delay_candidates(trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build tiered delay candidates from trace analysis."""
    # Build per-event info
    events = []
    for ev in trace:
        eid = ev.get("event_id", "")
        session = ev.get("session", "")
        sql = ev.get("sql", "")
        tk = _extract_table_key(sql)
        cls = _classify_sql(sql)
        events.append({"eid": eid, "session": session, "sql": sql, "table_key": tk, "cls": cls})

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

    # Find cross-session rw/wr/ww deps
    for i, ei in enumerate(events):
        if ei["table_key"] is None:
            continue
        for j, ej in enumerate(events):
            if i == j or ej["table_key"] is None or ei["session"] == ej["session"]:
                continue
            if ei["table_key"] != ej["table_key"]:
                continue
            table, key = ei["table_key"]
            if ei["cls"] == "read" and ej["cls"] == "write":
                _add(ei["eid"], ei["session"], "tier_rw_conflict", 10, (0.03, 0.15),
                     f"Delay read {ei['eid']} for write {ej['eid']} on {table}[{key}]")
                _add(ej["eid"], ej["session"], "tier_rw_conflict_writer", 6, (0.02, 0.10),
                     f"Delay write {ej['eid']} to let read {ei['eid']} on {table}[{key}]")
            elif ei["cls"] == "write" and ej["cls"] == "read":
                _add(ej["eid"], ej["session"], "tier_wr_conflict", 7, (0.03, 0.12),
                     f"Delay read {ej['eid']} after write {ei['eid']} on {table}[{key}]")
            elif ei["cls"] == "write" and ej["cls"] == "write":
                _add(ej["eid"], ej["session"], "tier_ww_conflict", 5, (0.02, 0.08),
                     f"Delay write {ej['eid']} vs {ei['eid']} on {table}[{key}]")

    # Tier 3: commit/abort hold
    session_events: Dict[str, List] = {}
    for ev in events:
        session_events.setdefault(ev["session"], []).append(ev)
    for sess, sevs in session_events.items():
        for idx, sev in enumerate(sevs):
            if sev["cls"] in ("commit", "rollback"):
                has_write = any(s["cls"] == "write" for s in sevs[:idx])
                if has_write:
                    _add(sev["eid"], sess, "tier_commit_hold", 9, (0.04, 0.20),
                         f"Hold {sev['cls']} of {sess} to keep modified tuples in-flight")

    # Tier 5: fallback
    for ev in events:
        if ev["cls"] in ("read", "write"):
            _add(ev["eid"], ev["session"], "tier_fallback", 1, (0.01, 0.05),
                 f"Generic delay on {ev['cls']} event {ev['eid']}")

    return candidates


def select_delays(candidates: List[Dict[str, Any]], rng: random.Random, k: int = 2) -> Dict[str, float]:
    """Sample up to K delay points from candidates, weighted by tier priority."""
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


def select_random_delays(trace: List[Dict[str, Any]], rng: random.Random, k: int = 2) -> Dict[str, float]:
    """Pick K random events with uniform delays in [10, 200] ms."""
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


def oracle(
    execution_history: List[Dict[str, Any]],
    final_state: Any,
    trial_result: Optional[TrialResult] = None,
    **kwargs,
) -> OracleResult:
    """
    Check for the SSI update-conflict-out anomaly.
    Requires a TrialResult from run_trial().
    """
    if trial_result is None:
        return OracleResult(ok=True, details="No trial result provided; cannot evaluate.")

    r = trial_result
    if r.got_40001:
        return OracleResult(ok=True, details="Correct: 40001 raised, SSI prevented cycle.",
                            metrics={"got_40001": True})
    if r.bug_found:
        return OracleResult(
            ok=False,
            bug_type="ssi_conflict_out_miss",
            details=(
                "Non-serializable history accepted: foo and bar both committed with "
                "mutual invisibility (rw-antidependency cycle) but no 40001 raised. "
                f"foo_select_rows={r.foo_select_rows}, bar_select_rows={r.bar_select_rows}, "
                f"trouble_rows_affected={r.trouble_write_rows_affected}"
            ),
            metrics={"got_40001": False, "bug_found": True,
                      "trouble_rows_affected": r.trouble_write_rows_affected},
        )
    return OracleResult(ok=True, details="No cycle formed; transactions serialized correctly.",
                        metrics={"got_40001": False, "bug_found": False})
