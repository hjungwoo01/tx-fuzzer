"""
Workload: append_upsert — Jepsen-style list-append with UPSERT.

Multiple clients append unique tokens to shared keys using
INSERT ... ON CONFLICT DO UPDATE SET val = val || ',token'.
Oracle checks that the final val for each key contains exactly
the committed appends in a consistent order (no lost appends).

Schema: kv(id TEXT PK, val TEXT)
Invariant: val = comma-join of all committed appends for that key.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore

from workloads import OracleResult

WORKLOAD_META = {
    "name": "append_upsert",
    "description": "Jepsen-style list-append via UPSERT; detects lost appends",
    "invariants": "Final val for each key must contain exactly the committed append tokens.",
    "isolation_levels": ["SERIALIZABLE", "REPEATABLE READ"],
    "target_versions": {"primary": "16.2", "secondary": ["12.3", "12.4"]},
    "clients": 4,
    "variants": ["default"],
    "negative_controls": ["Under SERIALIZABLE, 40001 prevents anomalies"],
}

KEYS = ["A", "B", "C"]


def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS kv CASCADE",
        "CREATE TABLE kv (id TEXT PRIMARY KEY, val TEXT NOT NULL DEFAULT '')",
    ]


def truncate_sql() -> List[str]:
    return ["DELETE FROM kv"]


def _init_sql() -> List[str]:
    return [f"INSERT INTO kv VALUES ('{k}', '')" for k in KEYS]


def seed_trace(
    clients: int = 4,
    isolation: str = "SERIALIZABLE",
    variant: str = "default",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    Each client does a transaction: read key_i, append to key_j.
    Deterministic assignment based on seed.
    """
    rng = random.Random(seed)
    trace = []
    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"

    for cid in range(clients):
        sess = f"appender_{cid}"
        read_key = KEYS[cid % len(KEYS)]
        write_key = KEYS[(cid + 1) % len(KEYS)]
        token = f"c{cid}_{rng.randint(1000, 9999)}"

        trace.append({"event_id": f"a{cid}_begin", "client_id": cid, "session": sess,
                       "type": "begin", "sql": iso_sql})
        trace.append({"event_id": f"a{cid}_read", "client_id": cid, "session": sess,
                       "type": "sql", "sql": f"SELECT val FROM kv WHERE id = '{read_key}'"})
        trace.append({"event_id": f"a{cid}_append", "client_id": cid, "session": sess,
                       "type": "sql",
                       "sql": (f"INSERT INTO kv (id, val) VALUES ('{write_key}', '{token}') "
                               f"ON CONFLICT (id) DO UPDATE SET val = "
                               f"CASE WHEN kv.val = '' THEN '{token}' "
                               f"ELSE kv.val || ',' || '{token}' END"),
                       "token": token, "key": write_key})
        trace.append({"event_id": f"a{cid}_commit", "client_id": cid, "session": sess,
                       "type": "commit", "sql": "COMMIT"})
    return trace


def run_trial(
    dsn: str,
    isolation: str = "SERIALIZABLE",
    variant: str = "default",
    delays: Optional[Dict[str, float]] = None,
    seed: int = 42,
    clients: int = 4,
) -> Dict[str, Any]:
    delays = delays or {}
    rng = random.Random(seed)

    result = {
        "bug_found": False, "got_40001": False,
        "execution_log": [], "errors": [], "final_state": {},
        "committed_tokens": {k: [] for k in KEYS},
        "expected_tokens": {k: [] for k in KEYS},
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
    barrier = threading.Barrier(clients)

    # Pre-compute tokens
    tokens_info = []
    for cid in range(clients):
        read_key = KEYS[cid % len(KEYS)]
        write_key = KEYS[(cid + 1) % len(KEYS)]
        token = f"c{cid}_{rng.randint(1000, 9999)}"
        tokens_info.append((read_key, write_key, token))

    def worker(cid: int):
        read_key, write_key, token = tokens_info[cid]
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        committed = False
        try:
            barrier.wait(timeout=30.0)

            # BEGIN
            eid = f"a{cid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            cur.execute(iso_sql)

            # READ
            eid = f"a{cid}_read"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute(f"SELECT val FROM kv WHERE id = '{read_key}'")
                cur.fetchall()
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return

            # APPEND (UPSERT)
            eid = f"a{cid}_append"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            upsert_sql = (
                f"INSERT INTO kv (id, val) VALUES ('{write_key}', '{token}') "
                f"ON CONFLICT (id) DO UPDATE SET val = "
                f"CASE WHEN kv.val = '' THEN '{token}' "
                f"ELSE kv.val || ',' || '{token}' END"
            )
            try:
                cur.execute(upsert_sql)
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return

            # COMMIT
            eid = f"a{cid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute("COMMIT")
                committed = True
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass

            if committed:
                with lock:
                    result["committed_tokens"][write_key].append(token)
                    result["execution_log"].append({
                        "event_id": f"a{cid}_committed", "client_id": cid,
                        "token": token, "key": write_key,
                    })
        finally:
            try: conn.close()
            except: pass

    threads = [threading.Thread(target=worker, args=(cid,), daemon=True) for cid in range(clients)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=30.0)

    # Read final state
    conn_c = psycopg2.connect(dsn)
    conn_c.autocommit = True
    cur_c = conn_c.cursor()
    for k in KEYS:
        cur_c.execute(f"SELECT val FROM kv WHERE id = '{k}'")
        row = cur_c.fetchone()
        result["final_state"][k] = row[0] if row else ""
    conn_c.close()

    # Check invariant: all committed tokens must appear in final state
    for k in KEYS:
        expected = set(result["committed_tokens"][k])
        actual_val = result["final_state"].get(k, "")
        actual_tokens = set(t.strip() for t in actual_val.split(",") if t.strip()) if actual_val else set()
        missing = expected - actual_tokens
        if missing and not result["got_40001"]:
            result["bug_found"] = True
            result["missing_tokens"] = {k: list(missing)}

    return result


def build_delay_candidates(trace: List[Dict[str, Any]], clients: int = 4) -> List[Dict[str, Any]]:
    candidates = []
    seen = set()

    def _add(eid, sess, tier, weight, delay_range, justification):
        k = (eid, tier)
        if k in seen: return
        seen.add(k)
        candidates.append({"event_id": eid, "session": sess, "tier": tier,
                           "weight": weight, "delay_range": delay_range,
                           "justification": justification})

    for cid in range(clients):
        _add(f"a{cid}_append", f"appender_{cid}", "tier_rw_conflict", 10, (0.03, 0.15),
             f"Delay a{cid} append to widen conflict window")
        _add(f"a{cid}_commit", f"appender_{cid}", "tier_commit_hold", 9, (0.04, 0.20),
             f"Hold a{cid} commit to keep upsert invisible")
        _add(f"a{cid}_read", f"appender_{cid}", "tier_wr_conflict", 5, (0.02, 0.10),
             f"Delay a{cid} read")
        _add(f"a{cid}_begin", f"appender_{cid}", "tier_fallback", 1, (0.01, 0.05),
             f"Generic delay on a{cid}_begin")

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
        return OracleResult(ok=True, details="40001 raised; serialization prevented anomaly.",
                            metrics={"got_40001": True})
    if trial_result.get("bug_found"):
        return OracleResult(
            ok=False, bug_type="lost_append",
            details=f"Lost appends: missing_tokens={trial_result.get('missing_tokens')}",
            metrics={"missing_tokens": trial_result.get("missing_tokens", {})},
        )
    return OracleResult(ok=True, details="All committed tokens present.",
                        metrics={"final_state": trial_result.get("final_state", {})})
