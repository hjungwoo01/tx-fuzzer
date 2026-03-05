"""
Workload: inventory_oversell — e-commerce race condition.

Multiple clients try to purchase the last item. Buggy variant reads qty,
checks client-side, then decrements with a computed UPDATE. Fixed variant
uses atomic UPDATE … WHERE qty > 0 RETURNING qty.

Schema: inventory(sku TEXT PK, qty INT), orders(order_id SERIAL PK, sku TEXT, qty INT)
Invariant: inventory.qty >= 0; COUNT(orders) <= initial_qty.
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
    "name": "inventory_oversell",
    "description": "E-commerce inventory oversell race condition",
    "invariants": "inventory.qty >= 0; total orders for a SKU <= initial qty.",
    "isolation_levels": ["READ COMMITTED", "REPEATABLE READ"],
    "target_versions": {"primary": "16.2"},
    "clients": 3,
    "variants": ["buggy", "fixed"],
    "negative_controls": [
        "Fixed variant uses atomic UPDATE … WHERE qty > 0 RETURNING qty",
    ],
}

INITIAL_QTY = 1  # Only 1 item — maximizes contention
SKU = "WIDGET-001"


def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS orders CASCADE",
        "DROP TABLE IF EXISTS inventory CASCADE",
        "CREATE TABLE inventory (sku TEXT PRIMARY KEY, qty INT NOT NULL)",
        "CREATE TABLE orders (order_id SERIAL PRIMARY KEY, sku TEXT NOT NULL, qty INT NOT NULL)",
    ]


def truncate_sql() -> List[str]:
    return ["DELETE FROM orders", "DELETE FROM inventory"]


def _init_sql() -> List[str]:
    return [f"INSERT INTO inventory VALUES ('{SKU}', {INITIAL_QTY})"]


def seed_trace(
    clients: int = 3,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    N clients each try to purchase 1 unit.
    Buggy: SELECT qty → if qty>0 → INSERT order → UPDATE SET qty = qty_read - 1
    Fixed: UPDATE inventory SET qty=qty-1 WHERE sku=? AND qty>0 RETURNING qty;
           if returned → INSERT order
    """
    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"
    trace = []
    for cid in range(clients):
        sess = f"buyer_{cid}"
        trace.append({"event_id": f"b{cid}_begin", "client_id": cid, "session": sess,
                       "type": "begin", "sql": iso_sql})
        if variant == "fixed":
            trace.append({"event_id": f"b{cid}_atomic_update", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": f"UPDATE inventory SET qty = qty - 1 WHERE sku = '{SKU}' AND qty > 0 RETURNING qty"})
            trace.append({"event_id": f"b{cid}_insert_order", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": f"__CONDITIONAL_INSERT__"})
        else:
            trace.append({"event_id": f"b{cid}_read", "client_id": cid, "session": sess,
                           "type": "sql",
                           "sql": f"SELECT qty FROM inventory WHERE sku = '{SKU}'"})
            trace.append({"event_id": f"b{cid}_insert_order", "client_id": cid, "session": sess,
                           "type": "sql", "sql": "__CONDITIONAL_INSERT__"})
            trace.append({"event_id": f"b{cid}_update_qty", "client_id": cid, "session": sess,
                           "type": "sql", "sql": "__COMPUTED_UPDATE__"})
        trace.append({"event_id": f"b{cid}_commit", "client_id": cid, "session": sess,
                       "type": "commit", "sql": "COMMIT"})
    return trace


def run_trial(
    dsn: str,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    delays: Optional[Dict[str, float]] = None,
    seed: int = 42,
    clients: int = 3,
) -> Dict[str, Any]:
    delays = delays or {}
    result = {
        "bug_found": False, "got_40001": False,
        "execution_log": [], "errors": [], "final_state": {},
        "initial_qty": INITIAL_QTY, "orders_placed": 0,
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

    def worker_buggy(cid: int):
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            barrier.wait(timeout=30.0)
            # BEGIN
            eid = f"b{cid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            cur.execute(iso_sql)

            # READ qty
            eid = f"b{cid}_read"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute(f"SELECT qty FROM inventory WHERE sku = '{SKU}'")
                rows = cur.fetchall()
                qty = rows[0][0] if rows else 0
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return

            with lock:
                result["execution_log"].append({"event_id": eid, "qty_read": qty, "client_id": cid})

            if qty > 0:
                # INSERT order
                eid = f"b{cid}_insert_order"
                d = delays.get(eid, 0.0)
                if d > 0: time.sleep(d)
                try:
                    cur.execute(f"INSERT INTO orders (sku, qty) VALUES ('{SKU}', 1)")
                except Exception as e:
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {str(e)}")
                        if sqlstate == "40001": result["got_40001"] = True
                    try: cur.execute("ROLLBACK")
                    except: pass
                    return

                # UPDATE qty (computed client-side: stale!)
                eid = f"b{cid}_update_qty"
                d = delays.get(eid, 0.0)
                if d > 0: time.sleep(d)
                new_qty = qty - 1
                try:
                    cur.execute(f"UPDATE inventory SET qty = {new_qty} WHERE sku = '{SKU}'")
                except Exception as e:
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {str(e)}")
                        if sqlstate == "40001": result["got_40001"] = True
                    try: cur.execute("ROLLBACK")
                    except: pass
                    return

            # COMMIT
            eid = f"b{cid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute("COMMIT")
                if qty > 0:
                    with lock:
                        result["orders_placed"] += 1
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
        finally:
            try: conn.close()
            except: pass

    def worker_fixed(cid: int):
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            barrier.wait(timeout=30.0)
            eid = f"b{cid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            cur.execute(iso_sql)

            # Atomic UPDATE … WHERE qty > 0 RETURNING qty
            eid = f"b{cid}_atomic_update"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute(f"UPDATE inventory SET qty = qty - 1 WHERE sku = '{SKU}' AND qty > 0 RETURNING qty")
                rows = cur.fetchall()
                got_row = len(rows) > 0
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
                return

            if got_row:
                eid = f"b{cid}_insert_order"
                d = delays.get(eid, 0.0)
                if d > 0: time.sleep(d)
                try:
                    cur.execute(f"INSERT INTO orders (sku, qty) VALUES ('{SKU}', 1)")
                except Exception as e:
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {str(e)}")
                        if sqlstate == "40001": result["got_40001"] = True
                    try: cur.execute("ROLLBACK")
                    except: pass
                    return

            eid = f"b{cid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0: time.sleep(d)
            try:
                cur.execute("COMMIT")
                if got_row:
                    with lock:
                        result["orders_placed"] += 1
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {str(e)}")
                    if sqlstate == "40001": result["got_40001"] = True
                try: cur.execute("ROLLBACK")
                except: pass
        finally:
            try: conn.close()
            except: pass

    worker_fn = worker_fixed if variant == "fixed" else worker_buggy
    threads = [threading.Thread(target=worker_fn, args=(cid,), daemon=True) for cid in range(clients)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=30.0)

    # Check final state
    conn_c = psycopg2.connect(dsn)
    conn_c.autocommit = True
    cur_c = conn_c.cursor()
    cur_c.execute(f"SELECT qty FROM inventory WHERE sku = '{SKU}'")
    row = cur_c.fetchone()
    final_qty = row[0] if row else None
    cur_c.execute("SELECT COUNT(*) FROM orders")
    order_count = cur_c.fetchone()[0]
    conn_c.close()

    result["final_state"] = {"final_qty": final_qty, "order_count": order_count}
    result["orders_placed"] = order_count

    # Invariant check
    if final_qty is not None and final_qty < 0 and not result["got_40001"]:
        result["bug_found"] = True
    elif order_count > INITIAL_QTY and not result["got_40001"]:
        result["bug_found"] = True

    return result


def build_delay_candidates(trace: List[Dict[str, Any]], clients: int = 3) -> List[Dict[str, Any]]:
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
        # Delay updates so all reads complete first
        _add(f"b{cid}_update_qty", f"buyer_{cid}", "tier_rw_conflict", 10, (0.03, 0.15),
             f"Delay b{cid} update so others read stale qty first")
        _add(f"b{cid}_insert_order", f"buyer_{cid}", "tier_rw_conflict", 7, (0.02, 0.10),
             f"Delay b{cid} order insert to widen race window")
        _add(f"b{cid}_commit", f"buyer_{cid}", "tier_commit_hold", 9, (0.04, 0.20),
             f"Hold b{cid} commit to keep inventory update invisible")
        _add(f"b{cid}_read", f"buyer_{cid}", "tier_wr_conflict", 5, (0.02, 0.10),
             f"Delay b{cid} read")
        _add(f"b{cid}_begin", f"buyer_{cid}", "tier_fallback", 1, (0.01, 0.05),
             f"Generic delay on b{cid}_begin")

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
        return OracleResult(ok=True, details="40001 raised; serialization prevented oversell.",
                            metrics={"got_40001": True})
    if trial_result.get("bug_found"):
        fs = trial_result.get("final_state", {})
        return OracleResult(
            ok=False, bug_type="inventory_oversell",
            details=(f"Oversell: final_qty={fs.get('final_qty')}, "
                     f"orders={fs.get('order_count')}, initial_qty={INITIAL_QTY}"),
            metrics={"final_qty": fs.get("final_qty"), "order_count": fs.get("order_count")},
        )
    return OracleResult(ok=True, details="No oversell.",
                        metrics=trial_result.get("final_state", {}))
