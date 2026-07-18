"""
Extended bug corpus: 7 additional historical-bug workloads.

Augments ``micro_workloads.MICRO_WORKLOADS`` with cross-isolation-level
TOCTOU/SI-anomaly cases drawn from published Jepsen reports, the Adya
isolation taxonomy, and known DB CVEs.

Bug class taxonomy (Adya 1999, Berenson SQL-92 critique):
  G0          dirty write
  G1          dirty read / aborted read
  G2-item     write skew (predicate-free)
  G2          anti-dependency cycle
  G-SIa       single anti-dependency cycle
  G-SIb       long fork (read-only) anomaly
  P3          phantom under RR
  Lost update read-modify-write race
  TOCTOU      time-of-check / time-of-use predicate race

Each entry in ``EXTENDED_WORKLOADS`` adds:
  - setup_sql / run_buggy / run_fixed / canonical / oracle
  - bug_class, isolation_level, historical_ref, n_workers (recommended)

Importing this module patches MICRO_WORKLOADS by side effect; it also
registers the appropriate event-id template in MICRO_WORKLOAD_EVENTS so
the schedule pool generates the right delay points.
"""

from __future__ import annotations

import random as _random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from scripts.experiments.db_adapter import DBAdapter
from scripts.experiments.error_taxonomy import ErrorCategory
from scripts.experiments.micro_workloads import MICRO_WORKLOADS, MicroWorkload
from scripts.schedule_pool.event_id import MICRO_WORKLOAD_EVENTS
from scripts.schedule_pool.oracle import (
    FKPhantomOracle,
    InventoryOversellOracle,
    LongForkOracle,
    LostUpdateOracle,
    PhantomOracle,
    ReadSkewOracle,
    UniqueViolationOracle,
    WriteSkewOracle,
)


@dataclass(frozen=True)
class CorpusMeta:
    bug_class: str
    isolation_level: str
    historical_ref: str
    recommended_workers: int = 4


# ─────────────────────────────────────────────────────────────────────
# 1) Doctors-on-call write skew (G2-item, classic Berenson/Adya example)
# ─────────────────────────────────────────────────────────────────────

def doctors_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS doctors",
            "CREATE TABLE doctors (id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)",
            "INSERT INTO doctors (id, on_call) VALUES (1, TRUE), (2, TRUE)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS doctors",
            "CREATE TABLE doctors (id INTEGER PRIMARY KEY, on_call INTEGER NOT NULL)",
            "INSERT INTO doctors (id, on_call) VALUES (1, 1), (2, 1)",
        ]
    return [
        "DROP TABLE IF EXISTS doctors",
        "CREATE TABLE doctors (id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)",
        "INSERT INTO doctors (id, on_call) VALUES (1, TRUE), (2, TRUE)",
    ]


def _doctors_op(adapter: DBAdapter, my_id: int, isolation: str) -> Dict[str, Any]:
    """Buggy: read others, conclude >=1 still on call, then go off."""
    adapter.begin(isolation)
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT COUNT(*) FROM doctors WHERE on_call = 1")
    else:
        r = adapter.exec("SELECT COUNT(*) FROM doctors WHERE on_call = TRUE")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "doctor": my_id}
    count = r.rows[0][0] if r.rows else 0
    if count <= 1:
        adapter.rollback()
        return {"outcome": "stayed_on_call", "doctor": my_id}

    if adapter.db_name == "sqlite":
        adapter.exec("UPDATE doctors SET on_call = 0 WHERE id = ?", (my_id,))
    elif adapter.db_name == "mysql":
        adapter.exec("UPDATE doctors SET on_call = FALSE WHERE id = %s", (my_id,))
    else:
        adapter.exec("UPDATE doctors SET on_call = FALSE WHERE id = %s", (my_id,))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "doctor": my_id}
    return {"outcome": "went_off_call", "doctor": my_id}


def doctors_buggy(adapter, worker_id):
    return _doctors_op(adapter, (worker_id % 2) + 1, "READ COMMITTED")


def doctors_fixed(adapter, worker_id):
    # Atomic conditional: only go off if at least 2 still on
    my_id = (worker_id % 2) + 1
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        sql = ("UPDATE doctors SET on_call = 0 "
               "WHERE id = ? AND (SELECT COUNT(*) FROM doctors WHERE on_call = 1) > 1")
        r = adapter.exec(sql, (my_id,))
    elif adapter.db_name == "mysql":
        # MySQL forbids modifying same table inside subquery → use derived table
        sql = ("UPDATE doctors d JOIN (SELECT COUNT(*) AS c FROM doctors WHERE on_call = TRUE) x "
               "SET d.on_call = FALSE WHERE d.id = %s AND x.c > 1")
        r = adapter.exec(sql, (my_id,))
    else:
        sql = ("UPDATE doctors SET on_call = FALSE "
               "WHERE id = %s AND (SELECT COUNT(*) FROM doctors WHERE on_call = TRUE) > 1")
        r = adapter.exec(sql, (my_id,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "doctor": my_id}
    if r.rowcount == 0:
        adapter.rollback()
        return {"outcome": "stayed_on_call", "doctor": my_id}
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "doctor": my_id}
    return {"outcome": "went_off_call", "doctor": my_id}


def doctors_canonical(adapter: DBAdapter, outcomes=None) -> Dict[str, Any]:
    adapter.begin()
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT COUNT(*) FROM doctors WHERE on_call = 1")
    else:
        r = adapter.exec("SELECT COUNT(*) FROM doctors WHERE on_call = TRUE")
    adapter.commit()
    on_call = r.rows[0][0] if r.rows else 0
    return {
        "on_call_count": on_call,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 2) Phantom insert: SELECT-then-INSERT race (Berenson P3 / SI phantom)
# ─────────────────────────────────────────────────────────────────────

def phantom_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS members",
            "CREATE TABLE members (id INT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(50) NOT NULL, room INT NOT NULL)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS members",
            "CREATE TABLE members (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT NOT NULL, room INTEGER NOT NULL)",
        ]
    return [
        "DROP TABLE IF EXISTS members",
        "CREATE TABLE members (id SERIAL PRIMARY KEY, name TEXT NOT NULL, room INT NOT NULL)",
    ]


def phantom_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """If room is empty insert ourselves; race lets two workers both insert."""
    name = f"w{worker_id}"
    room = 7
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT COUNT(*) FROM members WHERE room = ?", (room,))
    else:
        r = adapter.exec("SELECT COUNT(*) FROM members WHERE room = %s", (room,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    cnt = r.rows[0][0] if r.rows else 0
    if cnt >= 1:
        adapter.rollback()
        return {"outcome": "room_taken", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO members (name, room) VALUES (?, ?)", (name, room))
    else:
        adapter.exec("INSERT INTO members (name, room) VALUES (%s, %s)", (name, room))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "moved_in", "worker": name}


def phantom_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Single atomic insert if-not-exists using INSERT ... WHERE NOT EXISTS."""
    name = f"w{worker_id}"
    room = 7
    adapter.begin("SERIALIZABLE")
    try:
        if adapter.db_name == "sqlite":
            r = adapter.exec(
                "INSERT INTO members (name, room) "
                "SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM members WHERE room = ?)",
                (name, room, room),
            )
        elif adapter.db_name == "mysql":
            r = adapter.exec(
                "INSERT INTO members (name, room) "
                "SELECT %s, %s FROM (SELECT 1) AS d "
                "WHERE NOT EXISTS (SELECT 1 FROM members WHERE room = %s)",
                (name, room, room),
            )
        else:
            r = adapter.exec(
                "INSERT INTO members (name, room) "
                "SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM members WHERE room = %s)",
                (name, room, room),
            )
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": name}
        cr = adapter.commit()
        if cr.error_category != ErrorCategory.OK:
            return {"outcome": "error", "error": cr.error_category.value, "worker": name}
        return {"outcome": ("moved_in" if r.rowcount else "room_taken"), "worker": name}
    except Exception as e:
        adapter.rollback()
        return {"outcome": "error", "error": "OTHER", "worker": name, "ex": str(e)}


def phantom_canonical(adapter: DBAdapter, outcomes=None) -> Dict[str, Any]:
    adapter.begin()
    r = adapter.exec("SELECT COUNT(*) FROM members WHERE room = 7")
    adapter.commit()
    inserts = r.rows[0][0] if r.rows else 0
    return {
        "inserts_in_range": inserts,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 3) Lost-update counter (classic RC race, no rowcount guard)
# ─────────────────────────────────────────────────────────────────────

def counter_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS counters",
            "CREATE TABLE counters (id INT PRIMARY KEY, val INT NOT NULL)",
            "INSERT INTO counters (id, val) VALUES (1, 0)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS counters",
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)",
            "INSERT INTO counters (id, val) VALUES (1, 0)",
        ]
    return [
        "DROP TABLE IF EXISTS counters",
        "CREATE TABLE counters (id INT PRIMARY KEY, val INT NOT NULL)",
        "INSERT INTO counters (id, val) VALUES (1, 0)",
    ]


def counter_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    name = f"w{worker_id}"
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT val FROM counters WHERE id = 1")
    else:
        r = adapter.exec("SELECT val FROM counters WHERE id = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    cur = r.rows[0][0] if r.rows else 0
    new = cur + 1
    # absolute write reintroduces lost-update race
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE counters SET val = ? WHERE id = 1", (new,))
    else:
        r2 = adapter.exec("UPDATE counters SET val = %s WHERE id = 1", (new,))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": name}
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "incremented", "worker": name, "from": cur, "to": new}


def counter_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    name = f"w{worker_id}"
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("UPDATE counters SET val = val + 1 WHERE id = 1")
    else:
        r = adapter.exec("UPDATE counters SET val = val + 1 WHERE id = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "incremented", "worker": name}


def counter_canonical(adapter: DBAdapter, outcomes=None) -> Dict[str, Any]:
    adapter.begin()
    r = adapter.exec("SELECT val FROM counters WHERE id = 1")
    adapter.commit()
    val = r.rows[0][0] if r.rows else 0
    return {
        "counter": val,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 4) Read-skew (A5A) — bank x→y transfer with a separate observer txn
# ─────────────────────────────────────────────────────────────────────

def readskew_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS rskew",
            "CREATE TABLE rskew (k VARCHAR(8) PRIMARY KEY, v INT NOT NULL)",
            "INSERT INTO rskew (k, v) VALUES ('x', 50), ('y', 50)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS rskew",
            "CREATE TABLE rskew (k TEXT PRIMARY KEY, v INTEGER NOT NULL)",
            "INSERT INTO rskew (k, v) VALUES ('x', 50), ('y', 50)",
        ]
    return [
        "DROP TABLE IF EXISTS rskew",
        "CREATE TABLE rskew (k TEXT PRIMARY KEY, v INT NOT NULL)",
        "INSERT INTO rskew (k, v) VALUES ('x', 50), ('y', 50)",
    ]


def _readskew_observe(adapter: DBAdapter, name: str) -> Dict[str, Any]:
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r1 = adapter.exec("SELECT v FROM rskew WHERE k = 'x'")
    else:
        r1 = adapter.exec("SELECT v FROM rskew WHERE k = 'x'")
    if r1.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r1.error_category.value, "worker": name}
    x = r1.rows[0][0] if r1.rows else 0
    # Subsequent read in same txn — under RC the snapshot may shift
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("SELECT v FROM rskew WHERE k = 'y'")
    else:
        r2 = adapter.exec("SELECT v FROM rskew WHERE k = 'y'")
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": name}
    y = r2.rows[0][0] if r2.rows else 0
    adapter.commit()
    if x + y != 100:
        return {"outcome": "skewed_read", "worker": name, "x": x, "y": y}
    return {"outcome": "consistent_read", "worker": name, "x": x, "y": y}


def _readskew_transfer(adapter: DBAdapter, name: str) -> Dict[str, Any]:
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("UPDATE rskew SET v = v - 10 WHERE k = 'x'")
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": name}
        r2 = adapter.exec("UPDATE rskew SET v = v + 10 WHERE k = 'y'")
    else:
        r = adapter.exec("UPDATE rskew SET v = v - 10 WHERE k = 'x'")
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": name}
        r2 = adapter.exec("UPDATE rskew SET v = v + 10 WHERE k = 'y'")
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": name}
    adapter.commit()
    return {"outcome": "transferred", "worker": name}


def readskew_buggy(adapter, worker_id):
    name = f"w{worker_id}"
    if worker_id % 2 == 0:
        return _readskew_observe(adapter, name)
    return _readskew_transfer(adapter, name)


def readskew_fixed(adapter, worker_id):
    """Fixed: observers use REPEATABLE READ for a stable snapshot."""
    name = f"w{worker_id}"
    if worker_id % 2 == 0:
        adapter.begin("REPEATABLE READ")
        if adapter.db_name == "sqlite":
            r1 = adapter.exec("SELECT v FROM rskew WHERE k = 'x'")
        else:
            r1 = adapter.exec("SELECT v FROM rskew WHERE k = 'x'")
        if r1.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r1.error_category.value, "worker": name}
        x = r1.rows[0][0] if r1.rows else 0
        if adapter.db_name == "sqlite":
            r2 = adapter.exec("SELECT v FROM rskew WHERE k = 'y'")
        else:
            r2 = adapter.exec("SELECT v FROM rskew WHERE k = 'y'")
        if r2.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r2.error_category.value, "worker": name}
        y = r2.rows[0][0] if r2.rows else 0
        adapter.commit()
        return {"outcome": "consistent_read", "worker": name, "x": x, "y": y}
    return _readskew_transfer(adapter, name)


def readskew_canonical(adapter, outcomes=None):
    adapter.begin()
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT k, v FROM rskew ORDER BY k")
    else:
        r = adapter.exec("SELECT k, v FROM rskew ORDER BY k")
    adapter.commit()
    rows = [(row[0], row[1]) for row in r.rows] if r.rows else []
    skewed = sum(1 for o in (outcomes or []) if o.get("outcome") == "skewed_read")
    return {
        "balances": rows,
        "skewed_read_count": skewed,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 5) Unique-violation TOCTOU: count-then-insert username race
# ─────────────────────────────────────────────────────────────────────

def unique_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS users",
            "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS users",
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        ]
    return [
        "DROP TABLE IF EXISTS users",
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    ]


def unique_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """All workers race to register the same username 'alice'."""
    target = "alice"
    name = f"w{worker_id}"
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT COUNT(*) FROM users WHERE name = ?", (target,))
    else:
        r = adapter.exec("SELECT COUNT(*) FROM users WHERE name = %s", (target,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    cnt = r.rows[0][0] if r.rows else 0
    if cnt > 0:
        adapter.rollback()
        return {"outcome": "name_taken", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO users (name) VALUES (?)", (target,))
    else:
        adapter.exec("INSERT INTO users (name) VALUES (%s)", (target,))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "registered", "worker": name}


def unique_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Same logic but the schema enforces a UNIQUE on name (created here)."""
    target = "alice"
    name = f"w{worker_id}"
    adapter.begin("READ COMMITTED")
    try:
        if adapter.db_name == "sqlite":
            r = adapter.exec("INSERT INTO users (name) VALUES (?)", (target,))
        else:
            r = adapter.exec("INSERT INTO users (name) VALUES (%s)", (target,))
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "name_taken", "worker": name}
        cr = adapter.commit()
        if cr.error_category != ErrorCategory.OK:
            return {"outcome": "name_taken", "worker": name}
        return {"outcome": "registered", "worker": name}
    except Exception:
        adapter.rollback()
        return {"outcome": "name_taken", "worker": name}


def unique_canonical(adapter, outcomes=None):
    adapter.begin()
    r = adapter.exec("SELECT name, COUNT(*) FROM users GROUP BY name")
    adapter.commit()
    rows = [(row[0], row[1]) for row in r.rows] if r.rows else []
    dups = sum(c - 1 for _n, c in rows if c > 1)
    return {
        "counts": rows,
        "duplicate_names": dups,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 6) Inventory oversell: classic e-commerce stock-decrement race
# ─────────────────────────────────────────────────────────────────────

def inventory_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS sales",
            "DROP TABLE IF EXISTS inventory",
            "CREATE TABLE inventory (sku INT PRIMARY KEY, stock INT NOT NULL)",
            "CREATE TABLE sales (id INT AUTO_INCREMENT PRIMARY KEY, sku INT, buyer VARCHAR(20))",
            "INSERT INTO inventory (sku, stock) VALUES (1, 5)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS sales",
            "DROP TABLE IF EXISTS inventory",
            "CREATE TABLE inventory (sku INTEGER PRIMARY KEY, stock INTEGER NOT NULL)",
            "CREATE TABLE sales (id INTEGER PRIMARY KEY AUTOINCREMENT, sku INTEGER, buyer TEXT)",
            "INSERT INTO inventory (sku, stock) VALUES (1, 5)",
        ]
    return [
        "DROP TABLE IF EXISTS sales",
        "DROP TABLE IF EXISTS inventory",
        "CREATE TABLE inventory (sku INT PRIMARY KEY, stock INT NOT NULL)",
        "CREATE TABLE sales (id SERIAL PRIMARY KEY, sku INT, buyer TEXT)",
        "INSERT INTO inventory (sku, stock) VALUES (1, 5)",
    ]


def inventory_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    name = f"buyer{worker_id}"
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT stock FROM inventory WHERE sku = 1")
    else:
        r = adapter.exec("SELECT stock FROM inventory WHERE sku = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    stock = r.rows[0][0] if r.rows else 0
    if stock <= 0:
        adapter.rollback()
        return {"outcome": "out_of_stock", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.exec("UPDATE inventory SET stock = stock - 1 WHERE sku = 1")
        adapter.exec("INSERT INTO sales (sku, buyer) VALUES (1, ?)", (name,))
    else:
        adapter.exec("UPDATE inventory SET stock = stock - 1 WHERE sku = 1")
        adapter.exec("INSERT INTO sales (sku, buyer) VALUES (1, %s)", (name,))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "sold", "worker": name}


def inventory_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    name = f"buyer{worker_id}"
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec(
            "UPDATE inventory SET stock = stock - 1 WHERE sku = 1 AND stock > 0"
        )
    else:
        r = adapter.exec(
            "UPDATE inventory SET stock = stock - 1 WHERE sku = 1 AND stock > 0"
        )
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    if r.rowcount == 0:
        adapter.rollback()
        return {"outcome": "out_of_stock", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO sales (sku, buyer) VALUES (1, ?)", (name,))
    else:
        adapter.exec("INSERT INTO sales (sku, buyer) VALUES (1, %s)", (name,))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "sold", "worker": name}


def inventory_canonical(adapter, outcomes=None):
    adapter.begin()
    r1 = adapter.exec("SELECT stock FROM inventory WHERE sku = 1")
    r2 = adapter.exec("SELECT COUNT(*) FROM sales")
    adapter.commit()
    stock = r1.rows[0][0] if r1.rows else 0
    sold = r2.rows[0][0] if r2.rows else 0
    return {
        "stock": stock,
        "sold_count": sold,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 7) FK phantom: parent delete vs child insert (orphan detection)
# ─────────────────────────────────────────────────────────────────────

def fk_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS children",
            "DROP TABLE IF EXISTS parents",
            "CREATE TABLE parents (id INT PRIMARY KEY)",
            "CREATE TABLE children (id INT AUTO_INCREMENT PRIMARY KEY, parent_id INT NOT NULL)",
            "INSERT INTO parents (id) VALUES (1)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS children",
            "DROP TABLE IF EXISTS parents",
            "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
            "CREATE TABLE children (id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER NOT NULL)",
            "INSERT INTO parents (id) VALUES (1)",
        ]
    return [
        "DROP TABLE IF EXISTS children",
        "DROP TABLE IF EXISTS parents",
        "CREATE TABLE parents (id INT PRIMARY KEY)",
        "CREATE TABLE children (id SERIAL PRIMARY KEY, parent_id INT NOT NULL)",
        "INSERT INTO parents (id) VALUES (1)",
    ]


def fk_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    name = f"w{worker_id}"
    if worker_id % 3 == 0:
        # Deleter
        adapter.begin("READ COMMITTED")
        if adapter.db_name == "sqlite":
            adapter.exec("DELETE FROM parents WHERE id = 1")
        else:
            adapter.exec("DELETE FROM parents WHERE id = 1")
        cr = adapter.commit()
        if cr.error_category != ErrorCategory.OK:
            return {"outcome": "error", "error": cr.error_category.value, "worker": name}
        return {"outcome": "deleted_parent", "worker": name}
    # Inserter: check parent exists, then insert child
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT 1 FROM parents WHERE id = 1")
    else:
        r = adapter.exec("SELECT 1 FROM parents WHERE id = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    if not r.rows:
        adapter.rollback()
        return {"outcome": "no_parent", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO children (parent_id) VALUES (?)", (1,))
    else:
        adapter.exec("INSERT INTO children (parent_id) VALUES (%s)", (1,))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "inserted_child", "worker": name}


def fk_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed: SELECT ... FOR UPDATE on parent (or SERIALIZABLE on SQLite)."""
    name = f"w{worker_id}"
    if worker_id % 3 == 0:
        adapter.begin("READ COMMITTED")
        if adapter.db_name == "sqlite":
            adapter.exec("DELETE FROM parents WHERE id = 1 AND "
                         "NOT EXISTS (SELECT 1 FROM children WHERE parent_id = 1)")
        else:
            adapter.exec("DELETE FROM parents WHERE id = 1 AND "
                         "NOT EXISTS (SELECT 1 FROM children WHERE parent_id = 1)")
        cr = adapter.commit()
        if cr.error_category != ErrorCategory.OK:
            return {"outcome": "error", "error": cr.error_category.value, "worker": name}
        return {"outcome": "deleted_parent", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.begin("EXCLUSIVE")
        r = adapter.exec("SELECT 1 FROM parents WHERE id = 1")
    else:
        adapter.begin("READ COMMITTED")
        r = adapter.exec("SELECT 1 FROM parents WHERE id = 1 FOR UPDATE")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": name}
    if not r.rows:
        adapter.rollback()
        return {"outcome": "no_parent", "worker": name}
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO children (parent_id) VALUES (?)", (1,))
    else:
        adapter.exec("INSERT INTO children (parent_id) VALUES (%s)", (1,))
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    return {"outcome": "inserted_child", "worker": name}


def fk_canonical(adapter, outcomes=None):
    adapter.begin()
    r1 = adapter.exec("SELECT COUNT(*) FROM parents WHERE id = 1")
    r2 = adapter.exec("SELECT COUNT(*) FROM children WHERE parent_id = 1")
    adapter.commit()
    parents_left = r1.rows[0][0] if r1.rows else 0
    children = r2.rows[0][0] if r2.rows else 0
    return {
        "parents_left": parents_left,
        "child_count": children,
        "orphan_children": children if parents_left == 0 else 0,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# 8) Long fork (G-SIb): two writers, two read-only observers
# ─────────────────────────────────────────────────────────────────────

def longfork_setup(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS lf",
            "CREATE TABLE lf (k VARCHAR(8) PRIMARY KEY, v INT NOT NULL)",
            "INSERT INTO lf (k, v) VALUES ('a', 0), ('b', 0)",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS lf",
            "CREATE TABLE lf (k TEXT PRIMARY KEY, v INTEGER NOT NULL)",
            "INSERT INTO lf (k, v) VALUES ('a', 0), ('b', 0)",
        ]
    return [
        "DROP TABLE IF EXISTS lf",
        "CREATE TABLE lf (k TEXT PRIMARY KEY, v INT NOT NULL)",
        "INSERT INTO lf (k, v) VALUES ('a', 0), ('b', 0)",
    ]


def longfork_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    name = f"w{worker_id}"
    role = worker_id % 4
    if role == 0:
        adapter.begin("READ COMMITTED")
        if adapter.db_name == "sqlite":
            adapter.exec("UPDATE lf SET v = 1 WHERE k = 'a'")
        else:
            adapter.exec("UPDATE lf SET v = 1 WHERE k = 'a'")
        adapter.commit()
        return {"outcome": "wrote_a", "worker": name}
    if role == 1:
        adapter.begin("READ COMMITTED")
        if adapter.db_name == "sqlite":
            adapter.exec("UPDATE lf SET v = 1 WHERE k = 'b'")
        else:
            adapter.exec("UPDATE lf SET v = 1 WHERE k = 'b'")
        adapter.commit()
        return {"outcome": "wrote_b", "worker": name}
    # Observers: read both, record what they saw
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        ra = adapter.exec("SELECT v FROM lf WHERE k = 'a'")
        rb = adapter.exec("SELECT v FROM lf WHERE k = 'b'")
    else:
        ra = adapter.exec("SELECT v FROM lf WHERE k = 'a'")
        rb = adapter.exec("SELECT v FROM lf WHERE k = 'b'")
    adapter.commit()
    a = ra.rows[0][0] if ra.rows else 0
    b = rb.rows[0][0] if rb.rows else 0
    # Encode "who's been written" view as a tuple
    return {
        "outcome": "observed",
        "worker": name,
        "observed_order": [a, b],
    }


def longfork_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Observers run SERIALIZABLE → all observers agree."""
    name = f"w{worker_id}"
    role = worker_id % 4
    if role < 2:
        return longfork_buggy(adapter, worker_id)
    adapter.begin("SERIALIZABLE")
    if adapter.db_name == "sqlite":
        ra = adapter.exec("SELECT v FROM lf WHERE k = 'a'")
        rb = adapter.exec("SELECT v FROM lf WHERE k = 'b'")
    else:
        ra = adapter.exec("SELECT v FROM lf WHERE k = 'a'")
        rb = adapter.exec("SELECT v FROM lf WHERE k = 'b'")
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": name}
    a = ra.rows[0][0] if ra.rows else 0
    b = rb.rows[0][0] if rb.rows else 0
    return {
        "outcome": "observed",
        "worker": name,
        "observed_order": [a, b],
    }


def longfork_canonical(adapter, outcomes=None):
    adapter.begin()
    r = adapter.exec("SELECT k, v FROM lf ORDER BY k")
    adapter.commit()
    rows = [(row[0], row[1]) for row in r.rows] if r.rows else []
    return {
        "values": rows,
        "outcome_kinds": sorted({o.get("outcome", "") for o in (outcomes or [])}),
    }


# ─────────────────────────────────────────────────────────────────────
# Workload registry + isolation/bug metadata
# ─────────────────────────────────────────────────────────────────────

EXTENDED_WORKLOADS: Dict[str, MicroWorkload] = {
    "doctors_on_call": MicroWorkload(
        name="doctors_on_call",
        setup_sql=doctors_setup,
        run_buggy=doctors_buggy,
        run_fixed=doctors_fixed,
        oracle_instance=WriteSkewOracle(),
        canonical=doctors_canonical,
    ),
    "phantom_insert": MicroWorkload(
        name="phantom_insert",
        setup_sql=phantom_setup,
        run_buggy=phantom_buggy,
        run_fixed=phantom_fixed,
        oracle_instance=PhantomOracle(),
        canonical=phantom_canonical,
    ),
    "lost_update_counter": MicroWorkload(
        name="lost_update_counter",
        setup_sql=counter_setup,
        run_buggy=counter_buggy,
        run_fixed=counter_fixed,
        oracle_instance=LostUpdateOracle(),
        canonical=counter_canonical,
    ),
    "read_skew": MicroWorkload(
        name="read_skew",
        setup_sql=readskew_setup,
        run_buggy=readskew_buggy,
        run_fixed=readskew_fixed,
        oracle_instance=ReadSkewOracle(),
        canonical=readskew_canonical,
    ),
    "unique_violation": MicroWorkload(
        name="unique_violation",
        setup_sql=unique_setup,
        run_buggy=unique_buggy,
        run_fixed=unique_fixed,
        oracle_instance=UniqueViolationOracle(),
        canonical=unique_canonical,
    ),
    "inventory_oversell": MicroWorkload(
        name="inventory_oversell",
        setup_sql=inventory_setup,
        run_buggy=inventory_buggy,
        run_fixed=inventory_fixed,
        oracle_instance=InventoryOversellOracle(initial_stock=5),
        canonical=inventory_canonical,
    ),
    "fk_phantom": MicroWorkload(
        name="fk_phantom",
        setup_sql=fk_setup,
        run_buggy=fk_buggy,
        run_fixed=fk_fixed,
        oracle_instance=FKPhantomOracle(),
        canonical=fk_canonical,
    ),
    "long_fork": MicroWorkload(
        name="long_fork",
        setup_sql=longfork_setup,
        run_buggy=longfork_buggy,
        run_fixed=longfork_fixed,
        oracle_instance=LongForkOracle(),
        canonical=longfork_canonical,
    ),
}


CORPUS_METADATA: Dict[str, CorpusMeta] = {
    # original 5
    "job_queue_claim": CorpusMeta(
        bug_class="TOCTOU/lost-update",
        isolation_level="READ COMMITTED",
        historical_ref="Sidekiq #3304 / Resque double-claim",
        recommended_workers=4,
    ),
    "bank_overdraft": CorpusMeta(
        bug_class="TOCTOU/lost-update",
        isolation_level="READ COMMITTED",
        historical_ref="Berenson 1995 SQL-92 critique (lost update)",
        recommended_workers=4,
    ),
    "list_append": CorpusMeta(
        bug_class="lost-update",
        isolation_level="READ COMMITTED",
        historical_ref="Jepsen list-append (Elle 2020)",
        recommended_workers=4,
    ),
    "smallbank": CorpusMeta(
        bug_class="TOCTOU/lost-update",
        isolation_level="READ COMMITTED",
        historical_ref="Cahill et al. SmallBank (SIGMOD 2008)",
        recommended_workers=4,
    ),
    "multikey_transfer": CorpusMeta(
        bug_class="lost-update (multi-key)",
        isolation_level="READ COMMITTED",
        historical_ref="Jepsen bank transfer (Cassandra/PG)",
        recommended_workers=4,
    ),
    # extended 8
    "doctors_on_call": CorpusMeta(
        bug_class="G2-item write skew",
        isolation_level="SNAPSHOT/RC",
        historical_ref="Berenson 1995, Adya 1999 (PG SI, Oracle SI write-skew)",
        recommended_workers=2,
    ),
    "phantom_insert": CorpusMeta(
        bug_class="P3 phantom",
        isolation_level="REPEATABLE READ",
        historical_ref="MySQL InnoDB RR phantom (#73479); PG SI phantom",
        recommended_workers=4,
    ),
    "lost_update_counter": CorpusMeta(
        bug_class="P4 lost update",
        isolation_level="READ COMMITTED",
        historical_ref="Berenson 1995 SQL-92 critique",
        recommended_workers=4,
    ),
    "read_skew": CorpusMeta(
        bug_class="A5A read skew",
        isolation_level="READ COMMITTED",
        historical_ref="Adya 1999, Jepsen YugabyteDB read-skew",
        recommended_workers=4,
    ),
    "unique_violation": CorpusMeta(
        bug_class="TOCTOU predicate",
        isolation_level="READ COMMITTED",
        historical_ref="Devise/Rails uniqueness validation race",
        recommended_workers=4,
    ),
    "inventory_oversell": CorpusMeta(
        bug_class="TOCTOU/lost-update",
        isolation_level="READ COMMITTED",
        historical_ref="Shopify/Stripe inventory oversell incidents",
        recommended_workers=4,
    ),
    "fk_phantom": CorpusMeta(
        bug_class="referential phantom",
        isolation_level="READ COMMITTED",
        historical_ref="PG bug #14796 (concurrent FK), MySQL #74132",
        recommended_workers=3,
    ),
    "long_fork": CorpusMeta(
        bug_class="G-SIb long fork",
        isolation_level="SNAPSHOT",
        historical_ref="Cerone 2015; Jepsen YugabyteDB SI long-fork",
        recommended_workers=4,
    ),
}


def register_extended_workloads() -> List[str]:
    """Patch the extended workloads into the global MICRO_WORKLOADS map.

    Idempotent: safe to call multiple times.
    Also extends MICRO_WORKLOAD_EVENTS so the schedule pool knows the
    delay-point space for each new workload.
    """
    added: List[str] = []
    for name, wl in EXTENDED_WORKLOADS.items():
        if name not in MICRO_WORKLOADS:
            MICRO_WORKLOADS[name] = wl
            added.append(name)
        if name not in MICRO_WORKLOAD_EVENTS:
            # Reasonable default: 2 exec queries per worker per txn
            MICRO_WORKLOAD_EVENTS[name] = {"queries_per_worker": 2}
    return added


# Register on import so callers just `import scripts.experiments.extended_workloads`
register_extended_workloads()
