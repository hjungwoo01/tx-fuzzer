"""
Bug-magnet template workload generator — cross-DB seed traces.

Produces JSON traces that intentionally create MVCC/locking stress ingredients:
- rmw_update: SELECT then UPDATE (classic stale read window)
- unique_insert: INSERT on unique key (forces index-level contention)
- insert_update_delete: version chain creation
- multi_table_rw: read T0 then write T1 (dependency cycles)
- abort_after_write: write then ROLLBACK (configurable rate)

All templates use small keyspaces (keys 0..K-1) to force conflicts.
SQL is kept simple with constant literals for key extraction.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TraceEvent:
    """A single event in a trace."""
    session: str
    event_id: str
    type: str  # begin, sql, commit, rollback
    sql: Optional[str] = None
    isolation: Optional[str] = None
    table: Optional[str] = None
    key: Optional[int] = None
    op: Optional[str] = None  # read, write, insert, delete

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "session": self.session,
            "event_id": self.event_id,
            "type": self.type,
        }
        if self.sql is not None:
            d["sql"] = self.sql
        if self.isolation is not None:
            d["isolation"] = self.isolation
        if self.table is not None:
            d["table"] = self.table
        if self.key is not None:
            d["key"] = self.key
        if self.op is not None:
            d["op"] = self.op
        return d


@dataclass
class SeedTrace:
    """A complete seed trace with metadata."""
    events: List[TraceEvent] = field(default_factory=list)
    seed: int = 0
    n_sessions: int = 0
    n_events: int = 0
    db: str = ""
    templates_used: List[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "seed": self.seed,
            "db": self.db,
            "n_sessions": self.n_sessions,
            "n_events": len(self.events),
            "templates_used": self.templates_used,
            "events": [e.to_dict() for e in self.events],
        }, indent=2)


def _rmw_update_txn(
    session: str,
    eid_base: int,
    rng: random.Random,
    table: str = "kv",
    keyspace: int = 4,
    db: str = "sqlite",
) -> List[TraceEvent]:
    """SELECT then UPDATE on same key — classic stale-read window."""
    k = rng.randint(0, keyspace - 1)
    events = [
        TraceEvent(session=session, event_id=f"e{eid_base}", type="begin",
                   isolation="READ COMMITTED"),
        TraceEvent(session=session, event_id=f"e{eid_base+1}", type="sql",
                   sql=f"SELECT v FROM {table} WHERE k = {k}",
                   table=table, key=k, op="read"),
        TraceEvent(session=session, event_id=f"e{eid_base+2}", type="sql",
                   sql=f"UPDATE {table} SET v = v + 1 WHERE k = {k}",
                   table=table, key=k, op="write"),
        TraceEvent(session=session, event_id=f"e{eid_base+3}", type="commit"),
    ]
    return events


def _unique_insert_txn(
    session: str,
    eid_base: int,
    rng: random.Random,
    table: str = "uniq",
    keyspace: int = 4,
    db: str = "sqlite",
) -> List[TraceEvent]:
    """INSERT on unique key — forces index-level contention."""
    k = rng.randint(0, keyspace - 1)
    # Use INSERT OR REPLACE for SQLite, INSERT ... ON DUPLICATE for MySQL
    if db == "sqlite":
        sql = f"INSERT OR REPLACE INTO {table} (k, v) VALUES ({k}, 1)"
    elif db == "mysql":
        sql = f"INSERT INTO {table} (k, v) VALUES ({k}, 1) ON DUPLICATE KEY UPDATE v = v + 1"
    else:
        sql = f"INSERT INTO {table} (k, v) VALUES ({k}, 1) ON CONFLICT (k) DO UPDATE SET v = {table}.v + 1"

    events = [
        TraceEvent(session=session, event_id=f"e{eid_base}", type="begin",
                   isolation="READ COMMITTED"),
        TraceEvent(session=session, event_id=f"e{eid_base+1}", type="sql",
                   sql=sql, table=table, key=k, op="insert"),
        TraceEvent(session=session, event_id=f"e{eid_base+2}", type="commit"),
    ]
    return events


def _insert_update_delete_txn(
    session: str,
    eid_base: int,
    rng: random.Random,
    table: str = "kv",
    keyspace: int = 4,
    db: str = "sqlite",
) -> List[TraceEvent]:
    """INSERT/UPDATE/DELETE cycle — creates version chains."""
    k = rng.randint(0, keyspace - 1)
    action = rng.choice(["update", "delete"])
    events = [
        TraceEvent(session=session, event_id=f"e{eid_base}", type="begin",
                   isolation="READ COMMITTED"),
    ]
    if action == "update":
        events.append(TraceEvent(
            session=session, event_id=f"e{eid_base+1}", type="sql",
            sql=f"UPDATE {table} SET v = v + 10 WHERE k = {k}",
            table=table, key=k, op="write"))
    else:
        events.append(TraceEvent(
            session=session, event_id=f"e{eid_base+1}", type="sql",
            sql=f"DELETE FROM {table} WHERE k = {k}",
            table=table, key=k, op="delete"))
    events.append(TraceEvent(
        session=session, event_id=f"e{eid_base+2}", type="commit"))
    return events


def _multi_table_rw_txn(
    session: str,
    eid_base: int,
    rng: random.Random,
    table_a: str = "kv",
    table_b: str = "uniq",
    keyspace: int = 4,
    db: str = "sqlite",
) -> List[TraceEvent]:
    """Read table A then write table B — enables dependency cycles."""
    ka = rng.randint(0, keyspace - 1)
    kb = rng.randint(0, keyspace - 1)
    events = [
        TraceEvent(session=session, event_id=f"e{eid_base}", type="begin",
                   isolation="REPEATABLE READ"),
        TraceEvent(session=session, event_id=f"e{eid_base+1}", type="sql",
                   sql=f"SELECT v FROM {table_a} WHERE k = {ka}",
                   table=table_a, key=ka, op="read"),
        TraceEvent(session=session, event_id=f"e{eid_base+2}", type="sql",
                   sql=f"UPDATE {table_b} SET v = v + 1 WHERE k = {kb}",
                   table=table_b, key=kb, op="write"),
        TraceEvent(session=session, event_id=f"e{eid_base+3}", type="commit"),
    ]
    return events


def _abort_after_write_txn(
    session: str,
    eid_base: int,
    rng: random.Random,
    table: str = "kv",
    keyspace: int = 4,
    db: str = "sqlite",
) -> List[TraceEvent]:
    """Write then ROLLBACK — creates aborted version chains."""
    k = rng.randint(0, keyspace - 1)
    events = [
        TraceEvent(session=session, event_id=f"e{eid_base}", type="begin",
                   isolation="READ COMMITTED"),
        TraceEvent(session=session, event_id=f"e{eid_base+1}", type="sql",
                   sql=f"UPDATE {table} SET v = v + 100 WHERE k = {k}",
                   table=table, key=k, op="write"),
        TraceEvent(session=session, event_id=f"e{eid_base+2}", type="rollback"),
    ]
    return events


# Template registry
TEMPLATES = {
    "rmw_update": _rmw_update_txn,
    "unique_insert": _unique_insert_txn,
    "insert_update_delete": _insert_update_delete_txn,
    "multi_table_rw": _multi_table_rw_txn,
    "abort_after_write": _abort_after_write_txn,
}


def setup_sql(db: str = "sqlite") -> List[str]:
    """Return DDL statements to create tables for templates."""
    stmts = [
        "DROP TABLE IF EXISTS kv",
        "DROP TABLE IF EXISTS uniq",
    ]
    if db == "sqlite":
        stmts += [
            "CREATE TABLE kv (k INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE uniq (k INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)",
        ]
    elif db == "mysql":
        stmts += [
            "CREATE TABLE kv (k INT PRIMARY KEY, v INT NOT NULL DEFAULT 0) ENGINE=InnoDB",
            "CREATE TABLE uniq (k INT PRIMARY KEY, v INT NOT NULL DEFAULT 0) ENGINE=InnoDB",
        ]
    else:  # postgres
        stmts += [
            "CREATE TABLE kv (k INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE uniq (k INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)",
        ]
    # Seed data: small keyspace
    for k in range(4):
        stmts.append(f"INSERT INTO kv (k, v) VALUES ({k}, 0)")
        stmts.append(f"INSERT INTO uniq (k, v) VALUES ({k}, 0)")
    return stmts


def generate_seed_trace(
    *,
    db: str = "sqlite",
    n_sessions: int = 4,
    txns_per_session: int = 10,
    seed: int = 42,
    keyspace: int = 4,
    abort_rate: float = 0.15,
    templates: Optional[List[str]] = None,
) -> SeedTrace:
    """Generate a seed trace from bug-magnet templates.

    Produces a multi-session trace where each session executes a sequence
    of transactions chosen from the template registry. The trace is fully
    deterministic given the seed.

    Args:
        db: Target DB engine.
        n_sessions: Number of concurrent sessions.
        txns_per_session: Transactions per session.
        seed: Random seed for reproducibility.
        keyspace: Key range [0, keyspace).
        abort_rate: Probability of choosing abort_after_write template.
        templates: Specific templates to use (default: all).
    """
    rng = random.Random(seed)
    if templates is None:
        templates = list(TEMPLATES.keys())

    trace = SeedTrace(seed=seed, n_sessions=n_sessions, db=db, templates_used=templates)
    eid_counter = 0

    for sid in range(n_sessions):
        session = f"s{sid}"
        for _ in range(txns_per_session):
            # Choose template
            if rng.random() < abort_rate:
                tpl_name = "abort_after_write"
            else:
                tpl_name = rng.choice([t for t in templates if t != "abort_after_write"])

            tpl_fn = TEMPLATES[tpl_name]
            # Multi-table templates need two table names
            if tpl_name == "multi_table_rw":
                events = tpl_fn(session, eid_counter, rng,
                                table_a="kv", table_b="uniq",
                                keyspace=keyspace, db=db)
            elif tpl_name == "unique_insert":
                events = tpl_fn(session, eid_counter, rng,
                                table="uniq", keyspace=keyspace, db=db)
            else:
                events = tpl_fn(session, eid_counter, rng,
                                table="kv", keyspace=keyspace, db=db)

            trace.events.extend(events)
            eid_counter += len(events)

    trace.n_events = len(trace.events)
    return trace
