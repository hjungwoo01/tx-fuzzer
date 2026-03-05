#!/usr/bin/env python3
"""
Discovery engine for the PostgreSQL SSI "update-conflict-out" anomaly.

This script demonstrates the full story:
  1. Baseline: run the workload naturally (no delays) → OS hides the bug
  2. Discovery: the heuristic mutation engine injects targeted delays and
     independently discovers an interleaving that exposes the anomaly
  3. Validation: the found schedule triggers on 12.3 but NOT on 12.4

Usage:
  # Baseline stats (no delays, N trials)
  python scripts/discover_update_conflict_out.py --mode baseline --pg 12.3 --trials 200

  # Discovery campaign
  python scripts/discover_update_conflict_out.py --mode discover --pg 12.3 --k 2 --budget 500

  # Cross-version validation of a found trace
  python scripts/discover_update_conflict_out.py --mode validate --trace results/update-conflict-out/found.json

  # Full demo (baseline + discover + validate)
  python scripts/discover_update_conflict_out.py --mode full
"""

from __future__ import annotations

import argparse
import copy
import datetime
import json
import os
import random
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.errors
except ImportError:
    print("ERROR: psycopg2 is required. Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# DSN helpers
# ---------------------------------------------------------------------------

DEFAULT_PORTS = {"12.3": 54323, "12.4": 54324}


def dsn_for_pg(version: str) -> str:
    port = DEFAULT_PORTS.get(version, 5432)
    return f"postgres://postgres:postgres@localhost:{port}/postgres?sslmode=disable"


def wait_for_pg(dsn: str, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SETUP_SQL = [
    "DROP TABLE IF EXISTS txn0 CASCADE",
    "DROP TABLE IF EXISTS txn1 CASCADE",
    "CREATE TABLE txn0 (id int4 PRIMARY KEY, val TEXT)",
    "CREATE TABLE txn1 (id int4 PRIMARY KEY, val TEXT)",
]

TRUNCATE_SQL = [
    "DELETE FROM txn1",
    "DELETE FROM txn0",
]


def setup_schema(dsn: str) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for sql in SETUP_SQL:
                cur.execute(sql)
    finally:
        conn.close()


def reset_tables(dsn: str) -> None:
    """Fast reset via DELETE (no DDL locks)."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for sql in TRUNCATE_SQL:
                try:
                    cur.execute(sql)
                except Exception:
                    pass
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Session definitions (natural seed trace)
# ---------------------------------------------------------------------------

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


def build_natural_trace(variant: str = "update") -> List[Dict[str, Any]]:
    """
    Build a 'natural' seed trace — per-session FIFO order, flat interleaving.
    This is NOT the canonical (bug-triggering) schedule; it's a simple
    round-robin that the concurrent runner will execute with thread-level
    nondeterminism.
    """
    sessions = {
        "foo": SESSION_FOO,
        "bar": SESSION_BAR,
        "trouble": SESSION_TROUBLE_UPDATE if variant == "update" else SESSION_TROUBLE_DELETE,
    }
    client_map = {"foo": 0, "bar": 1, "trouble": 2}
    trace: List[Dict[str, Any]] = []
    for sess_name, steps in sessions.items():
        cid = client_map[sess_name]
        for eid, sql in steps:
            evt_type = "begin"
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


# ---------------------------------------------------------------------------
# Concurrent natural runner (no delays — baseline)
# ---------------------------------------------------------------------------

@dataclass
class TrialResult:
    """Result of one concurrent trial."""
    variant: str = "update"
    bug_found: bool = False
    got_40001: bool = False
    foo_select_rows: Optional[List[Any]] = None
    bar_select_rows: Optional[List[Any]] = None
    foo_select_error: Optional[str] = None
    bar_select_error: Optional[str] = None
    bar_commit_error: Optional[str] = None
    foo_commit_error: Optional[str] = None
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
    """Run one session's steps on its own connection, with barrier sync at start."""
    conn = psycopg2.connect(dsn)
    # Use autocommit=True so we have explicit control over BEGIN/COMMIT/ABORT.
    # This prevents psycopg2 from sending an implicit BEGIN which would
    # acquire the snapshot too early.
    conn.autocommit = True
    try:
        cur = conn.cursor()
        barrier.wait(timeout=30.0)

        for eid, sql in session_steps:
            # Apply delay if specified for this event
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
                if upper.startswith("BEGIN"):
                    cur.execute(sql)
                elif upper.startswith("COMMIT"):
                    cur.execute("COMMIT")
                elif upper in ("ABORT", "ROLLBACK"):
                    cur.execute("ROLLBACK")
                elif upper.startswith("SELECT"):
                    cur.execute(sql)
                    rows = cur.fetchall()
                else:
                    cur.execute(sql)
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
                log_entry = {
                    "event_id": eid,
                    "session": session_name,
                    "sql": sql,
                    "duration_ms": round(elapsed * 1000, 3),
                }
                if rows is not None:
                    log_entry["rows"] = rows
                if rows_affected is not None:
                    log_entry["rows_affected"] = rows_affected
                if error_str:
                    log_entry["error"] = error_str
                    log_entry["sqlstate"] = sqlstate
                    result.all_errors.append(f"{eid}: {error_str}")
                result.execution_log.append(log_entry)

                # Track specific events
                if eid == "foo_select":
                    result.foo_select_rows = rows
                    result.foo_select_error = error_str
                elif eid == "bar_select":
                    result.bar_select_rows = rows
                    result.bar_select_error = error_str
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


def run_concurrent_trial(
    dsn: str,
    variant: str = "update",
    delays: Optional[Dict[str, float]] = None,
) -> TrialResult:
    """
    Run one concurrent trial of the update-conflict-out workload.
    3 sessions start together via barrier, each runs its FIFO steps.
    """
    delays = delays or {}
    result = TrialResult(variant=variant)
    result_lock = threading.Lock()

    trouble_session = SESSION_TROUBLE_UPDATE if variant == "update" else SESSION_TROUBLE_DELETE

    sessions = [
        (SESSION_FOO, "foo"),
        (SESSION_BAR, "bar"),
        (trouble_session, "trouble"),
    ]

    barrier = threading.Barrier(len(sessions))
    threads: List[threading.Thread] = []

    for steps, name in sessions:
        t = threading.Thread(
            target=_run_session,
            args=(dsn, steps, name, barrier, result, delays, result_lock),
            name=f"session-{name}",
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30.0)

    # Determine bug_found predicate
    # The SSI violation requires a rw-antidependency cycle:
    #   foo reads txn0 (empty) → bar writes txn0 = rw-dep foo→bar
    #   bar reads txn1 (empty) → foo writes txn1 = rw-dep bar→foo
    # This cycle is non-serializable. On vulnerable versions, bar commits
    # without 40001. On fixed versions, 40001 is raised.
    #
    # Both selects returning 0 rows + both transactions committing = cycle present
    foo_saw_empty = (result.foo_select_rows is not None and len(result.foo_select_rows) == 0)
    bar_saw_empty = (result.bar_select_rows is not None and len(result.bar_select_rows) == 0)
    bar_committed = (result.bar_commit_error is None)
    foo_committed = (result.foo_commit_error is None)

    if not result.got_40001 and foo_saw_empty and bar_saw_empty and foo_committed and bar_committed:
        result.bug_found = True

    return result


# ---------------------------------------------------------------------------
# Baseline runner (Stage 2)
# ---------------------------------------------------------------------------

def run_baseline(
    dsn: str,
    variant: str = "update",
    trials: int = 200,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Run N trials with NO delays and report statistics."""
    if verbose:
        print(f"\n{'='*60}")
        print(f"  BASELINE: {trials} trials, variant={variant}, NO delays")
        print(f"{'='*60}")

    setup_schema(dsn)
    bugs = 0
    serialization_failures = 0
    other_errors = 0

    for i in range(1, trials + 1):
        reset_tables(dsn)
        result = run_concurrent_trial(dsn, variant=variant)
        if result.bug_found:
            bugs += 1
        if result.got_40001:
            serialization_failures += 1
        if result.all_errors and not result.got_40001:
            other_errors += 1
        if verbose and (i % 50 == 0 or i == trials):
            print(f"  Trial {i}/{trials}: bugs={bugs}, 40001={serialization_failures}, other_err={other_errors}")

    stats = {
        "trials": trials,
        "variant": variant,
        "bugs_found": bugs,
        "serialization_failures": serialization_failures,
        "other_errors": other_errors,
        "bug_rate": f"{bugs}/{trials} ({100*bugs/trials:.1f}%)" if trials > 0 else "N/A",
    }
    if verbose:
        print(f"\n  BASELINE RESULTS:")
        print(f"    Anomalies detected: {bugs}/{trials} ({100*bugs/max(1,trials):.1f}%)")
        print(f"    Serialization failures (40001): {serialization_failures}/{trials}")
        print(f"    Other errors: {other_errors}/{trials}")
        if bugs == 0:
            print(f"    → OS scheduler hides the bug: 0 hits in {trials} natural trials")
        else:
            print(f"    → Bug appeared {bugs} times (rare / OS-dependent)")
    return stats


# ---------------------------------------------------------------------------
# Anti-dependency analysis (Stage 4 core)
# ---------------------------------------------------------------------------

@dataclass
class AntiDep:
    """A read-write anti-dependency edge between two events."""
    reader_eid: str
    reader_session: str
    writer_eid: str
    writer_session: str
    table: str
    key: Any
    dep_type: str  # "rw" | "wr" | "ww"

    def __str__(self) -> str:
        return (
            f"{self.dep_type}({self.reader_eid}[{self.reader_session}] → "
            f"{self.writer_eid}[{self.writer_session}] on {self.table}[{self.key}])"
        )


def extract_table_key(sql: str) -> Optional[Tuple[str, Any]]:
    """Extract (table, key) from SQL for the update-conflict-out schema."""
    upper = sql.upper().strip()
    # SELECT * FROM txnX WHERE id = N
    m = re.search(r'FROM\s+(\w+)\s+WHERE\s+ID\s*=\s*(\d+)', upper)
    if m:
        return (m.group(1).lower(), int(m.group(2)))
    # INSERT INTO txnX SELECT N, ...  or INSERT INTO txnX VALUES (N, ...)
    m = re.search(r'INSERT\s+INTO\s+(\w+)\s+SELECT\s+(\d+)', upper)
    if m:
        return (m.group(1).lower(), int(m.group(2)))
    m = re.search(r'INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(\s*(\d+)', upper)
    if m:
        return (m.group(1).lower(), int(m.group(2)))
    # UPDATE txnX SET ... WHERE id = N
    m = re.search(r'UPDATE\s+(\w+)\s+SET\s+.+WHERE\s+ID\s*=\s*(\d+)', upper)
    if m:
        return (m.group(1).lower(), int(m.group(2)))
    # DELETE FROM txnX WHERE id = N
    m = re.search(r'DELETE\s+FROM\s+(\w+)\s+WHERE\s+ID\s*=\s*(\d+)', upper)
    if m:
        return (m.group(1).lower(), int(m.group(2)))
    return None


def classify_sql(sql: str) -> str:
    """Classify SQL as read/write/begin/commit/rollback."""
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


def analyze_antideps(trace: List[Dict[str, Any]]) -> List[AntiDep]:
    """
    Statically analyze the trace and extract anti-dependency edges.
    This is the key input for the heuristic delay engine.
    """
    # Build per-event info
    events = []
    for ev in trace:
        eid = ev.get("event_id", "")
        session = ev.get("session", "")
        sql = ev.get("sql", "")
        tk = extract_table_key(sql)
        cls = classify_sql(sql)
        events.append({
            "eid": eid,
            "session": session,
            "sql": sql,
            "table_key": tk,
            "cls": cls,
        })

    deps: List[AntiDep] = []
    # Find cross-session read/write conflicts on same (table, key)
    for i, ei in enumerate(events):
        if ei["table_key"] is None:
            continue
        for j, ej in enumerate(events):
            if i == j or ej["table_key"] is None:
                continue
            if ei["session"] == ej["session"]:
                continue
            if ei["table_key"] != ej["table_key"]:
                continue
            table, key = ei["table_key"]
            # rw: reader then writer (in different sessions)
            if ei["cls"] == "read" and ej["cls"] == "write":
                deps.append(AntiDep(
                    reader_eid=ei["eid"], reader_session=ei["session"],
                    writer_eid=ej["eid"], writer_session=ej["session"],
                    table=table, key=key, dep_type="rw",
                ))
            elif ei["cls"] == "write" and ej["cls"] == "read":
                deps.append(AntiDep(
                    reader_eid=ej["eid"], reader_session=ej["session"],
                    writer_eid=ei["eid"], writer_session=ei["session"],
                    table=table, key=key, dep_type="wr",
                ))
            elif ei["cls"] == "write" and ej["cls"] == "write":
                deps.append(AntiDep(
                    reader_eid=ei["eid"], reader_session=ei["session"],
                    writer_eid=ej["eid"], writer_session=ej["session"],
                    table=table, key=key, dep_type="ww",
                ))
    return deps


# ---------------------------------------------------------------------------
# Heuristic delay injection engine (Stage 4)
# ---------------------------------------------------------------------------

@dataclass
class DelayCandidate:
    """A candidate delay point with justification."""
    event_id: str
    session: str
    tier: str
    weight: int
    delay_range: Tuple[float, float]
    justification: str


def build_delay_candidates(
    trace: List[Dict[str, Any]],
    antideps: List[AntiDep],
) -> List[DelayCandidate]:
    """
    Build ranked delay candidates from anti-dependency analysis.
    Uses tiered weighting:
      - tier_rw_conflict: delay a read to widen the window for a conflicting write
      - tier_wr_conflict: delay a write to let a read establish its snapshot first
      - tier_commit_hold: delay COMMIT/ABORT to keep modified tuples in-flight
      - tier_ww_conflict: delay writes that conflict with other writes
      - tier_fallback: any event (low weight)
    """
    candidates: List[DelayCandidate] = []
    eid_set = {ev.get("event_id") for ev in trace}

    # Index: session -> list of (eid, sql)
    session_events: Dict[str, List[Tuple[str, str]]] = {}
    for ev in trace:
        sess = ev.get("session", "")
        eid = ev.get("event_id", "")
        sql = ev.get("sql", "")
        session_events.setdefault(sess, []).append((eid, sql))

    seen_candidates = set()

    def _add(eid: str, sess: str, tier: str, weight: int,
             delay_range: Tuple[float, float], justification: str) -> None:
        key = (eid, tier)
        if key in seen_candidates:
            return
        seen_candidates.add(key)
        candidates.append(DelayCandidate(
            event_id=eid, session=sess, tier=tier,
            weight=weight, delay_range=delay_range, justification=justification,
        ))

    # --- Tier 1: rw-antidependency targets ---
    # Delay the reader so the writer can execute first (opens conflict window)
    for dep in antideps:
        if dep.dep_type == "rw":
            _add(
                dep.reader_eid, dep.reader_session,
                "tier_rw_conflict", 10,
                (0.03, 0.15),
                f"Delay read {dep.reader_eid} to widen window for write {dep.writer_eid} "
                f"on {dep.table}[{dep.key}] (rw-antidep)",
            )
            # Also consider delaying the writer to let the reader go first
            _add(
                dep.writer_eid, dep.writer_session,
                "tier_rw_conflict_writer", 6,
                (0.02, 0.10),
                f"Delay write {dep.writer_eid} to let read {dep.reader_eid} "
                f"establish snapshot on {dep.table}[{dep.key}]",
            )

    # --- Tier 2: wr-dependency targets ---
    for dep in antideps:
        if dep.dep_type == "wr":
            _add(
                dep.reader_eid, dep.reader_session,
                "tier_wr_conflict", 7,
                (0.03, 0.12),
                f"Delay read {dep.reader_eid} after write {dep.writer_eid} "
                f"on {dep.table}[{dep.key}] (wr-dep)",
            )

    # --- Tier 3: commit/abort hold (NEW HEURISTIC) ---
    # Delaying COMMIT or ABORT keeps modified tuple versions in-flight longer,
    # which expands the window where SSI conflict checks must reason about
    # uncommitted tuple chains. This is critical for the update-conflict-out bug.
    for sess, steps in session_events.items():
        for idx, (eid, sql) in enumerate(steps):
            cls = classify_sql(sql)
            if cls in ("commit", "rollback"):
                # Check if this session has a preceding write
                has_write = any(classify_sql(s) == "write" for _, s in steps[:idx])
                if has_write:
                    _add(
                        eid, sess,
                        "tier_commit_hold", 9,
                        (0.04, 0.20),
                        f"Hold {cls} of {sess} to keep modified tuples in-flight "
                        f"(exposes SSI conflict-check edge cases)",
                    )

    # --- Tier 4: ww-conflict ---
    for dep in antideps:
        if dep.dep_type == "ww":
            _add(
                dep.writer_eid, dep.writer_session,
                "tier_ww_conflict", 5,
                (0.02, 0.08),
                f"Delay write {dep.writer_eid} conflicting with {dep.reader_eid} "
                f"on {dep.table}[{dep.key}] (ww-conflict)",
            )

    # --- Tier 5: fallback (any SQL event) ---
    for ev in trace:
        eid = ev.get("event_id", "")
        sess = ev.get("session", "")
        sql = ev.get("sql", "")
        cls = classify_sql(sql)
        if cls in ("read", "write"):
            _add(
                eid, sess,
                "tier_fallback", 1,
                (0.01, 0.05),
                f"Generic delay on {cls} event {eid}",
            )

    return candidates


def select_delays_lottery(
    candidates: List[DelayCandidate],
    rng: random.Random,
    k: int = 2,
) -> Dict[str, float]:
    """
    Select up to K delay points via weighted lottery from candidates.
    Returns {event_id: delay_seconds}.
    """
    if not candidates or k <= 0:
        return {}

    # Build weighted pool
    pool: List[Tuple[DelayCandidate, int]] = []
    for c in candidates:
        pool.extend([(c, c.weight)] * c.weight)

    selected: Dict[str, float] = {}
    used_eids: set = set()
    attempts = 0
    max_attempts = k * 20

    while len(selected) < k and attempts < max_attempts:
        attempts += 1
        idx = rng.randint(0, len(pool) - 1)
        cand = pool[idx][0]
        if cand.event_id in used_eids:
            continue
        used_eids.add(cand.event_id)
        delay = rng.uniform(cand.delay_range[0], cand.delay_range[1])
        delay = round(delay, 3)
        selected[cand.event_id] = delay

    return selected


# ---------------------------------------------------------------------------
# Oracle: bug_found predicate (Stage 3)
# ---------------------------------------------------------------------------

def check_bug_found(result: TrialResult) -> Tuple[bool, str]:
    """
    Returns (is_bug, explanation).
    Bug = all of:
      - foo_select returned 0 rows (didn't see bar's insert in txn0)
      - bar_select returned 0 rows (didn't see foo's insert in txn1)
      - foo committed successfully
      - bar committed successfully
      - no 40001 raised anywhere
    This proves a non-serializable history was accepted.
    """
    if result.got_40001:
        return False, "Correct: serialization failure (40001) raised — SSI working"

    foo_saw_empty = (result.foo_select_rows is not None and len(result.foo_select_rows) == 0)
    bar_saw_empty = (result.bar_select_rows is not None and len(result.bar_select_rows) == 0)
    foo_committed = (result.foo_commit_error is None)
    bar_committed = (result.bar_commit_error is None)

    if not foo_saw_empty:
        return False, "No cycle: foo_select saw bar's insert (serializable as bar→foo)"
    if not bar_saw_empty:
        return False, "No cycle: bar_select saw foo's insert (serializable as foo→bar)"
    if not foo_committed:
        return False, f"foo commit failed: {result.foo_commit_error}"
    if not bar_committed:
        return False, f"bar commit failed: {result.bar_commit_error}"

    return True, (
        "BUG: non-serializable history accepted — foo and bar both committed with "
        "mutual invisibility (rw-antidependency cycle) but no 40001 was raised. "
        "SSI conflict-out check missed the cycle."
    )


def check_fixed_expected(result: TrialResult) -> Tuple[bool, str]:
    """
    On a fixed version (12.4+), when the dangerous interleaving occurs,
    we expect 40001. If the transactions didn't overlap enough to form
    the cycle, that's also fine (no bug either way).
    """
    if result.got_40001:
        return True, "Correct: fixed version raised 40001 as expected"

    foo_saw_empty = (result.foo_select_rows is not None and len(result.foo_select_rows) == 0)
    bar_saw_empty = (result.bar_select_rows is not None and len(result.bar_select_rows) == 0)

    if foo_saw_empty and bar_saw_empty and result.bar_commit_error is None and result.foo_commit_error is None:
        return False, (
            "UNEXPECTED: non-serializable history accepted on fixed version — "
            "the cycle condition was met but no 40001 raised"
        )
    return True, "OK: no cycle formed (transactions serialized naturally)"


# ---------------------------------------------------------------------------
# Discovery campaign (Stage 4)
# ---------------------------------------------------------------------------

def run_discovery_campaign(
    dsn: str,
    variant: str = "update",
    k: int = 2,
    budget: int = 500,
    trials_per_mutation: int = 3,
    rng_seed: Optional[int] = None,
    verbose: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Run the heuristic discovery campaign.
    Returns the found artifact dict on success, None if budget exhausted.
    """
    rng = random.Random(rng_seed)
    trace = build_natural_trace(variant)
    antideps = analyze_antideps(trace)
    candidates = build_delay_candidates(trace, antideps)

    if verbose:
        print(f"\n{'='*60}")
        print(f"  DISCOVERY CAMPAIGN: variant={variant}, K={k}, budget={budget}")
        print(f"{'='*60}")
        print(f"\n  Anti-dependencies found ({len(antideps)}):")
        for dep in antideps:
            print(f"    {dep}")
        print(f"\n  Delay candidates ({len(candidates)}):")
        for c in candidates:
            print(f"    [{c.tier}] {c.event_id} (w={c.weight}): {c.justification}")
        print()

    setup_schema(dsn)
    total_trials = 0
    mutations_tried = 0

    for mutation_round in range(1, budget + 1):
        # Select delays via weighted lottery
        delays = select_delays_lottery(candidates, rng, k=k)
        if not delays:
            continue

        mutations_tried += 1
        if verbose and mutations_tried <= 10:
            print(f"  Mutation #{mutations_tried}: {delays}")
            for eid, d in delays.items():
                # Find the justification
                for c in candidates:
                    if c.event_id == eid:
                        print(f"    → {eid}: delay={d:.3f}s [{c.tier}] {c.justification}")
                        break

        # Run this mutation multiple times to smooth OS nondeterminism
        for trial in range(trials_per_mutation):
            total_trials += 1
            reset_tables(dsn)
            result = run_concurrent_trial(dsn, variant=variant, delays=delays)

            is_bug, explanation = check_bug_found(result)
            if is_bug:
                # Found it!
                artifact = {
                    "discovered": True,
                    "variant": variant,
                    "mutation_round": mutation_round,
                    "total_trials": total_trials,
                    "delays": delays,
                    "delay_justifications": {},
                    "result": {
                        "bug_found": True,
                        "got_40001": result.got_40001,
                        "foo_select_rows": result.foo_select_rows,
                        "bar_select_rows": result.bar_select_rows,
                        "foo_commit_error": result.foo_commit_error,
                        "bar_commit_error": result.bar_commit_error,
                        "trouble_write_rows_affected": result.trouble_write_rows_affected,
                        "all_errors": result.all_errors,
                    },
                    "explanation": explanation,
                    "timestamps": {k: v for k, v in result.timestamps.items()},
                    "execution_log": result.execution_log,
                    "trace_with_delays": _build_trace_with_delays(trace, delays),
                    "antidependencies": [str(d) for d in antideps],
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }
                # Add justifications
                for eid in delays:
                    for c in candidates:
                        if c.event_id == eid:
                            artifact["delay_justifications"][eid] = {
                                "tier": c.tier,
                                "weight": c.weight,
                                "justification": c.justification,
                            }
                            break

                if verbose:
                    print(f"\n  {'*'*50}")
                    print(f"  BUG FOUND in mutation #{mutation_round}, trial {trial+1}!")
                    print(f"  Total trials: {total_trials}")
                    print(f"  Delays: {delays}")
                    print(f"  Explanation: {explanation}")
                    print(f"  {'*'*50}")

                    # Print schedule observation
                    if result.timestamps:
                        print(f"\n  Schedule observation (statement start times):")
                        sorted_ts = sorted(result.timestamps.items(), key=lambda x: x[1])
                        t0 = sorted_ts[0][1] if sorted_ts else 0
                        for eid, ts in sorted_ts:
                            print(f"    {eid:>20}: +{(ts-t0)*1000:.1f}ms")

                return artifact

        if verbose and mutations_tried % 50 == 0:
            print(f"  ... {mutations_tried} mutations, {total_trials} trials so far")

    if verbose:
        print(f"\n  Budget exhausted: {mutations_tried} mutations, {total_trials} trials, no bug found")
    return None


def _build_trace_with_delays(
    trace: List[Dict[str, Any]],
    delays: Dict[str, float],
) -> List[Dict[str, Any]]:
    """Build a copy of the trace with delay tags injected."""
    result = copy.deepcopy(trace)
    for ev in result:
        eid = ev.get("event_id", "")
        if eid in delays:
            ev["delay"] = delays[eid]
    return result


# ---------------------------------------------------------------------------
# Replay a found trace (for validation)
# ---------------------------------------------------------------------------

def replay_found_trace(
    dsn: str,
    artifact: Dict[str, Any],
    trials: int = 20,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Replay the found mutated trace N times and collect stats."""
    delays = artifact.get("delays", {})
    variant = artifact.get("variant", "update")

    if verbose:
        print(f"\n  Replaying found trace: {trials} trials, variant={variant}")
        print(f"  Delays: {delays}")

    setup_schema(dsn)
    bugs = 0
    failures_40001 = 0

    for i in range(1, trials + 1):
        reset_tables(dsn)
        result = run_concurrent_trial(dsn, variant=variant, delays=delays)
        is_bug, _ = check_bug_found(result)
        if is_bug:
            bugs += 1
        if result.got_40001:
            failures_40001 += 1

    stats = {
        "trials": trials,
        "bugs": bugs,
        "failures_40001": failures_40001,
        "bug_rate": f"{bugs}/{trials} ({100*bugs/max(1,trials):.1f}%)",
    }

    if verbose:
        print(f"  Results: {bugs}/{trials} bugs, {failures_40001}/{trials} 40001")

    return stats


# ---------------------------------------------------------------------------
# Cross-version validation (Stage 5)
# ---------------------------------------------------------------------------

def run_cross_version_validation(
    artifact: Dict[str, Any],
    replay_trials: int = 20,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Replay the found trace on both 12.3 and 12.4."""
    report: Dict[str, Any] = {}

    for version in ("12.3", "12.4"):
        dsn = dsn_for_pg(version)
        if verbose:
            print(f"\n{'#'*60}")
            print(f"#  PostgreSQL {version}")
            print(f"{'#'*60}")

        if not wait_for_pg(dsn, timeout=15):
            if verbose:
                print(f"  PostgreSQL {version} not available — skipping")
            report[version] = {"error": "not available"}
            continue

        stats = replay_found_trace(dsn, artifact, trials=replay_trials, verbose=verbose)
        report[version] = stats

    return report


# ---------------------------------------------------------------------------
# Full demo flow (Stage 5)
# ---------------------------------------------------------------------------

def run_full_demo(
    variant: str = "update",
    baseline_trials: int = 200,
    k: int = 2,
    budget: int = 500,
    replay_trials: int = 20,
    rng_seed: Optional[int] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Full discovery story:
    1) Baseline on 12.3 (no delays)
    2) Discovery campaign on 12.3
    3) Replay found trace on 12.3 (confirm reproducibility)
    4) Replay found trace on 12.4 (confirm fix)
    """
    report: Dict[str, Any] = {"variant": variant}
    dsn_123 = dsn_for_pg("12.3")
    dsn_124 = dsn_for_pg("12.4")

    # Check containers
    if verbose:
        print("\nChecking PostgreSQL containers...")
    for version, dsn in [("12.3", dsn_123), ("12.4", dsn_124)]:
        if verbose:
            print(f"  Waiting for PostgreSQL {version}...", end=" ", flush=True)
        if wait_for_pg(dsn, timeout=30):
            if verbose:
                print("ready.")
        else:
            if verbose:
                print("NOT AVAILABLE.")
            if version == "12.3":
                print("ERROR: PostgreSQL 12.3 is required. Start with: "
                      "docker compose -f docker-compose-repro.yml up -d")
                return {"error": "pg 12.3 not available"}

    # Step 1: Baseline
    if verbose:
        print(f"\n{'='*60}")
        print(f"  STEP 1: BASELINE (natural scheduling, no delays)")
        print(f"{'='*60}")
    report["baseline_12_3"] = run_baseline(dsn_123, variant=variant, trials=baseline_trials, verbose=verbose)

    # Also run baseline on 12.4 for comparison
    if wait_for_pg(dsn_124, timeout=5):
        report["baseline_12_4"] = run_baseline(dsn_124, variant=variant, trials=baseline_trials, verbose=verbose)

    # Step 2: Discovery
    if verbose:
        print(f"\n{'='*60}")
        print(f"  STEP 2: DISCOVERY CAMPAIGN (heuristic delay injection)")
        print(f"{'='*60}")
    artifact = run_discovery_campaign(
        dsn_123, variant=variant, k=k, budget=budget,
        rng_seed=rng_seed, verbose=verbose,
    )

    if artifact is None:
        report["discovery"] = {"found": False}
        if verbose:
            print("\n  FAILED: Could not discover the anomaly within budget.")
            print("  Try increasing --budget or --k.")
        return report

    report["discovery"] = {
        "found": True,
        "mutation_round": artifact["mutation_round"],
        "total_trials": artifact["total_trials"],
        "delays": artifact["delays"],
    }

    # Save artifact
    results_dir = ROOT / "results" / "update-conflict-out"
    results_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = results_dir / "found.json"
    with artifact_path.open("w") as f:
        json.dump(artifact, f, indent=2)
    if verbose:
        print(f"\n  Artifact saved to: {artifact_path}")

    # Step 3: Replay on 12.3 (confirm reproducibility)
    if verbose:
        print(f"\n{'='*60}")
        print(f"  STEP 3: REPLAY ON 12.3 (confirm reproducibility)")
        print(f"{'='*60}")
    report["replay_12_3"] = replay_found_trace(
        dsn_123, artifact, trials=replay_trials, verbose=verbose,
    )

    # Step 4: Replay on 12.4 (confirm fix)
    if wait_for_pg(dsn_124, timeout=5):
        if verbose:
            print(f"\n{'='*60}")
            print(f"  STEP 4: REPLAY ON 12.4 (confirm fix)")
            print(f"{'='*60}")
        report["replay_12_4"] = replay_found_trace(
            dsn_124, artifact, trials=replay_trials, verbose=verbose,
        )

    # Summary
    if verbose:
        print(f"\n{'='*60}")
        print(f"  SUMMARY")
        print(f"{'='*60}")

        baseline = report.get("baseline_12_3", {})
        baseline_bugs = baseline.get("bugs_found", 0)
        baseline_total = baseline.get("trials", 0)

        disc = report.get("discovery", {})
        disc_found = disc.get("found", False)
        disc_trials = disc.get("total_trials", "?")

        replay_123 = report.get("replay_12_3", {})
        replay_123_bugs = replay_123.get("bugs", 0)
        replay_123_total = replay_123.get("trials", 0)

        replay_124 = report.get("replay_12_4", {})
        replay_124_bugs = replay_124.get("bugs", 0)
        replay_124_40001 = replay_124.get("failures_40001", 0)
        replay_124_total = replay_124.get("trials", 0)

        print(f"  baseline hidden:     {baseline_bugs}/{baseline_total} anomalies in natural trials")
        print(f"  fuzzer discovered:   {'YES' if disc_found else 'NO'} (in {disc_trials} trials)")
        if disc_found:
            print(f"  12.3 vulnerable:     {replay_123_bugs}/{replay_123_total} reproductions")
            if replay_124_total > 0:
                print(f"  12.4 fixed:          {replay_124_bugs}/{replay_124_total} bugs, "
                      f"{replay_124_40001}/{replay_124_total} got 40001")
            print(f"\n  Delays that triggered the bug:")
            for eid, d in disc.get("delays", {}).items():
                just = artifact.get("delay_justifications", {}).get(eid, {})
                print(f"    {eid}: {d:.3f}s [{just.get('tier', '?')}]")
                if just.get("justification"):
                    print(f"      → {just['justification']}")

    # Save full report
    report_path = results_dir / "report.json"
    try:
        with report_path.open("w") as f:
            json.dump(report, f, indent=2, default=str)
    except Exception:
        pass

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Discovery engine for PostgreSQL SSI update-conflict-out anomaly.",
    )
    parser.add_argument(
        "--mode",
        default="full",
        choices=["baseline", "discover", "validate", "full"],
        help="Execution mode.",
    )
    parser.add_argument(
        "--pg",
        default=None,
        choices=["12.3", "12.4"],
        help="PostgreSQL version (uses default ports 54323/54324).",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Override PostgreSQL DSN.",
    )
    parser.add_argument(
        "--variant",
        default="update",
        choices=["update", "delete"],
        help="Trouble session variant (default: update).",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=200,
        help="Number of baseline trials (default: 200).",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=3,
        help="Maximum delay points per mutation (default: 3).",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=500,
        help="Maximum mutation rounds (default: 500).",
    )
    parser.add_argument(
        "--replay-trials",
        type=int,
        default=20,
        help="Trials for replay validation (default: 20).",
    )
    parser.add_argument(
        "--trace",
        type=Path,
        default=None,
        help="Path to found artifact JSON for validate mode.",
    )
    parser.add_argument(
        "--rng-seed",
        type=int,
        default=None,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
    )
    parser.add_argument(
        "--no-delays",
        action="store_true",
        help="Force no-delay baseline mode (alias for --mode baseline).",
    )
    args = parser.parse_args()

    verbose = not args.quiet
    if args.no_delays:
        args.mode = "baseline"

    dsn = args.dsn
    if dsn is None and args.pg:
        dsn = dsn_for_pg(args.pg)
    elif dsn is None:
        dsn = dsn_for_pg("12.3")

    if args.mode == "baseline":
        if verbose:
            print(f"Waiting for Postgres...", end=" ", flush=True)
        if not wait_for_pg(dsn, timeout=30):
            print("TIMEOUT — cannot connect.")
            return 2
        if verbose:
            print("ready.")

        stats = run_baseline(dsn, variant=args.variant, trials=args.trials, verbose=verbose)
        if args.quiet:
            print(json.dumps(stats, indent=2))
        return 0

    elif args.mode == "discover":
        if verbose:
            print(f"Waiting for Postgres...", end=" ", flush=True)
        if not wait_for_pg(dsn, timeout=30):
            print("TIMEOUT")
            return 2
        if verbose:
            print("ready.")

        artifact = run_discovery_campaign(
            dsn, variant=args.variant, k=args.k, budget=args.budget,
            rng_seed=args.rng_seed, verbose=verbose,
        )
        if artifact:
            results_dir = ROOT / "results" / "update-conflict-out"
            results_dir.mkdir(parents=True, exist_ok=True)
            path = results_dir / "found.json"
            with path.open("w") as f:
                json.dump(artifact, f, indent=2)
            print(f"\nArtifact saved to: {path}")
            return 0
        else:
            print("\nBug not found within budget.")
            return 1

    elif args.mode == "validate":
        trace_path = args.trace
        if trace_path is None:
            trace_path = ROOT / "results" / "update-conflict-out" / "found.json"
        if not trace_path.exists():
            print(f"Artifact not found: {trace_path}")
            return 2
        with trace_path.open() as f:
            artifact = json.load(f)
        report = run_cross_version_validation(artifact, replay_trials=args.replay_trials, verbose=verbose)
        print(json.dumps(report, indent=2, default=str))
        return 0

    elif args.mode == "full":
        report = run_full_demo(
            variant=args.variant,
            baseline_trials=args.trials,
            k=args.k,
            budget=args.budget,
            replay_trials=args.replay_trials,
            rng_seed=args.rng_seed,
            verbose=verbose,
        )
        return 0 if report.get("discovery", {}).get("found") else 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
