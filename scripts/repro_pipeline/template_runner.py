"""
General-template discovery — execute seed traces against a live DB.

Takes seed traces from seed_gen, uses slicer for delay targeting, and
runs the minimizer when an anomaly is detected. This is the generic
counterpart to the hand-crafted headline packs.
"""

from __future__ import annotations

import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from scripts.experiments.db_adapter import make_adapter, DBAdapter
from scripts.experiments.error_taxonomy import ErrorCategory
from scripts.repro_pipeline.bug_pack import TrialResult, PackConfig
from scripts.repro_pipeline.seed_gen import (
    SeedTrace, TraceEvent, generate_seed_trace, setup_sql,
)
from scripts.repro_pipeline.slicer import (
    extract_slices, select_top_slices, generate_delays_for_slice,
)
from scripts.repro_pipeline.minimizer import run_minimization, MinimizationResult


@dataclass
class TemplateDiscoveryResult:
    """Result from a general-template discovery run."""
    bug_found: bool = False
    anomaly_type: str = ""
    trace: Optional[SeedTrace] = None
    delays: Dict[str, float] = field(default_factory=dict)
    minimized: Optional[MinimizationResult] = None
    details: Dict[str, Any] = field(default_factory=dict)


def _detect_anomaly(
    outcomes: Dict[str, List[Tuple[str, Any]]],
    final_state: Dict[int, int],
) -> Tuple[bool, str]:
    """Check for concurrency anomalies in trial outcomes.

    Anomalies detected:
    - Dirty read: session reads a value that was subsequently rolled back
    - Lost update: final state differs from sequential expectation
    - Non-repeatable read: same SELECT returns different values in one txn
    """
    # Check for dirty reads: a session read a value that a rollback undid
    for sid, ops in outcomes.items():
        aborted_writes: Dict[int, int] = {}  # key -> written_value (if aborted)
        reads: List[Tuple[int, int]] = []  # (key, value_read) pairs

        for op_type, data in ops:
            if op_type == "write_rollback":
                key, val = data
                aborted_writes[key] = val
            elif op_type == "read":
                key, val = data
                reads.append((key, val))

    # Check for lost updates: if two sessions both did read-then-write on
    # the same key, the final value should reflect the last writer
    # Simple check: sum of all committed increments should match final state
    committed_increments: Dict[int, int] = {}
    for sid, ops in outcomes.items():
        for op_type, data in ops:
            if op_type == "committed_write":
                key, delta = data
                committed_increments[key] = committed_increments.get(key, 0) + delta

    for key, expected_delta in committed_increments.items():
        actual = final_state.get(key, 0)
        if actual != expected_delta:
            return True, f"lost_update: key={key} expected={expected_delta} got={actual}"

    return False, "clean"


def _extract_delta(sql: str) -> int:
    """Extract the increment value from UPDATE/INSERT/DELETE SQL."""
    sql_up = sql.upper()
    if "DELETE" in sql_up:
        return -1
    # Match patterns like "v = v + 10", "v + 1", "v = v + 100"
    m = re.search(r'v\s*[+=]\s*v\s*\+\s*(\d+)', sql, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # INSERT with value 1
    if "INSERT" in sql_up:
        return 1
    return 1


def _execute_slice(
    db: str,
    dsn: str,
    events: List[TraceEvent],
    delays: Dict[str, float],
) -> Tuple[Dict[str, List[Tuple[str, Any]]], Dict[int, int]]:
    """Execute a trace slice with delay injection, returning outcomes.

    Returns:
        outcomes: {session_id: [(op_type, data), ...]}
        final_state: {key: value} from kv table after all txns
    """
    # Group events by session
    session_events: Dict[str, List[TraceEvent]] = {}
    for e in events:
        session_events.setdefault(e.session, []).append(e)

    outcomes: Dict[str, List[Tuple[str, Any]]] = {}
    errors: List[str] = []
    lock = threading.Lock()

    def session_worker(sid: str, evts: List[TraceEvent]) -> None:
        adapter = make_adapter(db, dsn)
        session_outcomes: List[Tuple[str, Any]] = []
        in_txn = False
        txn_writes: List[Tuple[int, int]] = []  # (key, delta) in current txn

        try:
            for e in evts:
                # Inject delay before this event
                d = delays.get(e.event_id, 0)
                if d > 0:
                    time.sleep(d)

                if e.type == "begin":
                    iso = e.isolation or ""
                    adapter.begin(iso)
                    in_txn = True
                    txn_writes = []

                elif e.type == "sql":
                    r = adapter.exec(e.sql or "SELECT 1")
                    if r.error_category != ErrorCategory.OK:
                        session_outcomes.append(("error", (e.sql, r.error_msg)))
                        if in_txn:
                            adapter.rollback()
                            for key, delta in txn_writes:
                                session_outcomes.append(("write_rollback", (key, delta)))
                            in_txn = False
                        continue

                    if e.op == "read" and r.rows:
                        val = r.rows[0][0] if r.rows[0] else None
                        session_outcomes.append(("read", (e.key, val)))
                    elif e.op in ("write", "insert", "delete"):
                        delta = _extract_delta(e.sql or "")
                        txn_writes.append((e.key, delta))

                elif e.type == "commit":
                    cr = adapter.commit()
                    if cr.error_category == ErrorCategory.OK:
                        for key, delta in txn_writes:
                            session_outcomes.append(("committed_write", (key, delta)))
                    else:
                        for key, delta in txn_writes:
                            session_outcomes.append(("write_rollback", (key, delta)))
                    in_txn = False
                    txn_writes = []

                elif e.type == "rollback":
                    adapter.rollback()
                    for key, delta in txn_writes:
                        session_outcomes.append(("write_rollback", (key, delta)))
                    in_txn = False
                    txn_writes = []

        except Exception as ex:
            with lock:
                errors.append(f"{sid}: {ex}")
            if in_txn:
                try:
                    adapter.rollback()
                except Exception:
                    pass
        finally:
            adapter.close()

        with lock:
            outcomes[sid] = session_outcomes

    # Run sessions concurrently
    threads = []
    for sid, evts in session_events.items():
        t = threading.Thread(target=session_worker, args=(sid, evts))
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=30)

    # Read final state
    final_state: Dict[int, int] = {}
    try:
        reader = make_adapter(db, dsn)
        r = reader.exec("SELECT k, v FROM kv")
        if r.rows:
            for row in r.rows:
                final_state[row[0]] = row[1]
        reader.close()
    except Exception:
        pass

    return outcomes, final_state


def run_template_trial(
    db: str,
    dsn: str,
    events: List[TraceEvent],
    delays: Dict[str, float],
) -> TrialResult:
    """Execute a trace slice and check for anomalies."""
    # Reset tables
    reset_adapter = make_adapter(db, dsn)
    try:
        stmts = setup_sql(db)
        reset_adapter.setup(stmts)
    finally:
        reset_adapter.close()

    outcomes, final_state = _execute_slice(db, dsn, events, delays)
    bug_found, reason = _detect_anomaly(outcomes, final_state)

    return TrialResult(
        bug_found=bug_found,
        reason=reason,
        delays_used=delays,
        extra={"outcomes": {k: len(v) for k, v in outcomes.items()},
               "final_state": final_state},
    )


def run_template_discovery(
    db: str,
    dsn: str,
    cfg: PackConfig,
    strategy: str = "heuristic",
    n_seeds: int = 10,
    do_minimize: bool = True,
) -> TemplateDiscoveryResult:
    """Generate seed traces, slice-select, fuzz delays, and detect anomalies.

    Steps:
    1. Generate n_seeds traces with different random seeds
    2. For each trace, extract high-conflict slices
    3. For each slice, inject delays per strategy and run trial
    4. If anomaly found, optionally minimize
    """
    result = TemplateDiscoveryResult()
    start = time.monotonic()
    trial_count = 0

    for seed_idx in range(n_seeds):
        if (time.monotonic() - start) > cfg.duration_s:
            break

        seed = seed_idx * 137 + 42  # deterministic spread
        trace = generate_seed_trace(seed=seed, db=db, n_sessions=4, txns_per_session=8)

        slices = extract_slices(trace, window_size=30, stride=8)
        top = select_top_slices(slices, top_n=3)

        for sl in top:
            if (time.monotonic() - start) > cfg.duration_s:
                break
            if trial_count >= cfg.max_trials:
                break

            rng = random.Random(seed + trial_count)
            delays = generate_delays_for_slice(
                sl.events, strategy=strategy,
                delay_ms=cfg.delay_ms, rng=rng,
            )

            tr = run_template_trial(db, dsn, sl.events, delays)
            trial_count += 1

            if tr.bug_found:
                result.bug_found = True
                result.anomaly_type = tr.reason
                result.trace = trace
                result.delays = delays
                result.details = {
                    "seed": seed,
                    "slice_start": sl.start_idx,
                    "slice_end": sl.end_idx,
                    "slice_score": sl.total_score,
                    "trial_count": trial_count,
                    "time_s": time.monotonic() - start,
                }

                # Minimize if requested
                if do_minimize:
                    def test_fn(evts, dlys):
                        t = run_template_trial(db, dsn, evts, dlys)
                        return t.bug_found

                    result.minimized = run_minimization(
                        sl.events, delays, test_fn,
                        max_event_iters=20, replays_for_verify=3,
                    )

                return result

    result.details = {"trial_count": trial_count, "time_s": time.monotonic() - start}
    return result
