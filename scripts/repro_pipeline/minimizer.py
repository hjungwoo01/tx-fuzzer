"""
Trace + delay minimization — produce deterministic reproducers.

Two phases:
1. Event minimization: remove events while preserving per-session FIFO
   and transaction structure, keeping only what's needed to reproduce.
2. Delay minimization: remove/reduce delays while maintaining reproducibility.
"""

from __future__ import annotations

import copy
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from scripts.repro_pipeline.seed_gen import TraceEvent


@dataclass
class MinimizationResult:
    """Result of trace+delay minimization."""
    original_event_count: int = 0
    minimized_event_count: int = 0
    original_delay_count: int = 0
    minimized_delay_count: int = 0
    events: List[TraceEvent] = field(default_factory=list)
    delays: Dict[str, float] = field(default_factory=dict)
    reduction_ratio: float = 0.0
    replay_success_rate: float = 0.0


def _is_fifo_valid(events: List[TraceEvent]) -> bool:
    """Check that per-session FIFO order is preserved and txns are complete."""
    session_state: Dict[str, str] = {}  # session -> "idle" | "active"

    for e in events:
        sid = e.session
        state = session_state.get(sid, "idle")

        if e.type == "begin":
            if state == "active":
                return False  # nested begin without commit/rollback
            session_state[sid] = "active"
        elif e.type in ("commit", "rollback"):
            if state != "active":
                return False  # commit/rollback without begin
            session_state[sid] = "idle"
        elif e.type == "sql":
            if state != "active":
                return False  # sql outside transaction
    return True


def _complete_transactions(events: List[TraceEvent]) -> List[TraceEvent]:
    """Ensure all transactions are complete (add missing commit/rollback)."""
    result = list(events)
    session_active: Dict[str, bool] = {}
    for e in result:
        if e.type == "begin":
            session_active[e.session] = True
        elif e.type in ("commit", "rollback"):
            session_active[e.session] = False

    # Add commits for unclosed txns
    max_eid = max((int(e.event_id[1:]) for e in result if e.event_id.startswith("e")), default=0)
    for sid, active in session_active.items():
        if active:
            max_eid += 1
            result.append(TraceEvent(
                session=sid, event_id=f"e{max_eid}",
                type="commit",
            ))
    return result


def _try_remove_events(
    events: List[TraceEvent],
    indices_to_remove: List[int],
) -> Optional[List[TraceEvent]]:
    """Try removing events at given indices, return valid trace or None."""
    remaining = [e for i, e in enumerate(events) if i not in set(indices_to_remove)]
    remaining = _complete_transactions(remaining)
    if _is_fifo_valid(remaining):
        return remaining
    return None


def minimize_events(
    events: List[TraceEvent],
    delays: Dict[str, float],
    test_fn: Callable[[List[TraceEvent], Dict[str, float]], bool],
    max_iterations: int = 50,
) -> Tuple[List[TraceEvent], Dict[str, float]]:
    """Minimize trace events using ddmin-style binary search.

    Args:
        events: Original trace events.
        delays: Delay map {event_id: seconds}.
        test_fn: Function that returns True if bug still reproduces.
        max_iterations: Maximum minimization iterations.

    Returns:
        (minimized_events, filtered_delays)
    """
    current = list(events)
    n = 2  # granularity

    for _ in range(max_iterations):
        # Find removable SQL events (not begin/commit/rollback)
        removable_indices = [
            i for i, e in enumerate(current)
            if e.type == "sql"
        ]

        if not removable_indices:
            break

        chunk_size = max(1, len(removable_indices) // n)
        chunks = [
            removable_indices[i:i + chunk_size]
            for i in range(0, len(removable_indices), chunk_size)
        ]

        reduced = False
        for chunk in chunks:
            candidate = _try_remove_events(current, chunk)
            if candidate is None:
                continue
            # Filter delays to only reference remaining event IDs
            remaining_eids = {e.event_id for e in candidate}
            filtered_delays = {k: v for k, v in delays.items() if k in remaining_eids}
            if test_fn(candidate, filtered_delays):
                current = candidate
                delays = filtered_delays
                reduced = True
                break

        if not reduced:
            if n >= len(removable_indices):
                break  # Can't reduce further
            n = min(n * 2, len(removable_indices))

    # Remove empty transactions (begin immediately followed by commit/rollback)
    current = _remove_empty_txns(current)
    remaining_eids = {e.event_id for e in current}
    delays = {k: v for k, v in delays.items() if k in remaining_eids}

    return current, delays


def _remove_empty_txns(events: List[TraceEvent]) -> List[TraceEvent]:
    """Remove transactions that contain no SQL events."""
    # Find begin/end pairs with no SQL between them
    result: List[TraceEvent] = []
    i = 0
    while i < len(events):
        if events[i].type == "begin":
            # Look ahead for the matching commit/rollback
            j = i + 1
            has_sql = False
            while j < len(events) and events[j].session == events[i].session:
                if events[j].type == "sql":
                    has_sql = True
                    break
                if events[j].type in ("commit", "rollback"):
                    break
                j += 1
            if not has_sql and j < len(events) and events[j].type in ("commit", "rollback"):
                # Skip this empty transaction
                i = j + 1
                continue
        result.append(events[i])
        i += 1
    return result


def minimize_delays(
    events: List[TraceEvent],
    delays: Dict[str, float],
    test_fn: Callable[[List[TraceEvent], Dict[str, float]], bool],
    min_delay_ms: float = 0.5,
) -> Dict[str, float]:
    """Minimize delay magnitudes while maintaining reproducibility.

    1. Try removing each delay entirely
    2. Binary search on remaining delay magnitudes

    Args:
        events: Trace events.
        delays: Current delay map.
        test_fn: Returns True if bug still reproduces.
        min_delay_ms: Minimum delay magnitude in milliseconds.
    """
    current = dict(delays)

    # Phase 1: Remove each delay
    for eid in list(current.keys()):
        candidate = {k: v for k, v in current.items() if k != eid}
        if test_fn(events, candidate):
            current = candidate

    # Phase 2: Binary search on each remaining delay
    for eid in list(current.keys()):
        lo = min_delay_ms / 1000.0
        hi = current[eid]
        # Try halving
        for _ in range(8):  # max 8 binary search steps
            mid = (lo + hi) / 2.0
            if mid < lo + 0.0001:
                break
            candidate = dict(current)
            candidate[eid] = mid
            if test_fn(events, candidate):
                hi = mid
                current[eid] = mid
            else:
                lo = mid

    return current


def run_minimization(
    events: List[TraceEvent],
    delays: Dict[str, float],
    test_fn: Callable[[List[TraceEvent], Dict[str, float]], bool],
    max_event_iters: int = 50,
    replays_for_verify: int = 3,
) -> MinimizationResult:
    """Run full event + delay minimization.

    Args:
        events: Original found trace.
        delays: Original found delays.
        test_fn: Returns True if bug reproduces with given (events, delays).
        max_event_iters: Max iterations for event minimization.
        replays_for_verify: Replays to test during minimization (majority vote).

    Returns:
        MinimizationResult with minimized trace and delays.
    """
    original_event_count = len(events)
    original_delay_count = len(delays)

    # Wrap test_fn with majority-vote for robustness
    def robust_test(evts: List[TraceEvent], dlys: Dict[str, float]) -> bool:
        successes = sum(1 for _ in range(replays_for_verify) if test_fn(evts, dlys))
        return successes > replays_for_verify // 2

    # Phase 1: Event minimization
    min_events, min_delays = minimize_events(
        events, delays, robust_test, max_iterations=max_event_iters,
    )

    # Phase 2: Delay minimization
    min_delays = minimize_delays(min_events, min_delays, robust_test)

    result = MinimizationResult(
        original_event_count=original_event_count,
        minimized_event_count=len(min_events),
        original_delay_count=original_delay_count,
        minimized_delay_count=len(min_delays),
        events=min_events,
        delays=min_delays,
    )
    if original_event_count > 0:
        result.reduction_ratio = 1.0 - (len(min_events) / original_event_count)

    return result
