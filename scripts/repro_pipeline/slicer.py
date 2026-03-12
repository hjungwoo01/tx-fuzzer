"""
Slice selection + schedule fuzzing for general template seeds.

Two-level search:
1. Score candidate trace slices by conflict density features
2. Allocate fuzz budget across top-N slices
3. For each slice, run heuristic delay injection
"""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from scripts.repro_pipeline.seed_gen import SeedTrace, TraceEvent


@dataclass
class SliceScore:
    """Feature scores for a trace slice."""
    start_idx: int = 0
    end_idx: int = 0
    cross_session_conflicts: int = 0
    rw_antidep_count: int = 0
    abort_after_write_count: int = 0
    multi_table_count: int = 0
    total_score: float = 0.0
    events: List[TraceEvent] = field(default_factory=list)

    def compute_total(self) -> float:
        self.total_score = (
            3.0 * self.cross_session_conflicts
            + 2.0 * self.rw_antidep_count
            + 1.0 * self.abort_after_write_count
            + 1.0 * self.multi_table_count
        )
        return self.total_score


def _count_cross_session_conflicts(events: List[TraceEvent]) -> int:
    """Count key accesses shared by multiple sessions."""
    key_sessions: Dict[Tuple[Optional[str], Optional[int]], set] = defaultdict(set)
    for e in events:
        if e.type == "sql" and e.key is not None:
            key_sessions[(e.table, e.key)].add(e.session)
    return sum(1 for sessions in key_sessions.values() if len(sessions) > 1)


def _count_rw_antideps(events: List[TraceEvent]) -> int:
    """Estimate rw-antidependency potential: count (read, write) pairs
    on the same key from different sessions.
    """
    # Group events by (table, key)
    key_ops: Dict[Tuple[Optional[str], Optional[int]], List[Tuple[str, str]]] = defaultdict(list)
    for e in events:
        if e.type == "sql" and e.key is not None and e.op:
            key_ops[(e.table, e.key)].append((e.session, e.op))

    count = 0
    for ops in key_ops.values():
        readers = {s for s, o in ops if o == "read"}
        writers = {s for s, o in ops if o in ("write", "insert", "delete")}
        # rw-antidep: reader in session A, writer in session B
        for r in readers:
            for w in writers:
                if r != w:
                    count += 1
    return count


def _count_aborts(events: List[TraceEvent]) -> int:
    """Count rollback events that follow a write (abort-after-write)."""
    count = 0
    session_has_write: Dict[str, bool] = {}
    for e in events:
        if e.type == "sql" and e.op in ("write", "insert", "delete"):
            session_has_write[e.session] = True
        elif e.type == "rollback" and session_has_write.get(e.session, False):
            count += 1
            session_has_write[e.session] = False
        elif e.type in ("begin", "commit"):
            session_has_write[e.session] = False
    return count


def _count_multi_table(events: List[TraceEvent]) -> int:
    """Count transactions that touch multiple tables."""
    txn_tables: Dict[str, set] = defaultdict(set)
    current_txn: Dict[str, str] = {}
    txn_counter = 0

    for e in events:
        if e.type == "begin":
            txn_counter += 1
            current_txn[e.session] = f"txn_{txn_counter}"
        elif e.type == "sql" and e.table and e.session in current_txn:
            txn_tables[current_txn[e.session]].add(e.table)
        elif e.type in ("commit", "rollback"):
            current_txn.pop(e.session, None)

    return sum(1 for tables in txn_tables.values() if len(tables) > 1)


def score_slice(events: List[TraceEvent], start_idx: int = 0, end_idx: int = 0) -> SliceScore:
    """Score a trace slice by conflict density features."""
    s = SliceScore(start_idx=start_idx, end_idx=end_idx, events=events)
    s.cross_session_conflicts = _count_cross_session_conflicts(events)
    s.rw_antidep_count = _count_rw_antideps(events)
    s.abort_after_write_count = _count_aborts(events)
    s.multi_table_count = _count_multi_table(events)
    s.compute_total()
    return s


def extract_slices(
    trace: SeedTrace,
    window_size: int = 40,
    stride: int = 10,
) -> List[SliceScore]:
    """Extract and score sliding-window slices from a seed trace."""
    events = trace.events
    slices: List[SliceScore] = []
    for start in range(0, max(1, len(events) - window_size + 1), stride):
        end = min(start + window_size, len(events))
        chunk = events[start:end]
        s = score_slice(chunk, start_idx=start, end_idx=end)
        slices.append(s)
    return slices


def select_top_slices(
    slices: List[SliceScore],
    top_n: int = 5,
) -> List[SliceScore]:
    """Return the top-N slices by total score."""
    ranked = sorted(slices, key=lambda s: s.total_score, reverse=True)
    return ranked[:top_n]


def generate_delays_for_slice(
    events: List[TraceEvent],
    strategy: str = "heuristic",
    delay_ms: float = 5.0,
    rng: Optional[random.Random] = None,
    heuristic_k: int = 3,
) -> Dict[str, float]:
    """Generate delay injection points for a trace slice.

    Returns: {event_id: delay_seconds}
    """
    if rng is None:
        rng = random.Random()

    delays: Dict[str, float] = {}

    if strategy == "stress":
        return delays

    if strategy == "random":
        sql_events = [e for e in events if e.type == "sql"]
        for e in sql_events:
            delays[e.event_id] = rng.uniform(0, delay_ms / 1000.0)
        return delays

    if strategy == "heuristic":
        # Target events that precede cross-session rw-antidependencies
        # Score each SQL event by how many cross-session conflicts it's in
        event_scores: Dict[str, float] = {}
        key_sessions: Dict[Tuple[Optional[str], Optional[int]], List[TraceEvent]] = defaultdict(list)

        for e in events:
            if e.type == "sql" and e.key is not None:
                key_sessions[(e.table, e.key)].append(e)

        for key_events in key_sessions.values():
            sessions = {e.session for e in key_events}
            if len(sessions) > 1:
                # Multi-session conflict — score each participant
                for e in key_events:
                    score = 1.0
                    if e.op == "read":
                        score = 2.0  # reads before writes are prime delay targets
                    if e.op in ("write", "insert", "delete"):
                        score = 1.5
                    event_scores[e.event_id] = event_scores.get(e.event_id, 0) + score

        # Pick top-K scored events
        ranked = sorted(event_scores.items(), key=lambda x: x[1], reverse=True)
        for eid, score in ranked[:heuristic_k]:
            # Higher score → longer delay
            base = delay_ms / 1000.0
            delays[eid] = base * min(score, 3.0) / 3.0 + rng.uniform(0, base * 0.1)

        return delays

    return delays
