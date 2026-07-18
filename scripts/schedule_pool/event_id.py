"""
Stable event ID generation for the schedule exploration workflow.

Event IDs are deterministic within a workload trace given the same seed.
Format: s{session}_t{txn_idx}_q{query_idx}_{phase}

Phases:
  - begin     : transaction begin
  - exec      : SQL statement execution
  - commit    : transaction commit
  - rollback  : transaction rollback/abort
  - barrier   : synchronization barrier wait

For micro-workload runner (Python threads):
  session = worker_id
  txn_idx = 0 (one txn per trial in micro workloads)
  query_idx = incremental within the transaction
  phase = begin/exec/commit/rollback

Requirements:
  - same base workload + same seed → same event ids across runs
  - human-readable for debugging
  - survives serialization and minimization (plain strings)
"""

from __future__ import annotations

from typing import Dict, List, Optional

from .types import Event, Phase


def make_event_id(
    session: int,
    txn_idx: int = 0,
    query_idx: int = 0,
    phase: str = "exec",
) -> str:
    """
    Create a deterministic, human-readable event identifier.

    Examples:
      make_event_id(0, 0, 0, "begin")   → "s0_t0_q0_begin"
      make_event_id(2, 0, 1, "exec")    → "s2_t0_q1_exec"
      make_event_id(1, 0, 3, "commit")  �� "s1_t0_q3_commit"
    """
    return f"s{session}_t{txn_idx}_q{query_idx}_{phase}"


def make_event(
    session: int,
    txn_idx: int = 0,
    query_idx: int = 0,
    phase: Phase = Phase.EXEC,
) -> Event:
    """Create a structured Event object."""
    return Event(session=session, txn_idx=txn_idx, query_idx=query_idx, phase=phase)


def parse_event_id(eid: str) -> Dict[str, object]:
    """Parse a stable event ID back into its components."""
    parts = eid.split("_")
    return {
        "session": int(parts[0][1:]),     # "s0" → 0
        "txn_idx": int(parts[1][1:]),     # "t0" → 0
        "query_idx": int(parts[2][1:]),   # "q1" → 1
        "phase": parts[3],                # "exec"
    }


def event_from_id(eid: str) -> Event:
    """Parse a stable event ID string into a structured Event."""
    return Event.from_id(eid)


def make_trial_event_ids(
    n_workers: int,
    queries_per_worker: int = 1,
    phases: Optional[List[str]] = None,
) -> List[str]:
    """
    Generate the complete set of stable event IDs for a micro-workload trial.

    For a typical micro workload with n_workers, each worker executes:
      begin → query_0 → query_1 → ... → commit/rollback

    The number of exec steps depends on the workload.  For simplicity,
    we generate: begin + queries_per_worker exec steps + commit per worker.

    Args:
        n_workers: number of concurrent sessions
        queries_per_worker: number of SQL exec steps per worker
        phases: override phase list (default: begin, exec..., commit)

    Returns:
        Sorted list of all event IDs for the trial.
    """
    ids: List[str] = []
    for w in range(n_workers):
        ids.append(make_event_id(w, 0, 0, "begin"))
        for q in range(queries_per_worker):
            ids.append(make_event_id(w, 0, q + 1, "exec"))
        ids.append(make_event_id(w, 0, queries_per_worker + 1, "commit"))
    return sorted(ids)


# Pre-defined event ID templates for known micro workloads.
# These match the SQL step counts in micro_workloads.py.
MICRO_WORKLOAD_EVENTS: Dict[str, Dict[str, int]] = {
    # job_queue_claim: begin → SELECT status → UPDATE jobs → INSERT claims → commit
    "job_queue_claim": {"queries_per_worker": 3},
    # bank_overdraft: begin → SELECT balance → UPDATE balance → INSERT withdrawals → commit
    "bank_overdraft": {"queries_per_worker": 3},
    # list_append: begin → UPDATE list → SELECT list → commit
    "list_append": {"queries_per_worker": 2},
    # smallbank: begin → SELECT balance → UPDATE table_a → UPDATE table_b → commit
    "smallbank": {"queries_per_worker": 3},
    # multikey_transfer: begin → 5x SELECT balance → 5x UPDATE balance → commit
    "multikey_transfer": {"queries_per_worker": 10},
}


def get_workload_event_ids(workload_name: str, n_workers: int) -> List[str]:
    """Get the standard event IDs for a named micro workload."""
    cfg = MICRO_WORKLOAD_EVENTS.get(workload_name, {"queries_per_worker": 2})
    return make_trial_event_ids(n_workers, cfg["queries_per_worker"])
