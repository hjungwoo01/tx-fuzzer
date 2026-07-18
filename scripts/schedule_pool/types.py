"""
Core type definitions for the schedule exploration and replay tool.

These types form the public interface of isotest:
  - Phase / Event          — trackable database operations
  - DelayPoint             — client-side injection pause
  - FinalStateHash         — deterministic hash of canonical end-state
  - OracleVerdict          — result of an isolation check
  - ReplayResult           — outcome of a single replay
  - ReplayVerification     — aggregate replay determinism summary
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class Phase(str, Enum):
    """Lifecycle phase of a database operation."""
    BEGIN    = "begin"
    EXEC     = "exec"
    COMMIT   = "commit"
    ROLLBACK = "rollback"
    BARRIER  = "barrier"


@dataclass(frozen=True)
class Event:
    """A trackable database operation within a workload execution.

    An Event identifies a specific moment in a concurrent workload where
    the tool can observe or inject delays.  Events are deterministic:
    the same workload + seed always produces the same Event sequence.

    The string format ``s{session}_t{txn}_q{query}_{phase}`` is stable
    across serialization and is used as the canonical identifier.
    """
    session: int
    txn_idx: int
    query_idx: int
    phase: Phase

    @property
    def id(self) -> str:
        """Stable string identifier used in serialized artifacts."""
        return f"s{self.session}_t{self.txn_idx}_q{self.query_idx}_{self.phase.value}"

    @classmethod
    def from_id(cls, event_id: str) -> Event:
        """Parse a stable event ID string back into an Event."""
        parts = event_id.split("_")
        return cls(
            session=int(parts[0][1:]),
            txn_idx=int(parts[1][1:]),
            query_idx=int(parts[2][1:]),
            phase=Phase(parts[3]),
        )

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f"Event({self.id!r})"


@dataclass(frozen=True)
class DelayPoint:
    """A client-side pause injected before a specific Event executes.

    DelayPoints are the tool's sole mechanism for controlling interleavings.
    The tool never modifies SQL, schema, or data — only timing.
    """
    event: str        # stable event id string (e.g. "s0_t0_q1_exec")
    delay_ms: float   # non-negative delay in milliseconds

    def __post_init__(self) -> None:
        if self.delay_ms < 0:
            object.__setattr__(self, "delay_ms", 0.0)

    def to_dict(self) -> Dict[str, Any]:
        return {"event": self.event, "delay_ms": round(self.delay_ms, 4)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DelayPoint:
        return cls(event=str(d["event"]), delay_ms=float(d["delay_ms"]))


@dataclass(frozen=True)
class FinalStateHash:
    """Deterministic hash of a canonical database end-state.

    The hash is computed over the JSON-serialized canonical state dict
    using SHA-256, truncated to 16 hex characters.  Two executions that
    produce the same FinalStateHash reached the same logical end-state.
    """
    value: str  # 16-char hex digest

    @classmethod
    def compute(cls, canonical_state: Dict[str, Any]) -> FinalStateHash:
        payload = json.dumps(canonical_state, sort_keys=True, default=str)
        digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
        return cls(value=digest)

    def __str__(self) -> str:
        return self.value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FinalStateHash):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True)
class OracleVerdict:
    """Result of an Oracle check on an interleaving outcome."""
    bug_found: bool
    reason: str


@dataclass
class ReplayResult:
    """Outcome of a single replay of a ScheduleArtifact."""
    replay_idx: int
    expected_hash: str
    actual_hash: str
    match: bool
    bug_found: bool
    bug_reason: str
    elapsed_ms: float


@dataclass
class ReplayVerification:
    """Aggregate summary of replay verification for a ScheduleArtifact."""
    artifact_id: str
    expected_hash: str
    total_replays: int
    hash_match_count: int
    hash_match_rate: float
    bug_replay_count: int
    bug_replay_rate: float
    unique_hashes: int
    replays: List[ReplayResult]
