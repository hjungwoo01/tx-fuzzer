"""
First-class schedule representation.

A schedule is an ordered list of DelayPoint tuples:
  [{event: <stable_event_id>, delay_ms: <float>}, ...]

External (persisted) format is always the ordered list of tuples.
Internal compilation to executor-friendly formats is handled by adapters.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .types import DelayPoint, FinalStateHash


class Schedule:
    """
    Ordered list of {event, delay_ms} tuples.

    Invariants:
    - entries are ordered by event id (lexicographic) for canonical form
    - no duplicate event entries in the materialized representation
    - all delays are non-negative
    """

    def __init__(self, entries: Optional[List[DelayPoint]] = None) -> None:
        self._entries: List[DelayPoint] = []
        if entries:
            # Deduplicate by event, keeping last value
            seen: Dict[str, DelayPoint] = {}
            for e in entries:
                seen[e.event] = e
            self._entries = sorted(seen.values(), key=lambda x: x.event)

    @property
    def entries(self) -> List[DelayPoint]:
        return list(self._entries)

    @property
    def delay_points(self) -> List[DelayPoint]:
        """Alias for entries — preferred name in tool-track framing."""
        return list(self._entries)

    @property
    def delay_count(self) -> int:
        return len(self._entries)

    @property
    def total_delay_ms(self) -> float:
        return sum(e.delay_ms for e in self._entries)

    @property
    def is_empty(self) -> bool:
        return len(self._entries) == 0

    def get_delay(self, event: str) -> Optional[float]:
        """Get delay for a specific event, or None if not scheduled."""
        for e in self._entries:
            if e.event == event:
                return e.delay_ms
        return None

    def to_delay_map(self) -> Dict[str, float]:
        """Compile to executor-friendly {event: delay_seconds} dict."""
        return {e.event: e.delay_ms / 1000.0 for e in self._entries}

    def to_list(self) -> List[Dict[str, Any]]:
        """Canonical external representation: list of dicts."""
        return [e.to_dict() for e in self._entries]

    @classmethod
    def from_list(cls, entries: List[Dict[str, Any]]) -> Schedule:
        """Deserialize from list of dicts."""
        return cls([DelayPoint.from_dict(d) for d in entries])

    @classmethod
    def empty(cls) -> Schedule:
        return cls()

    def to_json(self) -> str:
        return json.dumps(self.to_list(), separators=(",", ":"))

    @classmethod
    def from_json(cls, s: str) -> Schedule:
        return cls.from_list(json.loads(s))

    def content_hash(self) -> str:
        """Deterministic hash of schedule content for deduplication."""
        payload = json.dumps(self.to_list(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def normalized(self) -> Schedule:
        """Return a new schedule with entries sorted by event id (canonical form)."""
        return Schedule(self._entries)

    def __repr__(self) -> str:
        if self.is_empty:
            return "Schedule(empty)"
        return (
            f"Schedule({self.delay_count} delays, "
            f"total={self.total_delay_ms:.1f}ms)"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schedule):
            return NotImplemented
        return self.content_hash() == other.content_hash()

    def __hash__(self) -> int:
        return hash(self.content_hash())

    def pretty(self) -> str:
        """Human-readable multi-line string for logging."""
        if self.is_empty:
            return "Schedule: (empty — no delays)"
        lines = [f"Schedule ({self.delay_count} delays, total={self.total_delay_ms:.1f}ms):"]
        for e in self._entries:
            lines.append(f"  {e.event}: {e.delay_ms:.2f}ms")
        return "\n".join(lines)


@dataclass
class ScheduleArtifact:
    """
    A complete, serializable record of one explored interleaving.

    Captures everything needed to reproduce and verify an interleaving:
    the delay points that produced it, the resulting FinalStateHash,
    and metadata about the workload context.

    This is the tool's primary output artifact.  It can be:
    - Saved to disk as JSON
    - Replayed against a fresh database to verify determinism
    - Shared as a minimal reproduction case
    """
    schedule_id: str = ""
    parent_schedule_id: Optional[str] = None
    db: str = ""
    workload: str = ""
    variant: str = "buggy"
    strategy: str = "schedule_pool"
    seed: Optional[int] = None
    schedule: Schedule = field(default_factory=Schedule.empty)
    discovered_state_hash: Optional[str] = None
    bug_found: bool = False
    bug_reason: str = ""
    control_clean: Optional[bool] = None
    replay_rate_buggy: Optional[float] = None
    replay_rate_control: Optional[float] = None
    total_delay_ms: float = 0.0
    delay_count: int = 0
    retention_reason: str = ""
    created_at: str = ""

    # --- Aliases for tool-track framing ---

    @property
    def artifact_id(self) -> str:
        return self.schedule_id

    @property
    def delay_points(self) -> List[DelayPoint]:
        return self.schedule.delay_points

    @property
    def final_state_hash(self) -> Optional[FinalStateHash]:
        if self.discovered_state_hash:
            return FinalStateHash(value=self.discovered_state_hash)
        return None

    @property
    def replay_rate(self) -> Optional[float]:
        return self.replay_rate_buggy

    def __post_init__(self) -> None:
        if not self.schedule_id:
            self.schedule_id = uuid.uuid4().hex[:12]
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        self.total_delay_ms = self.schedule.total_delay_ms
        self.delay_count = self.schedule.delay_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schedule_id": self.schedule_id,
            "parent_schedule_id": self.parent_schedule_id,
            "db": self.db,
            "workload": self.workload,
            "variant": self.variant,
            "strategy": self.strategy,
            "seed": self.seed,
            "schedule": self.schedule.to_list(),
            "discovered_state_hash": self.discovered_state_hash,
            "bug_found": self.bug_found,
            "bug_reason": self.bug_reason,
            "control_clean": self.control_clean,
            "replay_rate_buggy": self.replay_rate_buggy,
            "replay_rate_control": self.replay_rate_control,
            "total_delay_ms": self.total_delay_ms,
            "delay_count": self.delay_count,
            "retention_reason": self.retention_reason,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ScheduleArtifact:
        sched = Schedule.from_list(d.get("schedule", []))
        return cls(
            schedule_id=d.get("schedule_id", ""),
            parent_schedule_id=d.get("parent_schedule_id"),
            db=d.get("db", ""),
            workload=d.get("workload", ""),
            variant=d.get("variant", "buggy"),
            strategy=d.get("strategy", "schedule_pool"),
            seed=d.get("seed"),
            schedule=sched,
            discovered_state_hash=d.get("discovered_state_hash"),
            bug_found=d.get("bug_found", False),
            bug_reason=d.get("bug_reason", ""),
            control_clean=d.get("control_clean"),
            replay_rate_buggy=d.get("replay_rate_buggy"),
            replay_rate_control=d.get("replay_rate_control"),
            retention_reason=d.get("retention_reason", ""),
            created_at=d.get("created_at", ""),
        )

    def to_jsonl(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_jsonl(cls, line: str) -> ScheduleArtifact:
        return cls.from_dict(json.loads(line))

    def to_json(self, indent: int = 2) -> str:
        """Pretty JSON for standalone artifact files."""
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, path: Any) -> Any:
        """Write artifact to a JSON file."""
        from pathlib import Path as _Path
        p = _Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_json(), encoding="utf-8")
        return p

    @classmethod
    def load(cls, path: Any) -> ScheduleArtifact:
        """Load artifact from a JSON file."""
        from pathlib import Path as _Path
        text = _Path(path).read_text(encoding="utf-8")
        return cls.from_dict(json.loads(text))


# ---------------------------------------------------------------------------
# Backward-compatibility aliases
# ---------------------------------------------------------------------------
ScheduleEntry = DelayPoint
ScheduleRecord = ScheduleArtifact
