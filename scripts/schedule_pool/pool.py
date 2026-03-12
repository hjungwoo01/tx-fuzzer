"""
Persistent schedule pool with weighted sampling.

Stores schedules that reached new canonical states, found bugs,
or improved replay stability.  Persists to JSONL for portability.

Sampling policy (configurable, dual-objective):
  weight(schedule) ∝
    novelty_bonus *
    bug_bonus *
    replay_bonus *
    compact_bonus *
    (1 / (1 + total_delay_ms))^alpha *
    (1 / (1 + delay_count))^beta

Dual-objective search phases:
  EXPLORE  — maximize canonical state coverage (novelty-weighted)
  EXPLOIT  — after first bug hit, bias toward bug-producing schedules
"""

from __future__ import annotations

import json
import math
import random as _random
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from .schedule import Schedule, ScheduleRecord


class SearchPhase(Enum):
    """Dual-objective search phase."""
    EXPLORE = "explore"
    EXPLOIT = "exploit"


@dataclass
class PoolConfig:
    """Tunable parameters for pool sampling."""
    alpha: float = 0.3   # total_delay_ms penalty exponent
    beta: float = 0.5    # delay_count penalty exponent
    novelty_bonus: float = 2.0
    bug_bonus: float = 3.0        # bonus for schedules that found bugs
    replay_bonus: float = 2.0
    replay_threshold: float = 0.9  # min buggy replay rate for bonus
    compact_bonus: float = 1.5     # bonus for low delay_count bug schedules
    compact_threshold: int = 4     # max delay_count for compact bonus
    empty_schedule_prob: float = 0.15
    # Exploit phase: bias toward bug-producing parents
    exploit_bug_parent_prob: float = 0.6  # P(sample from bug schedules) in exploit


class SchedulePool:
    """
    In-memory schedule pool with JSONL persistence.

    API:
      add(record)          — add a schedule record
      sample(...)          — weighted sample from pool
      list(...)            — list records matching filters
      best_by_metric(...)  — best record by a metric
      deduplicate()        — remove duplicate schedules
      save() / load()      — JSONL persistence
    """

    def __init__(
        self,
        path: Optional[Path] = None,
        config: Optional[PoolConfig] = None,
    ) -> None:
        self._records: List[ScheduleRecord] = []
        self._path = path
        self._config = config or PoolConfig()
        self._content_hashes: set = set()
        self._phase: SearchPhase = SearchPhase.EXPLORE

    @property
    def size(self) -> int:
        return len(self._records)

    @property
    def records(self) -> List[ScheduleRecord]:
        return list(self._records)

    @property
    def phase(self) -> SearchPhase:
        return self._phase

    @property
    def has_bugs(self) -> bool:
        return any(r.bug_found for r in self._records)

    def set_phase(self, phase: SearchPhase) -> None:
        self._phase = phase

    def add(self, record: ScheduleRecord) -> bool:
        """
        Add a schedule record to the pool.
        Returns True if added (not a duplicate), False otherwise.
        Automatically transitions to EXPLOIT phase on first bug.
        """
        h = record.schedule.content_hash()
        if h in self._content_hashes:
            return False
        self._content_hashes.add(h)
        self._records.append(record)
        if self._path:
            self._append_jsonl(record)
        # Transition to exploit on first bug
        if record.bug_found and self._phase == SearchPhase.EXPLORE:
            self._phase = SearchPhase.EXPLOIT
        return True

    def sample(
        self,
        rng: _random.Random,
        *,
        workload: Optional[str] = None,
        db: Optional[str] = None,
        variant: Optional[str] = None,
    ) -> Optional[ScheduleRecord]:
        """
        Weighted sample from the pool.

        In EXPLOIT phase, preferentially samples from bug-producing schedules
        to enable focused mutation around known bug triggers.

        Returns None to signal "use empty schedule" (with probability
        empty_schedule_prob, or if the pool is empty).
        """
        # With some probability, return None → empty schedule
        if rng.random() < self._config.empty_schedule_prob:
            return None

        candidates = self._filter(workload=workload, db=db, variant=variant)
        if not candidates:
            return None

        # In EXPLOIT phase, bias toward bug-producing parents
        if self._phase == SearchPhase.EXPLOIT:
            bug_candidates = [r for r in candidates if r.bug_found]
            if bug_candidates and rng.random() < self._config.exploit_bug_parent_prob:
                candidates = bug_candidates

        weights = [self._weight(r) for r in candidates]
        return rng.choices(candidates, weights=weights, k=1)[0]

    def _weight(self, r: ScheduleRecord) -> float:
        """Compute sampling weight for a schedule record (dual-objective)."""
        cfg = self._config
        novelty = cfg.novelty_bonus if r.discovered_state_hash else 1.0
        bug = cfg.bug_bonus if r.bug_found else 1.0
        replay = 1.0
        if r.replay_rate_buggy is not None and r.replay_rate_buggy >= cfg.replay_threshold:
            replay = cfg.replay_bonus
        compact = 1.0
        if r.bug_found and r.delay_count <= cfg.compact_threshold:
            compact = cfg.compact_bonus
        delay_penalty = (1.0 / (1.0 + r.total_delay_ms)) ** cfg.alpha
        count_penalty = (1.0 / (1.0 + r.delay_count)) ** cfg.beta
        w = novelty * bug * replay * compact * delay_penalty * count_penalty
        return max(w, 1e-10)  # avoid zero weights

    def list(
        self,
        workload: Optional[str] = None,
        db: Optional[str] = None,
        variant: Optional[str] = None,
        bug_only: bool = False,
    ) -> List[ScheduleRecord]:
        """List records matching optional filters."""
        result = self._filter(workload=workload, db=db, variant=variant)
        if bug_only:
            result = [r for r in result if r.bug_found]
        return result

    def best_by_metric(
        self,
        key: Callable[[ScheduleRecord], float],
        *,
        workload: Optional[str] = None,
        db: Optional[str] = None,
        maximize: bool = True,
    ) -> Optional[ScheduleRecord]:
        """Return the best record by a user-supplied metric function."""
        candidates = self._filter(workload=workload, db=db)
        if not candidates:
            return None
        return max(candidates, key=key) if maximize else min(candidates, key=key)

    def deduplicate(self) -> int:
        """Remove duplicate schedules. Returns number removed."""
        seen: set = set()
        kept: List[ScheduleRecord] = []
        for r in self._records:
            h = r.schedule.content_hash()
            if h not in seen:
                seen.add(h)
                kept.append(r)
        removed = len(self._records) - len(kept)
        self._records = kept
        self._content_hashes = seen
        return removed

    def save(self, path: Optional[Path] = None) -> Path:
        """Write all records to JSONL."""
        p = path or self._path
        if p is None:
            raise ValueError("No path specified for pool save")
        p = Path(p)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            for r in self._records:
                f.write(r.to_jsonl() + "\n")
        return p

    def load(self, path: Optional[Path] = None) -> int:
        """Load records from JSONL. Returns count loaded."""
        p = path or self._path
        if p is None or not Path(p).exists():
            return 0
        p = Path(p)
        count = 0
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    r = ScheduleRecord.from_jsonl(line)
                    h = r.schedule.content_hash()
                    if h not in self._content_hashes:
                        self._content_hashes.add(h)
                        self._records.append(r)
                        count += 1
        return count

    def _append_jsonl(self, record: ScheduleRecord) -> None:
        """Append a single record to the JSONL file."""
        if self._path:
            p = Path(self._path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8") as f:
                f.write(record.to_jsonl() + "\n")

    def _filter(
        self,
        workload: Optional[str] = None,
        db: Optional[str] = None,
        variant: Optional[str] = None,
    ) -> List[ScheduleRecord]:
        result = self._records
        if workload:
            result = [r for r in result if r.workload == workload]
        if db:
            result = [r for r in result if r.db == db]
        if variant:
            result = [r for r in result if r.variant == variant]
        return result

    def summary(self) -> Dict[str, Any]:
        """Quick summary for logging."""
        bugs = sum(1 for r in self._records if r.bug_found)
        states = len(set(r.discovered_state_hash for r in self._records if r.discovered_state_hash))
        return {
            "pool_size": self.size,
            "bugs": bugs,
            "unique_states": states,
            "total_delay_ms_range": (
                (min(r.total_delay_ms for r in self._records),
                 max(r.total_delay_ms for r in self._records))
                if self._records else (0, 0)
            ),
        }
