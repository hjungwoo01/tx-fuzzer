"""
Retention criteria for the schedule pool.

A candidate schedule is retained if any of these hold:
1) It reaches a new canonical state not yet in the pool
2) It finds a bug not seen before for that workload/variant
3) It improves replay stability (higher replay_rate_buggy)
4) It reduces delay_count or total_delay_ms while preserving bug + replay
5) It becomes a new best schedule by a TTFSR-like metric
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Set

from .schedule import ScheduleRecord
from .pool import SchedulePool


class RetentionReason(Enum):
    """Why a schedule was retained."""
    NEW_STATE = "new_canonical_state"
    NEW_BUG = "new_bug"
    REPLAY_IMPROVEMENT = "replay_improvement"
    SMALLER_SCHEDULE = "smaller_schedule"
    BEST_TTFSR = "best_ttfsr_metric"
    NONE = "not_retained"


class RetentionPolicy:
    """
    Evaluate whether a candidate schedule should be retained in the pool.
    """

    def __init__(
        self,
        pool: SchedulePool,
        seen_states: Optional[Set[str]] = None,
        seen_bugs: Optional[Set[str]] = None,
    ) -> None:
        self._pool = pool
        self._seen_states = seen_states if seen_states is not None else set()
        self._seen_bugs = seen_bugs if seen_bugs is not None else set()

        # Initialize from existing pool
        for r in pool.records:
            if r.discovered_state_hash:
                self._seen_states.add(r.discovered_state_hash)
            if r.bug_found and r.bug_reason:
                self._seen_bugs.add(f"{r.workload}:{r.variant}:{r.bug_reason}")

    @property
    def seen_states(self) -> Set[str]:
        return set(self._seen_states)

    @property
    def unique_state_count(self) -> int:
        return len(self._seen_states)

    def evaluate(self, candidate: ScheduleRecord) -> RetentionReason:
        """
        Check whether a candidate should be retained.
        Returns the reason for retention or NONE.
        """
        # 1. New canonical state
        if candidate.discovered_state_hash:
            if candidate.discovered_state_hash not in self._seen_states:
                return RetentionReason.NEW_STATE

        # 2. New bug
        if candidate.bug_found and candidate.bug_reason:
            bug_key = f"{candidate.workload}:{candidate.variant}:{candidate.bug_reason}"
            if bug_key not in self._seen_bugs:
                return RetentionReason.NEW_BUG

        # 3. TTFSR: compact, high-replay bug schedules should be tagged
        # explicitly instead of being swallowed by the more generic replay /
        # size heuristics below.
        if candidate.bug_found and candidate.replay_rate_buggy is not None:
            if candidate.replay_rate_buggy >= 0.7 and candidate.delay_count <= 4:
                existing = self._pool.list(
                    workload=candidate.workload,
                    db=candidate.db,
                    bug_only=True,
                )
                # Retain if this is a new Pareto-optimal point:
                # no existing schedule dominates on both replay_rate AND compactness
                dominated = False
                for r in existing:
                    r_replay = r.replay_rate_buggy or 0.0
                    if (r_replay >= candidate.replay_rate_buggy and
                            r.delay_count <= candidate.delay_count and
                            r.total_delay_ms <= candidate.total_delay_ms):
                        dominated = True
                        break
                if not dominated:
                    return RetentionReason.BEST_TTFSR

        # 4. Replay improvement: higher buggy replay rate than existing bug schedules
        if candidate.bug_found and candidate.replay_rate_buggy is not None:
            existing_bug_schedules = self._pool.list(
                workload=candidate.workload,
                db=candidate.db,
                bug_only=True,
            )
            if existing_bug_schedules:
                best_replay = max(
                    (r.replay_rate_buggy or 0.0) for r in existing_bug_schedules
                )
                if candidate.replay_rate_buggy > best_replay:
                    return RetentionReason.REPLAY_IMPROVEMENT

        # 5. Smaller schedule preserving bug
        if candidate.bug_found:
            existing = self._pool.list(
                workload=candidate.workload,
                db=candidate.db,
                bug_only=True,
            )
            for r in existing:
                if (candidate.delay_count < r.delay_count or
                        candidate.total_delay_ms < r.total_delay_ms * 0.8):
                    return RetentionReason.SMALLER_SCHEDULE

        return RetentionReason.NONE

    def accept(self, candidate: ScheduleRecord) -> bool:
        """
        Evaluate and, if retained, update internal tracking.
        Returns True if the candidate was accepted.
        """
        reason = self.evaluate(candidate)
        if reason == RetentionReason.NONE:
            return False

        candidate.retention_reason = reason.value

        # Update tracking
        if candidate.discovered_state_hash:
            self._seen_states.add(candidate.discovered_state_hash)
        if candidate.bug_found and candidate.bug_reason:
            bug_key = f"{candidate.workload}:{candidate.variant}:{candidate.bug_reason}"
            self._seen_bugs.add(bug_key)

        # Add to pool
        self._pool.add(candidate)
        return True

    def force_accept(self, candidate: ScheduleRecord, reason: str = "forced") -> None:
        """Force-add a candidate (used for seeding the pool)."""
        candidate.retention_reason = reason
        if candidate.discovered_state_hash:
            self._seen_states.add(candidate.discovered_state_hash)
        if candidate.bug_found and candidate.bug_reason:
            bug_key = f"{candidate.workload}:{candidate.variant}:{candidate.bug_reason}"
            self._seen_bugs.add(bug_key)
        self._pool.add(candidate)
