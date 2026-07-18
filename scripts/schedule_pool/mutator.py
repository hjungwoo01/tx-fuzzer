"""
Schedule mutation operators for pool-based search.

Mutation takes a parent schedule and produces a candidate schedule by:
- adding delays at new events
- adjusting existing delays
- removing delays
- swapping delay targets
- applying small jitter

Mutation is biased toward events on conflict edges (begin/commit
boundaries and rw/wr/ww patterns) but retains exploration probability.

"Stacking" means: candidate inherits parent delays, then applies
local edits.  Caps prevent unbounded drift.
"""

from __future__ import annotations

import random as _random
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from .types import DelayPoint
from .schedule import Schedule
from .event_id import make_event_id


@dataclass
class MutationConfig:
    """Tunable mutation parameters."""
    # Caps
    max_per_event_delay_ms: float = 200.0
    max_total_delay_ms: float = 800.0
    max_delay_count: int = 12

    # Mutation probabilities (must sum to ~1.0 among the ops applied)
    p_add: float = 0.35
    p_adjust: float = 0.30
    p_remove: float = 0.15
    p_swap: float = 0.10
    p_jitter: float = 0.10

    # Delay magnitude ranges (ms)
    add_delay_min_ms: float = 1.0
    add_delay_max_ms: float = 100.0
    adjust_scale_min: float = 0.5
    adjust_scale_max: float = 2.0
    jitter_max_ms: float = 5.0

    # Bias: probability of choosing a "hot" event vs any event
    hot_event_bias: float = 0.7

    # Number of mutations per step
    mutations_per_step: int = 2


# Default "hot" event phases: boundary and exec events most likely to
# expose concurrency bugs when delayed.
HOT_PHASES: Set[str] = {"begin", "commit", "exec"}


class ScheduleMutator:
    """
    Produces candidate schedules by mutating a parent.

    Usage:
        mutator = ScheduleMutator(all_event_ids, config, rng)
        candidate = mutator.mutate(parent_schedule)
    """

    def __init__(
        self,
        all_event_ids: List[str],
        config: Optional[MutationConfig] = None,
        rng: Optional[_random.Random] = None,
        hot_events: Optional[Set[str]] = None,
    ) -> None:
        self._all_events = all_event_ids
        self._config = config or MutationConfig()
        self._rng = rng or _random.Random()
        # Hot events: events on conflict boundaries
        if hot_events is not None:
            self._hot_events = list(hot_events)
        else:
            self._hot_events = [
                eid for eid in all_event_ids
                if any(eid.endswith(f"_{p}") for p in HOT_PHASES)
            ]
        if not self._hot_events:
            self._hot_events = list(all_event_ids)

    def mutate(self, parent: Schedule) -> Schedule:
        """
        Produce a candidate schedule from a parent.

        Applies mutations_per_step independent mutations, then enforces caps.
        """
        # Start with parent entries as a mutable dict
        entries: Dict[str, float] = {}
        for e in parent.entries:
            entries[e.event] = e.delay_ms

        cfg = self._config
        for _ in range(cfg.mutations_per_step):
            op = self._pick_op(entries)
            if op == "add":
                self._op_add(entries)
            elif op == "adjust":
                self._op_adjust(entries)
            elif op == "remove":
                self._op_remove(entries)
            elif op == "swap":
                self._op_swap(entries)
            elif op == "jitter":
                self._op_jitter(entries)

        # Enforce caps
        entries = self._enforce_caps(entries)

        # Build new schedule
        result_entries = [
            DelayPoint(event=ev, delay_ms=d)
            for ev, d in entries.items()
            if d > 0
        ]
        return Schedule(result_entries)

    def _pick_op(self, entries: Dict[str, float]) -> str:
        """Weighted random choice of mutation operator."""
        cfg = self._config
        ops = []
        weights = []

        # Can always add (up to cap)
        if len(entries) < cfg.max_delay_count:
            ops.append("add")
            weights.append(cfg.p_add)

        # Can adjust/remove/swap only if there are existing entries
        if entries:
            ops.append("adjust")
            weights.append(cfg.p_adjust)
            ops.append("remove")
            weights.append(cfg.p_remove)
            if len(entries) >= 1 and len(self._all_events) > len(entries):
                ops.append("swap")
                weights.append(cfg.p_swap)

        # Jitter always possible
        ops.append("jitter")
        weights.append(cfg.p_jitter)

        if not ops:
            return "jitter"
        return self._rng.choices(ops, weights=weights, k=1)[0]

    def _pick_event(self, exclude: Optional[Set[str]] = None) -> str:
        """Pick an event, biased toward hot events."""
        cfg = self._config
        exclude = exclude or set()
        if self._rng.random() < cfg.hot_event_bias:
            candidates = [e for e in self._hot_events if e not in exclude]
            if candidates:
                return self._rng.choice(candidates)
        candidates = [e for e in self._all_events if e not in exclude]
        if candidates:
            return self._rng.choice(candidates)
        return self._rng.choice(self._all_events)

    def _op_add(self, entries: Dict[str, float]) -> None:
        """Add a delay at a new event."""
        cfg = self._config
        existing = set(entries.keys())
        ev = self._pick_event(exclude=existing)
        if ev in entries:
            return  # already has a delay
        delay = self._rng.uniform(cfg.add_delay_min_ms, cfg.add_delay_max_ms)
        entries[ev] = min(delay, cfg.max_per_event_delay_ms)

    def _op_adjust(self, entries: Dict[str, float]) -> None:
        """Scale an existing delay up or down."""
        if not entries:
            return
        cfg = self._config
        ev = self._rng.choice(list(entries.keys()))
        scale = self._rng.uniform(cfg.adjust_scale_min, cfg.adjust_scale_max)
        new_delay = entries[ev] * scale
        entries[ev] = max(0.1, min(new_delay, cfg.max_per_event_delay_ms))

    def _op_remove(self, entries: Dict[str, float]) -> None:
        """Remove a delay entirely."""
        if not entries:
            return
        ev = self._rng.choice(list(entries.keys()))
        del entries[ev]

    def _op_swap(self, entries: Dict[str, float]) -> None:
        """Move a delay from one event to another."""
        if not entries:
            return
        existing = set(entries.keys())
        old_ev = self._rng.choice(list(entries.keys()))
        new_ev = self._pick_event(exclude=existing)
        if new_ev == old_ev or new_ev in entries:
            return
        entries[new_ev] = entries.pop(old_ev)

    def _op_jitter(self, entries: Dict[str, float]) -> None:
        """Add small random jitter to all existing delays."""
        cfg = self._config
        for ev in list(entries.keys()):
            jitter = self._rng.uniform(-cfg.jitter_max_ms, cfg.jitter_max_ms)
            entries[ev] = max(0.1, entries[ev] + jitter)

    def _enforce_caps(self, entries: Dict[str, float]) -> Dict[str, float]:
        """Enforce per-event and total delay caps."""
        cfg = self._config

        # Per-event cap
        for ev in list(entries.keys()):
            if entries[ev] > cfg.max_per_event_delay_ms:
                entries[ev] = cfg.max_per_event_delay_ms
            if entries[ev] <= 0:
                del entries[ev]

        # Total delay cap: proportionally scale down if over budget
        total = sum(entries.values())
        if total > cfg.max_total_delay_ms and total > 0:
            scale = cfg.max_total_delay_ms / total
            for ev in entries:
                entries[ev] *= scale

        # Delay count cap: remove smallest delays
        if len(entries) > cfg.max_delay_count:
            sorted_entries = sorted(entries.items(), key=lambda x: x[1], reverse=True)
            entries = dict(sorted_entries[: cfg.max_delay_count])

        return entries
