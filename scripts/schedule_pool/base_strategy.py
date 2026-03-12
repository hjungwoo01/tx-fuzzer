"""
Base strategy interface for schedule seeding.

A BaseStrategy generates a fresh schedule (without a parent) to seed
the pool's mutation loop.  This makes the pool an *orthogonal* extension
layer that wraps any underlying schedule generation strategy:

    pool(random)     — pool + retain + mutate, seeded by RandomStrategy
    pool(heuristic)  — pool + retain + mutate, seeded by HeuristicStrategy

The "bare" random / heuristic strategies are IID (stateless, no memory).
The pool adds memory (retain novel states) and directed mutation (refine
from parents), so any improvement is attributable to the pool layer.
"""

from __future__ import annotations

import random as _random
from abc import ABC, abstractmethod
from typing import List

from .schedule import Schedule, ScheduleEntry


class BaseStrategy(ABC):
    """Generate a fresh schedule to seed pool mutation."""

    @abstractmethod
    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        """Produce a fresh schedule from the given event space."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...


class RandomStrategy(BaseStrategy):
    """
    Assign uniform [0, max_delay_ms] to every event.

    Mirrors ``_gen_strategy_delays("random", ...)``: each event
    receives an independent ``uniform(0, max_delay_ms)`` delay.
    """

    def __init__(self, max_delay_ms: float = 100.0) -> None:
        self.max_delay_ms = max_delay_ms

    @property
    def name(self) -> str:
        return "random"

    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        entries = [
            ScheduleEntry(event=eid, delay_ms=rng.uniform(0, self.max_delay_ms))
            for eid in event_ids
        ]
        return Schedule(entries)


class HeuristicStrategy(BaseStrategy):
    """
    Pick k random events, assign moderate delays [min_ms, max_ms].

    Mirrors ``_gen_strategy_delays("heuristic", ...)``: select k
    events at random and assign ``uniform(min_delay_ms, max_delay_ms)``.
    """

    def __init__(
        self,
        k: int = 4,
        min_delay_ms: float = 5.0,
        max_delay_ms: float = 50.0,
    ) -> None:
        self.k = k
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms

    @property
    def name(self) -> str:
        return "heuristic"

    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        k = min(self.k, len(event_ids))
        chosen = rng.sample(event_ids, k)
        entries = [
            ScheduleEntry(event=eid, delay_ms=rng.uniform(self.min_delay_ms, self.max_delay_ms))
            for eid in chosen
        ]
        return Schedule(entries)


class EmptyStrategy(BaseStrategy):
    """Return an empty schedule (original behaviour, kept for reference)."""

    @property
    def name(self) -> str:
        return "empty"

    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        return Schedule.empty()
