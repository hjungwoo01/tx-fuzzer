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

from .types import DelayPoint
from .schedule import Schedule


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
            DelayPoint(event=eid, delay_ms=rng.uniform(0, self.max_delay_ms))
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
            DelayPoint(event=eid, delay_ms=rng.uniform(self.min_delay_ms, self.max_delay_ms))
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


class PCTStrategy(BaseStrategy):
    """
    PCT (Probabilistic Concurrency Testing) seed generator.

    Burckhardt et al., ASPLOS 2010: "A Randomized Scheduler with
    Probabilistic Guarantees of Finding Bugs."

    Adapted to client-side delay injection: pick ``d`` priority-change
    points among the event space and apply strictly decreasing delays at
    those points. The depth ``d`` controls bug-depth coverage. With ``d``
    randomly drawn from [1, max_depth] and points placed uniformly, PCT
    provides a 1 / (n * k^(d-1)) lower bound on triggering depth-d bugs.
    """

    def __init__(
        self,
        max_depth: int = 3,
        max_delay_ms: float = 80.0,
        min_delay_ms: float = 10.0,
    ) -> None:
        self.max_depth = max(1, max_depth)
        self.max_delay_ms = max_delay_ms
        self.min_delay_ms = min_delay_ms

    @property
    def name(self) -> str:
        return "pct"

    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        if not event_ids:
            return Schedule.empty()
        d = rng.randint(1, min(self.max_depth, len(event_ids)))
        chosen = rng.sample(event_ids, d)
        # strictly decreasing delays so earlier-priority points dominate later
        step = (self.max_delay_ms - self.min_delay_ms) / max(1, d)
        entries = [
            DelayPoint(
                event=eid,
                delay_ms=self.max_delay_ms - i * step,
            )
            for i, eid in enumerate(chosen)
        ]
        return Schedule(entries)


class NaiveUniformStrategy(BaseStrategy):
    """
    Naive uniform-delay baseline: every event receives the same fixed delay.

    Mirrors a typical "stress with sleep" approach used in many ad-hoc
    concurrency testers — useful as a low bar against which PCT, random,
    and the heuristic should outperform.
    """

    def __init__(self, delay_ms: float = 25.0) -> None:
        self.delay_ms = delay_ms

    @property
    def name(self) -> str:
        return "naive"

    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        entries = [DelayPoint(event=eid, delay_ms=self.delay_ms) for eid in event_ids]
        return Schedule(entries)


class IIDBernoulliStrategy(BaseStrategy):
    """
    IID Bernoulli delay-or-not baseline.

    Each event independently receives a delay drawn from [min_ms, max_ms]
    with probability ``p_delay`` (no delay otherwise). Mirrors the
    PCT/RAPOS family: every event flips a coin.
    """

    def __init__(
        self,
        p_delay: float = 0.3,
        min_delay_ms: float = 5.0,
        max_delay_ms: float = 50.0,
    ) -> None:
        self.p_delay = p_delay
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms

    @property
    def name(self) -> str:
        return "iid_bernoulli"

    def seed(self, event_ids: List[str], rng: _random.Random) -> Schedule:
        entries: List[DelayPoint] = []
        for eid in event_ids:
            if rng.random() < self.p_delay:
                d = rng.uniform(self.min_delay_ms, self.max_delay_ms)
                entries.append(DelayPoint(event=eid, delay_ms=d))
        return Schedule(entries)
