"""
Utilities for coverage-guided fuzzing of database litmus tests.

- Schedule: the input (timing/interleaving) — list of delay floats with a unique signature.
- StateHasher: the output observer — LSH/stable hash of DB state (dict of append-only lists).
- Mutator: Perturb and Crossover to produce child schedules from a parent.
"""

from __future__ import annotations

import hashlib
import random
from typing import Any, Dict, List, Optional, Sequence

# Default number of delay slots for tests that use schedules (e.g. lost_update)
DEFAULT_SCHEDULE_LEN = 6


class Schedule:
    """
    The Input: timing/interleaving. A list of delays (seconds) injected before
    specific SQL operations.
    """

    def __init__(self, delays: Optional[Sequence[float]] = None) -> None:
        if delays is None:
            delays = []
        self.delays = list(delays)

    def __repr__(self) -> str:
        return self.signature()

    def signature(self) -> str:
        """Returns a unique signature of the delays (for logging and hashing)."""
        return ",".join(f"{d:.4f}" for d in self.delays)

    def __len__(self) -> int:
        return len(self.delays)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schedule):
            return NotImplemented
        return self.delays == other.delays

    def __hash__(self) -> int:
        return hash(self.signature())


class StateHasher:
    """
    The Output Observer: LSH / stable hash of database state.
    Hashes a dictionary of append-only lists (e.g. {'k1': ['T1', 'T2'], 'k2': ['T3']}).
    Identical states yield the same hash; order of lists is significant (e.g. ['T1','T2'] vs ['T2','T1']).
    """

    def __init__(self) -> None:
        pass

    def hash(self, db_state: Dict[str, List[str]]) -> str:
        """
        Input: A dictionary of the final database state (e.g. {'k1': ['T1', 'T2'], 'k2': ['T3']}).
        Normalize by sorting keys for determinism; value lists are kept in order (serialization order matters).
        Returns a stable hash signature. Exact uniqueness: identical state -> identical hash.
        """
        if not db_state:
            return self._digest("empty")
        # Sort keys for deterministic ordering; values stay in order (append-only list order matters)
        sorted_items = sorted(db_state.items(), key=lambda x: x[0])
        normalized = []
        for k, lst in sorted_items:
            normalized.append((k, tuple(str(x) for x in lst)))
        return self._digest(repr(normalized))

    @staticmethod
    def _digest(s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()


class Mutator:
    """
    Mutates a retained Schedule to produce a child Schedule.
    Applies random mutations: Perturb (±10% one delay), Crossover (swap one delay with random float).
    """

    def __init__(
        self,
        min_delay: float = 0.0,
        max_delay: float = 0.2,
        schedule_len: int = DEFAULT_SCHEDULE_LEN,
        seed: Optional[int] = None,
    ) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.schedule_len = schedule_len
        self._rng = random.Random(seed)

    def mutate(self, parent_schedule: Schedule) -> Schedule:
        """Takes a retained Schedule; returns a new child Schedule."""
        parent = list(parent_schedule.delays)
        while len(parent) < self.schedule_len:
            parent.append(self._rng.uniform(self.min_delay, self.max_delay))
        parent = parent[: self.schedule_len]

        mutation = self._rng.choice(["perturb", "crossover"])
        if mutation == "perturb":
            return Schedule(self._perturb(parent))
        return Schedule(self._crossover(parent))

    def _perturb(self, delays: List[float]) -> List[float]:
        """Perturb: change one delay slightly (±10%)."""
        out = list(delays)
        if not out:
            return out
        i = self._rng.randint(0, len(out) - 1)
        delta = 0.1 * out[i] * (1 if self._rng.random() > 0.5 else -1)
        out[i] = max(self.min_delay, min(self.max_delay, out[i] + delta))
        return out

    def _crossover(self, delays: List[float]) -> List[float]:
        """Crossover: swap one delay with a random new float."""
        out = list(delays)
        if not out:
            return out
        i = self._rng.randint(0, len(out) - 1)
        out[i] = self._rng.uniform(self.min_delay, self.max_delay)
        return out

    @staticmethod
    def random_schedule(
        length: int = DEFAULT_SCHEDULE_LEN,
        min_delay: float = 0.0,
        max_delay: float = 0.1,
        seed: Optional[int] = None,
    ) -> Schedule:
        """Generate a random schedule for bootstrap."""
        rng = random.Random(seed)
        return Schedule([
            rng.uniform(min_delay, max_delay) for _ in range(length)
        ])
