"""
BugPack — first-class abstraction for concurrency-bug reproducer packs.

Each pack defines everything needed to discover, replay, and verify a
schedule-sensitive anomaly against a specific DB engine.
"""

from __future__ import annotations

import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from scripts.experiments.error_taxonomy import ErrorCategory


@dataclass
class TrialResult:
    """Outcome of a single trial execution."""
    bug_found: bool = False
    reason: str = ""
    outcomes: List[Dict[str, Any]] = field(default_factory=list)
    canonical_state: Optional[Dict[str, Any]] = None
    elapsed_ms: float = 0.0
    delays_used: Dict[str, float] = field(default_factory=dict)
    error_categories: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayResult:
    """Outcome of N replay attempts."""
    bug_count: int = 0
    total: int = 0
    details: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PackConfig:
    """Runtime configuration for a mode (smoke/quick/long)."""
    duration_s: float = 20.0
    max_trials: int = 100
    reps: int = 5
    delay_ms: float = 5.0
    replay_count: int = 20


SMOKE_CONFIG = PackConfig(duration_s=10.0, max_trials=20, reps=1, delay_ms=5.0, replay_count=5)
QUICK_CONFIG = PackConfig(duration_s=20.0, max_trials=100, reps=5, delay_ms=5.0, replay_count=20)
LONG_CONFIG = PackConfig(duration_s=600.0, max_trials=1000, reps=10, delay_ms=5.0, replay_count=20)


class BugPack(ABC):
    """Interface for a concurrency-bug reproducer pack."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Unique pack identifier, e.g. 'pg_update_conflict_out'."""
        ...

    @property
    @abstractmethod
    def db(self) -> str:
        """Target DB engine: 'postgres', 'mysql', or 'sqlite'."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """One-line description of the bug."""
        ...

    @abstractmethod
    def buggy_dsn(self) -> str:
        """Return DSN for the buggy variant."""
        ...

    @abstractmethod
    def control_dsn(self) -> str:
        """Return DSN for the control (fixed) variant."""
        ...

    @abstractmethod
    def setup(self, dsn: str) -> None:
        """Create schema / seed data."""
        ...

    @abstractmethod
    def run_trial(
        self,
        dsn: str,
        strategy: str,
        delay_ms: float,
        rng: random.Random,
    ) -> TrialResult:
        """Execute one trial. Returns TrialResult with bug_found."""
        ...

    @abstractmethod
    def is_available(self, dsn: str) -> bool:
        """Check if the DB endpoint is reachable."""
        ...

    def default_delay_ms(self) -> float:
        """Default delay magnitude for this pack."""
        return 5.0

    def strategies(self) -> List[str]:
        """Strategies to benchmark."""
        return ["stress", "random", "heuristic"]


    def run_discovery(
        self,
        dsn: str,
        strategy: str,
        cfg: PackConfig,
        rep: int = 0,
    ) -> Dict[str, Any]:
        """Run trials until bug found or budget exhausted.

        Returns dict with: bug_found, time_to_first_s, trials_to_first,
        total_trials, total_time_s, found_result (TrialResult or None).
        """
        rng = random.Random()
        delay = self.default_delay_ms()
        start = time.monotonic()
        for trial_num in range(1, cfg.max_trials + 1):
            if (time.monotonic() - start) > cfg.duration_s:
                break
            tr = self.run_trial(dsn, strategy, delay, rng)
            if tr.bug_found:
                ttf = time.monotonic() - start
                return {
                    "bug_found": True,
                    "time_to_first_s": ttf,
                    "trials_to_first": trial_num,
                    "total_trials": trial_num,
                    "total_time_s": ttf,
                    "found_result": tr,
                }
        total = time.monotonic() - start
        return {
            "bug_found": False,
            "time_to_first_s": None,
            "trials_to_first": None,
            "total_trials": trial_num if 'trial_num' in dir() else 0,
            "total_time_s": total,
            "found_result": None,
        }

    def run_replay(
        self,
        dsn: str,
        n: int = 20,
    ) -> ReplayResult:
        """Replay the heuristic schedule n times.

        Uses strategy='heuristic' with the default delay.
        """
        rng = random.Random(42)  # deterministic seed for replay
        delay = self.default_delay_ms()
        details: List[Dict[str, Any]] = []
        bug_count = 0
        for i in range(n):
            tr = self.run_trial(dsn, "heuristic", delay, rng)
            details.append({
                "replay": i,
                "bug_found": tr.bug_found,
                "reason": tr.reason,
            })
            if tr.bug_found:
                bug_count += 1
        return ReplayResult(bug_count=bug_count, total=n, details=details)


_REGISTRY: Dict[str, BugPack] = {}
_ALIASES: Dict[str, str] = {}


def register_pack(pack: BugPack) -> BugPack:
    """Register a BugPack instance."""
    _REGISTRY[pack.id] = pack
    return pack


def register_alias(alias: str, pack_id: str) -> None:
    """Register an alternate user-facing name for a canonical pack id."""
    _ALIASES[alias] = pack_id


def _resolve_pack_id(pack_id: str) -> str:
    return _ALIASES.get(pack_id, pack_id)


def get_pack(pack_id: str) -> BugPack:
    """Look up a registered pack by ID."""
    resolved = _resolve_pack_id(pack_id)
    if resolved not in _REGISTRY:
        available_ids = sorted(set(_REGISTRY.keys()) | set(_ALIASES.keys()))
        available = ", ".join(available_ids)
        raise KeyError(f"Unknown pack '{pack_id}'. Available: {available}")
    return _REGISTRY[resolved]


def list_packs(db: Optional[str] = None) -> List[BugPack]:
    """List all registered packs, optionally filtered by DB."""
    packs = list(_REGISTRY.values())
    if db:
        packs = [p for p in packs if p.db == db]
    return sorted(packs, key=lambda p: p.id)
