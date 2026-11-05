"""
Schedule pool: client-side schedule exploration and replay toolkit.

Core types:
  Event, Phase, DelayPoint     — trackable operations and injection points
  FinalStateHash               — deterministic end-state hash
  OracleVerdict                — isolation check result
  Oracle                       — pluggable checker interface
  ScheduleArtifact             — serializable interleaving record
  ReplayVerification           — replay determinism summary

Schedules are stored as ordered lists of {event, delay_ms} tuples.
The pool retains schedules that discover new canonical states, bugs,
or improved replay stability.
"""

from .types import (
    Phase,
    Event,
    DelayPoint,
    FinalStateHash,
    BugCategory,
    OracleVerdict,
    ReplayResult,
    ReplayVerification,
)
from .oracle import (
    Oracle,
    DoubleClaimOracle,
    OverdraftOracle,
    LostAppendOracle,
)
from .schedule import Schedule, ScheduleArtifact
from .event_id import make_event_id, make_trial_event_ids, make_event, event_from_id
from .pool import SchedulePool, PoolConfig, SearchPhase
from .mutator import ScheduleMutator, MutationConfig
from .retention import RetentionPolicy, RetentionReason
from .base_strategy import (
    BaseStrategy,
    RandomStrategy,
    HeuristicStrategy,
    EmptyStrategy,
    PCTStrategy,
    NaiveUniformStrategy,
    IIDBernoulliStrategy,
)
# runner and replay are imported lazily via __getattr__ to break circular
# dependency: runner/replay → micro_runner → micro_workloads → schedule_pool.__init__
_LAZY_MODULES = {
    "run_pool_search": ".runner",
    "run_random_pool_search": ".runner",
    "run_heuristic_pool_search": ".runner",
    "run_pure_strategy": ".runner",
    "replay_schedule": ".runner",
    "verify_replay": ".replay",
    "assess_replay": ".replay",
    "ElleLogger": ".elle_logger",
    "execute_trial": ".execution",
    "InjectionMode": ".injection",
    "HarnessError": ".injection",
    "TraceEvent": ".injection",
    "minimize_schedule": ".minimize",
    "render_timeline": ".timeline",
}


def __getattr__(name: str):
    if name in _LAZY_MODULES:
        import importlib
        mod = importlib.import_module(_LAZY_MODULES[name], package=__name__)
        val = getattr(mod, name)
        globals()[name] = val
        return val
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# Backward-compatibility aliases
ScheduleEntry = DelayPoint
ScheduleRecord = ScheduleArtifact

__all__ = [
    # Core types
    "Phase",
    "Event",
    "DelayPoint",
    "FinalStateHash",
    "BugCategory",
    "OracleVerdict",
    "ReplayResult",
    "ReplayVerification",
    # Oracle
    "Oracle",
    "DoubleClaimOracle",
    "OverdraftOracle",
    "LostAppendOracle",
    # Schedule
    "Schedule",
    "ScheduleArtifact",
    # Event IDs
    "make_event_id",
    "make_trial_event_ids",
    "make_event",
    "event_from_id",
    # Pool
    "SchedulePool",
    "PoolConfig",
    "SearchPhase",
    # Mutation
    "ScheduleMutator",
    "MutationConfig",
    # Retention
    "RetentionPolicy",
    "RetentionReason",
    # Strategies
    "BaseStrategy",
    "RandomStrategy",
    "HeuristicStrategy",
    "EmptyStrategy",
    "PCTStrategy",
    "NaiveUniformStrategy",
    "IIDBernoulliStrategy",
    # Runners
    "run_pool_search",
    "run_random_pool_search",
    "run_heuristic_pool_search",
    "run_pure_strategy",
    "replay_schedule",
    "verify_replay",
    "assess_replay",
    # Statement-level execution
    "execute_trial",
    "InjectionMode",
    "HarnessError",
    "TraceEvent",
    "minimize_schedule",
    "render_timeline",
    # Elle logger
    "ElleLogger",
    # Backward-compat
    "ScheduleEntry",
    "ScheduleRecord",
]
