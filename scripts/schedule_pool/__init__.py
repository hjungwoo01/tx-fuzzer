"""
Schedule pool: first-class schedule abstraction and pool-based search workflow.

Schedules are stored as ordered lists of {event, delay_ms} tuples.
The pool retains schedules that discover new canonical states, bugs,
or improved replay stability.
"""

from .schedule import Schedule, ScheduleRecord, ScheduleEntry
from .event_id import make_event_id, make_trial_event_ids
from .pool import SchedulePool, PoolConfig, SearchPhase
from .mutator import ScheduleMutator, MutationConfig
from .retention import RetentionPolicy, RetentionReason
from .base_strategy import BaseStrategy, RandomStrategy, HeuristicStrategy, EmptyStrategy

__all__ = [
    "Schedule",
    "ScheduleRecord",
    "ScheduleEntry",
    "make_event_id",
    "make_trial_event_ids",
    "SchedulePool",
    "PoolConfig",
    "SearchPhase",
    "ScheduleMutator",
    "MutationConfig",
    "RetentionPolicy",
    "RetentionReason",
    "BaseStrategy",
    "RandomStrategy",
    "HeuristicStrategy",
    "EmptyStrategy",
]
