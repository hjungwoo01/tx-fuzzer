"""
Schedule minimization: delta debugging over delay points + magnitude shrinking.

Exploration finds *a* schedule that triggers a bug -- typically with several
delay points, most of them irrelevant.  Minimization answers "which pause(s)
actually cause it, and how long must they be?":

1. **ddmin** (Zeller & Hildebrandt, TSE 2002) over the set of delay points
   yields a *1-minimal* schedule: removing any single remaining point makes
   the bug disappear.  Under a serial baseline (``stagger_ms``) the number of
   remaining points is the number of preemptions the bug needs -- its depth.
2. **Magnitude shrinking** halves each remaining delay while the bug still
   reproduces, revealing the race window; the kept value gets a safety
   margin (default 2x the smallest passing value, capped at the original)
   so the minimized artifact stays a reliable regression test.

The predicate ``test(schedule) -> bool`` is supplied by the caller and should
re-execute the schedule several times, returning True only if the bug
reproduces reliably (timing is never perfectly deterministic).  Results are
cached by schedule content.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from .schedule import Schedule
from .types import DelayPoint

Test = Callable[[Schedule], bool]


@dataclass
class MinimizeResult:
    schedule: Schedule
    original: Schedule
    tests_run: int = 0
    cache_hits: int = 0
    reproduces_without_delays: bool = False
    #: smallest passing delay found per event during shrinking (ms)
    thresholds_ms: Dict[str, float] = field(default_factory=dict)
    log: List[str] = field(default_factory=list)

    @property
    def depth(self) -> int:
        return self.schedule.delay_count


class _CachedTest:
    def __init__(self, test: Test, log: List[str],
                 on_test: Optional[Callable[[Schedule, bool], None]] = None) -> None:
        self._test = test
        self._cache: Dict[str, bool] = {}
        self._log = log
        self._on_test = on_test
        self.runs = 0
        self.hits = 0

    def __call__(self, points: Sequence[DelayPoint]) -> bool:
        sched = Schedule(list(points))
        key = sched.content_hash()
        if key in self._cache:
            self.hits += 1
            return self._cache[key]
        self.runs += 1
        ok = bool(self._test(sched))
        self._cache[key] = ok
        if self._on_test:
            self._on_test(sched, ok)
        return ok


def ddmin(points: List[DelayPoint], test: Callable[[Sequence[DelayPoint]], bool]
          ) -> List[DelayPoint]:
    """Classic ddmin.  Assumes ``test(points)`` holds; returns a 1-minimal subset."""
    current = list(points)
    n = 2
    while len(current) >= 2:
        chunk = math.ceil(len(current) / n)
        subsets = [current[i:i + chunk] for i in range(0, len(current), chunk)]
        progressed = False
        for sub in subsets:                      # reduce to a subset
            if test(sub):
                current, n, progressed = sub, 2, True
                break
        if not progressed:
            for i in range(len(subsets)):        # reduce to a complement
                comp = [p for j, s in enumerate(subsets) if j != i for p in s]
                if comp and test(comp):
                    current, n, progressed = comp, max(n - 1, 2), True
                    break
        if not progressed:
            if n >= len(current):
                break
            n = min(len(current), n * 2)         # increase granularity
    return current


def minimize_schedule(
    schedule: Schedule,
    test: Test,
    *,
    shrink: bool = True,
    floor_ms: float = 1.0,
    margin: float = 2.0,
    on_test: Optional[Callable[[Schedule, bool], None]] = None,
) -> MinimizeResult:
    """Minimize ``schedule`` w.r.t. ``test`` (see module docstring)."""
    log: List[str] = []
    ct = _CachedTest(test, log, on_test)
    original = Schedule(schedule.entries)
    res = MinimizeResult(schedule=original, original=original)

    if not ct(original.entries):
        log.append("original schedule does not reproduce reliably; nothing to minimize")
        res.tests_run, res.cache_hits, res.log = ct.runs, ct.hits, log
        return res

    if ct([]):
        log.append("bug reproduces with NO injected delays under this baseline")
        res.schedule = Schedule.empty()
        res.reproduces_without_delays = True
        res.tests_run, res.cache_hits, res.log = ct.runs, ct.hits, log
        return res

    # Try big delays first so ddmin's early subsets are the likeliest culprits.
    pts = sorted(original.entries, key=lambda p: (-p.delay_ms, p.event))
    kept = ddmin(pts, ct)
    log.append(f"ddmin: {original.delay_count} -> {len(kept)} delay point(s)")

    if shrink:
        current = {p.event: p.delay_ms for p in kept}
        for event in sorted(current, key=lambda e: -current[e]):
            orig_d = current[event]
            d = orig_d
            while d / 2.0 >= floor_ms:
                trial = dict(current)
                trial[event] = d / 2.0
                if ct([DelayPoint(e, v) for e, v in trial.items()]):
                    d = d / 2.0
                else:
                    break
            res.thresholds_ms[event] = round(d, 3)
            safe = min(orig_d, d * margin) if margin > 1.0 else d
            if safe != d:
                trial = dict(current)
                trial[event] = safe
                if not ct([DelayPoint(e, v) for e, v in trial.items()]):
                    safe = d  # margin value failed (rare); keep the proven value
            current[event] = round(safe, 3)
            log.append(f"shrink {event}: {orig_d:.1f} ms -> threshold ~{d:.1f} ms, "
                       f"kept {current[event]:.1f} ms")
        kept = [DelayPoint(e, v) for e, v in current.items()]

    res.schedule = Schedule(kept)
    res.tests_run, res.cache_hits, res.log = ct.runs, ct.hits, log
    return res
