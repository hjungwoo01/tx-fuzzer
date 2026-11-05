"""
Statement-level delay injection and execution tracing.

A Schedule names delay points with stable event ids of the form
``s{session}_t{txn}_q{query}_{phase}``.  This module makes those delay
points *real*: each worker's DBAdapter is wrapped in an
:class:`InstrumentedAdapter` that pauses immediately before the matching
``begin()``, the q-th ``exec()`` of the transaction, or ``commit()`` /
``rollback()`` -- without touching the workload's SQL.

Two injection modes exist:

``step`` (InjectionMode.STEP)
    Every delay point is honored where it is named.  A delay on
    ``s0_t0_q2_exec`` pauses worker 0 *between* its first and second
    statement, which is exactly the window a check-then-act (TOCTOU) race
    needs.  Delays on one worker add up.

``start_skew`` (InjectionMode.START_SKEW)
    The historical behavior, kept so published results stay reproducible:
    before the transaction starts, sleep ``begin`` delay plus the *maximum*
    ``exec`` delay of that worker; ``commit`` delays are ignored.  Only the
    per-worker start offset is controlled.  Artifacts created before the
    mode was recorded load as ``start_skew``.

Every call through the proxy is recorded as a :class:`TraceEvent`, which
powers the ASCII interleaving timeline (``timeline.py``) and event
discovery (the set of delay points a workload actually reaches).
"""

from __future__ import annotations

import re
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .schedule import Schedule

_EVENT_RE = re.compile(r"^s(\d+)_t(\d+)_q(\d+)_([a-z]+)$")


class InjectionMode(str, Enum):
    STEP = "step"
    START_SKEW = "start_skew"

    @classmethod
    def parse(cls, value: Any) -> "InjectionMode":
        if isinstance(value, InjectionMode):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError:
            valid = ", ".join(m.value for m in cls)
            raise ValueError(f"unknown injection mode {value!r} (valid: {valid})") from None


class HarnessError(RuntimeError):
    """The harness (not the database under test) failed.

    Raised when a canonical-state read errors or a worker cannot be stopped.
    Such a trial has no meaningful end state, so it must never be scored as a
    finding.  (Silently swallowing these errors is how a failed
    ``WHERE success = 1`` on a PostgreSQL BOOLEAN column became a 100% false
    positive.)
    """


# ---------------------------------------------------------------------------
# Per-worker delay plan
# ---------------------------------------------------------------------------

@dataclass
class WorkerPlan:
    """Delay lookup (milliseconds) for one worker, derived from a schedule."""
    worker: int
    begin: Dict[int, float]                 # txn -> ms
    execs: Dict[Tuple[int, int], float]     # (txn, q) -> ms
    commit: Dict[int, float]                # txn -> ms (any q index)
    rollback: Dict[int, float]              # txn -> ms (any q index)

    @classmethod
    def from_schedule(cls, schedule: Schedule, worker: int) -> "WorkerPlan":
        plan = cls(worker=worker, begin={}, execs={}, commit={}, rollback={})
        for dp in schedule.entries:
            m = _EVENT_RE.match(dp.event)
            if not m or int(m.group(1)) != worker or dp.delay_ms <= 0:
                continue
            txn, q, phase = int(m.group(2)), int(m.group(3)), m.group(4)
            if phase == "begin":
                plan.begin[txn] = plan.begin.get(txn, 0.0) + dp.delay_ms
            elif phase == "exec":
                plan.execs[(txn, q)] = plan.execs.get((txn, q), 0.0) + dp.delay_ms
            elif phase == "commit":
                plan.commit[txn] = plan.commit.get(txn, 0.0) + dp.delay_ms
            elif phase == "rollback":
                plan.rollback[txn] = plan.rollback.get(txn, 0.0) + dp.delay_ms
        return plan

    def start_skew_ms(self) -> float:
        """Historical semantics: txn-0 begin delay + the max exec delay."""
        execs = list(self.execs.values())
        return self.begin.get(0, 0.0) + (max(execs) if execs else 0.0)


# ---------------------------------------------------------------------------
# Trace
# ---------------------------------------------------------------------------

@dataclass
class TraceEvent:
    """One adapter call observed during a trial (times relative to start)."""
    worker: int
    event: str          # stable event id, e.g. "s0_t0_q2_exec"
    phase: str          # begin | exec | commit | rollback
    sql: str            # normalized, truncated SQL ("" for begin/commit)
    delay_ms: float     # pause injected before this call
    start_ms: float     # when the call started (after the pause)
    end_ms: float       # when the call returned
    status: str         # "OK" or an error category / exception name
    result: str = ""    # short result summary ("100", "1 row", "∅")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for k in ("delay_ms", "start_ms", "end_ms"):
            d[k] = round(d[k], 3)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TraceEvent":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


def _short_sql(sql: str, limit: int = 64) -> str:
    s = " ".join(str(sql).split())
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _summarize(sql: str, result: Any) -> str:
    rows = getattr(result, "rows", None)
    verb = str(sql).lstrip().split(" ", 1)[0].upper() if sql else ""
    if verb in ("SELECT", "WITH", "SHOW") or rows:
        if not rows:
            return "∅"
        if len(rows) == 1:
            row = rows[0]
            if len(row) == 1:
                v = str(row[0])
            else:
                v = "(" + ", ".join(str(x) for x in row) + ")"
            return v if len(v) <= 24 else v[:23] + "…"
        return f"{len(rows)} rows"
    rc = getattr(result, "rowcount", None)
    if rc is not None and verb in ("UPDATE", "DELETE", "INSERT", "REPLACE"):
        return f"{rc} row" + ("" if rc == 1 else "s")
    return ""


def _status(result: Any) -> str:
    cat = getattr(result, "error_category", None)
    if cat is None:
        return "OK"
    return getattr(cat, "value", str(cat))


class InstrumentedAdapter:
    """Proxy around a DBAdapter that injects schedule delays and traces calls.

    Everything not intercepted (``db_name``, ``setup``, ``close``, private
    attributes...) is delegated to the wrapped adapter, so workload code
    cannot tell the difference.
    """

    def __init__(
        self,
        inner: Any,
        worker: int,
        plan: Optional[WorkerPlan],
        mode: InjectionMode,
        clock: Callable[[], float],
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._inner = inner
        self._worker = worker
        self._plan = plan
        self._mode = mode
        self._clock = clock
        self._sleep = sleep
        self._txn = -1
        self._q = 0
        self.trace: List[TraceEvent] = []

    # -- delegation -------------------------------------------------------
    def __getattr__(self, name: str) -> Any:
        if name == "_inner":  # not yet initialized (copy/unpickle): no recursion
            raise AttributeError(name)
        return getattr(self._inner, name)

    @property
    def inner(self) -> Any:
        return self._inner

    # -- helpers ----------------------------------------------------------
    def _cur_txn(self) -> int:
        return self._txn if self._txn >= 0 else 0

    def _event_id(self, q: int, phase: str) -> str:
        return f"s{self._worker}_t{self._cur_txn()}_q{q}_{phase}"

    def _pause_for(self, phase: str, q: int) -> float:
        if self._plan is None or self._mode is not InjectionMode.STEP:
            return 0.0
        txn = self._cur_txn()
        if phase == "begin":
            ms = self._plan.begin.get(txn, 0.0)
        elif phase == "exec":
            ms = self._plan.execs.get((txn, q), 0.0)
        elif phase == "commit":
            ms = self._plan.commit.get(txn, 0.0)
        elif phase == "rollback":
            ms = self._plan.rollback.get(txn, 0.0)
        else:
            ms = 0.0
        if ms > 0:
            self._sleep(ms / 1000.0)
        return ms

    def _call(self, phase: str, q: int, sql: str, fn: Callable[[], Any]) -> Any:
        delay = self._pause_for(phase, q)
        t0 = self._clock()
        try:
            result = fn()
        except BaseException as e:  # record, then propagate unchanged
            self.trace.append(TraceEvent(
                worker=self._worker, event=self._event_id(q, phase), phase=phase,
                sql=_short_sql(sql), delay_ms=delay, start_ms=t0,
                end_ms=self._clock(), status=type(e).__name__, result="",
            ))
            raise
        self.trace.append(TraceEvent(
            worker=self._worker, event=self._event_id(q, phase), phase=phase,
            sql=_short_sql(sql), delay_ms=delay, start_ms=t0,
            end_ms=self._clock(), status=_status(result),
            result=_summarize(sql, result) if phase == "exec" else "",
        ))
        return result

    # -- intercepted API --------------------------------------------------
    def begin(self, isolation_level: str = "") -> Any:
        self._txn += 1
        self._q = 0
        return self._call("begin", 0, "", lambda: self._inner.begin(isolation_level))

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        self._q += 1
        q = self._q
        return self._call("exec", q, sql, lambda: self._inner.exec(sql, params))

    def query_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        return self.exec(sql, params)

    def commit(self) -> Any:
        return self._call("commit", self._q + 1, "", self._inner.commit)

    def rollback(self) -> Any:
        return self._call("rollback", self._q + 1, "", self._inner.rollback)

    def exec_autocommit(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        # Route through our own begin/exec/commit so events are counted and
        # delays apply (mirrors DBAdapter.exec_autocommit).
        from scripts.experiments.error_taxonomy import ErrorCategory
        self.begin()
        r = self.exec(sql, params)
        if getattr(r, "error_category", ErrorCategory.OK) != ErrorCategory.OK:
            self.rollback()
            return r
        cr = self.commit()
        if getattr(cr, "error_category", ErrorCategory.OK) != ErrorCategory.OK:
            return cr
        return r


class StrictAdapter:
    """Proxy for canonical-state reads: any failed statement is a HarnessError.

    Canonical-state functions build the end state the oracle judges.  If one
    of their queries fails, the state is fiction -- so fail loudly instead of
    defaulting the value (the root cause of the PostgreSQL ``bank_overdraft``
    false positive).
    """

    def __init__(self, inner: Any, workload: str = "") -> None:
        self._inner = inner
        self._workload = workload

    def __getattr__(self, name: str) -> Any:
        if name == "_inner":  # not yet initialized (copy/unpickle): no recursion
            raise AttributeError(name)
        return getattr(self._inner, name)

    def _check(self, what: str, result: Any) -> Any:
        status = _status(result)
        if status != "OK":
            msg = getattr(result, "error_msg", "") or ""
            raise HarnessError(
                f"canonical-state read failed for workload {self._workload!r} "
                f"on {getattr(self._inner, 'db_name', '?')}: {what} -> {status}: "
                f"{msg.strip().splitlines()[0] if msg.strip() else ''}"
            )
        return result

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        return self._check(_short_sql(sql, 120), self._inner.exec(sql, params))

    def query_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        return self.exec(sql, params)

    def commit(self) -> Any:
        return self._check("COMMIT", self._inner.commit())


def interrupt_adapter(adapter: Any) -> None:
    """Best-effort: unblock a worker stuck inside a database call.

    Uses the driver's cross-thread cancellation where one exists
    (psycopg2 ``cancel()``, sqlite3 ``interrupt()``), otherwise closes the
    connection so the blocked socket read fails.
    """
    inner = getattr(adapter, "inner", adapter)
    conn = getattr(inner, "_conn", None)
    if conn is None:
        return
    for method in ("cancel", "interrupt"):
        fn = getattr(conn, method, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception:
                pass
    try:
        conn.close()
    except Exception:
        pass


def discover_events(trace: Iterable[TraceEvent]) -> List[str]:
    """The delay points a workload actually reached, in canonical order."""
    return sorted({t.event for t in trace})


def describe_events(trace: Iterable[TraceEvent]) -> Dict[str, str]:
    """Map each reached event id to a human-readable label (SQL or phase)."""
    out: Dict[str, str] = {}
    for t in trace:
        if t.event not in out:
            out[t.event] = t.sql if t.sql else t.phase.upper()
    return dict(sorted(out.items()))
