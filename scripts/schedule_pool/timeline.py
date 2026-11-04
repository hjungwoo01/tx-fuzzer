"""
Render a statement-level trace as a side-by-side interleaving timeline.

    time      │ w0                            │ w1
    ──────────┼───────────────────────────────┼──────────────────────────────
       0.0 ms │ BEGIN                         │
       0.3 ms │ SELECT balance FROM … → 100   │
       0.4 ms │ ⏸ pause 30.0 ms [q2_exec]     │
      12.0 ms │                               │ BEGIN
      12.4 ms │                               │ SELECT balance FROM … → 100
      ...

Pure functions over TraceEvent objects (or their dicts), so they work on a
live trial, on an artifact's stored ``evidence_trace``, and in tests.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .injection import TraceEvent

_PHASE_LABEL = {"begin": "BEGIN", "commit": "COMMIT", "rollback": "ROLLBACK"}


class Style:
    """Glyphs + optional ANSI colors; ``Style.detect()`` picks for the terminal."""

    def __init__(self, unicode: bool = True, color: bool = False) -> None:
        self.unicode = unicode
        self.color = color
        self.vbar = "│" if unicode else "|"
        self.hbar = "─" if unicode else "-"
        self.cross = "┼" if unicode else "+"
        self.pause = "⏸" if unicode else "||"
        self.arrow = "→" if unicode else "->"
        self.bad = "✗" if unicode else "x"
        self.ellipsis = "…" if unicode else "..."

    @classmethod
    def detect(cls, stream: Any = None, force_ascii: bool = False,
               force_color: Optional[bool] = None) -> "Style":
        stream = stream or sys.stdout
        enc = (getattr(stream, "encoding", "") or "").lower()
        unicode = (not force_ascii) and ("utf" in enc)
        if force_color is None:
            color = (hasattr(stream, "isatty") and stream.isatty()
                     and "NO_COLOR" not in os.environ
                     and os.environ.get("TERM", "") != "dumb"
                     # legacy Windows consoles print ANSI codes literally
                     and (os.name != "nt" or "FORCE_COLOR" in os.environ
                          or "WT_SESSION" in os.environ))
        else:
            color = force_color
        return cls(unicode=unicode, color=bool(color))

    def _c(self, code: str, s: str) -> str:
        return f"\x1b[{code}m{s}\x1b[0m" if self.color else s

    def red(self, s: str) -> str:
        return self._c("31", s)

    def green(self, s: str) -> str:
        return self._c("32", s)

    def yellow(self, s: str) -> str:
        return self._c("33", s)

    def bold(self, s: str) -> str:
        return self._c("1", s)

    def dim(self, s: str) -> str:
        return self._c("2", s)


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _visible_len(s: str) -> int:
    return len(_ANSI.sub("", s))


def _pad(s: str, width: int) -> str:
    return s + " " * max(0, width - _visible_len(s))


def _fit(s: str, width: int, style: Style) -> str:
    if len(s) <= width:
        return s
    ell = style.ellipsis
    return s[: max(0, width - len(ell))] + ell


def _as_events(trace: Iterable[Any]) -> List[TraceEvent]:
    out = []
    for t in trace:
        out.append(t if isinstance(t, TraceEvent) else TraceEvent.from_dict(t))
    return out


def _short_event(event_id: str) -> str:
    # "s0_t0_q2_exec" -> "q2_exec" (worker is the column; txn 0 is implicit)
    parts = event_id.split("_")
    if len(parts) == 4 and parts[1] == "t0":
        return f"{parts[2]}_{parts[3]}"
    return event_id


#: Calls that take at least this long are annotated with their duration --
#: almost always a lock wait, which is the "why" of most interleavings.
WAIT_ANNOTATE_MS = 1.5


def _call_text(ev: TraceEvent, width: int, style: Style) -> str:
    if ev.phase in _PHASE_LABEL:
        text = _PHASE_LABEL[ev.phase]
        suffix = ""
    else:
        text = ev.sql
        suffix = f" {style.arrow} {ev.result}" if ev.result else ""
    failed = ev.status != "OK"
    if failed:
        suffix = f" {style.bad} {ev.status}"
    waited = ev.end_ms - ev.start_ms
    if waited >= WAIT_ANNOTATE_MS:
        suffix += f" (waited {waited:.1f} ms)"
    body = _fit(text, max(8, width - len(suffix)), style) + suffix
    body = _fit(body, width, style)
    return style.red(body) if failed else body


def render_timeline(
    trace: Iterable[Any],
    n_workers: Optional[int] = None,
    *,
    style: Optional[Style] = None,
    width: Optional[int] = None,
    verdict: str = "",
    verdict_is_bug: Optional[bool] = None,
) -> str:
    """Return the interleaving as a multi-line string."""
    style = style or Style(unicode=True, color=False)
    events = _as_events(trace)
    if n_workers is None:
        n_workers = (max((e.worker for e in events), default=-1) + 1) or 1
    term_w = width or shutil.get_terminal_size((120, 24)).columns
    time_w = 12
    col_w = max(22, min(44, (term_w - time_w - 3 * n_workers) // max(1, n_workers)))

    rows: List[tuple] = []  # (t, order, worker, text)
    for e in events:
        if e.delay_ms > 0:
            label = f"{style.pause} pause {e.delay_ms:.1f} ms [{_short_event(e.event)}]"
            rows.append((e.start_ms - e.delay_ms, 0, e.worker,
                         style.yellow(_fit(label, col_w, style))))
        rows.append((e.start_ms, 1, e.worker, _call_text(e, col_w, style)))
    rows.sort(key=lambda r: (round(r[0], 3), r[2], r[1]))

    header = _pad("    time", time_w) + "".join(
        f"{style.vbar} " + _pad(style.bold(f"w{w}"), col_w) + " " for w in range(n_workers))
    rule = style.hbar * time_w + "".join(
        style.cross + style.hbar * (col_w + 2) for _ in range(n_workers))
    lines = [header.rstrip(), rule]
    for t, _order, w, text in rows:
        cells = []
        for col in range(n_workers):
            cells.append(f"{style.vbar} " + (_pad(text, col_w) if col == w else " " * col_w) + " ")
        lines.append((f"{max(t, 0.0):8.1f} ms ").ljust(time_w) + "".join(cells).rstrip())
    if verdict:
        lines.append(rule)
        if verdict_is_bug is None:
            lines.append(f"  oracle: {verdict}")
        elif verdict_is_bug:
            lines.append(style.red(style.bold(f"  BUG: {verdict}")))
        else:
            lines.append(style.green(f"  ok: {verdict}"))
    return "\n".join(lines)


def render_plan(
    schedule_entries: Sequence[Any],
    n_workers: int,
    event_labels: Optional[Dict[str, str]] = None,
    *,
    style: Optional[Style] = None,
) -> str:
    """Describe a schedule's delay points per worker (no database needed)."""
    style = style or Style(unicode=True, color=False)
    labels = event_labels or {}
    by_worker: Dict[int, List[Any]] = {w: [] for w in range(n_workers)}
    for dp in schedule_entries:
        event = dp.event if hasattr(dp, "event") else dp["event"]
        delay = dp.delay_ms if hasattr(dp, "delay_ms") else dp["delay_ms"]
        try:
            w = int(event.split("_")[0][1:])
        except (ValueError, IndexError):
            w = -1
        by_worker.setdefault(w, []).append((event, delay))
    lines = []
    for w in sorted(by_worker):
        pts = by_worker[w]
        name = f"w{w}" if w >= 0 else "?"
        if not pts:
            lines.append(f"  {name}: (no pauses)")
            continue
        for event, delay in sorted(pts):
            what = labels.get(event)
            hint = f"  before {what}" if what else ""
            lines.append(f"  {name}: {style.pause} {delay:7.1f} ms at {event}{hint}")
    return "\n".join(lines)
