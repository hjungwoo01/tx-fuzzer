"""
Execution history logger for litmus tests.

Writes a timestamped trace of operations (INVOKE / OK / FAIL) to a file
and stdout so reports can prove the order of operations.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional


class HistoryLogger:
    """
    Logs execution events with relative timestamps.
    Format: [TIMESTAMP] | [CLIENT_ID] | [EVENT] | [SQL]
    Resets the log file on initialization and echoes each line to stdout.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._start = time.perf_counter()
        self._file: Optional[object] = None
        self._open_and_reset()

    def _open_and_reset(self) -> None:
        self._file = open(self._path, "w", encoding="utf-8")

    def _elapsed(self) -> str:
        t = time.perf_counter() - self._start
        return f"{t:.3f}s"

    def log(self, client_id: str, event_type: str, sql_summary: str) -> None:
        """
        Write one log line: [TIMESTAMP] | [CLIENT_ID] | [EVENT] | [SQL]
        event_type should be INVOKE, OK, or FAIL (padded to 6 chars in output).
        """
        ts = f"[{self._elapsed()}]"
        event = event_type.ljust(6)[:6]
        line = f"{ts} | {client_id} | {event} | {sql_summary}\n"
        f = self._file
        if f is not None:
            f.write(line)
            f.flush()
        print(line.rstrip())

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> "HistoryLogger":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
