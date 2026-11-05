"""
Jepsen/Elle-compatible history logger for the Python execution pipeline.

Outputs transaction histories in EDN format that Elle can consume directly.
Each operation is a map with:
  :process  — worker/session id
  :type     — :invoke, :ok, :fail, or :info
  :f        — operation function (e.g. :txn, :read, :write)
  :value    — operation value (list of micro-ops for :txn)

Usage:
    logger = ElleLogger("history.edn")
    logger.log_invoke(process=0, f="txn", value=[...])
    logger.log_ok(process=0, f="txn", value=[...])
    logger.close()

Or as a context manager:
    with ElleLogger("history.edn") as logger:
        logger.log_invoke(...)
        logger.log_ok(...)
"""

from __future__ import annotations

import json
import math
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union


_KEYWORD_RE = re.compile(r"^:[A-Za-z*+!_?<>=/.-][A-Za-z0-9*+!_?<>=/.:#-]*$")
_KEY_NAME_RE = re.compile(r"^[A-Za-z*+!_?<>=/.-][A-Za-z0-9*+!_?<>=/.:#-]*$")


def _edn_str(s: str) -> str:
    """Quote a string as an EDN string literal.

    Without escaping, a database error such as ``relation "accounts" does
    not exist`` silently re-parsed as the string ``"relation "`` followed by
    a bogus map key ``accounts`` -- corrupting the history Elle checks.
    """
    out = (s.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t"))
    return f'"{out}"'


def _edn_key(k: Any) -> str:
    ks = str(k)
    if ks.startswith(":") and _KEYWORD_RE.match(ks):
        return ks
    if _KEY_NAME_RE.match(ks):
        return f":{ks}"
    return _edn_str(ks)


def _edn_value(v: Any) -> str:
    """Serialize a Python value to EDN format."""
    if v is None:
        return "nil"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):
            return "##NaN"
        if math.isinf(v):
            return "##Inf" if v > 0 else "##-Inf"
        return repr(v)
    if isinstance(v, str):
        if _KEYWORD_RE.match(v):
            return v  # already a keyword
        return _edn_str(v)
    if isinstance(v, (bytes, bytearray)):
        return _edn_str(bytes(v).decode("utf-8", "replace"))
    if isinstance(v, (list, tuple)):
        inner = " ".join(_edn_value(item) for item in v)
        return f"[{inner}]"
    if isinstance(v, (set, frozenset)):
        inner = " ".join(sorted(_edn_value(item) for item in v))
        return "#{" + inner + "}"
    if isinstance(v, dict):
        parts = []
        for k in sorted(v.keys(), key=str):
            parts.append(f"{_edn_key(k)} {_edn_value(v[k])}")
        return "{" + " ".join(parts) + "}"
    return _edn_str(str(v))


class ElleLogger:
    """Writes Jepsen/Elle-compatible EDN history files.

    Thread-safe: multiple workers can log concurrently.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open("w", encoding="utf-8")
        self._lock = threading.Lock()
        self._start_ns = time.monotonic_ns()
        self._closed = False

    def __enter__(self) -> ElleLogger:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._file.close()

    @property
    def path(self) -> Path:
        return self._path

    def _elapsed_ns(self) -> int:
        return time.monotonic_ns() - self._start_ns

    def _write_op(
        self,
        process: int,
        op_type: str,
        f: str,
        value: Any,
        *,
        error: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write a single operation to the history file."""
        parts = [
            f":type {op_type}",
            f":f :{f}" if not f.startswith(":") else f":f {f}",
            f":process {process}",
            f":time {self._elapsed_ns()}",
            f":wall {time.time_ns()}",
            f":value {_edn_value(value)}",
        ]
        if error:
            parts.append(f":error {_edn_str(str(error))}")
        if meta:
            for k, v in sorted(meta.items()):
                parts.append(f"{_edn_key(k)} {_edn_value(v)}")

        line = "{" + " ".join(parts) + "}\n"

        with self._lock:
            if not self._closed:
                self._file.write(line)
                self._file.flush()

    def log_invoke(
        self,
        process: int,
        f: str = "txn",
        value: Any = None,
        **meta: Any,
    ) -> None:
        """Log a :invoke operation (start of a transaction/operation)."""
        self._write_op(process, ":invoke", f, value, meta=meta or None)

    def log_ok(
        self,
        process: int,
        f: str = "txn",
        value: Any = None,
        **meta: Any,
    ) -> None:
        """Log an :ok operation (successful completion)."""
        self._write_op(process, ":ok", f, value, meta=meta or None)

    def log_fail(
        self,
        process: int,
        f: str = "txn",
        value: Any = None,
        error: Optional[str] = None,
        **meta: Any,
    ) -> None:
        """Log a :fail operation (definite failure)."""
        self._write_op(process, ":fail", f, value, error=error, meta=meta or None)

    def log_info(
        self,
        process: int,
        f: str = "txn",
        value: Any = None,
        error: Optional[str] = None,
        **meta: Any,
    ) -> None:
        """Log an :info operation (indeterminate outcome)."""
        self._write_op(process, ":info", f, value, error=error, meta=meta or None)

    def log_txn(
        self,
        process: int,
        op_type: str,
        mops: List[List[Any]],
        **meta: Any,
    ) -> None:
        """Log a transaction as a list of micro-ops.

        Each mop is [op_keyword, key, value]:
          [:r "x" nil]     — read of key "x" (value nil = not yet known)
          [:w "x" 1]       — write of value 1 to key "x"
          [:r "x" [1 2 3]] — read returning list

        Args:
            process: worker/session id
            op_type: ":invoke", ":ok", ":fail", or ":info"
            mops: list of micro-ops
        """
        self._write_op(process, op_type, "txn", mops, meta=meta or None)
