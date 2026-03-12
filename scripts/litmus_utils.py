from __future__ import annotations

"""
Shared utilities for litmus tests.

`LitmusTester` manages a simple two-connection setup against a `kv` table:
- SQL: kv(k INT PRIMARY KEY, v INT NOT NULL)
- Redis: keys kv:1, kv:2, etc.

Supports multiple backends via `DatabaseAdapter` (see adapters.py). Pass an adapter
for multi-backend use, or (dsn, isolation_level) for backward-compatible PostgreSQL.
"""

import sys
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from adapters import DatabaseAdapter

# Avoid circular import at runtime; LitmusTester(adapter=...) uses adapters when passed.
try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

_ISOLATION_LEVELS = {
    "READ COMMITTED": "READ COMMITTED",
    "REPEATABLE READ": "REPEATABLE READ",
    "SERIALIZABLE": "SERIALIZABLE",
}

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


@dataclass
class LitmusResult:

    anomaly: str
    safe: bool
    message: str
    error: Optional[BaseException] = None


class LitmusTester:
    """
    Two-connection litmus test driver. Uses a DatabaseAdapter when provided,
    otherwise constructs a PostgresAdapter from dsn and isolation_level.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        isolation_level: str = "READ COMMITTED",
        adapter: Optional[DatabaseAdapter] = None,
    ) -> None:
        if adapter is not None:
            self._adapter = adapter
            self.dsn = getattr(adapter, "dsn", dsn or "")
        else:
            if dsn is None:
                raise ValueError("LitmusTester requires dsn or adapter")
            from adapters import PostgresAdapter
            self._adapter = PostgresAdapter(dsn, isolation_level=isolation_level)
            self.dsn = dsn
        self._adapter.connect()
        self.conn1 = self._adapter.conn1
        self.cur1 = self._adapter.cur1
        self.conn2 = self._adapter.conn2
        self.cur2 = self._adapter.cur2

    def setup_db(self) -> None:
        """Reset the kv table and seed keys 1 and 2 with value 0."""
        self._adapter.setup_schema()

    def teardown(self) -> None:
        """Close cursors and connections, ignoring any cleanup errors."""
        self._adapter.teardown()

    def report(self, anomaly_name: str, result_bool: bool, message: str) -> None:
        status = f"{GREEN}PASS (Safe){RESET}" if result_bool else f"{RED}FAIL (Vulnerable){RESET}"
        print(f"[{anomaly_name}] {status} - {message}")

    def run_test(
        self,
        test_name: str,
        schedule: Optional[Sequence[float]] = None,
    ) -> List[Any]:
        """
        Run a litmus scenario with optional delay schedule; return final DB state.
        Does setup_db() and injects time.sleep(schedule[i]) at each step. Does not teardown.
        """
        if test_name == "lost_update":
            return self._run_lost_update(schedule or [])
        raise ValueError(f"Unknown test_name for run_test: {test_name!r}")

    def _run_lost_update(self, schedule: Sequence[float]) -> List[Any]:
        """G1 Lost Update with injectable delays; returns rows from SELECT * FROM kv."""
        self.setup_db()
        adapter = self._adapter
        delays = list(schedule)
        idx = 0

        def _sleep() -> None:
            nonlocal idx
            if idx < len(delays):
                time.sleep(delays[idx])
            idx += 1

        _sleep()
        self.cur1.execute("BEGIN")
        _sleep()
        self.cur2.execute("BEGIN")
        _sleep()
        adapter.read(1, conn_id=1)
        _sleep()
        adapter.write(1, 10, conn_id=2)
        _sleep()
        self.conn2.commit()
        _sleep()
        try:
            adapter.write(1, 1, conn_id=1)
            self.conn1.commit()
        except Exception:
            self.conn1.rollback()

        # Fresh read of full kv state (committed view)
        self.conn1.autocommit = True
        try:
            with self.conn1.cursor() as c:
                c.execute("SELECT k, v FROM kv ORDER BY k")
                rows = c.fetchall()
            return list(rows) if rows else []
        finally:
            self.conn1.autocommit = False
