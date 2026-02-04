from __future__ import annotations

"""
Shared utilities for PostgreSQL litmus tests.

`LitmusTester` manages a simple two-connection setup against a `kv` table:
- `kv(k INT PRIMARY KEY, v INT NOT NULL)`

Each test script is responsible for orchestrating transaction interleavings
using `conn1`/`cur1` and `conn2`/`cur2`.
"""

import sys
from dataclasses import dataclass
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection


GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


@dataclass
class LitmusResult:
    """Simple container for litmus outcomes."""

    anomaly: str
    safe: bool
    message: str
    error: Optional[BaseException] = None


_ISOLATION_LEVELS = {
    "READ COMMITTED": "READ COMMITTED",
    "REPEATABLE READ": "REPEATABLE READ",
    "SERIALIZABLE": "SERIALIZABLE",
}


class LitmusTester:
    def __init__(self, dsn: str, isolation_level: str = "READ COMMITTED") -> None:
        """
        Open two independent PostgreSQL connections and cursors.

        """
        self.dsn = dsn
        iso_normalized = " ".join(str(isolation_level).upper().split())
        if iso_normalized not in _ISOLATION_LEVELS:
            valid = ", ".join(sorted(_ISOLATION_LEVELS))
            raise ValueError(f"Unsupported isolation level {isolation_level!r}. Valid options: {valid}")
        try:
            self.conn1: PGConnection = psycopg2.connect(dsn)
            self.conn2: PGConnection = psycopg2.connect(dsn)
        except Exception as exc:
            print(f"[LitmusTester] Failed to connect using DSN={dsn!r}: {exc}", file=sys.stderr)
            raise

        self.conn1.autocommit = False
        self.conn2.autocommit = False

        level_sql = _ISOLATION_LEVELS[iso_normalized]
        try:
            with self.conn1.cursor() as c1:
                c1.execute(
                    f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level_sql}"
                )
            with self.conn2.cursor() as c2:
                c2.execute(
                    f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level_sql}"
                )
            
            self.conn1.commit()
            self.conn2.commit()
        except Exception:
            self.conn1.rollback()
            self.conn2.rollback()
            raise

        self.cur1 = self.conn1.cursor()
        self.cur2 = self.conn2.cursor()

    def setup_db(self) -> None:
        """
        Reset the `kv` table and seed keys 1 and 2 with value 0.
        """
        try:
            self.cur1.execute("TRUNCATE TABLE kv")
            self.cur1.execute("INSERT INTO kv (k, v) VALUES (1, 0), (2, 0)")
            self.conn1.commit()
        except Exception:
            self.conn1.rollback()
            raise

    def teardown(self) -> None:
        """Close cursors and connections, ignoring any cleanup errors."""
        for cur in (getattr(self, "cur1", None), getattr(self, "cur2", None)):
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass

        for conn in (getattr(self, "conn1", None), getattr(self, "conn2", None)):
            try:
                if conn is not None and not conn.closed:
                    conn.close()
            except Exception:
                pass

    def report(self, anomaly_name: str, result_bool: bool, message: str) -> None:

        status = f"{GREEN}PASS (Safe){RESET}" if result_bool else f"{RED}FAIL (Vulnerable){RESET}"
        print(f"[{anomaly_name}] {status} - {message}")

