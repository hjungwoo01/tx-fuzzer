"""
Multi-DB adapter layer for the experiment framework.

Provides a DBAdapter interface with normalized error handling, used by the
micro-workload runner, stress baseline, and case study drivers.

Adapters: PostgresAdapter, MySQLAdapter, SQLiteAdapter.
"""

from __future__ import annotations

import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .error_taxonomy import ErrorCategory, classify_error


#: The only isolation levels interpolated into SET TRANSACTION statements.
#: Whitelisting keeps a level read from a CLI flag / config file from ever
#: becoming SQL injection.
VALID_ISOLATION_LEVELS = (
    "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE",
)


def checked_isolation_level(level: str) -> str:
    """Normalize ``level`` and reject anything that is not a standard level."""
    norm = " ".join(str(level).upper().split())
    if norm not in VALID_ISOLATION_LEVELS:
        raise ValueError(
            f"invalid isolation level {level!r}; expected one of "
            f"{', '.join(VALID_ISOLATION_LEVELS)}")
    return norm


@dataclass
class ExecResult:
    """Result of a single SQL execution."""
    rows: List[Tuple[Any, ...]] = field(default_factory=list)
    rowcount: int = 0
    error_category: ErrorCategory = ErrorCategory.OK
    vendor_code: Optional[str] = None
    sqlstate: Optional[str] = None
    error_msg: Optional[str] = None
    elapsed_ms: float = 0.0


class DBAdapter(ABC):
    """Abstract interface for multi-DB experiment runner."""

    db_name: str = "unknown"

    @abstractmethod
    def connect(self, dsn: str) -> None: ...

    @abstractmethod
    def setup(self, schema_sql_list: Sequence[str]) -> None:
        """Run DDL/DML statements to set up schema."""
        ...

    @abstractmethod
    def begin(self, isolation_level: str = "") -> None: ...

    @abstractmethod
    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult: ...

    @abstractmethod
    def commit(self) -> ExecResult: ...

    @abstractmethod
    def rollback(self) -> ExecResult: ...

    @abstractmethod
    def close(self) -> None: ...

    def exec_autocommit(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        """Execute outside a transaction (autocommit). Default impl uses begin/exec/commit."""
        self.begin()
        r = self.exec(sql, params)
        if r.error_category != ErrorCategory.OK:
            self.rollback()
            return r
        cr = self.commit()
        if cr.error_category != ErrorCategory.OK:
            return cr
        return r

    def query_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        """Execute a query returning a single row. Same as exec but convenience name."""
        return self.exec(sql, params)


class PostgresExpAdapter(DBAdapter):
    db_name = "postgres"

    def __init__(self) -> None:
        self._conn = None
        self._dsn: str = ""

    def connect(self, dsn: str) -> None:
        import psycopg2
        self._dsn = dsn
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True  # we manage txns explicitly

    def setup(self, schema_sql_list: Sequence[str]) -> None:
        assert self._conn is not None
        old_ac = self._conn.autocommit
        self._conn.autocommit = True
        with self._conn.cursor() as cur:
            for sql in schema_sql_list:
                cur.execute(sql)
        self._conn.autocommit = old_ac

    def begin(self, isolation_level: str = "") -> None:
        assert self._conn is not None
        self._conn.autocommit = False
        if isolation_level:
            level = checked_isolation_level(isolation_level)
            with self._conn.cursor() as cur:
                cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                elapsed = (time.monotonic() - t0) * 1000
                rows: List[Tuple[Any, ...]] = []
                if cur.description is not None:
                    rows = cur.fetchall()
                return ExecResult(
                    rows=rows,
                    rowcount=cur.rowcount if cur.rowcount >= 0 else 0,
                    elapsed_ms=elapsed,
                )
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("postgres", e)
            # auto-rollback on error for PG (connection in error state)
            try:
                self._conn.rollback()
            except Exception:
                pass
            return ExecResult(
                error_category=cat,
                sqlstate=sqlstate,
                error_msg=msg,
                vendor_code=sqlstate,
                elapsed_ms=elapsed,
            )

    def commit(self) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            self._conn.commit()
            return ExecResult(elapsed_ms=(time.monotonic() - t0) * 1000)
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("postgres", e)
            try:
                self._conn.rollback()
            except Exception:
                pass
            return ExecResult(error_category=cat, sqlstate=sqlstate, error_msg=msg, elapsed_ms=elapsed)

    def rollback(self) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            self._conn.rollback()
            return ExecResult(elapsed_ms=(time.monotonic() - t0) * 1000)
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("postgres", e)
            return ExecResult(error_category=cat, sqlstate=sqlstate, error_msg=msg, elapsed_ms=elapsed)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def new_connection(self, dsn: Optional[str] = None) -> "PostgresExpAdapter":
        """Create a new adapter with a separate connection."""
        a = PostgresExpAdapter()
        a.connect(dsn or self._dsn)
        return a


class MySQLExpAdapter(DBAdapter):
    db_name = "mysql"

    def __init__(self) -> None:
        self._conn = None
        self._dsn: str = ""

    def _parse_dsn(self, dsn: str) -> Dict[str, Any]:
        from urllib.parse import urlparse
        u = urlparse(dsn)
        port = u.port or 3306
        db = (u.path or "/").lstrip("/") or None
        return {
            "host": u.hostname or "localhost",
            "port": port,
            "user": u.username or "root",
            "password": u.password or "",
            "database": db,
        }

    def connect(self, dsn: str) -> None:
        import pymysql
        self._dsn = dsn
        kwargs = self._parse_dsn(dsn)
        self._conn = pymysql.connect(**kwargs)
        self._conn.autocommit(True)

    def setup(self, schema_sql_list: Sequence[str]) -> None:
        assert self._conn is not None
        with self._conn.cursor() as cur:
            for sql in schema_sql_list:
                cur.execute(sql)
        self._conn.commit()

    def begin(self, isolation_level: str = "") -> None:
        assert self._conn is not None
        self._conn.autocommit(False)
        if isolation_level:
            level = checked_isolation_level(isolation_level)
            with self._conn.cursor() as cur:
                cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
        with self._conn.cursor() as cur:
            cur.execute("BEGIN")

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                elapsed = (time.monotonic() - t0) * 1000
                rows: List[Tuple[Any, ...]] = []
                if cur.description is not None:
                    rows = cur.fetchall()
                return ExecResult(
                    rows=rows,
                    rowcount=cur.rowcount if cur.rowcount >= 0 else 0,
                    elapsed_ms=elapsed,
                )
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("mysql", e)
            return ExecResult(
                error_category=cat,
                sqlstate=sqlstate,
                error_msg=msg,
                elapsed_ms=elapsed,
            )

    def commit(self) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            self._conn.commit()
            self._conn.autocommit(True)
            return ExecResult(elapsed_ms=(time.monotonic() - t0) * 1000)
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("mysql", e)
            try:
                self._conn.rollback()
            except Exception:
                pass
            return ExecResult(error_category=cat, sqlstate=sqlstate, error_msg=msg, elapsed_ms=elapsed)

    def rollback(self) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            self._conn.rollback()
            self._conn.autocommit(True)
            return ExecResult(elapsed_ms=(time.monotonic() - t0) * 1000)
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("mysql", e)
            return ExecResult(error_category=cat, sqlstate=sqlstate, error_msg=msg, elapsed_ms=elapsed)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def new_connection(self, dsn: Optional[str] = None) -> "MySQLExpAdapter":
        a = MySQLExpAdapter()
        a.connect(dsn or self._dsn)
        return a


class SQLiteExpAdapter(DBAdapter):
    db_name = "sqlite"

    def __init__(
        self,
        busy_timeout_ms: int = 5000,
        shared_cache: bool = False,
        read_uncommitted: bool = False,
    ) -> None:
        self._conn = None
        self._dsn: str = ""
        self._busy_timeout_ms = busy_timeout_ms
        self._shared_cache = shared_cache
        self._read_uncommitted = read_uncommitted
        self._in_txn = False

    def connect(self, dsn: str) -> None:
        import sqlite3
        self._dsn = dsn
        # Build connection URI if shared-cache requested
        if self._shared_cache and not dsn.startswith("file:"):
            db_path = dsn
            if db_path.startswith("sqlite://"):
                db_path = db_path.replace("sqlite://", "", 1)
            dsn = f"file:{db_path}?cache=shared"
        use_uri = dsn.startswith("file:")
        self._conn = sqlite3.connect(
            dsn, uri=use_uri,
            check_same_thread=False,
            timeout=max(1.0, self._busy_timeout_ms / 1000.0),
            isolation_level=None,  # manual txn control
        )
        self._conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        self._conn.execute("PRAGMA journal_mode = WAL")
        if self._read_uncommitted:
            self._conn.execute("PRAGMA read_uncommitted = ON")

    def setup(self, schema_sql_list: Sequence[str]) -> None:
        assert self._conn is not None
        for sql in schema_sql_list:
            self._conn.execute(sql)
        self._conn.commit()

    def begin(self, isolation_level: str = "") -> None:
        assert self._conn is not None
        # SQLite only supports DEFERRED / IMMEDIATE / EXCLUSIVE
        mode = "DEFERRED"
        iso_upper = (isolation_level or "").upper().strip()
        if "EXCLUSIVE" in iso_upper or "SERIALIZABLE" in iso_upper:
            mode = "EXCLUSIVE"
        elif "IMMEDIATE" in iso_upper:
            mode = "IMMEDIATE"
        try:
            self._conn.execute(f"BEGIN {mode}")
            self._in_txn = True
        except Exception:
            # Already in transaction or BUSY: ignore
            self._in_txn = True

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            cur = self._conn.execute(sql, params or ())
            elapsed = (time.monotonic() - t0) * 1000
            rows: List[Tuple[Any, ...]] = []
            if cur.description is not None:
                rows = cur.fetchall()
            return ExecResult(
                rows=rows,
                rowcount=cur.rowcount if cur.rowcount >= 0 else 0,
                elapsed_ms=elapsed,
            )
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("sqlite", e)
            return ExecResult(
                error_category=cat,
                error_msg=msg,
                elapsed_ms=elapsed,
            )

    def commit(self) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            self._conn.execute("COMMIT")
            self._in_txn = False
            return ExecResult(elapsed_ms=(time.monotonic() - t0) * 1000)
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("sqlite", e)
            return ExecResult(error_category=cat, error_msg=msg, elapsed_ms=elapsed)

    def rollback(self) -> ExecResult:
        assert self._conn is not None
        t0 = time.monotonic()
        try:
            self._conn.execute("ROLLBACK")
            self._in_txn = False
            return ExecResult(elapsed_ms=(time.monotonic() - t0) * 1000)
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            cat, sqlstate, msg = classify_error("sqlite", e)
            return ExecResult(error_category=cat, error_msg=msg, elapsed_ms=elapsed)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def new_connection(self, dsn: Optional[str] = None) -> "SQLiteExpAdapter":
        a = SQLiteExpAdapter(
            busy_timeout_ms=self._busy_timeout_ms,
            shared_cache=self._shared_cache,
            read_uncommitted=self._read_uncommitted,
        )
        a.connect(dsn or self._dsn)
        return a


DEFAULT_DSNS = {
    "postgres": "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
    "mysql": "mysql://root:root@localhost:3307/test",
    "sqlite": "/tmp/tx_fuzzer_multidb.sqlite",
}


def make_adapter(
    db: str,
    dsn: Optional[str] = None,
    *,
    sqlite_shared_cache: bool = False,
    sqlite_read_uncommitted: bool = False,
    sqlite_busy_timeout_ms: int = 5000,
) -> DBAdapter:
    """Create and connect a DBAdapter for the given database kind."""
    db = db.lower().strip()
    actual_dsn = dsn or DEFAULT_DSNS.get(db, "")
    if db == "postgres":
        a = PostgresExpAdapter()
        a.connect(actual_dsn)
        return a
    if db == "mysql":
        a = MySQLExpAdapter()
        a.connect(actual_dsn)
        return a
    if db == "sqlite":
        a = SQLiteExpAdapter(
            busy_timeout_ms=sqlite_busy_timeout_ms,
            shared_cache=sqlite_shared_cache,
            read_uncommitted=sqlite_read_uncommitted,
        )
        a.connect(actual_dsn)
        return a
    raise ValueError(f"Unknown db: {db!r}. Supported: postgres, mysql, sqlite")
