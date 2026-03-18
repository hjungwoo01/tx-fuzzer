"""Multi-DB execution adapters for PostgreSQL, MySQL, and SQLite."""

from __future__ import annotations

import os
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import pymysql
except ImportError:
    pymysql = None


MYSQL_DEADLOCK_ERRNO = 1213
MYSQL_LOCK_WAIT_TIMEOUT_ERRNO = 1205


@dataclass
class ExecResult:
    rows: List[Tuple[Any, ...]]
    status: str
    error_code: Optional[str]
    error_message: Optional[str]
    rowcount: int = -1


class DBAdapter(ABC):
    """Unified adapter interface used by the multi-DB suite runner."""

    def __init__(self) -> None:
        self.round_trips = 0

    @abstractmethod
    def connect(self, dsn: str, autocommit: bool = False) -> None:
        ...

    @abstractmethod
    def begin(self, isolation_level: Optional[str], begin_mode: Optional[str] = None) -> ExecResult:
        ...

    @abstractmethod
    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        ...

    @abstractmethod
    def commit(self) -> ExecResult:
        ...

    @abstractmethod
    def rollback(self) -> ExecResult:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def set_pragmas(self, settings: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def backend_name(self) -> str:
        ...

    @abstractmethod
    def version_string(self) -> str:
        ...


class PostgresAdapter(DBAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.conn: Any = None
        self.cur: Any = None

    def connect(self, dsn: str, autocommit: bool = False) -> None:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is required for PostgresAdapter")
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = autocommit
        self.cur = self.conn.cursor()

    def begin(self, isolation_level: Optional[str], begin_mode: Optional[str] = None) -> ExecResult:
        _ = begin_mode
        r = self.exec("BEGIN")
        if r.status != "ok":
            return r
        if isolation_level:
            return self.exec(
                f"SET TRANSACTION ISOLATION LEVEL {' '.join(isolation_level.upper().split())}"
            )
        return r

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        self.round_trips += 1
        try:
            if params is None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, tuple(params))
            rows: List[Tuple[Any, ...]] = []
            if self.cur.description is not None:
                rows = [tuple(r) for r in self.cur.fetchall()]
            return ExecResult(
                rows=rows,
                status="ok",
                error_code=None,
                error_message=None,
                rowcount=int(getattr(self.cur, "rowcount", -1)),
            )
        except Exception as e:
            return ExecResult(
                rows=[],
                status="error",
                error_code=str(getattr(e, "pgcode", None) or type(e).__name__),
                error_message=str(e),
                rowcount=-1,
            )

    def commit(self) -> ExecResult:
        try:
            self.conn.commit()
            return ExecResult([], "ok", None, None, 0)
        except Exception as e:
            return ExecResult([], "error", str(getattr(e, "pgcode", None) or type(e).__name__), str(e), -1)

    def rollback(self) -> ExecResult:
        try:
            self.conn.rollback()
            return ExecResult([], "ok", None, None, 0)
        except Exception as e:
            return ExecResult([], "error", str(getattr(e, "pgcode", None) or type(e).__name__), str(e), -1)

    def close(self) -> None:
        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            pass
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass

    def set_pragmas(self, settings: Dict[str, Any]) -> None:
        for key, val in settings.items():
            if isinstance(val, str):
                self.exec(f"SET SESSION {key} = '{val}'")
            else:
                self.exec(f"SET SESSION {key} = %s", [val])

    def backend_name(self) -> str:
        return "postgres"

    def version_string(self) -> str:
        r = self.exec("SELECT version()")
        if r.status != "ok" or not r.rows:
            return "unknown"
        return str(r.rows[0][0])


class MySQLAdapter(DBAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.conn: Any = None
        self.cur: Any = None

    def _parse_dsn(self, dsn: str) -> Dict[str, Any]:
        if pymysql is None:
            raise RuntimeError("pymysql is required for MySQLAdapter")
        parsed = urlparse(dsn)
        if parsed.scheme not in ("mysql", "mysql+pymysql"):
            raise ValueError(f"Unsupported MySQL DSN scheme: {parsed.scheme!r}")
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": parsed.username or "root",
            "password": parsed.password or "",
            "database": (parsed.path or "/").lstrip("/") or None,
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.Cursor,
        }

    def connect(self, dsn: str, autocommit: bool = False) -> None:
        kwargs = self._parse_dsn(dsn)
        kwargs["autocommit"] = autocommit
        self.conn = pymysql.connect(**kwargs)
        self.cur = self.conn.cursor()

    def begin(self, isolation_level: Optional[str], begin_mode: Optional[str] = None) -> ExecResult:
        _ = begin_mode
        if isolation_level:
            level = " ".join(isolation_level.upper().split())
            r = self.exec(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
            if r.status != "ok":
                return r
        return self.exec("BEGIN")

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        self.round_trips += 1
        try:
            if params is None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, tuple(params))
            rows: List[Tuple[Any, ...]] = []
            if self.cur.description is not None:
                rows = [tuple(r) for r in self.cur.fetchall()]
            return ExecResult(
                rows=rows,
                status="ok",
                error_code=None,
                error_message=None,
                rowcount=int(getattr(self.cur, "rowcount", -1)),
            )
        except Exception as e:
            code = None
            if hasattr(e, "args") and e.args:
                code = str(e.args[0])
            return ExecResult(
                rows=[],
                status="error",
                error_code=code or type(e).__name__,
                error_message=str(e),
                rowcount=-1,
            )

    def commit(self) -> ExecResult:
        try:
            self.conn.commit()
            return ExecResult([], "ok", None, None, 0)
        except Exception as e:
            code = None
            if hasattr(e, "args") and e.args:
                code = str(e.args[0])
            return ExecResult([], "error", code or type(e).__name__, str(e), -1)

    def rollback(self) -> ExecResult:
        try:
            self.conn.rollback()
            return ExecResult([], "ok", None, None, 0)
        except Exception as e:
            code = None
            if hasattr(e, "args") and e.args:
                code = str(e.args[0])
            return ExecResult([], "error", code or type(e).__name__, str(e), -1)

    def close(self) -> None:
        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            pass
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass

    def set_pragmas(self, settings: Dict[str, Any]) -> None:
        for key, val in settings.items():
            if isinstance(val, str):
                self.exec(f"SET SESSION {key} = '{val}'")
            else:
                self.exec(f"SET SESSION {key} = %s", [val])

    def backend_name(self) -> str:
        return "mysql"

    def version_string(self) -> str:
        r = self.exec("SELECT VERSION()")
        if r.status != "ok" or not r.rows:
            return "unknown"
        return str(r.rows[0][0])


class SQLiteAdapter(DBAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.conn: Optional[sqlite3.Connection] = None
        self.cur: Optional[sqlite3.Cursor] = None
        self._dsn: Optional[str] = None

    def _resolve_sqlite_target(self, dsn: str) -> Tuple[str, bool]:
        raw = (dsn or "").strip()
        if not raw:
            raise ValueError("sqlite dsn/path must not be empty")
        if raw.startswith("sqlite://file:"):
            return raw[len("sqlite://") :], True
        if raw.startswith("sqlite:///"):
            return raw[len("sqlite:///") :], False
        if raw.startswith("sqlite://"):
            return raw[len("sqlite://") :], False
        if raw.startswith("file:"):
            return raw, True
        return raw, False

    def connect(self, dsn: str, autocommit: bool = False) -> None:
        path, use_uri = self._resolve_sqlite_target(dsn)
        if not use_uri:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        self._dsn = dsn
        self.conn = sqlite3.connect(
            path,
            uri=use_uri,
            check_same_thread=False,
            timeout=5.0,
            isolation_level=None if autocommit else None,
        )
        self.cur = self.conn.cursor()
        self.set_pragmas(
            {
                "busy_timeout": 5000,
                "journal_mode": "WAL",
                "synchronous": "NORMAL",
            }
        )

    def begin(self, isolation_level: Optional[str], begin_mode: Optional[str] = None) -> ExecResult:
        _ = isolation_level
        mode = (begin_mode or "DEFERRED").strip().upper()
        if mode not in ("DEFERRED", "IMMEDIATE", "EXCLUSIVE"):
            mode = "DEFERRED"
        return self.exec(f"BEGIN {mode}")

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> ExecResult:
        self.round_trips += 1
        try:
            if params is None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, tuple(params))
            rows: List[Tuple[Any, ...]] = []
            if self.cur.description is not None:
                rows = [tuple(r) for r in self.cur.fetchall()]
            return ExecResult(
                rows=rows,
                status="ok",
                error_code=None,
                error_message=None,
                rowcount=int(getattr(self.cur, "rowcount", -1)),
            )
        except Exception as e:
            err_code = getattr(e, "sqlite_errorcode", None)
            return ExecResult(
                rows=[],
                status="error",
                error_code=str(err_code) if err_code is not None else type(e).__name__,
                error_message=str(e),
                rowcount=-1,
            )

    def commit(self) -> ExecResult:
        try:
            self.conn.commit()
            return ExecResult([], "ok", None, None, 0)
        except Exception as e:
            err_code = getattr(e, "sqlite_errorcode", None)
            return ExecResult([], "error", str(err_code) if err_code is not None else type(e).__name__, str(e), -1)

    def rollback(self) -> ExecResult:
        try:
            self.conn.rollback()
            return ExecResult([], "ok", None, None, 0)
        except Exception as e:
            err_code = getattr(e, "sqlite_errorcode", None)
            return ExecResult([], "error", str(err_code) if err_code is not None else type(e).__name__, str(e), -1)

    def close(self) -> None:
        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            pass
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass

    def set_pragmas(self, settings: Dict[str, Any]) -> None:
        for key, val in settings.items():
            if isinstance(val, str):
                self.exec(f"PRAGMA {key} = '{val}'")
            else:
                self.exec(f"PRAGMA {key} = {val}")

    def backend_name(self) -> str:
        return "sqlite"

    def version_string(self) -> str:
        r = self.exec("SELECT sqlite_version()")
        if r.status != "ok" or not r.rows:
            return "unknown"
        return str(r.rows[0][0])


def create_adapter(db: str) -> DBAdapter:
    norm = db.strip().lower()
    if norm == "postgres":
        return PostgresAdapter()
    if norm == "mysql":
        return MySQLAdapter()
    if norm == "sqlite":
        return SQLiteAdapter()
    raise ValueError(f"Unsupported db {db!r}")


def execute_sql_list(db: str, dsn: str, sqls: Iterable[str]) -> None:
    ad = create_adapter(db)
    ad.connect(dsn, autocommit=True)
    try:
        for sql in sqls:
            ad.exec(sql)
    finally:
        ad.close()


def read_db_version(db: str, dsn: str) -> str:
    ad = create_adapter(db)
    ad.connect(dsn, autocommit=True)
    try:
        return ad.version_string()
    finally:
        ad.close()


def wait_for_db(db: str, dsn: str, timeout_s: float = 30.0) -> bool:
    import time

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            _ = read_db_version(db, dsn)
            return True
        except Exception:
            time.sleep(0.5)
    return False

