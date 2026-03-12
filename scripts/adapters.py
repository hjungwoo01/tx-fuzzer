"""
Database adapters for litmus tests.

Provides a unified interface (connect, setup_schema, execute_transaction, teardown)
so LitmusTester and run_litmus can drive tests against PostgreSQL, MySQL, or Redis.
"""

from __future__ import annotations

import re
import sys
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from logger import HistoryLogger

try:
    import psycopg2
    from psycopg2.extensions import connection as PGConnection
except ImportError:
    psycopg2 = None
    PGConnection = None

try:
    import pymysql
    from pymysql.cursors import Cursor as MySQLCursor
except ImportError:
    pymysql = None
    MySQLCursor = None

try:
    import sqlite3
except ImportError:
    sqlite3 = None

try:
    import redis
except ImportError:
    redis = None


def nuke_postgres_connections(dsn: str) -> int:
    """
    Terminate all *other* connections to the target database (same DB as DSN).
    Uses pg_terminate_backend(pid) for every backend whose pid != current.
    Returns the number of connections killed. Logs: "Nuked N zombie connections."
    """
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required for nuke_postgres_connections")
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pid FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid <> pg_backend_pid()"
            )
            pids = [row[0] for row in cur.fetchall()]
        nuked = 0
        with conn.cursor() as cur:
            for pid in pids:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                if cur.fetchone()[0]:
                    nuked += 1
        print(f"💥 Nuked {nuked} zombie connection(s).")
        return nuked
    finally:
        conn.close()


def _sql_summary(sql: str, max_len: int = 60) -> str:
    """One-line summary of SQL for logging."""
    s = " ".join(sql.split()).strip()
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


class _LoggingCursor:
    """Wraps a DB cursor and logs INVOKE/OK/FAIL for execute()."""

    def __init__(self, real_cursor: Any, client_id: str, logger: "HistoryLogger") -> None:
        self._cur = real_cursor
        self._client_id = client_id
        self._logger = logger

    def __enter__(self) -> "_LoggingCursor":
        enter = getattr(self._cur, "__enter__", None)
        if callable(enter):
            enter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        exit_ = getattr(self._cur, "__exit__", None)
        if callable(exit_):
            exit_(exc_type, exc_val, exc_tb)
        else:
            try:
                self._cur.close()
            except Exception:
                pass

    def execute(self, operation: str, args: Optional[tuple] = None) -> None:
        summary = _sql_summary(operation)
        self._logger.log(self._client_id, "INVOKE", summary)
        try:
            if args is not None:
                self._cur.execute(operation, args)
            else:
                self._cur.execute(operation)
            self._logger.log(self._client_id, "OK", summary)
        except Exception as e:
            self._logger.log(self._client_id, "FAIL", f"{summary} -> {e!r}")
            raise

    def fetchone(self) -> Any:
        return self._cur.fetchone()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cur, name)


class _LoggingConn:
    """Wraps a DB connection and logs INVOKE/OK/FAIL for commit() and rollback()."""

    def __init__(self, real_conn: Any, client_id: str, logger: "HistoryLogger") -> None:
        self._conn = real_conn
        self._client_id = client_id
        self._logger = logger

    def commit(self) -> None:
        self._logger.log(self._client_id, "INVOKE", "COMMIT")
        try:
            self._conn.commit()
            self._logger.log(self._client_id, "OK", "COMMIT")
        except Exception as e:
            self._logger.log(self._client_id, "FAIL", f"COMMIT -> {e!r}")
            raise

    def rollback(self) -> None:
        self._logger.log(self._client_id, "INVOKE", "ROLLBACK")
        try:
            self._conn.rollback()
            self._logger.log(self._client_id, "OK", "ROLLBACK")
        except Exception as e:
            self._logger.log(self._client_id, "FAIL", f"ROLLBACK -> {e!r}")
            raise

    def cursor(self) -> Any:
        from logger import HistoryLogger
        real_cursor = self._conn.cursor()
        return _LoggingCursor(real_cursor, self._client_id, self._logger)

    @property
    def autocommit(self) -> Any:
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, value: Any) -> None:
        self._conn.autocommit = value

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class DatabaseAdapter(ABC):
    """Abstract base for database backends used by litmus tests."""

    _logger: Optional["HistoryLogger"] = None

    @abstractmethod
    def connect(self) -> None:
        """Open connections and prepare two sessions (conn1/cur1, conn2/cur2)."""
        ...

    @abstractmethod
    def setup_schema(self, key_count: int = 2) -> None:
        """Create/reset schema and seed the kv table (keys 1..key_count with value 0)."""
        ...

    def execute_transaction(self, logic_func: Callable[[Any], None]) -> None:
        """Run test logic with access to this adapter (e.g. adapter.conn1, adapter.cur1)."""
        logic_func(self)

    def ensure_key(self, key: int, value: int) -> None:
        """Ensure key exists with given value (idempotent). Used by mixed-attack runner."""
        raise NotImplementedError("ensure_key not implemented for this adapter")

    def _client_id(self, conn_id: int) -> str:
        return "T1" if conn_id == 1 else "T2"

    def _cursor(self, conn_id: int) -> Any:
        return self.cur1 if conn_id == 1 else self.cur2

    def read(self, key: int, conn_id: int = 1) -> int:
        """Read value for key; logs semantic 'READ Key=N' and 'RETURNED v' when logger is set."""
        c = self._cursor(conn_id)
        client_id = self._client_id(conn_id)
        if self._logger:
            self._logger.log(client_id, "INVOKE", f"READ Key={key}")
        c.execute("SELECT v FROM kv WHERE k = %s", (key,))
        row = c.fetchone()
        v = int(row[0]) if row is not None else 0
        if self._logger:
            self._logger.log(client_id, "OK", f"RETURNED {v}")
        return v

    def write(self, key: int, val: int, conn_id: int = 1) -> None:
        """Write value for key; logs semantic 'WRITE Key=N=V' when logger is set."""
        c = self._cursor(conn_id)
        client_id = self._client_id(conn_id)
        if self._logger:
            self._logger.log(client_id, "INVOKE", f"WRITE Key={key}={val}")
        c.execute("UPDATE kv SET v = %s WHERE k = %s", (val, key))
        if self._logger:
            self._logger.log(client_id, "OK", f"WRITE Key={key}={val}")

    @abstractmethod
    def teardown(self) -> None:
        """Close connections and release resources."""
        ...


_ISOLATION_LEVELS = {
    "READ COMMITTED": "READ COMMITTED",
    "REPEATABLE READ": "REPEATABLE READ",
    "SERIALIZABLE": "SERIALIZABLE",
}


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL backend using psycopg2."""

    def __init__(
        self,
        dsn: str,
        isolation_level: str = "READ COMMITTED",
        logger: Optional["HistoryLogger"] = None,
    ) -> None:
        if psycopg2 is None:
            raise RuntimeError(
                "psycopg2 is required for PostgresAdapter. Install for this Python with:\n"
                f"  {sys.executable} -m pip install psycopg2-binary"
            )
        self.dsn = dsn
        self._logger = logger
        iso = " ".join(str(isolation_level).upper().split())
        if iso not in _ISOLATION_LEVELS:
            raise ValueError(
                f"Unsupported isolation level {isolation_level!r}. "
                f"Valid: {', '.join(sorted(_ISOLATION_LEVELS))}"
            )
        self._isolation_level = _ISOLATION_LEVELS[iso]
        self.conn1: Optional[PGConnection] = None
        self.conn2: Optional[PGConnection] = None
        self.cur1 = None
        self.cur2 = None

    def connect(self) -> None:
        self.conn1 = psycopg2.connect(self.dsn)
        self.conn2 = psycopg2.connect(self.dsn)
        self.conn1.autocommit = False
        self.conn2.autocommit = False
        level_sql = self._isolation_level
        for conn in (self.conn1, self.conn2):
            with conn.cursor() as c:
                c.execute(
                    f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level_sql}"
                )
            conn.commit()
        self.cur1 = self.conn1.cursor()
        self.cur2 = self.conn2.cursor()
        if self._logger is not None:
            self.conn1 = _LoggingConn(self.conn1, "T1", self._logger)
            self.conn2 = _LoggingConn(self.conn2, "T2", self._logger)
            self.cur1 = _LoggingCursor(self.cur1, "T1", self._logger)
            self.cur2 = _LoggingCursor(self.cur2, "T2", self._logger)

    def setup_schema(self, key_count: int = 2) -> None:
        self.cur1.execute("TRUNCATE TABLE kv")
        values = ", ".join(f"({i}, 0)" for i in range(1, key_count + 1))
        self.cur1.execute(f"INSERT INTO kv (k, v) VALUES {values}")
        self.conn1.commit()

    def ensure_key(self, key: int, value: int) -> None:
        self.cur1.execute("BEGIN")
        self.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, %s) ON CONFLICT (k) DO NOTHING",
            (key, value),
        )
        self.conn1.commit()

    def teardown(self) -> None:
        for cur in (self.cur1, self.cur2):
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
        for conn in (self.conn1, self.conn2):
            try:
                if conn is not None and not conn.closed:
                    conn.close()
            except Exception:
                pass


class MySQLAdapter(DatabaseAdapter):
    """MySQL backend using pymysql. Uses MySQL transaction isolation and locking syntax (e.g. FOR UPDATE)."""

    def __init__(
        self,
        dsn: str,
        isolation_level: str = "READ COMMITTED",
        logger: Optional["HistoryLogger"] = None,
    ) -> None:
        if pymysql is None:
            raise RuntimeError(
                "pymysql is required for MySQLAdapter. Install for this Python with:\n"
                f"  {sys.executable} -m pip install pymysql"
            )
        self.dsn = dsn
        self._logger = logger
        iso = " ".join(str(isolation_level).upper().split())
        if iso not in _ISOLATION_LEVELS:
            raise ValueError(
                f"Unsupported isolation level {isolation_level!r}. "
                f"Valid: {', '.join(sorted(_ISOLATION_LEVELS))}"
            )
        self._isolation_level = iso
        self.conn1 = None
        self.conn2 = None
        self.cur1 = None
        self.cur2 = None

    def _parse_dsn(self) -> dict[str, Any]:
        from urllib.parse import urlparse
        u = urlparse(self.dsn)
        if u.scheme not in ("mysql", "mysql+pymysql"):
            raise ValueError(f"MySQL DSN must start with mysql:// or mysql+pymysql://, got {self.dsn!r}")
        port = u.port or 3306
        db = (u.path or "/").lstrip("/") or None
        return {
            "host": u.hostname or "localhost",
            "port": port,
            "user": u.username or "root",
            "password": u.password or "",
            "database": db,
            "cursorclass": pymysql.cursors.DictCursor,
        }

    def connect(self) -> None:
        kwargs = self._parse_dsn()
        
        kwargs["cursorclass"] = pymysql.cursors.Cursor
        try:
            self.conn1 = pymysql.connect(**kwargs)
            self.conn2 = pymysql.connect(**kwargs)
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1698:
                raise RuntimeError(
                    "MySQL access denied (1698): 'root' on this system often uses socket auth.\n"
                    "Use a user with password auth and pass --dsn, e.g.:\n"
                    "  --dsn 'mysql://USER:PASSWORD@localhost:3306/test'\n"
                    "Or create one: CREATE USER 'test'@'localhost' IDENTIFIED BY 'test'; GRANT ALL ON test.* TO 'test'@'localhost';"
                ) from e
            raise
        self.conn1.autocommit(False)
        self.conn2.autocommit(False)
        level_sql = f"SET SESSION TRANSACTION ISOLATION LEVEL {self._isolation_level}"
        for conn in (self.conn1, self.conn2):
            with conn.cursor() as c:
                c.execute(level_sql)
        self.cur1 = self.conn1.cursor()
        self.cur2 = self.conn2.cursor()
        if self._logger is not None:
            self.conn1 = _LoggingConn(self.conn1, "T1", self._logger)
            self.conn2 = _LoggingConn(self.conn2, "T2", self._logger)
            self.cur1 = _LoggingCursor(self.cur1, "T1", self._logger)
            self.cur2 = _LoggingCursor(self.cur2, "T2", self._logger)

    def setup_schema(self, key_count: int = 2) -> None:
        
        self.cur1.execute(
            "CREATE TABLE IF NOT EXISTS kv ("
            "k INT PRIMARY KEY, "
            "v INT NOT NULL, "
            "ts BIGINT NOT NULL DEFAULT 0"
            ")"
        )
        self.conn1.commit()
        self.cur1.execute("TRUNCATE TABLE kv")
        values = ", ".join(f"({i}, 0, 0)" for i in range(1, key_count + 1))
        self.cur1.execute(f"INSERT INTO kv (k, v, ts) VALUES {values}")
        self.conn1.commit()

    def ensure_key(self, key: int, value: int) -> None:
        self.cur1.execute("BEGIN")
        self.cur1.execute(
            "INSERT INTO kv (k, v, ts) VALUES (%s, %s, 0) "
            "ON DUPLICATE KEY UPDATE k=k",
            (key, value),
        )
        self.conn1.commit()

    def teardown(self) -> None:
        for cur in (self.cur1, self.cur2):
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
        for conn in (self.conn1, self.conn2):
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass


class SQLiteAdapter(DatabaseAdapter):
    """SQLite backend using stdlib sqlite3 with file-backed database for concurrency tests."""

    def __init__(
        self,
        dsn: str,
        isolation_level: str = "READ COMMITTED",
        logger: Optional["HistoryLogger"] = None,
        busy_timeout_ms: int = 5000,
        journal_mode: str = "WAL",
        synchronous: str = "NORMAL",
    ) -> None:
        if sqlite3 is None:
            raise RuntimeError("sqlite3 stdlib module is required for SQLiteAdapter.")
        self.dsn = dsn
        self._isolation_level = isolation_level
        self._logger = logger
        self._busy_timeout_ms = int(max(0, busy_timeout_ms))
        self._journal_mode = journal_mode
        self._synchronous = synchronous
        self.conn1 = None
        self.conn2 = None
        self.cur1 = None
        self.cur2 = None

    def _parse_dsn(self) -> tuple[str, bool]:
        raw = (self.dsn or "").strip()
        if not raw:
            raise ValueError("SQLite DSN/path must not be empty")

        # Accept:
        # - file:/tmp/db.sqlite?cache=shared   (URI mode)
        # - sqlite:///tmp/db.sqlite            (path mode)
        # - sqlite://file:/tmp/db.sqlite?...   (URI mode)
        # - /tmp/db.sqlite                     (path mode)
        if raw.startswith("file:"):
            return (raw, True)
        if raw.startswith("sqlite://file:"):
            return (raw[len("sqlite://") :], True)
        if raw.startswith("sqlite:///"):
            return (raw[len("sqlite:///") :], False)
        if raw.startswith("sqlite://"):
            return (raw[len("sqlite://") :], False)
        return (raw, False)

    def _apply_pragmas(self, conn: Any) -> None:
        c = conn.cursor()
        try:
            c.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            if self._journal_mode:
                c.execute(f"PRAGMA journal_mode = {self._journal_mode}")
            if self._synchronous:
                c.execute(f"PRAGMA synchronous = {self._synchronous}")
        finally:
            try:
                c.close()
            except Exception:
                pass
        conn.commit()

    def connect(self) -> None:
        db_path, use_uri = self._parse_dsn()
        kwargs = {
            "database": db_path,
            "uri": use_uri,
            "check_same_thread": False,
            "timeout": max(0.0, self._busy_timeout_ms / 1000.0),
            # Manual transaction control via explicit BEGIN/COMMIT in runners.
            "isolation_level": None,
        }
        self.conn1 = sqlite3.connect(**kwargs)
        self.conn2 = sqlite3.connect(**kwargs)
        self._apply_pragmas(self.conn1)
        self._apply_pragmas(self.conn2)
        self.cur1 = self.conn1.cursor()
        self.cur2 = self.conn2.cursor()
        if self._logger is not None:
            self.conn1 = _LoggingConn(self.conn1, "T1", self._logger)
            self.conn2 = _LoggingConn(self.conn2, "T2", self._logger)
            self.cur1 = _LoggingCursor(self.cur1, "T1", self._logger)
            self.cur2 = _LoggingCursor(self.cur2, "T2", self._logger)

    def setup_schema(self, key_count: int = 2) -> None:
        self.cur1.execute(
            "CREATE TABLE IF NOT EXISTS kv ("
            "k INTEGER PRIMARY KEY, "
            "v INTEGER NOT NULL, "
            "ts INTEGER NOT NULL DEFAULT 0"
            ")"
        )
        self.conn1.commit()
        self.cur1.execute("DELETE FROM kv")
        values = [(i, 0, 0) for i in range(1, key_count + 1)]
        self.cur1.executemany("INSERT INTO kv (k, v, ts) VALUES (?, ?, ?)", values)
        self.conn1.commit()

    def ensure_key(self, key: int, value: int) -> None:
        self.cur1.execute("BEGIN")
        self.cur1.execute(
            "INSERT OR IGNORE INTO kv (k, v, ts) VALUES (?, ?, 0)",
            (key, value),
        )
        self.conn1.commit()

    def read(self, key: int, conn_id: int = 1) -> int:
        c = self._cursor(conn_id)
        client_id = self._client_id(conn_id)
        if self._logger:
            self._logger.log(client_id, "INVOKE", f"READ Key={key}")
        c.execute("SELECT v FROM kv WHERE k = ?", (key,))
        row = c.fetchone()
        v = int(row[0]) if row is not None else 0
        if self._logger:
            self._logger.log(client_id, "OK", f"RETURNED {v}")
        return v

    def write(self, key: int, val: int, conn_id: int = 1) -> None:
        c = self._cursor(conn_id)
        client_id = self._client_id(conn_id)
        if self._logger:
            self._logger.log(client_id, "INVOKE", f"WRITE Key={key}={val}")
        c.execute("UPDATE kv SET v = ? WHERE k = ?", (val, key))
        if self._logger:
            self._logger.log(client_id, "OK", f"WRITE Key={key}={val}")

    def teardown(self) -> None:
        for cur in (self.cur1, self.cur2):
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
        for conn in (self.conn1, self.conn2):
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass


_SELECT_KV = re.compile(r"SELECT\s+v\s+FROM\s+kv\s+WHERE\s+k\s*=\s*(\d+|\?|%s)", re.I)
_UPDATE_KV = re.compile(r"UPDATE\s+kv\s+SET\s+v\s*=\s*(\d+|\?|%s)\s+WHERE\s+k\s*=\s*(\d+|\?|%s)", re.I)


def _redis_key(k: int) -> str:
    return f"kv:{k}"


class _RedisFakeCursor:
    """Cursor-like object that maps SQL to Redis commands. Begin -> pipeline.multi(), Commit -> pipeline.execute()."""

    def __init__(self, client: redis.Redis, conn: "_RedisFakeConn") -> None:
        self._client = client
        self._conn = conn
        self._last_result: Optional[tuple] = None

    def __enter__(self) -> "_RedisFakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def execute(self, operation: str, args: Optional[tuple] = None) -> None:
        op = (operation or "").strip().upper()
        params = args or ()

        if op == "BEGIN":
            self._conn._begin()
            return
        if op == "COMMIT":
            self._conn._commit()
            return
        if op == "ROLLBACK":
            self._conn._rollback()
            return

        m = _SELECT_KV.match(operation.strip())
        if m:
            g = m.group(1)
            k = int(params[0]) if params and (g in ("?", "%s")) else int(g)
            if self._conn._in_transaction and self._conn._pipe is not None:
                self._conn._pipe.get(_redis_key(k))
                self._last_result = None
            else:
                raw = self._client.get(_redis_key(k))
                self._last_result = (int(raw),) if raw is not None else (0,)
            return

        m = _UPDATE_KV.match(operation.strip())
        if m:
            g1, g2 = m.group(1), m.group(2)
            if params and len(params) >= 2:
                v, k = params[0], params[1]
            elif g1 in ("?", "%s") and params:
                v = params[0]
                k = int(params[1]) if len(params) > 1 and g2 in ("?", "%s") else int(g2)
            else:
                v = int(g1)
                k = int(g2)
            key = _redis_key(k)
            if self._conn._in_transaction:
                if self._conn._pipe is None:
                    self._conn._pipe = self._client.pipeline(transaction=True)
                    self._conn._pipe.multi()
                self._conn._pipe.set(key, str(v))
            else:
                self._client.set(key, str(v))
            self._last_result = None
            return

        raise NotImplementedError(f"Redis adapter does not support: {operation!r}")

    def fetchone(self) -> Optional[tuple]:
        if self._conn._in_transaction and self._conn._pipe is not None:
            results = self._conn._pipe.execute()
            self._conn._pipe = None
            if results and results[0] is not None:
                self._last_result = (int(results[0]),) if isinstance(results[0], bytes) else (results[0],)
            else:
                self._last_result = (0,)
        out = self._last_result
        self._last_result = None
        return out


class _RedisFakeConn:
    """Connection-like object: autocommit flag and commit/rollback map to pipeline execute/discard."""

    def __init__(self, client: redis.Redis, cursor: _RedisFakeCursor) -> None:
        self._client = client
        self._cursor = cursor
        self._autocommit = False
        self._in_transaction = False
        self._pipe: Optional[redis.client.Pipeline] = None

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._autocommit = value

    def _begin(self) -> None:
        self._pipe = self._client.pipeline(transaction=True)
        self._pipe.multi()
        self._in_transaction = True

    def _commit(self) -> None:
        if self._pipe is not None:
            self._pipe.execute()
        self._pipe = None
        self._in_transaction = False

    def _rollback(self) -> None:
        if self._pipe is not None:
            try:
                self._pipe.reset()
            except Exception:
                pass
        self._pipe = None
        self._in_transaction = False

    def commit(self) -> None:
        self._commit()

    def rollback(self) -> None:
        self._rollback()

    def cursor(self) -> _RedisFakeCursor:
        return self._cursor

    @property
    def closed(self) -> bool:
        return False


class RedisAdapter(DatabaseAdapter):
    """
    Redis backend using redis-py. No SQL; maps logical operations to Redis:
    - Begin -> pipeline.multi()
    - Commit -> pipeline.execute()
    - SELECT v FROM kv WHERE k = ? -> GET kv:k
    - UPDATE kv SET v = ? WHERE k = ? -> SET kv:k v
    """

    def __init__(
        self,
        dsn: str,
        isolation_level: str = "READ COMMITTED",
        logger: Optional["HistoryLogger"] = None,
    ) -> None:
        if redis is None:
            raise RuntimeError(
                "redis is required for RedisAdapter. Install for this Python with:\n"
                f"  {sys.executable} -m pip install redis"
            )
        self.dsn = dsn
        self._isolation_level = isolation_level
        self._logger = logger
        self._client1: Optional[redis.Redis] = None
        self._client2: Optional[redis.Redis] = None
        self.conn1: Optional[_RedisFakeConn] = None
        self.conn2: Optional[_RedisFakeConn] = None
        self.cur1: Optional[_RedisFakeCursor] = None
        self.cur2: Optional[_RedisFakeCursor] = None

    def _parse_dsn(self) -> dict[str, Any]:
        from urllib.parse import urlparse
        u = urlparse(self.dsn)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError(f"Redis DSN must start with redis:// or rediss://, got {self.dsn!r}")
        return {
            "host": u.hostname or "localhost",
            "port": u.port or 6379,
            "password": u.password or None,
            "db": int(u.path.lstrip("/")) if u.path and u.path != "/" else 0,
            "decode_responses": False,
        }

    def connect(self) -> None:
        kwargs = self._parse_dsn()
        self._client1 = redis.Redis(**kwargs)
        self._client2 = redis.Redis(**kwargs)
        cur1 = _RedisFakeCursor(self._client1, None)
        cur2 = _RedisFakeCursor(self._client2, None)
        self.conn1 = _RedisFakeConn(self._client1, cur1)
        self.conn2 = _RedisFakeConn(self._client2, cur2)
        cur1._conn = self.conn1
        cur2._conn = self.conn2
        self.cur1 = cur1
        self.cur2 = cur2
        if self._logger is not None:
            self.conn1 = _LoggingConn(self.conn1, "T1", self._logger)
            self.conn2 = _LoggingConn(self.conn2, "T2", self._logger)
            self.cur1 = _LoggingCursor(self.cur1, "T1", self._logger)
            self.cur2 = _LoggingCursor(self.cur2, "T2", self._logger)

    def setup_schema(self, key_count: int = 2) -> None:
        for k in range(1, key_count + 1):
            self._client1.set(_redis_key(k), "0")
        self._client1.delete(_redis_key(0))
    
    def ensure_key(self, key: int, value: int) -> None:
        self._client1.set(_redis_key(key), str(value))

    def teardown(self) -> None:
        for conn in (self.conn1, self.conn2):
            try:
                if conn is not None:
                    conn._rollback()
            except Exception:
                pass
        for c in (self._client1, self._client2):
            try:
                if c is not None:
                    c.close()
            except Exception:
                pass
