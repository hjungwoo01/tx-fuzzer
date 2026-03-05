"""
Replay a recorded random workload trace deterministically using Python adapters.

Reads logs/trace_latest.json produced by the Go runner (RandomRunner) and
replays the SQL statements in the exact recorded order. Supports injecting
an artificial delay before a specific step to stretch schedules and explore
race conditions.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from adapters import nuke_postgres_connections  # type: ignore[import]
from run_litmus import DEFAULT_DSN, get_adapter  # type: ignore[import]

try:
    import psycopg2.errors as pg_errors
except ImportError:
    pg_errors = None  # type: ignore[assignment]

_REPLAY_STMT_TIMEOUT_SECONDS = 10  # Used in deadlock-detection message


def load_trace(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("trace file must contain a JSON array")
    return data  # type: ignore[return-value]


_PG_PARAM_RE = re.compile(r"\$\d+")
_DEFAULT_STMT_TIMEOUT_MS = 5000  # Avoid infinite hangs per statement during replay


def _normalize_sql(sql: str, target: str) -> str:
    """
    Normalize SQL bind placeholders for the active backend:
      - postgres/mysql: $1,$2,... -> %s
      - sqlite: $1,$2,... and %s -> ?
    """
    if target == "sqlite":
        sql_q = _PG_PARAM_RE.sub("?", sql)
        return re.sub(r"%s", "?", sql_q)
    return _PG_PARAM_RE.sub("%s", sql)


def _to_jsonable(value: Any) -> Any:
    """Best-effort conversion to JSON-safe structures."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return repr(value)
    if isinstance(value, tuple):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    return repr(value)


def _is_select_sql(sql: str) -> bool:
    return sql.lstrip().upper().startswith("SELECT")


def _is_update_sql(sql: str) -> bool:
    return sql.lstrip().upper().startswith("UPDATE")


def _is_insert_sql(sql: str) -> bool:
    return sql.lstrip().upper().startswith("INSERT")


def _has_returning_clause(sql: str) -> bool:
    return bool(re.search(r"\bRETURNING\b", sql, flags=re.IGNORECASE))


def _append_returning_all(sql: str) -> Optional[str]:
    """
    Append RETURNING * for simple single-statement UPDATE/INSERT SQL.
    Returns None if query shape is not safely rewritable.
    """
    stripped = sql.strip()
    if not stripped:
        return None
    has_trailing_semicolon = stripped.endswith(";")
    if has_trailing_semicolon:
        stripped = stripped[:-1].rstrip()
    if ";" in stripped:
        return None
    if _has_returning_clause(stripped):
        return stripped
    if not (_is_update_sql(stripped) or _is_insert_sql(stripped)):
        return None
    return f"{stripped} RETURNING *"


def _extract_where_k_param_index(sql: str) -> Optional[int]:
    """
    Return 0-based arg index for WHERE k = $N / %s patterns when identifiable.
    """
    m = re.search(r"WHERE\s+K\s*=\s*\$([0-9]+)", sql, flags=re.IGNORECASE)
    if m:
        return int(m.group(1)) - 1
    m = re.search(r"WHERE\s+K\s*=\s*%s", sql, flags=re.IGNORECASE)
    if m:
        return -1  # fallback: assume last arg
    m = re.search(r"WHERE\s+K\s*=\s*\?", sql, flags=re.IGNORECASE)
    if m:
        return -1  # fallback: assume last arg
    return None


def _synthesize_missing_args(sql: str, step_idx: int, client_id: int) -> List[int]:
    """
    Deterministically synthesize SQL args when trace omitted them but SQL uses $N placeholders.
    """
    param_ids = sorted({int(m.group(1)) for m in re.finditer(r"\$(\d+)", sql)})
    if not param_ids:
        return []
    arg_count = max(param_ids)
    args = [((step_idx * 9973) + (client_id * 101) + i) % 1_000_000 for i in range(1, arg_count + 1)]
    upper_sql = sql.upper()
    for m in re.finditer(r"WHERE\s+K\s*=\s*\$(\d+)", upper_sql):
        key_idx = int(m.group(1)) - 1
        if 0 <= key_idx < len(args):
            args[key_idx] = ((step_idx + client_id + key_idx) % 100) + 1
    return args


class ReplayLitmus:
    """
    Transactional replay engine with true client concurrency.

    Given a trace of events that include client IDs and control markers
    (BEGIN/COMMIT/ROLLBACK/SQL), this class:
      - Resets the kv table to a known state.
      - Creates a persistent connection per logical client.
      - Replays per-client event lists on separate threads.
      - Uses a barrier so all client threads begin execution together.
      - Honors transaction boundaries on each connection.
    """

    def __init__(
        self,
        trace: List[Dict[str, Any]],
        target: str,
        dsn: str,
        isolation: str,
        inject_step: int | None,
        delay_seconds: float,
    ) -> None:
        self.trace = trace
        self.target = target
        self.dsn = dsn
        self.isolation = isolation
        self.inject_step = inject_step
        self.delay_seconds = delay_seconds

        # Logical client_id -> underlying DB artifacts
        self.adapters: Dict[int, Any] = {}
        self.connections: Dict[int, Any] = {}
        self.cursors: Dict[int, Any] = {}

        # Heartbeat state to log progress even if a statement takes a long time.
        self._last_state: Dict[str, Any] = {
            "step": 0,
            "client": None,
            "event_type": None,
            "sql": None,
            "done": False,
        }
        self.execution_history: List[Dict[str, Any]] = []
        self._history_lock = threading.Lock()
        self._txn_lock = threading.Lock()
        self._client_txn_seq: Dict[int, int] = {}
        self._client_active_txn: Dict[int, str] = {}
        self._start_barrier: Optional[threading.Barrier] = None
        self._stop_event = threading.Event()
        self._timeout_detected = threading.Event()
        self._worker_errors: List[str] = []
        self._worker_lock = threading.Lock()

    def _next_txn_label(self, client_id: int) -> str:
        with self._txn_lock:
            seq = self._client_txn_seq.get(client_id, 0) + 1
            self._client_txn_seq[client_id] = seq
        return f"c{client_id}-tx{seq}"

    def _begin_txn_label(self, client_id: int) -> str:
        txn_label = self._next_txn_label(client_id)
        with self._txn_lock:
            self._client_active_txn[client_id] = txn_label
        return txn_label

    def _active_or_implicit_txn_label(self, client_id: int) -> str:
        with self._txn_lock:
            active = self._client_active_txn.get(client_id)
        if active:
            return active
        return self._next_txn_label(client_id)

    def _end_txn_label(self, client_id: int) -> str:
        with self._txn_lock:
            active = self._client_active_txn.get(client_id)
            if active:
                del self._client_active_txn[client_id]
        if active:
            return active
        return self._next_txn_label(client_id)

    def _active_or_new_for_terminal(self, client_id: int) -> str:
        with self._txn_lock:
            active = self._client_active_txn.get(client_id)
        return active if active else self._next_txn_label(client_id)

    def _clear_active_txn(self, client_id: int) -> None:
        with self._txn_lock:
            self._client_active_txn.pop(client_id, None)

    def _record_execution(
        self,
        *,
        step: int,
        client_id: int,
        txn_id: str,
        event_type: str,
        sql: str,
        args: List[Any],
        result: Any,
        error: Optional[str] = None,
    ) -> None:
        rec = {
            "step": step,
            "client": client_id,
            "txn_id": txn_id,
            "event_type": event_type,
            "sql": sql,
            "args": _to_jsonable(args),
            "result": _to_jsonable(result),
        }
        if error:
            rec["error"] = error
        with self._history_lock:
            self.execution_history.append(rec)

    def _reset_db(self) -> None:
        """
        Truncate kv and insert initial data: keys 1..100 with value 0.

        For Postgres: nuke other connections first, then truncate. If truncate
        still fails, raise a clear CRITICAL error.
        """
        if self.target == "postgres":
            nuke_postgres_connections(self.dsn)

        print("[replay] resetting database state: TRUNCATE kv; INSERT keys 1..100")
        adapter = get_adapter(self.target, self.dsn, self.isolation, logger=None)
        adapter.connect()
        try:
            if self.target == "postgres":
                try:
                    conn = getattr(adapter, "conn1", None)
                    if conn is not None:
                        with conn.cursor() as c:
                            c.execute("SET LOCAL lock_timeout = '5s'")
                            c.execute("SET LOCAL statement_timeout = '5s'")
                except Exception:
                    pass
            try:
                adapter.setup_schema(key_count=100)
            except Exception as e:
                raise RuntimeError(
                    "CRITICAL: Could not clean DB even after nuking. "
                    "Check for external clients."
                ) from e
        finally:
            adapter.teardown()

    def _init_clients(self, client_ids: Optional[List[int]] = None) -> None:
        """Create a persistent connection and cursor per logical client."""
        if client_ids is None:
            # Determine unique client IDs from the trace.
            client_ids = sorted(
                {self._event_client_id(ev) for ev in self.trace}
            )
        if not client_ids:
            raise ValueError("trace is empty; no client_id found")

        print(f"[replay] creating adapters/connections for clients: {client_ids}")
        for cid in client_ids:
            ad = get_adapter(self.target, self.dsn, self.isolation, logger=None)
            ad.connect()
            _try_set_statement_timeout(ad, _DEFAULT_STMT_TIMEOUT_MS)
            # Use adapter.conn1/cur1 as this client's session.
            conn = getattr(ad, "conn1", None)
            cur = getattr(ad, "cur1", None)
            if conn is None or cur is None:
                raise RuntimeError(
                    f"adapter for client {cid} did not expose conn1/cur1"
                )
            self.adapters[cid] = ad
            self.connections[cid] = conn
            self.cursors[cid] = cur

    def _fetch_final_state(self) -> List[List[Any]]:
        """
        On successful replay, read the kv table from a fresh connection.
        """
        # Use a fresh connection to avoid reading from a client session that may
        # still be inside an open transaction snapshot.
        adapter = get_adapter(self.target, self.dsn, self.isolation, logger=None)
        adapter.connect()
        try:
            cur = getattr(adapter, "cur1", None)
            if cur is None:
                raise RuntimeError("adapter did not expose cur1")
            cur.execute("SELECT k, v FROM kv ORDER BY k ASC")
            rows = cur.fetchall()
            return [list(r) for r in rows]
        except Exception as e:
            print(f"[replay] WARNING: could not dump kv state: {e}", file=sys.stderr)
            return []
        finally:
            try:
                adapter.teardown()
            except Exception:
                pass

    def _emit_replay_payload(self) -> None:
        payload = {
            "execution_history": _to_jsonable(self.execution_history),
            "final_state": self._fetch_final_state(),
        }
        # Single JSON line so campaign can parse/hash deterministically.
        print(json.dumps(payload, sort_keys=True))

    def _heartbeat(self) -> None:
        while not self._last_state.get("done", False):
            time.sleep(30.0)
            step = self._last_state.get("step")
            client = self._last_state.get("client")
            ev_type = self._last_state.get("event_type")
            sql = self._last_state.get("sql")
            if step:
                print(
                    f"[replay-heartbeat] last_step={step} client={client} "
                    f"type={ev_type} sql={sql!r}"
                )
            else:
                print(
                    "[replay-heartbeat] still initializing "
                    "(no events executed yet)"
                )

    def _has_explicit_txn_control(self) -> bool:
        """
        True if trace explicitly encodes BEGIN/COMMIT/ROLLBACK boundaries.
        If absent (common for run_random invoke/ok traces), replay should
        commit each SQL statement so writes are externally visible.
        """
        for ev in self.trace:
            raw_type = str(ev.get("type", "")).lower()
            if raw_type in {"begin", "commit", "rollback"}:
                return True
            if raw_type in {"sql", "invoke"}:
                sql = str(ev.get("sql", "")).strip().upper()
                if sql.startswith("BEGIN") or sql.startswith("COMMIT") or sql.startswith("ROLLBACK"):
                    return True
        return False

    def _observe_update_without_returning(
        self,
        cur: Any,
        sql: str,
        args: List[Any],
    ) -> Dict[str, Any]:
        key_idx = _extract_where_k_param_index(sql)
        if key_idx == -1:
            key_idx = len(args) - 1
        before_rows: List[Any] = []
        after_rows: List[Any] = []
        key_val: Any = None
        if key_idx is not None and 0 <= key_idx < len(args):
            key_val = args[key_idx]
            bind_sql = "SELECT k, v FROM kv WHERE k = ?" if self.target == "sqlite" else "SELECT k, v FROM kv WHERE k = %s"
            cur.execute(bind_sql, [key_val])
            before_rows = cur.fetchall()

        cur.execute(sql, args)
        rows_affected = int(getattr(cur, "rowcount", -1))

        if key_val is not None:
            bind_sql = "SELECT k, v FROM kv WHERE k = ?" if self.target == "sqlite" else "SELECT k, v FROM kv WHERE k = %s"
            cur.execute(bind_sql, [key_val])
            after_rows = cur.fetchall()

        return {
            "mode": "select-before-update",
            "key": key_val,
            "before": before_rows,
            "after": after_rows,
            "rows_affected": rows_affected,
        }

    def _execute_mutation_with_observation(
        self,
        cur: Any,
        sql: str,
        args: List[Any],
    ) -> Dict[str, Any]:
        if _is_update_sql(sql):
            if self.target == "postgres":
                rewritten = _append_returning_all(sql)
                if rewritten is not None:
                    cur.execute(rewritten, args)
                    rows = cur.fetchall()
                    return {
                        "mode": "returning",
                        "executed_sql": rewritten,
                        "rows": rows,
                        "rows_affected": int(getattr(cur, "rowcount", -1)),
                    }
            return self._observe_update_without_returning(cur, sql, args)

        if _is_insert_sql(sql):
            if self.target == "postgres":
                rewritten = _append_returning_all(sql)
                if rewritten is not None:
                    cur.execute(rewritten, args)
                    rows = cur.fetchall()
                    return {
                        "mode": "returning",
                        "executed_sql": rewritten,
                        "rows": rows,
                        "rows_affected": int(getattr(cur, "rowcount", -1)),
                    }
            cur.execute(sql, args)
            return {
                "mode": "insert",
                "rows_affected": int(getattr(cur, "rowcount", -1)),
            }

        cur.execute(sql, args)
        return {
            "mode": "exec",
            "rows_affected": int(getattr(cur, "rowcount", -1)),
        }

    def _normalize_event_type(self, raw_type: str, sql: str) -> Optional[str]:
        raw = (raw_type or "").lower()
        if raw in {"begin", "commit", "rollback", "sql"}:
            return raw.upper()
        if raw != "invoke":
            return None
        upper_sql = (sql or "").upper()
        if upper_sql.startswith("BEGIN"):
            return "BEGIN"
        if upper_sql.startswith("COMMIT"):
            return "COMMIT"
        if upper_sql.startswith("ROLLBACK"):
            return "ROLLBACK"
        return "SQL"

    def _event_client_id(self, ev: Dict[str, Any]) -> int:
        """
        Read logical client id from common trace field variants.
        Defaults to 0 when absent/unparseable.
        """
        raw = ev.get("client_id", ev.get("client", 0))
        try:
            return int(raw)
        except Exception:
            return 0

    def _build_client_event_groups(self) -> Dict[int, List[Dict[str, Any]]]:
        """
        Convert the flat trace into per-client replay queues preserving each
        client's original event order. A global replay step is assigned so
        --inject-delay-at-step remains deterministic.
        """
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        global_step = 0
        for ev in self.trace:
            sql = str(ev.get("sql", "")).strip() if "sql" in ev else ""
            event_type_raw = self._normalize_event_type(str(ev.get("type", "")), sql)
            if event_type_raw is None:
                # Skip bookkeeping events such as ok/fail.
                continue
            global_step += 1
            client_id = self._event_client_id(ev)
            raw_args = ev.get("args")
            args = list(raw_args) if isinstance(raw_args, list) else []
            delay_seconds = 0.0
            if "delay" in ev:
                try:
                    delay_seconds = max(delay_seconds, float(ev["delay"]))
                except Exception:
                    pass
            if self.inject_step is not None and global_step == self.inject_step:
                delay_seconds = max(delay_seconds, float(self.delay_seconds))
            grouped.setdefault(client_id, []).append(
                {
                    "step": global_step,
                    "client_id": client_id,
                    "event_type": event_type_raw,
                    "sql": sql,
                    "args": args,
                    "delay": delay_seconds,
                }
            )
        return grouped

    def _run_client_events(
        self,
        client_id: int,
        events: List[Dict[str, Any]],
        auto_commit_each_sql: bool,
    ) -> None:
        conn = self.connections.get(client_id)
        cur = self.cursors.get(client_id)
        if conn is None or cur is None:
            raise ValueError(f"no connection mapped for client_id {client_id}")
        barrier = self._start_barrier
        if barrier is not None:
            try:
                token = barrier.wait(timeout=30.0)
                if token == 0:
                    print("[replay] all client threads synchronized; starting concurrent replay")
            except threading.BrokenBarrierError as e:
                self._stop_event.set()
                raise RuntimeError("client start barrier failed") from e

        for local_step, ev in enumerate(events, start=1):
            if self._stop_event.is_set():
                break

            step_idx = int(ev.get("step", 0))
            event_type_raw = str(ev.get("event_type", "SQL"))
            sql = str(ev.get("sql", ""))
            args = list(ev.get("args", []))
            delay_seconds = float(ev.get("delay", 0.0) or 0.0)

            if delay_seconds > 0:
                print(
                    f"[replay] client={client_id} local_step={local_step} "
                    f"global_step={step_idx} sleeping {delay_seconds}s before execution"
                )
                time.sleep(delay_seconds)

            if not args and "$" in sql:
                args = _synthesize_missing_args(sql, step_idx, client_id)

            if event_type_raw == "BEGIN":
                txn_label = self._begin_txn_label(client_id)
            elif event_type_raw in {"COMMIT", "ROLLBACK"}:
                txn_label = self._active_or_new_for_terminal(client_id)
            else:
                txn_label = self._active_or_implicit_txn_label(client_id)

            # Heartbeat signal from whichever thread executed most recently.
            self._last_state["step"] = step_idx
            self._last_state["client"] = client_id
            self._last_state["event_type"] = event_type_raw
            self._last_state["sql"] = sql

            if event_type_raw == "SQL":
                print(
                    f"[replay] step={step_idx} client={client_id} local_step={local_step} "
                    f"type=SQL sql={sql!r} args={args!r}"
                )
            else:
                print(
                    f"[replay] step={step_idx} client={client_id} local_step={local_step} "
                    f"type={event_type_raw}"
                )

            try:
                if self.target == "postgres":
                    try:
                        with conn.cursor() as c_timeout:
                            c_timeout.execute("SET LOCAL lock_timeout = '5s'")
                            c_timeout.execute("SET LOCAL statement_timeout = '5s'")
                    except Exception:
                        pass

                result_details: Dict[str, Any] = {}
                if event_type_raw == "BEGIN":
                    cur.execute("BEGIN")
                    result_details = {"action": "begin"}
                elif event_type_raw == "COMMIT":
                    conn.commit()
                    result_details = {"action": "commit"}
                    self._clear_active_txn(client_id)
                elif event_type_raw == "ROLLBACK":
                    conn.rollback()
                    result_details = {"action": "rollback"}
                    self._clear_active_txn(client_id)
                elif event_type_raw == "SQL":
                    norm_sql = _normalize_sql(sql, self.target)
                    if not norm_sql:
                        self._record_execution(
                            step=step_idx,
                            client_id=client_id,
                            txn_id=txn_label,
                            event_type=event_type_raw,
                            sql=sql,
                            args=args,
                            result={"skipped": True, "reason": "empty_sql"},
                        )
                        continue
                    if _is_select_sql(norm_sql):
                        cur.execute(norm_sql, args)
                        rows = cur.fetchall()
                        result_details = {
                            "mode": "select",
                            "executed_sql": norm_sql,
                            "rows": rows,
                            "rows_returned": len(rows),
                        }
                    else:
                        result_details = self._execute_mutation_with_observation(cur, norm_sql, args)
                        if "executed_sql" not in result_details:
                            result_details["executed_sql"] = norm_sql
                    if auto_commit_each_sql:
                        conn.commit()
                        result_details["auto_commit"] = True

                self._record_execution(
                    step=step_idx,
                    client_id=client_id,
                    txn_id=txn_label,
                    event_type=event_type_raw,
                    sql=sql,
                    args=args,
                    result=result_details,
                )
            except Exception as e:
                is_timeout = (
                    pg_errors is not None and isinstance(e, pg_errors.QueryCanceled)
                )
                if is_timeout:
                    print(
                        f"🔴 POTENTIAL DEADLOCK DETECTED: Statement timed out "
                        f"after {_REPLAY_STMT_TIMEOUT_SECONDS} seconds."
                    )
                    print(
                        f"[replay] step={step_idx} client={client_id} "
                        f"type={event_type_raw} sql={sql!r}"
                    )
                else:
                    print(
                        f"[replay-error] step={step_idx} client={client_id} "
                        f"type={event_type_raw} sql={sql!r} err={e!r}"
                    )
                self._record_execution(
                    step=step_idx,
                    client_id=client_id,
                    txn_id=txn_label,
                    event_type=event_type_raw,
                    sql=sql,
                    args=args,
                    result={"status": "error"},
                    error=repr(e),
                )
                try:
                    conn.rollback()
                except Exception:
                    pass
                self._clear_active_txn(client_id)
                if is_timeout:
                    self._timeout_detected.set()
                    self._stop_event.set()
                    return
                continue

    def _thread_entry(
        self,
        client_id: int,
        events: List[Dict[str, Any]],
        auto_commit_each_sql: bool,
    ) -> None:
        try:
            self._run_client_events(client_id, events, auto_commit_each_sql)
        except Exception as e:
            with self._worker_lock:
                self._worker_errors.append(f"client={client_id} err={e!r}")
            self._stop_event.set()

    def run(self) -> bool:
        """
        Run replay with true concurrency:
          - group events by client
          - one thread per client
          - barrier-synchronized start
        Returns True on success, False if a timeout/deadlock signal is detected.
        """
        hb_thread = threading.Thread(target=self._heartbeat, daemon=True)
        hb_thread.start()

        try:
            self._reset_db()
            grouped = self._build_client_event_groups()
            client_ids = sorted(grouped.keys())
            if not client_ids:
                raise ValueError("trace had no replayable events")

            self._init_clients(client_ids)
            auto_commit_each_sql = not self._has_explicit_txn_control()
            if auto_commit_each_sql:
                print("[replay] no explicit txn boundaries in trace; auto-committing each SQL step")

            self._start_barrier = threading.Barrier(len(client_ids))
            threads: List[threading.Thread] = []
            for cid in client_ids:
                t = threading.Thread(
                    target=self._thread_entry,
                    args=(cid, grouped[cid], auto_commit_each_sql),
                    name=f"replay-client-{cid}",
                    daemon=False,
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            if self._worker_errors:
                raise RuntimeError("; ".join(self._worker_errors))

            if self._timeout_detected.is_set():
                return False

            self._emit_replay_payload()
            return True
        finally:
            self._last_state["done"] = True
            for ad in self.adapters.values():
                try:
                    ad.teardown()
                except Exception:
                    pass


def _try_set_statement_timeout(adapter: Any, timeout_ms: int) -> None:
    """
    Best-effort: set per-statement timeout where supported (e.g., PostgreSQL).
    Safe no-op on backends that don't recognize statement_timeout.
    """
    if timeout_ms <= 0:
        return
    try:
        conn = getattr(adapter, "conn1", None)
        if conn is None:
            return
        with conn.cursor() as c:
            c.execute(f"SET statement_timeout = {int(timeout_ms)}")
    except Exception:
        # Ignore if backend doesn't support it
        return


def replay_trace(
    trace: List[Dict[str, Any]],
    target: str,
    dsn: str,
    isolation: str,
    inject_step: int | None,
    delay_seconds: float,
) -> int:
    """
    Replay a trace with up to N logical clients.
    Returns 0 on success, 1 if a potential deadlock (timeout) was detected.
    """
    engine = ReplayLitmus(
        trace=trace,
        target=target,
        dsn=dsn,
        isolation=isolation,
        inject_step=inject_step,
        delay_seconds=delay_seconds,
    )
    return 0 if engine.run() else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay a recorded Go runner trace using Python adapters (Litmus-style)."
    )
    parser.add_argument(
        "--trace",
        default="logs/trace_latest.json",
        help="Path to JSON trace file produced by the Go runner.",
    )
    parser.add_argument(
        "--db",
        default=None,
        choices=["postgres", "mysql", "sqlite"],
        help="Database backend to use for replay.",
    )
    parser.add_argument(
        "--target",
        default=None,
        choices=["postgres", "mysql", "sqlite"],
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Connection DSN (defaults match run_litmus.DEFAULT_DSN).",
    )
    parser.add_argument(
        "--sqlite-path",
        default=None,
        help="Convenience for SQLite: path or file: URI to the DB file. Used when --db sqlite and --dsn is omitted.",
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Isolation level for SQL backends.",
    )
    parser.add_argument(
        "--inject-delay-at-step",
        type=int,
        default=None,
        help="Before executing this (1-based) step, sleep for --delay-seconds.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Delay in seconds to inject at the specified step.",
    )
    parser.add_argument(
        "--mode",
        default="replay",
        choices=["replay", "scheduled"],
        help=(
            "Execution mode: 'replay' (default) runs per-client threads with optional delay; "
            "'scheduled' runs a deterministic step-by-step schedule from a trace that has "
            "a 'schedule' field or uses event_id ordering."
        ),
    )
    args = parser.parse_args()

    args.isolation = (args.isolation or "").strip().upper().replace("_", " ")
    if args.isolation == "SERIALIZATION":
        args.isolation = "SERIALIZABLE"

    target = args.db or args.target or "postgres"
    dsn = args.dsn
    if target == "sqlite" and not dsn and args.sqlite_path:
        dsn = args.sqlite_path
    if not dsn:
        dsn = DEFAULT_DSN.get(target)
    if not dsn:
        print(f"No default DSN for target {target!r}. Pass --dsn.", file=sys.stderr)
        return 2

    trace_path = Path(args.trace)
    if not trace_path.exists():
        print(f"Trace file not found: {trace_path}", file=sys.stderr)
        return 2

    trace = load_trace(trace_path)
    print(f"[replay] loaded {len(trace)} events from {trace_path}")

    if args.mode == "scheduled":
        # Scheduled mode: dispatch to the deterministic executor
        from repro_update_conflict_out import DeterministicExecutor, print_result, result_to_json
        import json as _json

        # Build schedule from event_ids in trace order
        schedule = [ev["event_id"] for ev in trace if "event_id" in ev]
        if not schedule:
            print("ERROR: scheduled mode requires events with 'event_id' fields.", file=sys.stderr)
            return 2

        # Auto-detect variant
        variant = "update"
        for ev in trace:
            eid = ev.get("event_id", "")
            if "delete" in eid:
                variant = "delete"
                break

        executor = DeterministicExecutor(dsn, variant)
        sr = executor.run(schedule)
        print_result(sr)
        # Output JSON result
        print(_json.dumps(result_to_json(sr), indent=2))
        return 0

    return replay_trace(
        trace=trace,
        target=target,
        dsn=dsn,
        isolation=args.isolation,
        inject_step=args.inject_delay_at_step,
        delay_seconds=args.delay_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
