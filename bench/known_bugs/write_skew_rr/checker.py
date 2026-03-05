from __future__ import annotations

import threading
import time
from typing import Any, Dict, List

import psycopg2

from bench.known_bugs.common import HistoryRecorder, sqlstate_from_exception


def _normalize_delays(schedule_ms: List[List[float]]) -> List[List[float]]:
    base = [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]
    for i, row in enumerate(schedule_ms[:2]):
        for j, value in enumerate(row[:4]):
            base[i][j] = max(0.0, float(value))
    return base


def _sleep_ms(value: float) -> None:
    if value > 0:
        time.sleep(value / 1000.0)


def run_trial(
    *,
    dsn: str,
    isolation_level: str,
    schedule_ms: List[List[float]],
    clients: int,
    ops: int,
    concurrency: int,
    timeout_s: float,
    seed: int,
) -> Dict[str, Any]:
    del ops, concurrency, seed
    if clients < 2:
        raise ValueError("write_skew_rr requires at least 2 clients")

    delays = _normalize_delays(schedule_ms)
    recorder = HistoryRecorder()
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    results: Dict[str, Dict[str, Any]] = {}
    timed_out = False

    def worker(client_id: int, doctor_id: int, client_delays: List[float]) -> None:
        nonlocal timed_out
        local: Dict[str, Any] = {
            "committed": False,
            "read_on_call_count": None,
            "updated": False,
            "error_code": None,
            "error": None,
        }
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        cur = conn.cursor()
        name = f"T{client_id}"
        try:
            stmt_timeout_ms = max(1000, int(timeout_s * 1000))
            cur.execute("SET SESSION statement_timeout = %s", (stmt_timeout_ms,))
            cur.execute("SET SESSION lock_timeout = %s", (stmt_timeout_ms,))
            conn.commit()
            barrier.wait(timeout=timeout_s)
            _sleep_ms(client_delays[0])
            recorder.log(client=name, event="begin", sql=f"BEGIN ISOLATION LEVEL {isolation_level}")
            cur.execute(f"BEGIN ISOLATION LEVEL {isolation_level}")

            _sleep_ms(client_delays[1])
            sql_read = "SELECT COUNT(*) FROM on_call WHERE on_call"
            recorder.log(client=name, event="read", sql=sql_read)
            cur.execute(sql_read)
            row = cur.fetchone()
            current = int(row[0]) if row is not None else 0
            local["read_on_call_count"] = current

            if current > 1:
                _sleep_ms(client_delays[2])
                sql_update = "UPDATE on_call SET on_call = FALSE WHERE doctor_id = %s"
                recorder.log(client=name, event="write", sql=sql_update, details={"doctor_id": doctor_id})
                cur.execute(sql_update, (doctor_id,))
                local["updated"] = cur.rowcount > 0

            _sleep_ms(client_delays[3])
            recorder.log(client=name, event="commit", sql="COMMIT")
            conn.commit()
            local["committed"] = True
        except threading.BrokenBarrierError as exc:
            timed_out = True
            local["error"] = repr(exc)
            local["error_code"] = "BARRIER_TIMEOUT"
            try:
                conn.rollback()
            except Exception:
                pass
            recorder.log(client=name, event="error", status="fail", details={"error": local["error"]})
        except Exception as exc:
            local["error"] = repr(exc)
            local["error_code"] = sqlstate_from_exception(exc)
            try:
                conn.rollback()
            except Exception:
                pass
            recorder.log(
                client=name,
                event="error",
                status="fail",
                details={"error": local["error"], "sqlstate": local["error_code"]},
            )
        finally:
            cur.close()
            conn.close()
            with lock:
                results[str(client_id)] = local

    start = time.monotonic()
    t1 = threading.Thread(target=worker, args=(1, 1, delays[0]), daemon=True)
    t2 = threading.Thread(target=worker, args=(2, 2, delays[1]), daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=timeout_s)
    t2.join(timeout=timeout_s)

    if t1.is_alive() or t2.is_alive():
        timed_out = True

    final_state: Dict[str, Any] = {"rows": [], "on_call_count": None}
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT doctor_id, on_call FROM on_call ORDER BY doctor_id")
            rows = cur.fetchall()
            final_state["rows"] = [[int(r[0]), bool(r[1])] for r in rows]
            final_state["on_call_count"] = sum(1 for _, on_call in rows if on_call)
    finally:
        conn.close()

    return {
        "duration_s": round(time.monotonic() - start, 6),
        "history": recorder.snapshot(),
        "client_results": results,
        "final_state": final_state,
        "timed_out": timed_out,
    }


def check_trial(trial: Dict[str, Any]) -> Dict[str, Any]:
    clients = trial.get("client_results", {})
    c1 = clients.get("1", {})
    c2 = clients.get("2", {})
    both_committed = bool(c1.get("committed") and c2.get("committed"))

    final_state = trial.get("final_state", {})
    on_call_count = final_state.get("on_call_count")

    found = both_committed and on_call_count == 0
    if found:
        return {
            "found": True,
            "reason": "Both transactions committed and no doctors remained on call (write skew).",
            "score": 1.0,
        }

    read1 = c1.get("read_on_call_count")
    read2 = c2.get("read_on_call_count")
    near_miss = 0.0
    if read1 == 2 and read2 == 2:
        near_miss = 0.8
    elif both_committed and on_call_count == 1:
        near_miss = 0.4

    return {
        "found": False,
        "reason": "Invariant held for this trial.",
        "score": near_miss,
    }
