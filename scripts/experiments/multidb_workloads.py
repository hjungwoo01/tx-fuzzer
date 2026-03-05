#!/usr/bin/env python3
"""Workload library for multi-DB evaluation (Postgres, MySQL, SQLite)."""

from __future__ import annotations

import hashlib
import json
import random
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from .multidb_db import (
        MYSQL_DEADLOCK_ERRNO,
        MYSQL_LOCK_WAIT_TIMEOUT_ERRNO,
        DBAdapter,
        ExecResult,
        create_adapter,
    )
except ImportError:
    from multidb_db import (
        MYSQL_DEADLOCK_ERRNO,
        MYSQL_LOCK_WAIT_TIMEOUT_ERRNO,
        DBAdapter,
        ExecResult,
        create_adapter,
    )


EventFn = Callable[[DBAdapter], Dict[str, Any]]


@dataclass
class TrialResult:
    bug_found: bool
    canonical_state: Dict[str, Any]
    state_hash: str
    errors: List[str]
    execution_log: List[Dict[str, Any]]
    artifact: Optional[Dict[str, Any]] = None
    round_trips: int = 0
    elapsed_s: float = 0.0
    final_state: Dict[str, Any] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)


def hash_canonical_state(state: Dict[str, Any]) -> str:
    raw = json.dumps(state, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _result_dict(r: ExecResult) -> Dict[str, Any]:
    return {
        "status": r.status,
        "error_code": r.error_code,
        "error_message": r.error_message,
        "rowcount": r.rowcount,
        "rows": r.rows,
    }


def _round_robin_schedule(lengths: Sequence[int]) -> List[int]:
    rem = list(lengths)
    out: List[int] = []
    while any(x > 0 for x in rem):
        for cid in range(len(rem)):
            if rem[cid] > 0:
                out.append(cid)
                rem[cid] -= 1
    return out


def _random_interleaving(lengths: Sequence[int], rng: random.Random) -> List[int]:
    rem = list(lengths)
    out: List[int] = []
    while any(x > 0 for x in rem):
        ready = [cid for cid, remaining in enumerate(rem) if remaining > 0]
        cid = rng.choice(ready)
        out.append(cid)
        rem[cid] -= 1
    return out


def _choose_delays(
    rng: random.Random,
    event_ids: Sequence[str],
    k: int,
    delay_min_ms: int = 3,
    delay_max_ms: int = 25,
) -> Dict[str, float]:
    if k <= 0 or not event_ids:
        return {}
    chosen = rng.sample(list(event_ids), min(k, len(event_ids)))
    out: Dict[str, float] = {}
    for eid in chosen:
        out[eid] = rng.uniform(delay_min_ms / 1000.0, delay_max_ms / 1000.0)
    return out


def _exec_schema(db: str, dsn: str, setup_sql: Iterable[str], init_sql: Iterable[Tuple[str, Optional[Sequence[Any]]]]) -> None:
    ad = create_adapter(db)
    ad.connect(dsn, autocommit=True)
    try:
        for sql in setup_sql:
            ad.exec(sql)
        for sql, params in init_sql:
            ad.exec(sql, params)
    finally:
        ad.close()


def _query_rows(
    db: str,
    dsn: str,
    sql: str,
    params: Optional[Sequence[Any]] = None,
    pragmas: Optional[Dict[str, Any]] = None,
) -> List[Tuple[Any, ...]]:
    ad = create_adapter(db)
    ad.connect(dsn, autocommit=True)
    try:
        if pragmas:
            ad.set_pragmas(pragmas)
        r = ad.exec(sql, params)
        return r.rows if r.status == "ok" else []
    finally:
        ad.close()


def _run_scheduled_program(
    *,
    db: str,
    dsn: str,
    setup_sql: Sequence[str],
    init_sql: Sequence[Tuple[str, Optional[Sequence[Any]]]],
    isolation: str,
    events_by_client: Sequence[List[Tuple[str, EventFn]]],
    schedule: Sequence[int],
    delays: Dict[str, float],
    analysis_only: bool,
    session_pragmas: Optional[Sequence[Optional[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    _exec_schema(db, dsn, setup_sql, init_sql)

    sessions: List[DBAdapter] = []
    execution_log: List[Dict[str, Any]] = []
    errors: List[str] = []
    next_idx = [0] * len(events_by_client)

    t0 = time.monotonic()
    try:
        for cid in range(len(events_by_client)):
            ad = create_adapter(db)
            ad.connect(dsn, autocommit=False)
            if session_pragmas and session_pragmas[cid]:
                ad.set_pragmas(session_pragmas[cid] or {})
            sessions.append(ad)

        for step_idx, cid in enumerate(schedule, start=1):
            if cid < 0 or cid >= len(events_by_client):
                continue
            if next_idx[cid] >= len(events_by_client[cid]):
                continue

            event_id, fn = events_by_client[cid][next_idx[cid]]
            next_idx[cid] += 1
            delay_s = float(delays.get(event_id, 0.0))
            if delay_s > 0 and not analysis_only:
                time.sleep(delay_s)

            try:
                out = fn(sessions[cid])
            except Exception as e:
                out = {
                    "status": "error",
                    "error_code": type(e).__name__,
                    "error_message": str(e),
                    "rowcount": -1,
                    "rows": [],
                }

            if out.get("status") == "error":
                errors.append(f"{event_id}: {out.get('error_code')} {out.get('error_message')}")
                try:
                    sessions[cid].rollback()
                except Exception:
                    pass

            execution_log.append(
                {
                    "step": step_idx,
                    "client": cid,
                    "event_id": event_id,
                    "delay_s": round(delay_s, 6),
                    **out,
                }
            )
    finally:
        for ad in sessions:
            try:
                ad.close()
            except Exception:
                pass

    return {
        "execution_log": execution_log,
        "errors": errors,
        "round_trips": sum(getattr(ad, "round_trips", 0) for ad in sessions),
        "elapsed_s": time.monotonic() - t0,
    }


def _auto_inc_type(db: str) -> str:
    if db == "postgres":
        return "BIGSERIAL PRIMARY KEY"
    if db == "mysql":
        return "BIGINT AUTO_INCREMENT PRIMARY KEY"
    return "INTEGER PRIMARY KEY AUTOINCREMENT"


def _concat_assign_expr(db: str) -> str:
    if db == "mysql":
        return "CONCAT(%s, %s)"
    if db == "postgres":
        return "%s || %s"
    # sqlite
    return "? || ?"


def _placeholder(db: str) -> str:
    return "%s" if db in ("postgres", "mysql") else "?"


def _job_queue_setup_sql(db: str) -> List[str]:
    claim_id = _auto_inc_type(db)
    return [
        "DROP TABLE IF EXISTS claims",
        "DROP TABLE IF EXISTS jobs",
        "CREATE TABLE jobs (id INT PRIMARY KEY, status TEXT NOT NULL, worker TEXT)",
        f"CREATE TABLE claims (id {claim_id}, job_id INT NOT NULL, worker TEXT NOT NULL)",
    ]


def _job_queue_init_sql() -> List[Tuple[str, Optional[Sequence[Any]]]]:
    return [("INSERT INTO jobs(id, status, worker) VALUES (1, 'ready', NULL)", None)]


def _fetch_job_queue_state(db: str, dsn: str) -> Dict[str, Any]:
    jobs = _query_rows(
        db,
        dsn,
        "SELECT id, status, COALESCE(worker, '') FROM jobs ORDER BY id",
    )
    claims = _query_rows(
        db,
        dsn,
        "SELECT job_id, worker FROM claims ORDER BY id",
    )
    return {"jobs": [list(r) for r in jobs], "claims": [list(r) for r in claims]}


def run_job_queue_claim_micro_trial(
    *,
    db: str,
    dsn: str,
    seed: int,
    strategy: str,
    variant: str = "buggy",
    isolation: str = "READ COMMITTED",
    k: int = 3,
    analysis_only: bool = False,
) -> TrialResult:
    rng = random.Random(seed)
    workers = ["worker_a", "worker_b"]
    local = [
        {"ready": False, "updated": 0, "should_commit": False},
        {"ready": False, "updated": 0, "should_commit": False},
    ]
    ph = _placeholder(db)

    def mk_events(cid: int) -> List[Tuple[str, EventFn]]:
        worker = workers[cid]

        def ev_begin(ad: DBAdapter) -> Dict[str, Any]:
            return _result_dict(ad.begin(isolation))

        def ev_check(ad: DBAdapter) -> Dict[str, Any]:
            r = ad.exec("SELECT status FROM jobs WHERE id = 1")
            if r.status == "ok" and r.rows:
                local[cid]["ready"] = str(r.rows[0][0]) == "ready"
            else:
                local[cid]["ready"] = False
            return _result_dict(r)

        def ev_update(ad: DBAdapter) -> Dict[str, Any]:
            if variant == "fixed":
                r = ad.exec(
                    f"UPDATE jobs SET status = 'claimed', worker = {ph} "
                    "WHERE id = 1 AND status = 'ready'",
                    [worker],
                )
                local[cid]["updated"] = max(0, r.rowcount)
                local[cid]["should_commit"] = r.status == "ok" and r.rowcount == 1
                return _result_dict(r)
            if not local[cid]["ready"]:
                local[cid]["should_commit"] = False
                return {"status": "ok", "rowcount": 0, "rows": [], "error_code": None, "error_message": None, "skipped": True}
            r = ad.exec(
                f"UPDATE jobs SET status = 'claimed', worker = {ph} WHERE id = 1",
                [worker],
            )
            local[cid]["updated"] = max(0, r.rowcount)
            local[cid]["should_commit"] = r.status == "ok"
            return _result_dict(r)

        def ev_insert(ad: DBAdapter) -> Dict[str, Any]:
            if not local[cid]["should_commit"]:
                return {"status": "ok", "rowcount": 0, "rows": [], "error_code": None, "error_message": None, "skipped": True}
            r = ad.exec(
                f"INSERT INTO claims(job_id, worker) VALUES (1, {ph})",
                [worker],
            )
            if r.status != "ok":
                local[cid]["should_commit"] = False
            return _result_dict(r)

        def ev_end(ad: DBAdapter) -> Dict[str, Any]:
            if local[cid]["should_commit"]:
                return _result_dict(ad.commit())
            return _result_dict(ad.rollback())

        return [
            (f"c{cid}_begin", ev_begin),
            (f"c{cid}_check", ev_check),
            (f"c{cid}_update", ev_update),
            (f"c{cid}_insert", ev_insert),
            (f"c{cid}_end", ev_end),
        ]

    events_by_client = [mk_events(0), mk_events(1)]
    lengths = [len(e) for e in events_by_client]
    safe_templates = [
        # Double-claim candidate (both read "ready", then session 0 commits first).
        [0, 1, 0, 1, 0, 0, 0, 1, 1, 1],
        # Double-claim candidate (both read "ready", then session 1 commits first).
        [0, 1, 0, 1, 1, 1, 1, 0, 0, 0],
        # Single-claim candidates.
        [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
        [1, 1, 1, 1, 1, 0, 0, 0, 0, 0],
    ]

    if strategy == "heuristic_fuzzer":
        schedule = safe_templates[seed % 2]  # Prefer double-claim templates.
        delay_candidates = [
            "c0_check",
            "c1_check",
            "c0_update",
            "c1_update",
            "c0_end",
            "c1_end",
        ]
        delays = _choose_delays(rng, delay_candidates, k)
    elif strategy == "random_delays":
        schedule = list(rng.choice(safe_templates))
        all_ids = [eid for evs in events_by_client for eid, _ in evs if "begin" not in eid]
        delays = _choose_delays(rng, all_ids, k)
    else:
        schedule = safe_templates[2]
        delays = {}

    raw = _run_scheduled_program(
        db=db,
        dsn=dsn,
        setup_sql=_job_queue_setup_sql(db),
        init_sql=_job_queue_init_sql(),
        isolation=isolation,
        events_by_client=events_by_client,
        schedule=schedule,
        delays=delays,
        analysis_only=analysis_only,
    )
    final_state = _fetch_job_queue_state(db, dsn)
    claims = final_state["claims"]
    claim_workers = sorted({row[1] for row in claims})
    bug_found = len(claims) > 1 or len(claim_workers) > 1
    canonical = {
        "variant": variant,
        "jobs": final_state["jobs"],
        "claims": claims,
        "claim_count": len(claims),
        "distinct_workers": claim_workers,
    }
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=raw["errors"],
        execution_log=raw["execution_log"],
        artifact={"schedule": schedule, "delays": delays, "variant": variant},
        round_trips=raw["round_trips"],
        elapsed_s=raw["elapsed_s"],
        final_state=final_state,
        details={"strategy": strategy, "seed": seed},
    )


def run_job_queue_claim_micro_stress_trial(
    *,
    db: str,
    dsn: str,
    seed: int,
    variant: str = "buggy",
    isolation: str = "READ COMMITTED",
) -> TrialResult:
    rng = random.Random(seed)
    _exec_schema(db, dsn, _job_queue_setup_sql(db), _job_queue_init_sql())
    ph = _placeholder(db)
    errors: List[str] = []
    lock = threading.Lock()

    def worker(worker_name: str, offset_ms: float) -> None:
        time.sleep(offset_ms / 1000.0)
        ad = create_adapter(db)
        ad.connect(dsn, autocommit=False)
        try:
            ad.begin(isolation)
            time.sleep(rng.uniform(0.0, 0.002))
            r = ad.exec("SELECT status FROM jobs WHERE id = 1")
            ready = r.status == "ok" and bool(r.rows) and str(r.rows[0][0]) == "ready"
            if variant == "fixed":
                ru = ad.exec(
                    f"UPDATE jobs SET status = 'claimed', worker = {ph} "
                    "WHERE id = 1 AND status = 'ready'",
                    [worker_name],
                )
                if ru.status == "ok" and ru.rowcount == 1:
                    ad.exec(f"INSERT INTO claims(job_id, worker) VALUES (1, {ph})", [worker_name])
                    ad.commit()
                else:
                    ad.rollback()
            else:
                if ready:
                    ad.exec(f"UPDATE jobs SET status = 'claimed', worker = {ph} WHERE id = 1", [worker_name])
                    ad.exec(f"INSERT INTO claims(job_id, worker) VALUES (1, {ph})", [worker_name])
                    ad.commit()
                else:
                    ad.rollback()
        except Exception as e:
            with lock:
                errors.append(str(e))
            try:
                ad.rollback()
            except Exception:
                pass
        finally:
            ad.close()

    t0 = time.monotonic()
    threads = [
        threading.Thread(target=worker, args=("worker_a", rng.uniform(0.0, 2.0)), daemon=True),
        threading.Thread(target=worker, args=("worker_b", rng.uniform(0.0, 2.0)), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    final_state = _fetch_job_queue_state(db, dsn)
    claims = final_state["claims"]
    claim_workers = sorted({row[1] for row in claims})
    bug_found = len(claims) > 1 or len(claim_workers) > 1
    canonical = {
        "variant": variant,
        "jobs": final_state["jobs"],
        "claims": claims,
        "claim_count": len(claims),
        "distinct_workers": claim_workers,
    }
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=errors,
        execution_log=[],
        artifact=None,
        elapsed_s=time.monotonic() - t0,
        final_state=final_state,
    )


def _list_append_setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS txn0",
        "CREATE TABLE txn0 (id INT PRIMARY KEY, val TEXT NOT NULL)",
    ]


def _list_append_init_sql() -> List[Tuple[str, Optional[Sequence[Any]]]]:
    return [("INSERT INTO txn0(id, val) VALUES (1, '')", None)]


def _fetch_list_append_state(db: str, dsn: str) -> Dict[str, Any]:
    rows = _query_rows(db, dsn, "SELECT val FROM txn0 WHERE id = 1")
    val = str(rows[0][0]) if rows else ""
    parsed = sorted({int(x) for x in val.split(",") if x.strip().isdigit()})
    return {"val": val, "parsed": parsed}


def run_list_append_micro_trial(
    *,
    db: str,
    dsn: str,
    seed: int,
    strategy: str,
    clients: int = 3,
    isolation: str = "READ COMMITTED",
    k: int = 3,
    analysis_only: bool = False,
) -> TrialResult:
    rng = random.Random(seed)
    local: List[Dict[str, Any]] = [
        {"read1": "", "read2": "", "new_val": "", "committed": False, "elem": str(i + 1)}
        for i in range(clients)
    ]
    ph = _placeholder(db)
    concat_expr = _concat_assign_expr(db)

    def mk_events(cid: int) -> List[Tuple[str, EventFn]]:
        elem = local[cid]["elem"]

        def ev_begin(ad: DBAdapter) -> Dict[str, Any]:
            return _result_dict(ad.begin(isolation))

        def ev_read1(ad: DBAdapter) -> Dict[str, Any]:
            r = ad.exec("SELECT val FROM txn0 WHERE id = 1")
            if r.status == "ok" and r.rows:
                local[cid]["read1"] = str(r.rows[0][0])
            else:
                local[cid]["read1"] = ""
            return _result_dict(r)

        def ev_update(ad: DBAdapter) -> Dict[str, Any]:
            base = local[cid]["read1"]
            suffix = f"{elem},"
            local[cid]["new_val"] = f"{base}{suffix}"
            if db == "sqlite":
                r = ad.exec(f"UPDATE txn0 SET val = {concat_expr} WHERE id = 1", [base, suffix])
            else:
                r = ad.exec(f"UPDATE txn0 SET val = {concat_expr} WHERE id = 1", [base, suffix])
            return _result_dict(r)

        def ev_read2(ad: DBAdapter) -> Dict[str, Any]:
            r = ad.exec("SELECT val FROM txn0 WHERE id = 1")
            if r.status == "ok" and r.rows:
                local[cid]["read2"] = str(r.rows[0][0])
            return _result_dict(r)

        def ev_end(ad: DBAdapter) -> Dict[str, Any]:
            r = ad.commit()
            local[cid]["committed"] = r.status == "ok"
            return _result_dict(r)

        return [
            (f"c{cid}_begin", ev_begin),
            (f"c{cid}_read1", ev_read1),
            (f"c{cid}_update", ev_update),
            (f"c{cid}_read2", ev_read2),
            (f"c{cid}_end", ev_end),
        ]

    events_by_client = [mk_events(cid) for cid in range(clients)]
    lengths = [len(events) for events in events_by_client]

    if strategy == "heuristic_fuzzer":
        schedule = []
        for cid in range(clients):
            schedule.extend([cid, cid])  # begin + read for all first
        for cid in range(clients):
            schedule.extend([cid, cid, cid])  # update + read2 + end
        delay_candidates = [eid for evs in events_by_client for eid, _ in evs if eid.endswith(("read1", "update", "end"))]
        delays = _choose_delays(rng, delay_candidates, k)
    elif strategy == "random_delays":
        schedule = _random_interleaving(lengths, rng)
        all_ids = [eid for evs in events_by_client for eid, _ in evs if not eid.endswith("begin")]
        delays = _choose_delays(rng, all_ids, k)
    else:
        schedule = _round_robin_schedule(lengths)
        delays = {}

    raw = _run_scheduled_program(
        db=db,
        dsn=dsn,
        setup_sql=_list_append_setup_sql(),
        init_sql=_list_append_init_sql(),
        isolation=isolation,
        events_by_client=events_by_client,
        schedule=schedule,
        delays=delays,
        analysis_only=analysis_only,
    )
    final_state = _fetch_list_append_state(db, dsn)
    committed = sorted(int(x["elem"]) for x in local if x.get("committed"))
    final_elems = final_state["parsed"]
    missing = sorted([v for v in committed if v not in final_elems])
    bug_found = len(missing) > 0
    canonical = {
        "final_val": final_state["val"],
        "final_parsed": final_elems,
        "committed": committed,
        "missing": missing,
        "commit_count": len(committed),
    }
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=raw["errors"],
        execution_log=raw["execution_log"],
        artifact={"schedule": schedule, "delays": delays, "clients": clients},
        round_trips=raw["round_trips"],
        elapsed_s=raw["elapsed_s"],
        final_state=final_state,
        details={"strategy": strategy, "seed": seed},
    )


def run_list_append_micro_stress_trial(
    *,
    db: str,
    dsn: str,
    seed: int,
    clients: int = 3,
    isolation: str = "READ COMMITTED",
) -> TrialResult:
    rng = random.Random(seed)
    _exec_schema(db, dsn, _list_append_setup_sql(), _list_append_init_sql())
    errors: List[str] = []
    lock = threading.Lock()
    ph = _placeholder(db)
    concat_expr = _concat_assign_expr(db)
    committed: List[int] = []

    def worker(cid: int) -> None:
        ad = create_adapter(db)
        ad.connect(dsn, autocommit=False)
        try:
            ad.begin(isolation)
            time.sleep(rng.uniform(0.0, 0.002))
            r1 = ad.exec("SELECT val FROM txn0 WHERE id = 1")
            base = str(r1.rows[0][0]) if r1.status == "ok" and r1.rows else ""
            suffix = f"{cid + 1},"
            ad.exec(f"UPDATE txn0 SET val = {concat_expr} WHERE id = 1", [base, suffix])
            ad.exec("SELECT val FROM txn0 WHERE id = 1")
            rc = ad.commit()
            if rc.status == "ok":
                with lock:
                    committed.append(cid + 1)
        except Exception as e:
            with lock:
                errors.append(str(e))
            try:
                ad.rollback()
            except Exception:
                pass
        finally:
            ad.close()

    t0 = time.monotonic()
    threads = [threading.Thread(target=worker, args=(cid,), daemon=True) for cid in range(clients)]
    for t in threads:
        t.start()
        time.sleep(rng.uniform(0.0, 0.002))
    for t in threads:
        t.join(timeout=5.0)

    final_state = _fetch_list_append_state(db, dsn)
    final_elems = final_state["parsed"]
    committed_sorted = sorted(committed)
    missing = sorted([v for v in committed_sorted if v not in final_elems])
    bug_found = len(missing) > 0
    canonical = {
        "final_val": final_state["val"],
        "final_parsed": final_elems,
        "committed": committed_sorted,
        "missing": missing,
        "commit_count": len(committed_sorted),
    }
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=errors,
        execution_log=[],
        elapsed_s=time.monotonic() - t0,
        final_state=final_state,
    )


def _unique_insert_setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS uniq",
        "CREATE TABLE uniq (id INT PRIMARY KEY)",
    ]


def run_unique_insert_race_micro_trial(
    *,
    db: str,
    dsn: str,
    seed: int,
    strategy: str,
    isolation: str = "READ COMMITTED",
    k: int = 3,
    analysis_only: bool = False,
) -> TrialResult:
    rng = random.Random(seed)
    outcomes = ["init", "init"]
    ph = _placeholder(db)

    def mk_events(cid: int) -> List[Tuple[str, EventFn]]:
        def ev_begin(ad: DBAdapter) -> Dict[str, Any]:
            return _result_dict(ad.begin(isolation))

        def ev_check(ad: DBAdapter) -> Dict[str, Any]:
            r = ad.exec("SELECT COUNT(*) FROM uniq WHERE id = 1")
            return _result_dict(r)

        def ev_insert(ad: DBAdapter) -> Dict[str, Any]:
            # Re-read condition to keep logic deterministic against scheduled interleavings.
            rc = ad.exec("SELECT COUNT(*) FROM uniq WHERE id = 1")
            cnt = int(rc.rows[0][0]) if rc.status == "ok" and rc.rows else 0
            if cnt != 0:
                outcomes[cid] = "skip"
                return {
                    "status": "ok",
                    "rowcount": 0,
                    "rows": [],
                    "error_code": None,
                    "error_message": None,
                    "skipped": True,
                }
            r = ad.exec(f"INSERT INTO uniq(id) VALUES ({ph})", [1])
            if r.status == "ok":
                outcomes[cid] = "insert_ok"
            else:
                outcomes[cid] = f"insert_err_{r.error_code}"
            return _result_dict(r)

        def ev_end(ad: DBAdapter) -> Dict[str, Any]:
            if outcomes[cid].startswith("insert_err"):
                rr = ad.rollback()
                outcomes[cid] = f"{outcomes[cid]}_rollback"
                return _result_dict(rr)
            rc = ad.commit()
            if outcomes[cid] == "skip":
                outcomes[cid] = "commit_skip" if rc.status == "ok" else "commit_skip_err"
            elif outcomes[cid] == "insert_ok":
                outcomes[cid] = "commit_insert" if rc.status == "ok" else "commit_insert_err"
            return _result_dict(rc)

        return [
            (f"c{cid}_begin", ev_begin),
            (f"c{cid}_check", ev_check),
            (f"c{cid}_insert", ev_insert),
            (f"c{cid}_end", ev_end),
        ]

    events_by_client = [mk_events(0), mk_events(1)]
    lengths = [len(evs) for evs in events_by_client]
    if strategy == "heuristic_fuzzer":
        schedule = [0, 1, 0, 1, 0, 1, 0, 1]
        delays = _choose_delays(rng, ["c0_check", "c1_check", "c0_insert", "c1_insert"], k)
    elif strategy == "random_delays":
        schedule = _random_interleaving(lengths, rng)
        delays = _choose_delays(rng, [eid for evs in events_by_client for eid, _ in evs if not eid.endswith("begin")], k)
    else:
        schedule = _round_robin_schedule(lengths)
        delays = {}

    raw = _run_scheduled_program(
        db=db,
        dsn=dsn,
        setup_sql=_unique_insert_setup_sql(),
        init_sql=[],
        isolation=isolation,
        events_by_client=events_by_client,
        schedule=schedule,
        delays=delays,
        analysis_only=analysis_only,
    )
    rows = _query_rows(db, dsn, "SELECT id FROM uniq ORDER BY id")
    row_exists = len(rows) > 0
    canonical = {
        "row_exists": row_exists,
        "outcomes": outcomes,
        "row_count": len(rows),
    }
    # This workload is primarily for state-coverage, not a bug oracle.
    bug_found = len(rows) > 1
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=raw["errors"],
        execution_log=raw["execution_log"],
        artifact={"schedule": schedule, "delays": delays},
        round_trips=raw["round_trips"],
        elapsed_s=raw["elapsed_s"],
        final_state={"rows": [list(r) for r in rows]},
        details={"strategy": strategy, "seed": seed},
    )


def run_unique_insert_race_micro_stress_trial(
    *,
    db: str,
    dsn: str,
    seed: int,
    isolation: str = "READ COMMITTED",
) -> TrialResult:
    rng = random.Random(seed)
    _exec_schema(db, dsn, _unique_insert_setup_sql(), [])
    outcomes = ["init", "init"]
    errors: List[str] = []
    lock = threading.Lock()
    ph = _placeholder(db)

    def worker(cid: int) -> None:
        ad = create_adapter(db)
        ad.connect(dsn, autocommit=False)
        try:
            ad.begin(isolation)
            time.sleep(rng.uniform(0.0, 0.002))
            rc = ad.exec("SELECT COUNT(*) FROM uniq WHERE id = 1")
            cnt = int(rc.rows[0][0]) if rc.status == "ok" and rc.rows else 0
            if cnt == 0:
                ri = ad.exec(f"INSERT INTO uniq(id) VALUES ({ph})", [1])
                if ri.status == "ok":
                    outcomes[cid] = "insert_ok"
                else:
                    outcomes[cid] = f"insert_err_{ri.error_code}"
            else:
                outcomes[cid] = "skip"
            if outcomes[cid].startswith("insert_err"):
                ad.rollback()
                outcomes[cid] = f"{outcomes[cid]}_rollback"
            else:
                rc2 = ad.commit()
                outcomes[cid] = f"commit_{outcomes[cid]}" if rc2.status == "ok" else f"commit_err_{outcomes[cid]}"
        except Exception as e:
            with lock:
                errors.append(str(e))
            try:
                ad.rollback()
            except Exception:
                pass
        finally:
            ad.close()

    t0 = time.monotonic()
    t1 = threading.Thread(target=worker, args=(0,), daemon=True)
    t2 = threading.Thread(target=worker, args=(1,), daemon=True)
    t1.start()
    time.sleep(rng.uniform(0.0, 0.002))
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    rows = _query_rows(db, dsn, "SELECT id FROM uniq ORDER BY id")
    row_exists = len(rows) > 0
    canonical = {
        "row_exists": row_exists,
        "outcomes": outcomes,
        "row_count": len(rows),
    }
    return TrialResult(
        bug_found=len(rows) > 1,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=errors,
        execution_log=[],
        elapsed_s=time.monotonic() - t0,
        final_state={"rows": [list(r) for r in rows]},
    )


def run_micro_trial(
    *,
    db: str,
    dsn: str,
    workload: str,
    strategy: str,
    seed: int,
    k: int = 3,
    analysis_only: bool = False,
) -> TrialResult:
    if workload == "job_queue_claim_micro":
        return run_job_queue_claim_micro_trial(
            db=db,
            dsn=dsn,
            seed=seed,
            strategy=strategy,
            variant="buggy",
            k=k,
            analysis_only=analysis_only,
        )
    if workload == "list_append_micro":
        return run_list_append_micro_trial(
            db=db,
            dsn=dsn,
            seed=seed,
            strategy=strategy,
            k=k,
            analysis_only=analysis_only,
        )
    if workload == "unique_insert_race_micro":
        return run_unique_insert_race_micro_trial(
            db=db,
            dsn=dsn,
            seed=seed,
            strategy=strategy,
            k=k,
            analysis_only=analysis_only,
        )
    raise ValueError(f"unknown micro workload {workload!r}")


def run_micro_stress_trial(
    *,
    db: str,
    dsn: str,
    workload: str,
    seed: int,
) -> TrialResult:
    if workload == "job_queue_claim_micro":
        return run_job_queue_claim_micro_stress_trial(db=db, dsn=dsn, seed=seed, variant="buggy")
    if workload == "list_append_micro":
        return run_list_append_micro_stress_trial(db=db, dsn=dsn, seed=seed)
    if workload == "unique_insert_race_micro":
        return run_unique_insert_race_micro_stress_trial(db=db, dsn=dsn, seed=seed)
    raise ValueError(f"unknown micro workload {workload!r}")


MICRO_WORKLOADS = [
    "job_queue_claim_micro",
    "list_append_micro",
    "unique_insert_race_micro",
]


def sqlite_version_string(sqlite_dsn: str) -> str:
    rows = _query_rows("sqlite", sqlite_dsn, "SELECT sqlite_version()")
    return str(rows[0][0]) if rows else "unknown"


def run_case_postgres_update_conflict_out_trial(
    *,
    dsn: str,
    strategy: str,
    seed: int,
    k: int = 3,
    variant: str = "update",
    delays: Optional[Dict[str, float]] = None,
) -> TrialResult:
    from workloads import update_conflict_out as uco

    rng = random.Random(seed)
    chosen_delays: Dict[str, float] = dict(delays or {})
    trace = uco.seed_trace(variant=variant, seed=seed)
    if not chosen_delays:
        if strategy == "random_delays":
            chosen_delays = uco.select_random_delays(trace, rng, k=k)
        elif strategy == "heuristic_fuzzer":
            candidates = uco.build_delay_candidates(trace)
            chosen_delays = uco.select_delays(candidates, rng, k=k)
    t0 = time.monotonic()
    result = uco.run_trial(dsn, variant=variant, delays=chosen_delays, seed=seed)
    elapsed = time.monotonic() - t0
    canonical = uco.canonical_state(result)
    return TrialResult(
        bug_found=bool(result.bug_found),
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=list(getattr(result, "all_errors", [])),
        execution_log=list(getattr(result, "execution_log", [])),
        artifact={"delays": chosen_delays, "variant": variant},
        elapsed_s=elapsed,
        final_state=canonical,
    )


def _mysql_replace_bug_setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS replace_bug",
        "CREATE TABLE replace_bug ("
        "id INT NOT NULL, "
        "uk INT NOT NULL, "
        "payload INT NOT NULL, "
        "PRIMARY KEY (id), "
        "UNIQUE KEY uk_idx (uk)"
        ") ENGINE=InnoDB",
    ]


def _mysql_replace_bug_init_sql(seed: int) -> List[Tuple[str, Optional[Sequence[Any]]]]:
    return [
        ("INSERT INTO replace_bug(id, uk, payload) VALUES (1, 1, %s)", [100 + seed % 7]),
        ("INSERT INTO replace_bug(id, uk, payload) VALUES (2, 2, %s)", [200 + seed % 11]),
    ]


def _mysql_replace_bug_check(dsn: str) -> Dict[str, Any]:
    dup = _query_rows("mysql", dsn, "SELECT uk, COUNT(*) FROM replace_bug GROUP BY uk HAVING COUNT(*) > 1")
    rows = _query_rows("mysql", dsn, "SELECT id, uk, payload FROM replace_bug ORDER BY id")
    check_rows = _query_rows("mysql", dsn, "CHECK TABLE replace_bug QUICK")
    check_ok = True
    for row in check_rows:
        row_txt = " ".join(str(x) for x in row).upper()
        if "OK" not in row_txt:
            check_ok = False
    unique_count = len({r[1] for r in rows})
    invariant_ok = len(rows) == unique_count
    return {
        "duplicates": [list(r) for r in dup],
        "rows": [list(r) for r in rows],
        "check_rows": [list(r) for r in check_rows],
        "check_ok": check_ok,
        "invariant_ok": invariant_ok,
    }


def run_case_mysql_replace_bug_trial(
    *,
    dsn: str,
    strategy: str,
    seed: int,
    k: int = 3,
    schedule: Optional[Sequence[int]] = None,
    delays: Optional[Dict[str, float]] = None,
) -> TrialResult:
    rng = random.Random(seed)
    ph = "%s"
    local = [{"ok": True}, {"ok": True}]

    def mk_events(cid: int) -> List[Tuple[str, EventFn]]:
        swap_val = 2 if cid == 0 else 1
        own_id = 1 if cid == 0 else 2
        payload = 1000 + seed + cid

        def ev_begin(ad: DBAdapter) -> Dict[str, Any]:
            r1 = ad.exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            if r1.status != "ok":
                return _result_dict(r1)
            # Short lock wait timeout to prevent the sequential executor from
            # hanging when cross-session lock waits occur.
            ad.exec("SET SESSION innodb_lock_wait_timeout = 1")
            return _result_dict(ad.exec("BEGIN"))

        def ev_replace(ad: DBAdapter) -> Dict[str, Any]:
            r = ad.exec(
                f"REPLACE INTO replace_bug(id, uk, payload) VALUES ({ph}, {ph}, {ph})",
                [own_id, swap_val, payload],
            )
            if r.status != "ok":
                local[cid]["ok"] = False
            return _result_dict(r)

        def ev_end(ad: DBAdapter) -> Dict[str, Any]:
            if local[cid]["ok"]:
                return _result_dict(ad.commit())
            return _result_dict(ad.rollback())

        return [
            (f"c{cid}_begin", ev_begin),
            (f"c{cid}_replace", ev_replace),
            (f"c{cid}_end", ev_end),
        ]

    events_by_client = [mk_events(0), mk_events(1)]
    lengths = [len(evs) for evs in events_by_client]
    if strategy == "stress":
        _exec_schema("mysql", dsn, _mysql_replace_bug_setup_sql(), _mysql_replace_bug_init_sql(seed))
        errors: List[str] = []
        lock = threading.Lock()
        rt = [0, 0]

        def worker(cid: int) -> None:
            swap_val = 2 if cid == 0 else 1
            own_id = 1 if cid == 0 else 2
            payload = 1000 + seed + cid
            ad = create_adapter("mysql")
            ad.connect(dsn, autocommit=False)
            try:
                ad.exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                ad.exec("SET SESSION innodb_lock_wait_timeout = 1")
                ad.exec("BEGIN")
                rr = ad.exec(
                    f"REPLACE INTO replace_bug(id, uk, payload) VALUES ({ph}, {ph}, {ph})",
                    [own_id, swap_val, payload],
                )
                if rr.status == "ok":
                    ad.commit()
                else:
                    with lock:
                        errors.append(f"replace_c{cid}: {rr.error_code} {rr.error_message}")
                    ad.rollback()
            except Exception as e:
                with lock:
                    errors.append(str(e))
                try:
                    ad.rollback()
                except Exception:
                    pass
            finally:
                rt[cid] = ad.round_trips
                ad.close()

        t0 = time.monotonic()
        t1 = threading.Thread(target=worker, args=(0,), daemon=True)
        t2 = threading.Thread(target=worker, args=(1,), daemon=True)
        t1.start()
        time.sleep(rng.uniform(0.0, 0.002))
        t2.start()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)
        raw = {
            "errors": errors,
            "execution_log": [],
            "round_trips": sum(rt),
            "elapsed_s": time.monotonic() - t0,
        }
        chosen_schedule = []
        chosen_delays = {}
    else:
        chosen_schedule = list(schedule) if schedule else []
        chosen_delays = dict(delays or {})
        if not chosen_schedule:
            if strategy == "heuristic_fuzzer":
                chosen_schedule = [0, 1, 0, 1, 0, 1]
            elif strategy == "random_delays":
                chosen_schedule = _random_interleaving(lengths, rng)
            else:
                chosen_schedule = _round_robin_schedule(lengths)
        if not chosen_delays:
            if strategy == "heuristic_fuzzer":
                chosen_delays = _choose_delays(rng, ["c0_replace", "c1_replace", "c0_end"], k)
            elif strategy == "random_delays":
                chosen_delays = _choose_delays(rng, ["c0_replace", "c1_replace"], k)

        raw = _run_scheduled_program(
            db="mysql",
            dsn=dsn,
            setup_sql=_mysql_replace_bug_setup_sql(),
            init_sql=_mysql_replace_bug_init_sql(seed),
            isolation="READ COMMITTED",
            events_by_client=events_by_client,
            schedule=chosen_schedule,
            delays=chosen_delays,
            analysis_only=False,
        )
    check = _mysql_replace_bug_check(dsn)
    # Bug = silent unique key violation (duplicates in uk column) or table corruption.
    # Transient errors (1213 deadlock, 1205 lock wait timeout, 1062 dup key during REPLACE)
    # are expected under concurrency and are NOT bugs.
    transient_codes = {"1213", "1205", "1062"}
    non_transient_errors = [e for e in raw["errors"]
                            if not any(code in e for code in transient_codes)]
    bug_found = (not check["invariant_ok"]) or (not check["check_ok"]) or bool(check["duplicates"])
    canonical = {
        "rows": check["rows"],
        "duplicates": check["duplicates"],
        "check_ok": check["check_ok"],
        "errors": raw["errors"],
    }
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=raw["errors"],
        execution_log=raw["execution_log"],
        artifact={"schedule": chosen_schedule, "delays": chosen_delays},
        round_trips=raw["round_trips"],
        elapsed_s=raw["elapsed_s"],
        final_state=check,
    )


def run_case_mysql_rr_internal_trial(
    *,
    dsn: str,
    strategy: str,
    seed: int,
    k: int = 3,
    fixed: bool = False,
    schedule: Optional[Sequence[int]] = None,
    delays: Optional[Dict[str, float]] = None,
) -> TrialResult:
    rng = random.Random(seed)
    ph = "%s"
    names = [f"name_{seed}_a", f"name_{seed}_b", f"name_{seed}_c"]
    local = {"r1": None, "r2": None}

    setup_sql = [
        "DROP TABLE IF EXISTS people",
        "CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(128), gender VARCHAR(16)) ENGINE=InnoDB",
    ]
    init_sql = [("INSERT INTO people(id, name, gender) VALUES (0, 'alice', 'f')", None)]

    def ev_r_begin(ad: DBAdapter) -> Dict[str, Any]:
        ad.exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        return _result_dict(ad.exec("BEGIN"))

    def ev_r_read1(ad: DBAdapter) -> Dict[str, Any]:
        sql = "SELECT name FROM people WHERE id = 0 FOR UPDATE" if fixed else "SELECT name FROM people WHERE id = 0"
        r = ad.exec(sql)
        if r.status == "ok" and r.rows:
            local["r1"] = str(r.rows[0][0])
        return _result_dict(r)

    def ev_r_update_gender(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(
            ad.exec(
                "UPDATE people SET gender = CASE WHEN gender = 'f' THEN 'm' ELSE 'f' END WHERE id = 0"
            )
        )

    def ev_r_read2(ad: DBAdapter) -> Dict[str, Any]:
        r = ad.exec("SELECT name FROM people WHERE id = 0")
        if r.status == "ok" and r.rows:
            local["r2"] = str(r.rows[0][0])
        return _result_dict(r)

    def ev_r_end(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(ad.commit())

    def ev_w_begin(ad: DBAdapter) -> Dict[str, Any]:
        ad.exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        return _result_dict(ad.exec("BEGIN"))

    def ev_w_update(ad: DBAdapter) -> Dict[str, Any]:
        new_name = rng.choice(names)
        return _result_dict(ad.exec(f"UPDATE people SET name = {ph} WHERE id = 0", [new_name]))

    def ev_w_end(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(ad.commit())

    events_by_client = [
        [
            ("r_begin", ev_r_begin),
            ("r_read1", ev_r_read1),
            ("r_update_gender", ev_r_update_gender),
            ("r_read2", ev_r_read2),
            ("r_end", ev_r_end),
        ],
        [
            ("w_begin", ev_w_begin),
            ("w_update", ev_w_update),
            ("w_end", ev_w_end),
        ],
    ]
    lengths = [len(x) for x in events_by_client]

    chosen_schedule = list(schedule) if schedule else []
    chosen_delays = dict(delays or {})
    if strategy == "stress":
        _exec_schema("mysql", dsn, setup_sql, init_sql)
        errors: List[str] = []
        lock = threading.Lock()
        rt = [0, 0]

        def worker_r() -> None:
            ad = create_adapter("mysql")
            ad.connect(dsn, autocommit=False)
            try:
                ad.exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                ad.exec("BEGIN")
                sql = "SELECT name FROM people WHERE id = 0 FOR UPDATE" if fixed else "SELECT name FROM people WHERE id = 0"
                r = ad.exec(sql)
                if r.status == "ok" and r.rows:
                    local["r1"] = str(r.rows[0][0])
                time.sleep(rng.uniform(0.0, 0.002))
                ad.exec("UPDATE people SET gender = CASE WHEN gender = 'f' THEN 'm' ELSE 'f' END WHERE id = 0")
                r2 = ad.exec("SELECT name FROM people WHERE id = 0")
                if r2.status == "ok" and r2.rows:
                    local["r2"] = str(r2.rows[0][0])
                ad.commit()
            except Exception as e:
                with lock:
                    errors.append(str(e))
                try:
                    ad.rollback()
                except Exception:
                    pass
            finally:
                rt[0] = ad.round_trips
                ad.close()

        def worker_w() -> None:
            ad = create_adapter("mysql")
            ad.connect(dsn, autocommit=False)
            try:
                ad.exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                ad.exec("BEGIN")
                time.sleep(rng.uniform(0.0, 0.002))
                ad.exec("UPDATE people SET name = %s WHERE id = 0", [rng.choice(names)])
                ad.commit()
            except Exception as e:
                with lock:
                    errors.append(str(e))
                try:
                    ad.rollback()
                except Exception:
                    pass
            finally:
                rt[1] = ad.round_trips
                ad.close()

        t0 = time.monotonic()
        tr = threading.Thread(target=worker_r, daemon=True)
        tw = threading.Thread(target=worker_w, daemon=True)
        tr.start()
        time.sleep(rng.uniform(0.0, 0.002))
        tw.start()
        tr.join(timeout=5.0)
        tw.join(timeout=5.0)
        raw = {
            "errors": errors,
            "execution_log": [],
            "round_trips": sum(rt),
            "elapsed_s": time.monotonic() - t0,
        }
        chosen_schedule = []
        chosen_delays = {}
    else:
        if not chosen_schedule:
            if strategy == "heuristic_fuzzer":
                chosen_schedule = [0, 1, 0, 1, 1, 0, 0, 0]
            elif strategy == "random_delays":
                chosen_schedule = _random_interleaving(lengths, rng)
            else:
                chosen_schedule = _round_robin_schedule(lengths)
        if not chosen_delays:
            if strategy in ("heuristic_fuzzer", "random_delays"):
                chosen_delays = _choose_delays(rng, ["r_read1", "w_update", "r_read2"], k)

        raw = _run_scheduled_program(
            db="mysql",
            dsn=dsn,
            setup_sql=setup_sql,
            init_sql=init_sql,
            isolation="REPEATABLE READ",
            events_by_client=events_by_client,
            schedule=chosen_schedule,
            delays=chosen_delays,
            analysis_only=False,
        )
    rows = _query_rows("mysql", dsn, "SELECT name, gender FROM people WHERE id = 0")
    final_name = str(rows[0][0]) if rows else None
    bug_found = (local["r1"] is not None and local["r2"] is not None and local["r1"] != local["r2"])
    canonical = {
        "r1": local["r1"],
        "r2": local["r2"],
        "final_name": final_name,
        "fixed": fixed,
        "errors": raw["errors"],
    }
    return TrialResult(
        bug_found=bug_found,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=raw["errors"],
        execution_log=raw["execution_log"],
        artifact={"schedule": chosen_schedule, "delays": chosen_delays, "fixed": fixed},
        round_trips=raw["round_trips"],
        elapsed_s=raw["elapsed_s"],
        final_state={"rows": [list(r) for r in rows]},
    )


def run_case_sqlite_dirty_read_trial(
    *,
    sqlite_dsn: str,
    strategy: str,
    seed: int,
    k: int = 3,
    read_uncommitted: bool = True,
    shared_cache_expected: bool = True,
    schedule: Optional[Sequence[int]] = None,
    delays: Optional[Dict[str, float]] = None,
) -> TrialResult:
    rng = random.Random(seed)
    read_pragmas = {"read_uncommitted": 1 if read_uncommitted else 0}
    writer_local = {"updated": False}
    read_local = {"seen": None}

    setup_sql = [
        "DROP TABLE IF EXISTS dirty_read",
        "CREATE TABLE dirty_read (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)",
    ]
    init_sql = [("INSERT INTO dirty_read(id, val) VALUES (1, 0)", None)]

    def ev_w_begin(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(ad.begin("READ UNCOMMITTED", begin_mode="IMMEDIATE"))

    def ev_w_update(ad: DBAdapter) -> Dict[str, Any]:
        r = ad.exec("UPDATE dirty_read SET val = 1 WHERE id = 1")
        writer_local["updated"] = r.status == "ok"
        return _result_dict(r)

    def ev_w_rollback(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(ad.rollback())

    def ev_r_begin(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(ad.begin("READ UNCOMMITTED", begin_mode="DEFERRED"))

    def ev_r_read(ad: DBAdapter) -> Dict[str, Any]:
        r = ad.exec("SELECT val FROM dirty_read WHERE id = 1")
        if r.status == "ok" and r.rows:
            read_local["seen"] = int(r.rows[0][0])
        return _result_dict(r)

    def ev_r_end(ad: DBAdapter) -> Dict[str, Any]:
        return _result_dict(ad.commit())

    events_by_client = [
        [("w_begin", ev_w_begin), ("w_update", ev_w_update), ("w_rollback", ev_w_rollback)],
        [("r_begin", ev_r_begin), ("r_read", ev_r_read), ("r_end", ev_r_end)],
    ]
    lengths = [len(x) for x in events_by_client]

    chosen_schedule = list(schedule) if schedule else []
    chosen_delays = dict(delays or {})
    if strategy == "stress":
        _exec_schema("sqlite", sqlite_dsn, setup_sql, init_sql)
        errors: List[str] = []
        lock = threading.Lock()
        rt = [0, 0]

        def writer() -> None:
            ad = create_adapter("sqlite")
            ad.connect(sqlite_dsn, autocommit=False)
            try:
                ad.begin("READ UNCOMMITTED", begin_mode="IMMEDIATE")
                ad.exec("UPDATE dirty_read SET val = 1 WHERE id = 1")
                writer_local["updated"] = True
                time.sleep(rng.uniform(0.0, 0.003))
                ad.rollback()
            except Exception as e:
                with lock:
                    errors.append(str(e))
                try:
                    ad.rollback()
                except Exception:
                    pass
            finally:
                rt[0] = ad.round_trips
                ad.close()

        def reader() -> None:
            ad = create_adapter("sqlite")
            ad.connect(sqlite_dsn, autocommit=False)
            try:
                ad.set_pragmas(read_pragmas)
                time.sleep(rng.uniform(0.0, 0.002))
                ad.begin("READ UNCOMMITTED", begin_mode="DEFERRED")
                rr = ad.exec("SELECT val FROM dirty_read WHERE id = 1")
                if rr.status == "ok" and rr.rows:
                    read_local["seen"] = int(rr.rows[0][0])
                ad.commit()
            except Exception as e:
                with lock:
                    errors.append(str(e))
                try:
                    ad.rollback()
                except Exception:
                    pass
            finally:
                rt[1] = ad.round_trips
                ad.close()

        t0 = time.monotonic()
        tw = threading.Thread(target=writer, daemon=True)
        tr = threading.Thread(target=reader, daemon=True)
        tw.start()
        tr.start()
        tw.join(timeout=5.0)
        tr.join(timeout=5.0)
        raw = {
            "errors": errors,
            "execution_log": [],
            "round_trips": sum(rt),
            "elapsed_s": time.monotonic() - t0,
        }
        chosen_schedule = []
        chosen_delays = {}
    else:
        if not chosen_schedule:
            if strategy == "heuristic_fuzzer":
                chosen_schedule = [0, 0, 1, 1, 0, 1]
            elif strategy == "random_delays":
                chosen_schedule = _random_interleaving(lengths, rng)
            else:
                chosen_schedule = _round_robin_schedule(lengths)
        if not chosen_delays:
            if strategy == "heuristic_fuzzer":
                chosen_delays = _choose_delays(rng, ["w_rollback", "r_read"], k)
            elif strategy == "random_delays":
                chosen_delays = _choose_delays(rng, ["w_update", "r_read", "w_rollback"], k)

        raw = _run_scheduled_program(
            db="sqlite",
            dsn=sqlite_dsn,
            setup_sql=setup_sql,
            init_sql=init_sql,
            isolation="READ UNCOMMITTED",
            events_by_client=events_by_client,
            schedule=chosen_schedule,
            delays=chosen_delays,
            analysis_only=False,
            session_pragmas=[None, read_pragmas],
        )
    final_rows = _query_rows("sqlite", sqlite_dsn, "SELECT val FROM dirty_read WHERE id = 1")
    final_val = int(final_rows[0][0]) if final_rows else 0
    saw_dirty = read_local["seen"] == 1 and final_val == 0 and writer_local["updated"]
    # If shared-cache is disabled, dirty-read should be impossible even with read_uncommitted=1.
    if not shared_cache_expected:
        saw_dirty = False if read_local["seen"] == 1 else False
    canonical = {
        "reader_seen": read_local["seen"],
        "final_val": final_val,
        "writer_updated": writer_local["updated"],
        "read_uncommitted": read_uncommitted,
        "shared_cache_expected": shared_cache_expected,
        "errors": raw["errors"],
    }
    return TrialResult(
        bug_found=saw_dirty,
        canonical_state=canonical,
        state_hash=hash_canonical_state(canonical),
        errors=raw["errors"],
        execution_log=raw["execution_log"],
        artifact={"schedule": chosen_schedule, "delays": chosen_delays, "read_uncommitted": read_uncommitted},
        round_trips=raw["round_trips"],
        elapsed_s=raw["elapsed_s"],
        final_state={"final_val": final_val},
    )


def replay_trial(
    fn: Callable[..., TrialResult],
    kwargs: Dict[str, Any],
    repeats: int,
) -> Dict[str, Any]:
    successes = 0
    runs: List[Dict[str, Any]] = []
    for i in range(repeats):
        trial_kwargs = dict(kwargs)
        base_seed = int(trial_kwargs.get("seed", 0))
        trial_kwargs["seed"] = base_seed + i * 10000
        r = fn(**trial_kwargs)
        runs.append(
            {
                "seed": trial_kwargs["seed"],
                "bug_found": r.bug_found,
                "state_hash": r.state_hash,
                "errors": list(r.errors),
            }
        )
        if r.bug_found:
            successes += 1
    return {
        "replays": repeats,
        "successes": successes,
        "rate": successes / max(1, repeats),
        "runs": runs,
    }
