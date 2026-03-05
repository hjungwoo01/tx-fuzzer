"""
Workload: job_queue_double_claim — realistic job-queue race condition.

Multiple workers race to claim the next available job using a
SELECT-then-UPDATE pattern under READ COMMITTED. The TOCTOU gap
between reading the "ready" row and updating it allows two workers
to claim the same job — a classic double-claim bug.

Schema:
    jobs(id INT PK, status TEXT, worker TEXT, claimed_at TIMESTAMPTZ)

Invariant:
    Each job id with status='running' must have been selected by exactly
    one worker's SELECT.  If two workers both SELECTed the same job id
    (both saw status='ready') and both issued UPDATE, that is a double
    claim — even though READ COMMITTED last-writer-wins means only one
    worker value survives in the DB.

Design for low baseline bug rate:
    - NO barrier: workers start with a small random stagger (0-2ms).
    - Under natural execution the stagger + transaction overhead means
      the first worker often COMMITS before the second worker's SELECT,
      so the second worker never sees the job as 'ready'.
    - With heuristic delays the fuzzer holds the first worker's COMMIT,
      keeping its UPDATE invisible, so the second worker's SELECT still
      sees status='ready'.

Fixed variant:
    SELECT … FOR UPDATE SKIP LOCKED — the DB-level lock serializes
    the claim and SKIP LOCKED allows other workers to move to the
    next available job without blocking.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Any, Dict, List, Optional

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore

from workloads import OracleResult

WORKLOAD_META = {
    "name": "job_queue_double_claim",
    "description": "Job-queue double-claim race: SELECT-then-UPDATE TOCTOU under READ COMMITTED",
    "invariants": "Each running job must be claimed by exactly one worker.",
    "isolation_levels": ["READ COMMITTED"],
    "target_versions": {"primary": "16.2"},
    "clients": 2,
    "variants": ["buggy", "fixed"],
    "negative_controls": [
        "Fixed variant uses SELECT … FOR UPDATE SKIP LOCKED",
    ],
}

NUM_JOBS = 1
NUM_WORKERS = 2


# ── Schema ──────────────────────────────────────────────────────────────────

def setup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS jobs CASCADE",
        "CREATE TABLE jobs (id INT PRIMARY KEY, status TEXT NOT NULL, "
        "worker TEXT, claimed_at TIMESTAMPTZ)",
    ]


def _init_sql(num_jobs: int = NUM_JOBS) -> List[str]:
    return [f"INSERT INTO jobs VALUES ({jid}, 'ready', NULL, NULL)"
            for jid in range(1, num_jobs + 1)]


def truncate_sql() -> List[str]:
    return ["DELETE FROM jobs"]


# ── Seed trace ──────────────────────────────────────────────────────────────

def seed_trace(
    clients: int = NUM_WORKERS,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    seed: int = 42,
    **kwargs,
) -> List[Dict[str, Any]]:
    """Build a seed trace: each worker does BEGIN → SELECT → UPDATE → COMMIT."""
    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"
    trace = []
    for wid in range(clients):
        sess = f"worker_{wid}"
        trace.append({"event_id": f"w{wid}_begin", "client_id": wid, "session": sess,
                       "type": "begin", "sql": iso_sql})
        if variant == "fixed":
            trace.append({"event_id": f"w{wid}_select", "client_id": wid, "session": sess,
                           "type": "sql",
                           "sql": "SELECT id FROM jobs WHERE status='ready' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"})
        else:
            trace.append({"event_id": f"w{wid}_select", "client_id": wid, "session": sess,
                           "type": "sql",
                           "sql": "SELECT id FROM jobs WHERE status='ready' ORDER BY id LIMIT 1"})
        trace.append({"event_id": f"w{wid}_update", "client_id": wid, "session": sess,
                       "type": "sql", "sql": "__CONDITIONAL_UPDATE__"})
        trace.append({"event_id": f"w{wid}_commit", "client_id": wid, "session": sess,
                       "type": "commit", "sql": "COMMIT"})
    return trace


# ── Canonical state ─────────────────────────────────────────────────────────

def canonical_state(trial_result: Dict[str, Any]) -> Dict[str, Any]:
    """Return timing-free logical snapshot for hashing."""
    fs = trial_result.get("final_state", {})
    return {
        "bug_found": trial_result.get("bug_found", False),
        "double_claims": fs.get("double_claims", []),
        "job_states": fs.get("job_states", []),
    }


# ── Concurrent trial runner ─────────────────────────────────────────────────

def run_trial(
    dsn: str,
    isolation: str = "READ COMMITTED",
    variant: str = "buggy",
    delays: Optional[Dict[str, float]] = None,
    seed: int = 42,
    clients: int = NUM_WORKERS,
    num_jobs: int = NUM_JOBS,
) -> Dict[str, Any]:
    """Run one concurrent trial with real threads.

    Key design choice: NO barrier.  Workers start with a tiny random
    stagger (0-2 ms) determined by `seed`, simulating realistic arrival
    jitter.  Under baseline (no delays) this stagger is often enough for
    one worker to finish its full transaction before the next one starts
    its SELECT, preventing the double-claim.  Under heuristic delays the
    fuzzer stretches the first worker's transaction to overlap with the
    second worker's SELECT.
    """
    delays = delays or {}
    rng = random.Random(seed)
    result: Dict[str, Any] = {
        "bug_found": False, "got_40001": False,
        "execution_log": [], "errors": [], "final_state": {},
    }
    lock = threading.Lock()

    # Setup
    conn_s = psycopg2.connect(dsn)
    conn_s.autocommit = True
    cur_s = conn_s.cursor()
    for sql in setup_sql():
        cur_s.execute(sql)
    for jid in range(1, num_jobs + 1):
        cur_s.execute("INSERT INTO jobs VALUES (%s, 'ready', NULL, NULL)", (jid,))
    conn_s.close()

    iso_sql = f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}"

    # Per-worker stagger (0–20 ms) — simulates real-world arrival jitter.
    # A single claim txn takes ~3-5 ms, so 20 ms max stagger means the
    # second worker often starts AFTER the first one committed, preventing
    # the double-claim under natural execution.
    staggers = [rng.uniform(0.0, 0.020) for _ in range(clients)]

    def worker_buggy(wid: int):
        # Stagger start
        time.sleep(staggers[wid])

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            # BEGIN
            eid = f"w{wid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            cur.execute(iso_sql)

            # SELECT ready job
            eid = f"w{wid}_select"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute("SELECT id FROM jobs WHERE status='ready' ORDER BY id LIMIT 1")
                rows = cur.fetchall()
                job_id = rows[0][0] if rows else None
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            with lock:
                result["execution_log"].append({
                    "event_id": eid, "worker": wid, "job_selected": job_id,
                })

            if job_id is not None:
                # UPDATE: claim the job
                eid = f"w{wid}_update"
                d = delays.get(eid, 0.0)
                if d > 0:
                    time.sleep(d)
                try:
                    cur.execute(
                        "UPDATE jobs SET status='running', worker=%s, "
                        "claimed_at=now() WHERE id=%s",
                        (f"worker_{wid}", job_id),
                    )
                except Exception as e:
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {e}")
                        if sqlstate == "40001":
                            result["got_40001"] = True
                    try:
                        cur.execute("ROLLBACK")
                    except Exception:
                        pass
                    return

            # COMMIT
            eid = f"w{wid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute("COMMIT")
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def worker_fixed(wid: int):
        time.sleep(staggers[wid])

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            eid = f"w{wid}_begin"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            cur.execute(iso_sql)

            # SELECT … FOR UPDATE SKIP LOCKED
            eid = f"w{wid}_select"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute(
                    "SELECT id FROM jobs WHERE status='ready' "
                    "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
                )
                rows = cur.fetchall()
                job_id = rows[0][0] if rows else None
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                return

            with lock:
                result["execution_log"].append({
                    "event_id": eid, "worker": wid, "job_selected": job_id,
                })

            if job_id is not None:
                eid = f"w{wid}_update"
                d = delays.get(eid, 0.0)
                if d > 0:
                    time.sleep(d)
                try:
                    cur.execute(
                        "UPDATE jobs SET status='running', worker=%s, "
                        "claimed_at=now() WHERE id=%s",
                        (f"worker_{wid}", job_id),
                    )
                except Exception as e:
                    sqlstate = getattr(e, "pgcode", None)
                    with lock:
                        result["errors"].append(f"{eid}: {e}")
                        if sqlstate == "40001":
                            result["got_40001"] = True
                    try:
                        cur.execute("ROLLBACK")
                    except Exception:
                        pass
                    return

            eid = f"w{wid}_commit"
            d = delays.get(eid, 0.0)
            if d > 0:
                time.sleep(d)
            try:
                cur.execute("COMMIT")
            except Exception as e:
                sqlstate = getattr(e, "pgcode", None)
                with lock:
                    result["errors"].append(f"{eid}: {e}")
                    if sqlstate == "40001":
                        result["got_40001"] = True
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    worker_fn = worker_fixed if variant == "fixed" else worker_buggy
    threads = [threading.Thread(target=worker_fn, args=(wid,), daemon=True)
               for wid in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)

    # Evaluate invariant: check for double claims via execution log
    conn_c = psycopg2.connect(dsn)
    conn_c.autocommit = True
    cur_c = conn_c.cursor()

    cur_c.execute("SELECT id, status, worker FROM jobs ORDER BY id")
    job_rows = cur_c.fetchall()
    job_states = [{"id": r[0], "status": r[1], "worker": r[2]} for r in job_rows]
    conn_c.close()

    # Double-claim detection: two workers selected the same job_id
    claimed_by: Dict[int, List[int]] = {}
    for entry in result["execution_log"]:
        jid = entry.get("job_selected")
        wid_entry = entry.get("worker")
        if jid is not None:
            claimed_by.setdefault(jid, []).append(wid_entry)

    double_claims = []
    for jid, workers in claimed_by.items():
        if len(workers) > 1:
            double_claims.append({"job_id": jid, "workers": workers})

    result["final_state"] = {
        "job_states": job_states,
        "double_claims": double_claims,
        "claimed_by": {str(k): v for k, v in claimed_by.items()},
    }

    if double_claims and not result["got_40001"]:
        result["bug_found"] = True

    return result


# ── Anti-dependency analysis (for heuristic delay injection) ────────────────

def build_delay_candidates(
    trace: List[Dict[str, Any]],
    clients: int = NUM_WORKERS,
) -> List[Dict[str, Any]]:
    """Build tiered delay candidates for the job-queue workload.

    The key heuristic insight: delaying one worker's UPDATE/COMMIT keeps
    its claim invisible to other workers, widening the TOCTOU window.
    """
    candidates = []
    seen = set()

    def _add(eid, sess, tier, weight, delay_range, justification):
        k = (eid, tier)
        if k in seen:
            return
        seen.add(k)
        candidates.append({
            "event_id": eid, "session": sess, "tier": tier,
            "weight": weight, "delay_range": delay_range,
            "justification": justification,
        })

    for wid in range(clients):
        sess = f"worker_{wid}"
        # Delay UPDATE so other workers' SELECTs still see 'ready'
        _add(f"w{wid}_update", sess, "tier_rw_conflict", 10, (0.03, 0.15),
             f"Delay w{wid} UPDATE so other workers SELECT same ready job")
        # Hold COMMIT to keep claim invisible
        _add(f"w{wid}_commit", sess, "tier_commit_hold", 9, (0.04, 0.20),
             f"Hold w{wid} COMMIT so its claim is invisible to others")
        # Delay SELECT to let another worker's SELECT run first (creates overlap)
        _add(f"w{wid}_select", sess, "tier_wr_conflict", 7, (0.02, 0.12),
             f"Delay w{wid} SELECT to overlap with another worker")
        # Fallback
        _add(f"w{wid}_begin", sess, "tier_fallback", 1, (0.01, 0.05),
             f"Generic delay on w{wid} begin")

    return candidates


def select_delays(
    candidates: List[Dict[str, Any]],
    rng: random.Random,
    k: int = 3,
) -> Dict[str, float]:
    """Weighted lottery selection of up to K delay points."""
    if not candidates or k <= 0:
        return {}
    pool = []
    for c in candidates:
        pool.extend([c] * c["weight"])
    selected: Dict[str, float] = {}
    used = set()
    for _ in range(k * 20):
        if len(selected) >= k:
            break
        c = rng.choice(pool)
        if c["event_id"] in used:
            continue
        used.add(c["event_id"])
        selected[c["event_id"]] = round(rng.uniform(*c["delay_range"]), 3)
    return selected


def select_random_delays(
    trace: List[Dict[str, Any]],
    rng: random.Random,
    k: int = 3,
) -> Dict[str, float]:
    """Random baseline: pick K random events, random delay 10–200ms."""
    eids = [ev.get("event_id", f"step_{i}") for i, ev in enumerate(trace)
            if ev.get("type") in ("sql", "begin", "commit", "rollback")]
    if not eids:
        return {}
    selected: Dict[str, float] = {}
    for _ in range(min(k, len(eids))):
        eid = rng.choice(eids)
        if eid not in selected:
            selected[eid] = round(rng.uniform(0.01, 0.20), 3)
    return selected


# ── Oracle ──────────────────────────────────────────────────────────────────

def oracle(
    execution_history: List[Dict[str, Any]],
    final_state: Any,
    trial_result: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> OracleResult:
    """Check for the double-claim invariant violation."""
    if trial_result is None:
        return OracleResult(ok=True, details="No trial result provided.")

    if trial_result.get("got_40001"):
        return OracleResult(ok=True, details="40001 raised; serialization prevented race.",
                            metrics={"got_40001": True})

    if trial_result.get("bug_found"):
        fs = trial_result.get("final_state", {})
        dc = fs.get("double_claims", [])
        return OracleResult(
            ok=False,
            bug_type="job_queue_double_claim",
            details=(
                f"Double claim detected: {len(dc)} job(s) claimed by multiple workers. "
                f"Details: {dc}"
            ),
            metrics={"double_claims": dc, "job_states": fs.get("job_states", [])},
        )

    return OracleResult(ok=True, details="No double claim detected.",
                        metrics=trial_result.get("final_state", {}))
