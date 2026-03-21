"""
Cross-DB micro workloads for evaluation.

Each workload defines:
  - setup_sql(db) -> list of SQL DDL/DML to prepare schema
  - run_buggy(adapter, worker_id) -> canonical state dict
  - run_fixed(adapter, worker_id) -> canonical state dict
  - oracle(states: list[dict]) -> (bug_found: bool, reason: str)
  - canonical_hash(state: dict) -> str

All workloads use the same logical semantics across Postgres, MySQL, SQLite.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .db_adapter import DBAdapter, ExecResult
from .error_taxonomy import ErrorCategory


def _hash_dict(d: Dict[str, Any]) -> str:
    """Deterministic hash of a dict."""
    return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _concat_expr(db: str, col: str, token: str) -> str:
    """Return SQL expression for appending token to column value."""
    if db == "mysql":
        return f"CONCAT({col}, ',', '{token}')"
    # Postgres and SQLite both support ||
    return f"{col} || ',' || '{token}'"


def _returning_clause(db: str) -> str:
    """RETURNING clause for Postgres (MySQL/SQLite don't support RETURNING in UPDATE reliably)."""
    if db == "postgres":
        return " RETURNING *"
    return ""


def job_queue_setup_sql(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS claims",
            "DROP TABLE IF EXISTS jobs",
            "CREATE TABLE IF NOT EXISTS jobs (id INT PRIMARY KEY, status VARCHAR(20) NOT NULL, worker VARCHAR(20))",
            "CREATE TABLE IF NOT EXISTS claims (job_id INT, worker VARCHAR(20), ts BIGINT)",
            "DELETE FROM claims",
            "DELETE FROM jobs",
            "INSERT INTO jobs (id, status, worker) VALUES (1, 'ready', NULL) "
            "ON DUPLICATE KEY UPDATE status='ready', worker=NULL",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS claims",
            "DROP TABLE IF EXISTS jobs",
            "CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, status TEXT NOT NULL, worker TEXT)",
            "CREATE TABLE IF NOT EXISTS claims (job_id INTEGER, worker TEXT, ts INTEGER)",
            "DELETE FROM claims",
            "DELETE FROM jobs",
            "INSERT OR REPLACE INTO jobs (id, status, worker) VALUES (1, 'ready', NULL)",
        ]
    # postgres
    return [
        "DROP TABLE IF EXISTS claims",
        "DROP TABLE IF EXISTS jobs",
        "CREATE TABLE IF NOT EXISTS jobs (id INT PRIMARY KEY, status TEXT NOT NULL, worker TEXT)",
        "CREATE TABLE IF NOT EXISTS claims (job_id INT, worker TEXT, ts BIGINT)",
        "DELETE FROM claims",
        "DELETE FROM jobs",
        "INSERT INTO jobs (id, status, worker) VALUES (1, 'ready', NULL) "
        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, worker = EXCLUDED.worker",
    ]


def job_queue_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Buggy: SELECT status then UPDATE without WHERE guard."""
    wname = f"w{worker_id}"
    isolation = "READ COMMITTED"
    adapter.begin(isolation)
    # Step 1: read status
    r = adapter.exec("SELECT status FROM jobs WHERE id = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    if not r.rows:
        adapter.rollback()
        return {"outcome": "no_job", "worker": wname}

    status = r.rows[0][0]
    if status != "ready":
        adapter.rollback()
        return {"outcome": "already_claimed", "worker": wname}

    # Bug: no WHERE status='ready' guard -- TOCTOU
    r2 = adapter.exec(
        "UPDATE jobs SET status = 'claimed', worker = %s WHERE id = 1"
        if adapter.db_name != "sqlite"
        else "UPDATE jobs SET status = 'claimed', worker = ? WHERE id = 1",
        (wname,))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname}

    # Record claim
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO claims (job_id, worker, ts) VALUES (1, ?, ?)", (wname, int(time.time() * 1000)))
    else:
        adapter.exec("INSERT INTO claims (job_id, worker, ts) VALUES (1, %s, %s)", (wname, int(time.time() * 1000)))

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "claimed", "worker": wname}


def job_queue_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed: UPDATE ... WHERE status='ready' with rowcount guard."""
    wname = f"w{worker_id}"
    isolation = "READ COMMITTED"
    adapter.begin(isolation)

    # Atomic claim: only update if still ready
    if adapter.db_name == "sqlite":
        r = adapter.exec(
            "UPDATE jobs SET status = 'claimed', worker = ? WHERE id = 1 AND status = 'ready'",
            (wname,))
    else:
        r = adapter.exec(
            "UPDATE jobs SET status = 'claimed', worker = %s WHERE id = 1 AND status = 'ready'",
            (wname,))

    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    if r.rowcount == 0:
        adapter.rollback()
        return {"outcome": "already_claimed", "worker": wname}

    # Record successful claim
    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO claims (job_id, worker, ts) VALUES (1, ?, ?)", (wname, int(time.time() * 1000)))
    else:
        adapter.exec("INSERT INTO claims (job_id, worker, ts) VALUES (1, %s, %s)", (wname, int(time.time() * 1000)))

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "claimed", "worker": wname}


def job_queue_oracle(outcomes: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Bug if >1 distinct worker successfully claimed the same job."""
    claimed = [o for o in outcomes if o.get("outcome") == "claimed"]
    workers = set(o.get("worker") for o in claimed)
    if len(workers) > 1:
        return True, f"Double claim! Workers: {sorted(workers)}"
    return False, f"{len(claimed)} claim(s), {len(workers)} unique worker(s)"


def job_queue_canonical(adapter: DBAdapter, outcomes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Read final jobs + claims state, including error outcome variants."""
    adapter.begin()
    r1 = adapter.exec("SELECT id, status, worker FROM jobs ORDER BY id")
    r2 = adapter.exec("SELECT job_id, worker FROM claims ORDER BY job_id, worker")
    adapter.commit()
    error_cats = sorted(set(
        o.get("error", "OK") for o in (outcomes or []) if o.get("outcome") == "error"
    ))
    outcome_kinds = sorted(set(o.get("outcome", "") for o in (outcomes or []) if o))
    return {
        "jobs": [(row[0], row[1], row[2]) for row in r1.rows] if r1.rows else [],
        "claims": [(row[0], row[1]) for row in r2.rows] if r2.rows else [],
        "error_categories": error_cats,
        "outcome_kinds": outcome_kinds,
    }


def bank_setup_sql(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS withdrawals",
            "DROP TABLE IF EXISTS accounts",
            "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS withdrawals (id INT AUTO_INCREMENT PRIMARY KEY, account_id INT, amount INT, worker VARCHAR(20), success BOOLEAN)",
            "DELETE FROM withdrawals",
            "DELETE FROM accounts",
            "INSERT INTO accounts (id, balance) VALUES (1, 100) "
            "ON DUPLICATE KEY UPDATE balance=100",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS withdrawals",
            "DROP TABLE IF EXISTS accounts",
            "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS withdrawals (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER, amount INTEGER, worker TEXT, success INTEGER)",
            "DELETE FROM withdrawals",
            "DELETE FROM accounts",
            "INSERT OR REPLACE INTO accounts (id, balance) VALUES (1, 100)",
        ]
    return [
        "DROP TABLE IF EXISTS withdrawals",
        "DROP TABLE IF EXISTS accounts",
        "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS withdrawals (id SERIAL PRIMARY KEY, account_id INT, amount INT, worker TEXT, success BOOLEAN)",
        "DELETE FROM withdrawals",
        "DELETE FROM accounts",
        "INSERT INTO accounts (id, balance) VALUES (1, 100) "
        "ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance",
    ]


def bank_buggy(adapter: DBAdapter, worker_id: int, amount: int = 80) -> Dict[str, Any]:
    """Buggy: SELECT balance then UPDATE without WHERE guard."""
    wname = f"w{worker_id}"
    adapter.begin("READ COMMITTED")

    # Read balance
    r = adapter.exec("SELECT balance FROM accounts WHERE id = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    balance = r.rows[0][0] if r.rows else 0
    if balance < amount:
        # Record failed withdrawal
        if adapter.db_name == "sqlite":
            adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, ?, ?, 0)", (amount, wname))
        else:
            adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, %s, %s, false)", (amount, wname))
        adapter.commit()
        return {"outcome": "insufficient", "worker": wname, "balance": balance}

    # Bug: TOCTOU -- balance could have changed
    if adapter.db_name == "sqlite":
        adapter.exec("UPDATE accounts SET balance = balance - ? WHERE id = 1", (amount,))
        adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, ?, ?, 1)", (amount, wname))
    else:
        adapter.exec("UPDATE accounts SET balance = balance - %s WHERE id = 1", (amount,))
        adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, %s, %s, true)", (amount, wname))

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "withdrawn", "worker": wname, "amount": amount}


def bank_fixed(adapter: DBAdapter, worker_id: int, amount: int = 80) -> Dict[str, Any]:
    """Fixed: UPDATE ... WHERE balance >= amt with rowcount guard."""
    wname = f"w{worker_id}"
    adapter.begin("READ COMMITTED")

    if adapter.db_name == "sqlite":
        r = adapter.exec("UPDATE accounts SET balance = balance - ? WHERE id = 1 AND balance >= ?", (amount, amount))
    else:
        r = adapter.exec("UPDATE accounts SET balance = balance - %s WHERE id = 1 AND balance >= %s", (amount, amount))

    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    if r.rowcount == 0:
        # Insufficient funds (atomic check)
        if adapter.db_name == "sqlite":
            adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, ?, ?, 0)", (amount, wname))
        else:
            adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, %s, %s, false)", (amount, wname))
        adapter.commit()
        return {"outcome": "insufficient", "worker": wname}

    if adapter.db_name == "sqlite":
        adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, ?, ?, 1)", (amount, wname))
    else:
        adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) VALUES (1, %s, %s, true)", (amount, wname))

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "withdrawn", "worker": wname, "amount": amount}


def bank_oracle(outcomes: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Bug if final balance < 0 OR more successful withdrawals than possible."""
    return False, "use bank_oracle_with_state instead"


def bank_oracle_with_state(outcomes: List[Dict[str, Any]], state: Dict[str, Any]) -> Tuple[bool, str]:
    balance = state.get("balance", 0)
    successful = state.get("successful_withdrawals", 0)
    if balance < 0:
        return True, f"Overdraft! balance={balance}"
    # Initial balance 100, each withdrawal 80 → only 1 can succeed
    if successful > 1:
        return True, f"Multiple overdraft-risk withdrawals succeeded: {successful}"
    return False, f"balance={balance}, successful={successful}"


def bank_canonical(adapter: DBAdapter, outcomes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    adapter.begin()
    r1 = adapter.exec("SELECT balance FROM accounts WHERE id = 1")
    r2 = adapter.exec("SELECT COUNT(*) FROM withdrawals WHERE success = 1" if adapter.db_name != "sqlite"
                       else "SELECT COUNT(*) FROM withdrawals WHERE success = 1")
    r3 = adapter.exec("SELECT COUNT(*) FROM withdrawals")
    adapter.commit()
    balance = r1.rows[0][0] if r1.rows else 0
    successful = r2.rows[0][0] if r2.rows else 0
    total_attempts = r3.rows[0][0] if r3.rows else 0
    # Include error outcome categories for coverage diversity
    error_cats = sorted(set(
        o.get("error", "OK") for o in (outcomes or []) if o.get("outcome") in ("error",)
    ))
    outcome_kinds = sorted(set(o.get("outcome", "") for o in (outcomes or []) if o))
    return {
        "balance": balance,
        "successful_withdrawals": successful,
        "total_attempts": total_attempts,
        "error_categories": error_cats,
        "outcome_kinds": outcome_kinds,
    }


def list_append_setup_sql(db: str) -> List[str]:
    if db == "mysql":
        return [
            "DROP TABLE IF EXISTS kv",
            "CREATE TABLE IF NOT EXISTS kv (id INT PRIMARY KEY, val TEXT NOT NULL)",
            "INSERT INTO kv (id, val) VALUES (1, '') "
            "ON DUPLICATE KEY UPDATE val=''",
        ]
    if db == "sqlite":
        return [
            "DROP TABLE IF EXISTS kv",
            "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
            "INSERT OR REPLACE INTO kv (id, val) VALUES (1, '')",
        ]
    return [
        "DROP TABLE IF EXISTS kv",
        "CREATE TABLE IF NOT EXISTS kv (id INT PRIMARY KEY, val TEXT NOT NULL)",
        "INSERT INTO kv (id, val) VALUES (1, '') "
        "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
    ]


def list_append_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Buggy: read val then write back val||token (read-modify-write race)."""
    token = f"t{worker_id}"
    adapter.begin("READ COMMITTED")

    # Read current value
    r = adapter.exec("SELECT val FROM kv WHERE id = 1")
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": token}

    current = r.rows[0][0] if r.rows else ""
    # Build new value with append
    if current:
        new_val = current + "," + token
    else:
        new_val = token

    # Bug: TOCTOU -- other worker may have appended between read and write
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE kv SET val = ? WHERE id = 1", (new_val,))
    else:
        r2 = adapter.exec("UPDATE kv SET val = %s WHERE id = 1", (new_val,))

    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": token}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": token}
    return {"outcome": "appended", "worker": token}


def list_append_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed: atomic append using single UPDATE with concat."""
    token = f"t{worker_id}"
    adapter.begin("READ COMMITTED")

    # Atomic append
    if adapter.db_name == "mysql":
        sql = "UPDATE kv SET val = CASE WHEN val = '' THEN %s ELSE CONCAT(val, ',', %s) END WHERE id = 1"
        r = adapter.exec(sql, (token, token))
    elif adapter.db_name == "sqlite":
        sql = "UPDATE kv SET val = CASE WHEN val = '' THEN ? ELSE val || ',' || ? END WHERE id = 1"
        r = adapter.exec(sql, (token, token))
    else:
        sql = "UPDATE kv SET val = CASE WHEN val = '' THEN %s ELSE val || ',' || %s END WHERE id = 1"
        r = adapter.exec(sql, (token, token))

    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": token}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": token}
    return {"outcome": "appended", "worker": token}


def list_append_oracle(outcomes: List[Dict[str, Any]], state: Dict[str, Any]) -> Tuple[bool, str]:
    """Bug if any appended token is missing from the final value."""
    expected_tokens = set()
    for o in outcomes:
        if o.get("outcome") == "appended":
            expected_tokens.add(o.get("worker", ""))
    actual_tokens = state.get("tokens", set())
    if isinstance(actual_tokens, list):
        actual_tokens = set(actual_tokens)
    missing = expected_tokens - actual_tokens
    if missing:
        return True, f"Lost append! Missing tokens: {sorted(missing)}"
    return False, f"All {len(expected_tokens)} tokens present"


def list_append_canonical(adapter: DBAdapter, outcomes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    adapter.begin()
    r = adapter.exec("SELECT val FROM kv WHERE id = 1")
    adapter.commit()
    val = r.rows[0][0] if r.rows else ""
    tokens = [t.strip() for t in val.split(",") if t.strip()] if val else []
    error_cats = sorted(set(
        o.get("error", "OK") for o in (outcomes or []) if o.get("outcome") == "error"
    ))
    return {"val": val, "tokens": sorted(tokens), "error_categories": error_cats}


@dataclass
class MicroWorkload:
    name: str
    setup_sql: Callable[[str], List[str]]
    run_buggy: Callable[[DBAdapter, int], Dict[str, Any]]
    run_fixed: Callable[[DBAdapter, int], Dict[str, Any]]
    oracle: Callable  # (outcomes, state) -> (bool, str)
    canonical: Callable[[DBAdapter], Dict[str, Any]]
    needs_state_for_oracle: bool = True  # whether oracle needs canonical state

    def canonical_hash(self, state: Dict[str, Any]) -> str:
        return _hash_dict(state)


MICRO_WORKLOADS = {
    "job_queue_claim": MicroWorkload(
        name="job_queue_claim",
        setup_sql=job_queue_setup_sql,
        run_buggy=job_queue_buggy,
        run_fixed=job_queue_fixed,
        oracle=job_queue_oracle,
        canonical=job_queue_canonical,
        needs_state_for_oracle=False,
    ),
    "bank_overdraft": MicroWorkload(
        name="bank_overdraft",
        setup_sql=bank_setup_sql,
        run_buggy=bank_buggy,
        run_fixed=bank_fixed,
        oracle=bank_oracle_with_state,
        canonical=bank_canonical,
        needs_state_for_oracle=True,
    ),
    "list_append": MicroWorkload(
        name="list_append",
        setup_sql=list_append_setup_sql,
        run_buggy=list_append_buggy,
        run_fixed=list_append_fixed,
        oracle=list_append_oracle,
        canonical=list_append_canonical,
        needs_state_for_oracle=True,
    ),
}
