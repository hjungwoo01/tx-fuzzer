"""
Cross-DB micro workloads for evaluation.

Each workload defines:
  - setup_sql(db) -> list of SQL DDL/DML to prepare schema
  - run_buggy(adapter, worker_id) -> canonical state dict
  - run_fixed(adapter, worker_id) -> canonical state dict
  - oracle: Oracle instance for isolation checking
  - canonical_hash(state: dict) -> str

All workloads use the same logical semantics across Postgres, MySQL, SQLite.
"""

from __future__ import annotations

import hashlib
import json
import random as _random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .db_adapter import DBAdapter, ExecResult
from .error_taxonomy import ErrorCategory

from scripts.schedule_pool.oracle import (
    Oracle,
    DoubleClaimOracle,
    OverdraftOracle,
    LostAppendOracle,
    SmallBankOracle,
    MultiKeyOracle,
)
from scripts.schedule_pool.types import FinalStateHash, OracleVerdict


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
    # `success` is BOOLEAN on PostgreSQL, where `success = 1` is a type error
    # ("operator does not exist: boolean = integer").  That error used to be
    # swallowed, the count defaulted to 0, and the oracle then flagged 100%
    # of PostgreSQL trials -- buggy *and* fixed -- as lost writes.
    r2 = adapter.exec("SELECT COUNT(*) FROM withdrawals WHERE success = TRUE"
                      if adapter.db_name == "postgres"
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


# ─── SmallBank workload ─────────────────────────────────────────────
#
# All 5 SmallBank transactions: Balance, DepositChecking, TransactSavings,
# Amalgamate, WriteCheck.  The MicroWorkload dispatches workers to the
# 3 TOCTOU-prone operations (Amalgamate, TransactSavings, WriteCheck).
# Balance and DepositChecking are available as standalone helpers.
#
# All dispatched operations are zero-sum transfers between savings and
# checking tables, enabling a conservation oracle.

_SB_NUM_CUSTOMERS = 10
_SB_INIT_SAVINGS = 10000
_SB_INIT_CHECKING = 10000
_SB_TOTAL = _SB_NUM_CUSTOMERS * (_SB_INIT_SAVINGS + _SB_INIT_CHECKING)
_SB_AMOUNT = 8000
_SB_HOT_CUST = 1


def smallbank_setup_sql(db: str) -> List[str]:
    stmts: List[str] = []
    if db == "mysql":
        stmts.extend([
            "DROP TABLE IF EXISTS checking",
            "DROP TABLE IF EXISTS savings",
            "CREATE TABLE IF NOT EXISTS savings (cust_id INT PRIMARY KEY, balance INT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS checking (cust_id INT PRIMARY KEY, balance INT NOT NULL)",
            "DELETE FROM savings",
            "DELETE FROM checking",
        ])
        for i in range(1, _SB_NUM_CUSTOMERS + 1):
            stmts.append(
                f"INSERT INTO savings (cust_id, balance) VALUES ({i}, {_SB_INIT_SAVINGS}) "
                f"ON DUPLICATE KEY UPDATE balance={_SB_INIT_SAVINGS}"
            )
            stmts.append(
                f"INSERT INTO checking (cust_id, balance) VALUES ({i}, {_SB_INIT_CHECKING}) "
                f"ON DUPLICATE KEY UPDATE balance={_SB_INIT_CHECKING}"
            )
    elif db == "sqlite":
        stmts.extend([
            "DROP TABLE IF EXISTS checking",
            "DROP TABLE IF EXISTS savings",
            "CREATE TABLE IF NOT EXISTS savings (cust_id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS checking (cust_id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
            "DELETE FROM savings",
            "DELETE FROM checking",
        ])
        for i in range(1, _SB_NUM_CUSTOMERS + 1):
            stmts.append(
                f"INSERT OR REPLACE INTO savings (cust_id, balance) VALUES ({i}, {_SB_INIT_SAVINGS})"
            )
            stmts.append(
                f"INSERT OR REPLACE INTO checking (cust_id, balance) VALUES ({i}, {_SB_INIT_CHECKING})"
            )
    else:  # postgres
        stmts.extend([
            "DROP TABLE IF EXISTS checking",
            "DROP TABLE IF EXISTS savings",
            "CREATE TABLE IF NOT EXISTS savings (cust_id INT PRIMARY KEY, balance INT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS checking (cust_id INT PRIMARY KEY, balance INT NOT NULL)",
            "DELETE FROM savings",
            "DELETE FROM checking",
        ])
        for i in range(1, _SB_NUM_CUSTOMERS + 1):
            stmts.append(
                f"INSERT INTO savings (cust_id, balance) VALUES ({i}, {_SB_INIT_SAVINGS}) "
                f"ON CONFLICT (cust_id) DO UPDATE SET balance = EXCLUDED.balance"
            )
            stmts.append(
                f"INSERT INTO checking (cust_id, balance) VALUES ({i}, {_SB_INIT_CHECKING}) "
                f"ON CONFLICT (cust_id) DO UPDATE SET balance = EXCLUDED.balance"
            )
    return stmts


def smallbank_balance(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Balance: read savings + checking for hot customer (read-only, no TOCTOU)."""
    wname = f"w{worker_id}"
    cust = _SB_HOT_CUST
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r1 = adapter.exec("SELECT balance FROM savings WHERE cust_id = ?", (cust,))
    else:
        r1 = adapter.exec("SELECT balance FROM savings WHERE cust_id = %s", (cust,))
    if r1.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r1.error_category.value, "worker": wname}
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("SELECT balance FROM checking WHERE cust_id = ?", (cust,))
    else:
        r2 = adapter.exec("SELECT balance FROM checking WHERE cust_id = %s", (cust,))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname}
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    sav = r1.rows[0][0] if r1.rows else 0
    chk = r2.rows[0][0] if r2.rows else 0
    return {"outcome": "balance_read", "worker": wname, "total": sav + chk}


def smallbank_deposit_checking(adapter: DBAdapter, worker_id: int, amount: int = 500) -> Dict[str, Any]:
    """DepositChecking: blind deposit to checking (no TOCTOU). Not zero-sum."""
    wname = f"w{worker_id}"
    cust = _SB_HOT_CUST
    adapter.begin("READ COMMITTED")
    if adapter.db_name == "sqlite":
        r = adapter.exec("UPDATE checking SET balance = balance + ? WHERE cust_id = ?", (amount, cust))
    else:
        r = adapter.exec("UPDATE checking SET balance = balance + %s WHERE cust_id = %s", (amount, cust))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname}
    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "deposited", "worker": wname, "amount": amount}


def _sb_amalgamate_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Buggy Amalgamate: TOCTOU on savings read — stale amount used for credit."""
    wname = f"w{worker_id}"
    src = _SB_HOT_CUST
    dst = 2 + (worker_id % (_SB_NUM_CUSTOMERS - 1))
    adapter.begin("READ COMMITTED")

    # Step 1: Read source savings
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT balance FROM savings WHERE cust_id = ?", (src,))
    else:
        r = adapter.exec("SELECT balance FROM savings WHERE cust_id = %s", (src,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname, "txn_type": "amalgamate"}
    amt = r.rows[0][0] if r.rows else 0
    if amt <= 0:
        adapter.rollback()
        return {"outcome": "empty_source", "worker": wname, "txn_type": "amalgamate"}

    # Step 2: Zero source savings — TOCTOU: amt from step 1 may be stale
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE savings SET balance = 0 WHERE cust_id = ?", (src,))
    else:
        r2 = adapter.exec("UPDATE savings SET balance = 0 WHERE cust_id = %s", (src,))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname, "txn_type": "amalgamate"}

    # Step 3: Credit destination checking with (potentially stale) amt
    if adapter.db_name == "sqlite":
        r3 = adapter.exec("UPDATE checking SET balance = balance + ? WHERE cust_id = ?", (amt, dst))
    else:
        r3 = adapter.exec("UPDATE checking SET balance = balance + %s WHERE cust_id = %s", (amt, dst))
    if r3.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r3.error_category.value, "worker": wname, "txn_type": "amalgamate"}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname, "txn_type": "amalgamate"}
    return {"outcome": "amalgamated", "worker": wname, "txn_type": "amalgamate", "amount": amt}


def _sb_amalgamate_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed Amalgamate: SERIALIZABLE prevents stale-read credit."""
    wname = f"w{worker_id}"
    src = _SB_HOT_CUST
    dst = 2 + (worker_id % (_SB_NUM_CUSTOMERS - 1))
    adapter.begin("SERIALIZABLE")

    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT balance FROM savings WHERE cust_id = ?", (src,))
    else:
        r = adapter.exec("SELECT balance FROM savings WHERE cust_id = %s", (src,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname, "txn_type": "amalgamate"}
    amt = r.rows[0][0] if r.rows else 0
    if amt <= 0:
        adapter.rollback()
        return {"outcome": "empty_source", "worker": wname, "txn_type": "amalgamate"}

    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE savings SET balance = 0 WHERE cust_id = ?", (src,))
    else:
        r2 = adapter.exec("UPDATE savings SET balance = 0 WHERE cust_id = %s", (src,))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname, "txn_type": "amalgamate"}

    if adapter.db_name == "sqlite":
        r3 = adapter.exec("UPDATE checking SET balance = balance + ? WHERE cust_id = ?", (amt, dst))
    else:
        r3 = adapter.exec("UPDATE checking SET balance = balance + %s WHERE cust_id = %s", (amt, dst))
    if r3.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r3.error_category.value, "worker": wname, "txn_type": "amalgamate"}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname, "txn_type": "amalgamate"}
    return {"outcome": "amalgamated", "worker": wname, "txn_type": "amalgamate", "amount": amt}


def _sb_transact_savings_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Buggy TransactSavings: TOCTOU on savings read before deduct+credit."""
    wname = f"w{worker_id}"
    cust = _SB_HOT_CUST
    amount = _SB_AMOUNT
    adapter.begin("READ COMMITTED")

    # Step 1: Read savings balance
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT balance FROM savings WHERE cust_id = ?", (cust,))
    else:
        r = adapter.exec("SELECT balance FROM savings WHERE cust_id = %s", (cust,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname, "txn_type": "transact_savings"}
    bal = r.rows[0][0] if r.rows else 0
    if bal < amount:
        adapter.rollback()
        return {"outcome": "insufficient", "worker": wname, "txn_type": "transact_savings", "balance": bal}

    # Step 2: Deduct from savings — TOCTOU: bal may be stale
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE savings SET balance = balance - ? WHERE cust_id = ?", (amount, cust))
    else:
        r2 = adapter.exec("UPDATE savings SET balance = balance - %s WHERE cust_id = %s", (amount, cust))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname, "txn_type": "transact_savings"}

    # Step 3: Credit to checking (same customer)
    if adapter.db_name == "sqlite":
        r3 = adapter.exec("UPDATE checking SET balance = balance + ? WHERE cust_id = ?", (amount, cust))
    else:
        r3 = adapter.exec("UPDATE checking SET balance = balance + %s WHERE cust_id = %s", (amount, cust))
    if r3.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r3.error_category.value, "worker": wname, "txn_type": "transact_savings"}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname, "txn_type": "transact_savings"}
    return {"outcome": "transacted", "worker": wname, "txn_type": "transact_savings", "amount": amount}


def _sb_transact_savings_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed TransactSavings: atomic WHERE guard prevents overdraft."""
    wname = f"w{worker_id}"
    cust = _SB_HOT_CUST
    amount = _SB_AMOUNT
    adapter.begin("READ COMMITTED")

    # Atomic deduct: only succeeds if balance is sufficient
    if adapter.db_name == "sqlite":
        r = adapter.exec(
            "UPDATE savings SET balance = balance - ? WHERE cust_id = ? AND balance >= ?",
            (amount, cust, amount),
        )
    else:
        r = adapter.exec(
            "UPDATE savings SET balance = balance - %s WHERE cust_id = %s AND balance >= %s",
            (amount, cust, amount),
        )
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname, "txn_type": "transact_savings"}
    if r.rowcount == 0:
        adapter.rollback()
        return {"outcome": "insufficient", "worker": wname, "txn_type": "transact_savings"}

    # Credit checking
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE checking SET balance = balance + ? WHERE cust_id = ?", (amount, cust))
    else:
        r2 = adapter.exec("UPDATE checking SET balance = balance + %s WHERE cust_id = %s", (amount, cust))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname, "txn_type": "transact_savings"}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname, "txn_type": "transact_savings"}
    return {"outcome": "transacted", "worker": wname, "txn_type": "transact_savings", "amount": amount}


def _sb_write_check_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Buggy WriteCheck: TOCTOU on checking read before debit+credit."""
    wname = f"w{worker_id}"
    cust = _SB_HOT_CUST
    amount = _SB_AMOUNT
    adapter.begin("READ COMMITTED")

    # Step 1: Read checking balance
    if adapter.db_name == "sqlite":
        r = adapter.exec("SELECT balance FROM checking WHERE cust_id = ?", (cust,))
    else:
        r = adapter.exec("SELECT balance FROM checking WHERE cust_id = %s", (cust,))
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname, "txn_type": "write_check"}
    bal = r.rows[0][0] if r.rows else 0
    if bal < amount:
        adapter.rollback()
        return {"outcome": "insufficient", "worker": wname, "txn_type": "write_check", "balance": bal}

    # Step 2: Debit checking — TOCTOU: bal may be stale
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE checking SET balance = balance - ? WHERE cust_id = ?", (amount, cust))
    else:
        r2 = adapter.exec("UPDATE checking SET balance = balance - %s WHERE cust_id = %s", (amount, cust))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname, "txn_type": "write_check"}

    # Step 3: Credit savings (zero-sum transfer)
    if adapter.db_name == "sqlite":
        r3 = adapter.exec("UPDATE savings SET balance = balance + ? WHERE cust_id = ?", (amount, cust))
    else:
        r3 = adapter.exec("UPDATE savings SET balance = balance + %s WHERE cust_id = %s", (amount, cust))
    if r3.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r3.error_category.value, "worker": wname, "txn_type": "write_check"}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname, "txn_type": "write_check"}
    return {"outcome": "check_written", "worker": wname, "txn_type": "write_check", "amount": amount}


def _sb_write_check_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed WriteCheck: atomic WHERE guard prevents overdraft."""
    wname = f"w{worker_id}"
    cust = _SB_HOT_CUST
    amount = _SB_AMOUNT
    adapter.begin("READ COMMITTED")

    # Atomic debit: only succeeds if balance is sufficient
    if adapter.db_name == "sqlite":
        r = adapter.exec(
            "UPDATE checking SET balance = balance - ? WHERE cust_id = ? AND balance >= ?",
            (amount, cust, amount),
        )
    else:
        r = adapter.exec(
            "UPDATE checking SET balance = balance - %s WHERE cust_id = %s AND balance >= %s",
            (amount, cust, amount),
        )
    if r.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": wname, "txn_type": "write_check"}
    if r.rowcount == 0:
        adapter.rollback()
        return {"outcome": "insufficient", "worker": wname, "txn_type": "write_check"}

    # Credit savings
    if adapter.db_name == "sqlite":
        r2 = adapter.exec("UPDATE savings SET balance = balance + ? WHERE cust_id = ?", (amount, cust))
    else:
        r2 = adapter.exec("UPDATE savings SET balance = balance + %s WHERE cust_id = %s", (amount, cust))
    if r2.error_category != ErrorCategory.OK:
        adapter.rollback()
        return {"outcome": "error", "error": r2.error_category.value, "worker": wname, "txn_type": "write_check"}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname, "txn_type": "write_check"}
    return {"outcome": "check_written", "worker": wname, "txn_type": "write_check", "amount": amount}


_SB_BUGGY_OPS = [_sb_amalgamate_buggy, _sb_transact_savings_buggy, _sb_write_check_buggy]
_SB_FIXED_OPS = [_sb_amalgamate_fixed, _sb_transact_savings_fixed, _sb_write_check_fixed]


def smallbank_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Dispatch worker to one of 3 TOCTOU-prone SmallBank operations."""
    return _SB_BUGGY_OPS[worker_id % 3](adapter, worker_id)


def smallbank_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Dispatch worker to fixed version of a SmallBank operation."""
    return _SB_FIXED_OPS[worker_id % 3](adapter, worker_id)


def smallbank_canonical(adapter: DBAdapter, outcomes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Read full SmallBank state for oracle checking and hash computation."""
    adapter.begin()
    r1 = adapter.exec("SELECT SUM(balance) FROM savings")
    r2 = adapter.exec("SELECT SUM(balance) FROM checking")
    r3 = adapter.exec("SELECT MIN(balance) FROM savings")
    r4 = adapter.exec("SELECT MIN(balance) FROM checking")
    r5 = adapter.exec("SELECT cust_id, balance FROM savings ORDER BY cust_id")
    r6 = adapter.exec("SELECT cust_id, balance FROM checking ORDER BY cust_id")
    adapter.commit()

    total_sav = r1.rows[0][0] if r1.rows and r1.rows[0][0] is not None else 0
    total_chk = r2.rows[0][0] if r2.rows and r2.rows[0][0] is not None else 0
    min_sav = r3.rows[0][0] if r3.rows and r3.rows[0][0] is not None else 0
    min_chk = r4.rows[0][0] if r4.rows and r4.rows[0][0] is not None else 0
    sav_balances = [(row[0], row[1]) for row in r5.rows] if r5.rows else []
    chk_balances = [(row[0], row[1]) for row in r6.rows] if r6.rows else []

    error_cats = sorted(set(
        o.get("error", "OK") for o in (outcomes or []) if o.get("outcome") == "error"
    ))
    outcome_kinds = sorted(set(o.get("outcome", "") for o in (outcomes or []) if o))

    return {
        "total_balance": total_sav + total_chk,
        "total_savings": total_sav,
        "total_checking": total_chk,
        "min_savings_balance": min_sav,
        "min_checking_balance": min_chk,
        "savings_balances": sav_balances,
        "checking_balances": chk_balances,
        "error_categories": error_cats,
        "outcome_kinds": outcome_kinds,
    }


# ─── Multi-Key Transfer workload (Jepsen-style) ────────────────────
#
# Each worker picks exactly 5 distinct accounts from a 20-account pool,
# debits the first 2 (30 each) and credits the remaining 3 (20 each).
# Zero-sum: total debit (60) == total credit (60).

_MK_NUM_ACCOUNTS = 20
_MK_INIT_BALANCE = 1000
_MK_TOTAL = _MK_NUM_ACCOUNTS * _MK_INIT_BALANCE
_MK_KEYS_PER_TXN = 5
_MK_DEBIT_AMOUNT = 30
_MK_CREDIT_AMOUNT = 20


def multikey_transfer_setup_sql(db: str) -> List[str]:
    stmts: List[str] = []
    if db == "mysql":
        stmts.extend([
            "DROP TABLE IF EXISTS accounts",
            "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
            "DELETE FROM accounts",
        ])
        for i in range(1, _MK_NUM_ACCOUNTS + 1):
            stmts.append(
                f"INSERT INTO accounts (id, balance) VALUES ({i}, {_MK_INIT_BALANCE}) "
                f"ON DUPLICATE KEY UPDATE balance={_MK_INIT_BALANCE}"
            )
    elif db == "sqlite":
        stmts.extend([
            "DROP TABLE IF EXISTS accounts",
            "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
            "DELETE FROM accounts",
        ])
        for i in range(1, _MK_NUM_ACCOUNTS + 1):
            stmts.append(
                f"INSERT OR REPLACE INTO accounts (id, balance) VALUES ({i}, {_MK_INIT_BALANCE})"
            )
    else:  # postgres
        stmts.extend([
            "DROP TABLE IF EXISTS accounts",
            "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
            "DELETE FROM accounts",
        ])
        for i in range(1, _MK_NUM_ACCOUNTS + 1):
            stmts.append(
                f"INSERT INTO accounts (id, balance) VALUES ({i}, {_MK_INIT_BALANCE}) "
                f"ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance"
            )
    return stmts


def multikey_transfer_buggy(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Buggy multi-key transfer: read all balances, compute new values, write absolute.

    The TOCTOU: balances read in the read-phase may be stale by the write-phase.
    Absolute writes overwrite concurrent modifications, violating conservation.
    """
    wname = f"w{worker_id}"
    rng = _random.Random(worker_id)
    keys = sorted(rng.sample(range(1, _MK_NUM_ACCOUNTS + 1), _MK_KEYS_PER_TXN))
    sources = keys[:2]
    destinations = keys[2:]

    adapter.begin("READ COMMITTED")

    # Read phase: read all 5 balances
    balances: Dict[int, int] = {}
    for k in keys:
        if adapter.db_name == "sqlite":
            r = adapter.exec("SELECT balance FROM accounts WHERE id = ?", (k,))
        else:
            r = adapter.exec("SELECT balance FROM accounts WHERE id = %s", (k,))
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": wname}
        balances[k] = r.rows[0][0] if r.rows else 0

    # Compute new balances in app memory
    for k in sources:
        balances[k] -= _MK_DEBIT_AMOUNT
    for k in destinations:
        balances[k] += _MK_CREDIT_AMOUNT

    # Write phase: absolute writes (stale values cause lost updates)
    for k in keys:
        if adapter.db_name == "sqlite":
            r = adapter.exec("UPDATE accounts SET balance = ? WHERE id = ?", (balances[k], k))
        else:
            r = adapter.exec("UPDATE accounts SET balance = %s WHERE id = %s", (balances[k], k))
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "transferred", "worker": wname, "keys": keys}


def multikey_transfer_fixed(adapter: DBAdapter, worker_id: int) -> Dict[str, Any]:
    """Fixed multi-key transfer: relative updates, no stale reads possible."""
    wname = f"w{worker_id}"
    rng = _random.Random(worker_id)
    keys = sorted(rng.sample(range(1, _MK_NUM_ACCOUNTS + 1), _MK_KEYS_PER_TXN))
    sources = keys[:2]
    destinations = keys[2:]

    adapter.begin("READ COMMITTED")

    # Relative debits
    for k in sources:
        if adapter.db_name == "sqlite":
            r = adapter.exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", (_MK_DEBIT_AMOUNT, k))
        else:
            r = adapter.exec("UPDATE accounts SET balance = balance - %s WHERE id = %s", (_MK_DEBIT_AMOUNT, k))
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    # Relative credits
    for k in destinations:
        if adapter.db_name == "sqlite":
            r = adapter.exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", (_MK_CREDIT_AMOUNT, k))
        else:
            r = adapter.exec("UPDATE accounts SET balance = balance + %s WHERE id = %s", (_MK_CREDIT_AMOUNT, k))
        if r.error_category != ErrorCategory.OK:
            adapter.rollback()
            return {"outcome": "error", "error": r.error_category.value, "worker": wname}

    cr = adapter.commit()
    if cr.error_category != ErrorCategory.OK:
        return {"outcome": "error", "error": cr.error_category.value, "worker": wname}
    return {"outcome": "transferred", "worker": wname, "keys": keys}


def multikey_transfer_canonical(adapter: DBAdapter, outcomes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Read full accounts state for oracle checking and hash computation."""
    adapter.begin()
    r1 = adapter.exec("SELECT SUM(balance) FROM accounts")
    r2 = adapter.exec("SELECT MIN(balance) FROM accounts")
    r3 = adapter.exec("SELECT id, balance FROM accounts ORDER BY id")
    adapter.commit()

    total = r1.rows[0][0] if r1.rows and r1.rows[0][0] is not None else 0
    min_bal = r2.rows[0][0] if r2.rows and r2.rows[0][0] is not None else 0
    balances = [(row[0], row[1]) for row in r3.rows] if r3.rows else []

    error_cats = sorted(set(
        o.get("error", "OK") for o in (outcomes or []) if o.get("outcome") == "error"
    ))
    outcome_kinds = sorted(set(o.get("outcome", "") for o in (outcomes or []) if o))

    return {
        "total_balance": total,
        "min_balance": min_bal,
        "balances": balances,
        "error_categories": error_cats,
        "outcome_kinds": outcome_kinds,
    }


@dataclass
class MicroWorkload:
    name: str
    setup_sql: Callable[[str], List[str]]
    run_buggy: Callable[[DBAdapter, int], Dict[str, Any]]
    run_fixed: Callable[[DBAdapter, int], Dict[str, Any]]
    oracle_instance: Oracle                          # pluggable Oracle implementation
    canonical: Callable[[DBAdapter], Dict[str, Any]]
    #: Optional setup used only by the "fixed" variant (e.g. a fix that is a
    #: schema constraint, such as a UNIQUE index).  Defaults to setup_sql.
    setup_sql_fixed: Optional[Callable[[str], List[str]]] = None
    #: Additional named variants, e.g. {"naive_fix": fn} for a plausible fix
    #: that does NOT hold under every interleaving -- useful for teaching.
    extra_variants: Dict[str, Callable[[DBAdapter, int], Dict[str, Any]]] = field(
        default_factory=dict)

    @property
    def variants(self) -> List[str]:
        return ["buggy", "fixed", *sorted(self.extra_variants)]

    def setup_for(self, db: str, variant: str = "buggy") -> List[str]:
        """Setup statements for ``variant`` on ``db``."""
        if variant == "fixed" and self.setup_sql_fixed is not None:
            return self.setup_sql_fixed(db)
        return self.setup_sql(db)

    def variant_fn(self, variant: str) -> Callable[[DBAdapter, int], Dict[str, Any]]:
        """The transaction function for a named variant."""
        if variant == "buggy":
            return self.run_buggy
        if variant == "fixed":
            return self.run_fixed
        if variant in self.extra_variants:
            return self.extra_variants[variant]
        raise KeyError(
            f"workload {self.name!r} has no variant {variant!r} "
            f"(available: {', '.join(self.variants)})")

    def verdict(self, outcomes: List[Dict[str, Any]],
                state: Optional[Dict[str, Any]] = None) -> OracleVerdict:
        """Full, categorized oracle verdict."""
        return self.oracle_instance.check(outcomes, state or {})

    def oracle(self, outcomes: List[Dict[str, Any]], state: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Unified oracle interface — delegates to the Oracle instance.

        Accepts both the legacy (outcomes,) and (outcomes, state) signatures.
        """
        verdict = self.verdict(outcomes, state)
        return verdict.bug_found, verdict.reason

    def canonical_hash(self, state: Dict[str, Any]) -> str:
        return FinalStateHash.compute(state).value

    # Retained for backward compatibility with callers that check this flag.
    @property
    def needs_state_for_oracle(self) -> bool:
        return not isinstance(self.oracle_instance, DoubleClaimOracle)


MICRO_WORKLOADS = {
    "job_queue_claim": MicroWorkload(
        name="job_queue_claim",
        setup_sql=job_queue_setup_sql,
        run_buggy=job_queue_buggy,
        run_fixed=job_queue_fixed,
        oracle_instance=DoubleClaimOracle(),
        canonical=job_queue_canonical,
    ),
    "bank_overdraft": MicroWorkload(
        name="bank_overdraft",
        setup_sql=bank_setup_sql,
        run_buggy=bank_buggy,
        run_fixed=bank_fixed,
        oracle_instance=OverdraftOracle(),
        canonical=bank_canonical,
    ),
    "list_append": MicroWorkload(
        name="list_append",
        setup_sql=list_append_setup_sql,
        run_buggy=list_append_buggy,
        run_fixed=list_append_fixed,
        oracle_instance=LostAppendOracle(),
        canonical=list_append_canonical,
    ),
    "smallbank": MicroWorkload(
        name="smallbank",
        setup_sql=smallbank_setup_sql,
        run_buggy=smallbank_buggy,
        run_fixed=smallbank_fixed,
        oracle_instance=SmallBankOracle(expected_total=_SB_TOTAL),
        canonical=smallbank_canonical,
    ),
    "multikey_transfer": MicroWorkload(
        name="multikey_transfer",
        setup_sql=multikey_transfer_setup_sql,
        run_buggy=multikey_transfer_buggy,
        run_fixed=multikey_transfer_fixed,
        oracle_instance=MultiKeyOracle(expected_total=_MK_TOTAL),
        canonical=multikey_transfer_canonical,
    ),
}
