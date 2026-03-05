"""
Normalized error categories for multi-DB evaluation.

Maps vendor-specific error codes to a common taxonomy:
  SERIALIZATION_FAIL  – transaction serialization failure (retry expected)
  DEADLOCK            – deadlock detected
  LOCK_TIMEOUT        – lock wait timeout
  BUSY                – SQLite BUSY / LOCKED (not a serialization failure)
  OTHER               – anything else
"""

from __future__ import annotations

import enum
from typing import Optional, Tuple


class ErrorCategory(str, enum.Enum):
    SERIALIZATION_FAIL = "SERIALIZATION_FAIL"
    DEADLOCK = "DEADLOCK"
    LOCK_TIMEOUT = "LOCK_TIMEOUT"
    BUSY = "BUSY"
    OK = "OK"
    OTHER = "OTHER"


def classify_pg_error(exc: BaseException) -> Tuple[ErrorCategory, Optional[str], Optional[str]]:
    """Classify a PostgreSQL exception.

    Returns (category, sqlstate, vendor_msg).
    Postgres:
      40001 = serialization_failure
      40P01 = deadlock_detected
    """
    sqlstate: Optional[str] = None
    # psycopg2 style
    code = getattr(exc, "pgcode", None)
    if code:
        sqlstate = str(code)
    else:
        diag = getattr(exc, "diag", None)
        if diag is not None:
            sqlstate = getattr(diag, "sqlstate", None)
    # pgx / psycopg3 style: sqlstate in args or .sqlstate attr
    if sqlstate is None:
        sqlstate = getattr(exc, "sqlstate", None)

    vendor_msg = str(exc)

    if sqlstate == "40001":
        return ErrorCategory.SERIALIZATION_FAIL, sqlstate, vendor_msg
    if sqlstate == "40P01":
        return ErrorCategory.DEADLOCK, sqlstate, vendor_msg
    # Lock timeout (55P03)
    if sqlstate == "55P03":
        return ErrorCategory.LOCK_TIMEOUT, sqlstate, vendor_msg
    return ErrorCategory.OTHER, sqlstate, vendor_msg


def classify_mysql_error(exc: BaseException) -> Tuple[ErrorCategory, Optional[str], Optional[str]]:
    """Classify a MySQL exception.

    MySQL:
      errno 1213 / SQLSTATE 40001 = Deadlock (NOT serialization failure)
      errno 1205 = Lock wait timeout
    """
    errno: Optional[int] = None
    sqlstate: Optional[str] = None
    vendor_msg = str(exc)

    args = getattr(exc, "args", ())
    if args and len(args) >= 1:
        try:
            errno = int(args[0])
        except (ValueError, TypeError):
            pass
    if args and len(args) >= 2:
        vendor_msg = str(args[1]) if len(args) > 1 else vendor_msg

    # PyMySQL sets .args = (errno, msg)
    # mysql-connector sets .errno / .sqlstate
    if errno is None:
        errno = getattr(exc, "errno", None)
    sqlstate = getattr(exc, "sqlstate", None)

    if errno == 1213:
        # MySQL deadlock — SQLSTATE is 40001 but it's really a deadlock
        return ErrorCategory.DEADLOCK, sqlstate or "40001", vendor_msg
    if errno == 1205:
        return ErrorCategory.LOCK_TIMEOUT, sqlstate or "HY000", vendor_msg
    # MySQL serialization failure under SERIALIZABLE (errno 1062 dup / ER_LOCK_DEADLOCK mapped)
    if sqlstate == "40001" and errno != 1213:
        return ErrorCategory.SERIALIZATION_FAIL, sqlstate, vendor_msg
    return ErrorCategory.OTHER, sqlstate, vendor_msg


def classify_sqlite_error(exc: BaseException) -> Tuple[ErrorCategory, Optional[str], Optional[str]]:
    """Classify a SQLite exception.

    SQLite:
      SQLITE_BUSY  (error code 5)  – database is locked
      SQLITE_LOCKED (error code 6) – table/page is locked
    These are NOT serialization failures; they are BUSY conditions.
    """
    vendor_msg = str(exc)
    msg_lower = vendor_msg.lower()
    if "database is locked" in msg_lower or "database table is locked" in msg_lower:
        return ErrorCategory.BUSY, None, vendor_msg
    if "locked" in msg_lower:
        return ErrorCategory.BUSY, None, vendor_msg
    return ErrorCategory.OTHER, None, vendor_msg


def classify_error(db: str, exc: BaseException) -> Tuple[ErrorCategory, Optional[str], Optional[str]]:
    """Classify an exception from any supported DB.

    Args:
        db: one of 'postgres', 'mysql', 'sqlite'
        exc: the exception raised

    Returns:
        (ErrorCategory, sqlstate_or_none, vendor_msg)
    """
    db = db.lower().strip()
    if db == "postgres":
        return classify_pg_error(exc)
    if db == "mysql":
        return classify_mysql_error(exc)
    if db == "sqlite":
        return classify_sqlite_error(exc)
    return ErrorCategory.OTHER, None, str(exc)


def fisher_exact_test(a: int, b: int, c: int, d: int) -> float:
    """
    Two-sided Fisher exact test for 2x2 table:
        [[a, b],
         [c, d]]
    Returns p-value.  Uses math.comb for exact computation.
    """
    import math

    n = a + b + c + d
    row1 = a + b
    row2 = c + d
    col1 = a + c
    col2 = b + d

    # Probability of any table configuration
    def _hyper_prob(x: int) -> float:
        return (
            math.comb(col1, x)
            * math.comb(col2, row1 - x)
            / math.comb(n, row1)
        )

    p_obs = _hyper_prob(a)
    p_value = 0.0
    lo = max(0, row1 - col2)
    hi = min(row1, col1)
    for x in range(lo, hi + 1):
        p_x = _hyper_prob(x)
        if p_x <= p_obs + 1e-15:
            p_value += p_x
    return min(1.0, p_value)


def rate_ratio_ci(n1_success: int, n1_total: int, n2_success: int, n2_total: int,
                  alpha: float = 0.05) -> Tuple[float, float, float]:
    """
    Rate ratio with Katz log-interval CI.

    Returns (rate_ratio, ci_lower, ci_upper).
    rate_ratio = (n1_success/n1_total) / (n2_success/n2_total)
    """
    import math

    r1 = n1_success / max(n1_total, 1)
    r2 = n2_success / max(n2_total, 1)
    if r2 == 0:
        return float("inf"), float("nan"), float("nan")
    rr = r1 / r2
    if r1 == 0:
        return 0.0, 0.0, float("nan")

    # z for 95% CI
    z = 1.96 if abs(alpha - 0.05) < 0.001 else _norm_ppf(1 - alpha / 2)
    se_log = math.sqrt(
        (1 - r1) / (n1_success) + (1 - r2) / (n2_success)
    )
    log_rr = math.log(rr)
    ci_lo = math.exp(log_rr - z * se_log)
    ci_hi = math.exp(log_rr + z * se_log)
    return rr, ci_lo, ci_hi


def _norm_ppf(p: float) -> float:
    """Approximate inverse normal CDF using rational approximation."""
    import math
    if p <= 0:
        return float("-inf")
    if p >= 1:
        return float("inf")
    if p == 0.5:
        return 0.0
    if p < 0.5:
        return -_norm_ppf(1 - p)
    t = math.sqrt(-2 * math.log(1 - p))
    c0, c1, c2 = 2.515517, 0.802853, 0.010328
    d1, d2, d3 = 1.432788, 0.189269, 0.001308
    return t - (c0 + c1 * t + c2 * t * t) / (1 + d1 * t + d2 * t * t + d3 * t * t * t)
