"""
Oracle interface for pluggable isolation-level checking.

An Oracle inspects per-worker outcomes and the canonical final state
to determine whether an interleaving exposed a concurrency bug.

Concrete implementations:
  - DoubleClaimOracle   (job_queue_claim)
  - OverdraftOracle     (bank_overdraft)
  - LostAppendOracle    (list_append)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from .types import BugCategory, OracleVerdict


def _reported_vs_committed(
    noun: str, reported: int, committed: int, committed_label: str,
) -> OracleVerdict:
    """Verdict for 'workers reported N successes, database committed M'.

    When workers report more successes than the database committed, a write
    failed (e.g. SQLite BUSY, a lock timeout) and the application ignored the
    error -- a swallowed-error bug, not an isolation anomaly.  The reverse is
    a client/server outcome mismatch.
    """
    if reported > committed:
        return OracleVerdict(
            True,
            f"Phantom success: {reported} worker(s) reported {noun} but "
            f"{committed_label}={committed} (a failed write was reported as success)",
            BugCategory.SWALLOWED_ERROR,
        )
    return OracleVerdict(
        True,
        f"Unreported commit: {reported} worker(s) reported {noun} but "
        f"{committed_label}={committed}",
        BugCategory.OUTCOME_MISMATCH,
    )


class Oracle(ABC):
    """Pluggable isolation-level checker.

    Users can implement custom Oracles for their own isolation properties.
    The ``check`` method always receives both per-worker outcomes and the
    canonical final state, so implementations can use whichever they need.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name (e.g. 'double_claim', 'overdraft')."""
        ...

    @abstractmethod
    def check(
        self,
        outcomes: List[Dict[str, Any]],
        final_state: Dict[str, Any],
    ) -> OracleVerdict:
        """Evaluate whether the interleaving constitutes a bug."""
        ...


class DoubleClaimOracle(Oracle):
    """Bug if more than one worker successfully claimed the same job."""

    @property
    def name(self) -> str:
        return "double_claim"

    def check(
        self,
        outcomes: List[Dict[str, Any]],
        final_state: Dict[str, Any],
    ) -> OracleVerdict:
        claimed = [o for o in outcomes if o.get("outcome") == "claimed"]
        workers = set(o.get("worker") for o in claimed)
        if len(workers) > 1:
            return OracleVerdict(True, f"Double claim! Workers: {sorted(workers)}")
        return OracleVerdict(False, f"{len(claimed)} claim(s), {len(workers)} unique worker(s)")


class OverdraftOracle(Oracle):
    """Bug if final balance < 0 or more successful withdrawals than possible.

    ``initial_balance`` / ``amount`` must match the workload's seed data and
    withdrawal size (``bank_overdraft``: 100 and 80, so at most one
    withdrawal can succeed).
    """

    def __init__(self, initial_balance: int = 100, amount: int = 80) -> None:
        self._initial = initial_balance
        self._amount = amount

    @property
    def name(self) -> str:
        return "overdraft"

    def check(
        self,
        outcomes: List[Dict[str, Any]],
        final_state: Dict[str, Any],
    ) -> OracleVerdict:
        balance = final_state.get("balance", 0)
        successful = final_state.get("successful_withdrawals", 0)
        if balance < 0:
            return OracleVerdict(True, f"Overdraft! balance={balance}",
                                 BugCategory.ISOLATION_ANOMALY)
        max_ok = self._initial // self._amount if self._amount else successful
        if successful > max_ok:
            return OracleVerdict(
                True,
                f"Multiple overdraft-risk withdrawals succeeded: {successful}",
                BugCategory.ISOLATION_ANOMALY,
            )
        n_withdrawn = sum(1 for o in outcomes if o.get("outcome") == "withdrawn")
        if n_withdrawn != successful:
            return _reported_vs_committed(
                "withdrawn", n_withdrawn, successful, "successful_withdrawals")
        expected_balance = self._initial - self._amount * n_withdrawn
        if balance != expected_balance:
            return OracleVerdict(
                True,
                f"Conservation violated: expected balance={expected_balance} "
                f"after {n_withdrawn} withdrawal(s), observed={balance}",
                BugCategory.ISOLATION_ANOMALY,
            )
        return OracleVerdict(False, f"balance={balance}, successful={successful}")


class LostAppendOracle(Oracle):
    """Bug if any appended token is missing from the final value."""

    @property
    def name(self) -> str:
        return "lost_append"

    def check(
        self,
        outcomes: List[Dict[str, Any]],
        final_state: Dict[str, Any],
    ) -> OracleVerdict:
        expected_tokens = set()
        for o in outcomes:
            if o.get("outcome") == "appended":
                expected_tokens.add(o.get("worker", ""))
        actual_tokens = final_state.get("tokens", set())
        if isinstance(actual_tokens, list):
            actual_tokens = set(actual_tokens)
        missing = expected_tokens - actual_tokens
        if missing:
            return OracleVerdict(True, f"Lost append! Missing tokens: {sorted(missing)}")
        return OracleVerdict(False, f"All {len(expected_tokens)} tokens present")


class SmallBankOracle(Oracle):
    """Bug if total money is not conserved or any balance is negative.

    Invariants:
      1. Conservation: SUM(savings) + SUM(checking) == expected_total
      2. Non-negative savings: MIN(savings.balance) >= 0
      3. Non-negative checking: MIN(checking.balance) >= 0
    """

    def __init__(self, expected_total: int = 200000) -> None:
        self._expected_total = expected_total

    @property
    def name(self) -> str:
        return "smallbank"

    def check(
        self,
        outcomes: List[Dict[str, Any]],
        final_state: Dict[str, Any],
    ) -> OracleVerdict:
        total = final_state.get("total_balance", 0)
        min_sav = final_state.get("min_savings_balance", 0)
        min_chk = final_state.get("min_checking_balance", 0)

        if total != self._expected_total:
            return OracleVerdict(
                True,
                f"Conservation violated! expected={self._expected_total}, "
                f"actual={total}, diff={total - self._expected_total}",
            )
        if min_sav < 0:
            return OracleVerdict(True, f"Negative savings balance! min={min_sav}")
        if min_chk < 0:
            return OracleVerdict(True, f"Negative checking balance! min={min_chk}")
        return OracleVerdict(
            False,
            f"total={total} (expected={self._expected_total}), "
            f"min_sav={min_sav}, min_chk={min_chk}",
        )


class WriteSkewOracle(Oracle):
    """Bug if no doctor is on call after concurrent off-call updates (G2-item).

    Adya/Berenson "doctors-on-call" classic. Both doctors saw the other
    on-call, then both updated themselves to off-call → constraint
    violated.
    """

    def __init__(self, table: str = "doctors") -> None:
        self._table = table

    @property
    def name(self) -> str:
        return "write_skew"

    def check(self, outcomes, final_state) -> OracleVerdict:
        on_call = final_state.get("on_call_count", 0)
        if on_call <= 0:
            return OracleVerdict(
                True,
                f"Write skew (G2-item)! on_call={on_call}, expected >= 1",
            )
        return OracleVerdict(False, f"on_call={on_call}")


class PhantomOracle(Oracle):
    """Bug if a 'count then insert if zero' produced two rows.

    Range-query phantom under RR / SI: T1 reads count(*)=0 in a range
    and inserts a row with key X; concurrently T2 does the same. If
    the database fails to detect the phantom, both inserts succeed,
    producing >1 row.
    """

    @property
    def name(self) -> str:
        return "phantom"

    def check(self, outcomes, final_state) -> OracleVerdict:
        rows = final_state.get("inserts_in_range", 0)
        if rows > 1:
            return OracleVerdict(
                True,
                f"Phantom anomaly! {rows} rows inserted in 'check then insert' range",
            )
        return OracleVerdict(False, f"inserts_in_range={rows}")


class LostUpdateOracle(Oracle):
    """Bug if final counter < expected number of successful increments.

    Classic lost-update under RC: two transactions read C, write C+1,
    second write overwrites first → one increment is lost.
    """

    @property
    def name(self) -> str:
        return "lost_update"

    def check(self, outcomes, final_state) -> OracleVerdict:
        successful = sum(1 for o in outcomes if o.get("outcome") == "incremented")
        actual = final_state.get("counter", 0)
        if actual < successful:
            return OracleVerdict(
                True,
                f"Lost update! counter={actual}, expected_increments={successful}",
            )
        return OracleVerdict(
            False, f"counter={actual}, increments={successful}"
        )


class ReadSkewOracle(Oracle):
    """Bug if any reader observed an inconsistent (x, y) pair.

    A5A read skew (Adya): T1 reads x, T2 transfers x→y, T1 reads y.
    Conservation says x+y is constant; an inconsistent read sees
    pre-transfer x with post-transfer y.
    """

    def __init__(self, expected_total: int = 100) -> None:
        self._expected_total = expected_total

    @property
    def name(self) -> str:
        return "read_skew"

    def check(self, outcomes, final_state) -> OracleVerdict:
        bad_reads = [o for o in outcomes if o.get("outcome") == "skewed_read"]
        if bad_reads:
            return OracleVerdict(
                True,
                f"Read skew (A5A)! {len(bad_reads)} skewed reads observed",
            )
        return OracleVerdict(False, f"0 skewed reads of {len(outcomes)}")


class UniqueViolationOracle(Oracle):
    """Bug if unique-check then insert produced duplicates.

    TOCTOU on uniqueness: ``SELECT COUNT(*) WHERE name=?`` then INSERT.
    Two transactions both observe count=0, both insert the same name.
    Duplicates indicate the DB did not serialize the predicate read.
    """

    @property
    def name(self) -> str:
        return "unique_violation"

    def check(self, outcomes, final_state) -> OracleVerdict:
        dups = final_state.get("duplicate_names", 0)
        if dups > 0:
            return OracleVerdict(
                True,
                f"Unique constraint TOCTOU! {dups} duplicate name(s)",
            )
        n_registered = sum(1 for o in outcomes if o.get("outcome") == "registered")
        counts = final_state.get("counts", [])
        total_rows = sum(c for _n, c in counts) if counts else 0
        if n_registered > 0 and total_rows != n_registered:
            return _reported_vs_committed(
                "registered", n_registered, total_rows, "rows")
        return OracleVerdict(False, "no duplicates")


class InventoryOversellOracle(Oracle):
    """Bug if final stock < 0 OR more sales than initial stock allows."""

    def __init__(self, initial_stock: int = 5) -> None:
        self._initial_stock = initial_stock

    @property
    def name(self) -> str:
        return "inventory_oversell"

    def check(self, outcomes, final_state) -> OracleVerdict:
        stock = final_state.get("stock", 0)
        sold = final_state.get("sold_count", 0)
        if stock < 0:
            return OracleVerdict(
                True,
                f"Oversell! stock={stock} < 0",
            )
        if sold > self._initial_stock:
            return OracleVerdict(
                True,
                f"Oversell! sold={sold} > initial_stock={self._initial_stock}",
            )
        n_sold_outcomes = sum(1 for o in outcomes if o.get("outcome") == "sold")
        if sold != n_sold_outcomes:
            return _reported_vs_committed(
                "sold", n_sold_outcomes, sold, "sold_count")
        if stock + sold != self._initial_stock:
            return OracleVerdict(
                True,
                f"Conservation violated: stock={stock} + sold={sold} "
                f"!= initial={self._initial_stock}",
            )
        return OracleVerdict(
            False, f"stock={stock}, sold={sold}/{self._initial_stock}"
        )


class FKPhantomOracle(Oracle):
    """Bug if any child row references a deleted parent (orphan)."""

    @property
    def name(self) -> str:
        return "fk_phantom"

    def check(self, outcomes, final_state) -> OracleVerdict:
        orphans = final_state.get("orphan_children", 0)
        if orphans > 0:
            return OracleVerdict(
                True,
                f"FK phantom! {orphans} orphan child row(s) "
                f"referencing deleted parent",
            )
        return OracleVerdict(False, "no orphans")


class LongForkOracle(Oracle):
    """Bug if two read-only observers disagree on the order of two writes
    they both witnessed (G-SIb / long-fork anomaly under SI).

    Cerone et al., "A Framework for Transactional Consistency Models" (2015).
    """

    @property
    def name(self) -> str:
        return "long_fork"

    def check(self, outcomes, final_state) -> OracleVerdict:
        # Each observer's view is the *set* of writes it saw (a, b).  Observers
        # that run at different times legitimately see different prefixes of
        # one commit order -- e.g. {} and {a} -- and those views form a chain
        # under set inclusion.  A long fork is two *incomparable* views: one
        # observer saw a-but-not-b while another saw b-but-not-a.  (The old
        # check flagged any two distinct views, which is unsound: it reported
        # [(0, 0), (1, 0)] as a long fork on a serial history.)
        views = []
        for o in outcomes:
            order = o.get("observed_order")
            if o.get("outcome") != "observed" or not isinstance(order, (list, tuple)):
                continue
            if len(order) != 2:
                continue  # unobservable / incomplete view
            a, b = order
            views.append(frozenset(k for k, v in (("a", a), ("b", b)) if v))
        for i in range(len(views)):
            for j in range(i + 1, len(views)):
                vi, vj = views[i], views[j]
                if not (vi <= vj or vj <= vi):
                    return OracleVerdict(
                        True,
                        f"Long fork (G-SIb)! observers saw incomparable writes: "
                        f"{sorted(vi)} vs {sorted(vj)}",
                        BugCategory.ISOLATION_ANOMALY,
                    )
        shown = sorted({tuple(sorted(v)) for v in views})
        return OracleVerdict(False, f"observer views form a chain: {shown}")


class MultiKeyOracle(Oracle):
    """Bug if total balance across all accounts is not conserved."""

    def __init__(self, expected_total: int = 20000) -> None:
        self._expected_total = expected_total

    @property
    def name(self) -> str:
        return "multikey_transfer"

    def check(
        self,
        outcomes: List[Dict[str, Any]],
        final_state: Dict[str, Any],
    ) -> OracleVerdict:
        total = final_state.get("total_balance", 0)
        if total != self._expected_total:
            return OracleVerdict(
                True,
                f"Conservation violated! expected={self._expected_total}, "
                f"actual={total}, diff={total - self._expected_total}",
            )
        return OracleVerdict(
            False,
            f"total={total} (expected={self._expected_total})",
        )
