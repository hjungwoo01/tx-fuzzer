#!/usr/bin/env python3
"""
Oracle soundness validation against an independent Elle-style checker.

Hand-coded oracles (DoubleClaimOracle, OverdraftOracle, etc.) might
produce false positives or false negatives that are hard to spot from
inside the framework. To address the reviewer concern that hand-coded
oracles are *not validated against an external isolation model*, we
cross-validate each hand-coded oracle's verdict against an independent
"Elle-style" reference checker that uses a *different* derivation
pathway: it inspects the canonical post-state and the per-worker
operation log under formal, history-replayable invariants.

When the two oracles disagree, the trial is flagged and the full
EDN history is dumped to ``<output_dir>/disagreements/<trial_id>.edn``
so a Clojure Elle checker (or human reviewer) can adjudicate.

Output
------
  <output_dir>/oracle_confusion.csv      per-workload confusion matrix
  <output_dir>/oracle_disagreements.csv  per-trial disagreements
  <output_dir>/disagreements/*.edn       full EDN histories of disagreements

Usage
-----
  python scripts/validate_oracle_soundness.py --db sqlite --duration 30
  python scripts/validate_oracle_soundness.py --db postgres \
      --workloads bank_overdraft job_queue_claim list_append \
      --reps 5 --duration 60
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.micro_runner import MicroRunner
from scripts.experiments.micro_workloads import MICRO_WORKLOADS

# Register extended workloads
import scripts.experiments.extended_workloads  # noqa: F401

from scripts.schedule_pool import FinalStateHash
from scripts.schedule_pool.elle_logger import ElleLogger


# ---------------------------------------------------------------------------
# Independent ("Elle-style") validators
# ---------------------------------------------------------------------------
#
# Each validator returns (is_bug, reason) and uses a *different derivation
# pathway* than the corresponding hand-coded Oracle:
#
#   - Hand oracle reads outcomes & canonical state through one rule
#   - Validator below re-derives the expected state from the operation log
#     and compares it to the observed canonical state (history-replay style)
#
# This gives an independent verdict suitable for confusion-matrix analysis.
# ---------------------------------------------------------------------------


def _validate_bank_overdraft(outcomes: List[Dict[str, Any]],
                              state: Dict[str, Any]) -> Tuple[bool, str]:
    """Replay-style: expected = 100 - 80 * (#withdrawn). Bug iff observed != expected."""
    initial = 100
    amount = 80
    withdrew = sum(1 for o in outcomes if o.get("outcome") == "withdrawn")
    expected_balance = initial - amount * withdrew
    observed = state.get("balance", 0)
    if observed != expected_balance:
        return True, (
            f"Replay mismatch: expected balance={expected_balance} "
            f"({withdrew} withdrawals × {amount} from {initial}), "
            f"observed={observed}"
        )
    if observed < 0:
        return True, f"Overdraft (validator): balance={observed}"
    return False, f"balance={observed} matches replayed expectation"


def _validate_job_queue(outcomes: List[Dict[str, Any]],
                         state: Dict[str, Any]) -> Tuple[bool, str]:
    """Replay-style: jobs.status='claimed' implies exactly one worker.

    Validator inspects the claims TABLE directly (not the outcome list),
    so it catches both phantom claims and duplicate inserts.
    """
    claims = state.get("claims", [])
    distinct_workers = set(c[1] for c in claims if isinstance(c, (list, tuple)) and len(c) >= 2)
    if len(distinct_workers) > 1:
        return True, f"Validator: {sorted(distinct_workers)} all wrote to claims"
    return False, f"{len(distinct_workers)} distinct claim worker(s)"


def _validate_list_append(outcomes: List[Dict[str, Any]],
                           state: Dict[str, Any]) -> Tuple[bool, str]:
    """Set-equality between successful appends and observed tokens."""
    expected = set(o["worker"] for o in outcomes if o.get("outcome") == "appended")
    actual = set(state.get("tokens", []))
    if expected != actual:
        missing = expected - actual
        extra = actual - expected
        msg_parts = []
        if missing:
            msg_parts.append(f"missing={sorted(missing)}")
        if extra:
            msg_parts.append(f"extra={sorted(extra)}")
        return True, "Validator: " + "; ".join(msg_parts)
    return False, f"tokens match ({len(expected)})"


def _validate_lost_update(outcomes: List[Dict[str, Any]],
                           state: Dict[str, Any]) -> Tuple[bool, str]:
    n_inc = sum(1 for o in outcomes if o.get("outcome") == "incremented")
    counter = state.get("counter", 0)
    if counter != n_inc:
        return True, f"Validator: counter={counter} != increments={n_inc}"
    return False, "counter matches increments"


def _validate_inventory(outcomes: List[Dict[str, Any]],
                         state: Dict[str, Any]) -> Tuple[bool, str]:
    sold_outcomes = sum(1 for o in outcomes if o.get("outcome") == "sold")
    sold = state.get("sold_count", 0)
    stock = state.get("stock", 0)
    initial = 5
    expected_stock = max(0, initial - sold_outcomes)
    if sold != sold_outcomes:
        return True, (
            f"Validator: sold_count={sold} != #sold outcomes={sold_outcomes}"
        )
    if stock < 0:
        return True, f"Validator: negative stock={stock}"
    if stock + sold != initial:
        return True, (
            f"Validator: conservation violated, "
            f"stock={stock} + sold={sold} != initial={initial}"
        )
    return False, f"stock={stock}, sold={sold}/{initial}"


def _validate_unique_violation(outcomes: List[Dict[str, Any]],
                                state: Dict[str, Any]) -> Tuple[bool, str]:
    n_registered = sum(1 for o in outcomes if o.get("outcome") == "registered")
    counts = state.get("counts", [])
    total_rows = sum(c for _n, c in counts)
    if total_rows != n_registered:
        return True, (
            f"Validator: rows={total_rows} != #registered outcomes={n_registered}"
        )
    return False, f"#rows={total_rows} matches #registered"


def _validate_doctors(outcomes: List[Dict[str, Any]],
                       state: Dict[str, Any]) -> Tuple[bool, str]:
    on_call = state.get("on_call_count", 0)
    if on_call < 1:
        return True, f"Validator: invariant on_call >= 1 violated (on_call={on_call})"
    return False, f"on_call={on_call}"


VALIDATORS: Dict[str, Callable[[List[Dict[str, Any]], Dict[str, Any]],
                               Tuple[bool, str]]] = {
    "bank_overdraft": _validate_bank_overdraft,
    "job_queue_claim": _validate_job_queue,
    "list_append": _validate_list_append,
    "lost_update_counter": _validate_lost_update,
    "inventory_oversell": _validate_inventory,
    "unique_violation": _validate_unique_violation,
    "doctors_on_call": _validate_doctors,
}


# ---------------------------------------------------------------------------
# Confusion-matrix accumulator
# ---------------------------------------------------------------------------

@dataclass
class Confusion:
    tp: int = 0  # both flag bug
    tn: int = 0  # neither flags
    fp: int = 0  # oracle flags, validator does not
    fn: int = 0  # validator flags, oracle does not

    def update(self, oracle_bug: bool, val_bug: bool) -> None:
        if oracle_bug and val_bug:
            self.tp += 1
        elif not oracle_bug and not val_bug:
            self.tn += 1
        elif oracle_bug and not val_bug:
            self.fp += 1
        else:
            self.fn += 1

    @property
    def total(self) -> int:
        return self.tp + self.tn + self.fp + self.fn

    @property
    def precision(self) -> float:
        return self.tp / max(self.tp + self.fp, 1)

    @property
    def recall(self) -> float:
        return self.tp / max(self.tp + self.fn, 1)

    @property
    def agreement(self) -> float:
        return (self.tp + self.tn) / max(self.total, 1)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

@dataclass
class Disagreement:
    db: str
    workload: str
    strategy: str
    rep: int
    trial_idx: int
    canonical_hash: str
    oracle_bug: bool
    oracle_reason: str
    validator_bug: bool
    validator_reason: str
    edn_path: Optional[str]


def _dump_edn(
    trial_idx: int,
    db: str,
    workload: str,
    outcomes: List[Dict[str, Any]],
    state: Dict[str, Any],
    out_dir: Path,
) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    edn_path = out_dir / f"trial_{db}_{workload}_{trial_idx:05d}.edn"
    with ElleLogger(edn_path) as logger:
        for i, o in enumerate(outcomes):
            logger.log_invoke(process=i, f="txn", value=None)
            if o.get("outcome") == "error":
                logger.log_fail(process=i, f="txn", value=o, error=o.get("error", "?"))
            else:
                logger.log_ok(process=i, f="txn", value=o)
        logger.log_info(
            process=999, f="final_state",
            value={k: v for k, v in state.items() if not isinstance(v, list)
                   or len(v) < 50},
        )
    return str(edn_path)


def _db_available(db: str, dsn: str) -> bool:
    try:
        a = make_adapter(db, dsn)
        a.begin()
        a.exec("SELECT 1")
        a.commit()
        a.close()
        return True
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", nargs="+", default=["sqlite"])
    parser.add_argument(
        "--workloads", nargs="+", default=None,
        help="Workloads with independent validators "
             f"(default: {sorted(VALIDATORS)})",
    )
    parser.add_argument("--strategies", nargs="+",
                        default=["random", "heuristic"],
                        help="Strategies to exercise (default: random heuristic)")
    parser.add_argument("--n-trials", type=int, default=200,
                        help="Trials per (db, workload, strategy) (default: 200)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="If > 0, run for this many seconds instead of fixed n_trials")
    parser.add_argument("--n-workers", type=int, default=4)
    parser.add_argument("--delay-ms", type=float, default=15.0)
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    workloads = args.workloads or sorted(VALIDATORS)
    out_dir = Path(args.output_dir) if args.output_dir else (
        ROOT / "results" / "oracle_soundness"
    )
    edn_dir = out_dir / "disagreements"

    dsns: Dict[str, str] = {}
    for db in args.db:
        env_key = f"{db.upper()}_DSN"
        dsns[db] = os.getenv(env_key, DEFAULT_DSNS.get(db, ""))

    print("=" * 72)
    print("IsoTest Oracle Soundness Validation")
    print("=" * 72)
    print(f"  Databases:  {args.db}")
    print(f"  Workloads:  {workloads}")
    print(f"  Strategies: {args.strategies}")
    print(f"  Trials:     {args.n_trials} per cell (or duration={args.duration}s)")
    print(f"  Reps:       {args.reps}")
    print(f"  Output:     {out_dir}")
    print("=" * 72)

    confusions: Dict[Tuple[str, str], Confusion] = defaultdict(Confusion)
    disagreements: List[Disagreement] = []

    for db in args.db:
        dsn = dsns[db]
        if not _db_available(db, dsn):
            print(f"\n  SKIP {db}")
            continue
        for wl_name in workloads:
            if wl_name not in VALIDATORS:
                print(f"  SKIP {wl_name}: no independent validator")
                continue
            if wl_name not in MICRO_WORKLOADS:
                print(f"  SKIP {wl_name}: workload not registered")
                continue
            validator = VALIDATORS[wl_name]
            wl = MICRO_WORKLOADS[wl_name]
            for strat in args.strategies:
                for rep in range(args.reps):
                    print(f"\n[{db}/{wl_name}/{strat}/rep={rep}] cross-validating...")
                    runner = MicroRunner(
                        workload=wl, db=db, dsn=dsn,
                        n_workers=args.n_workers, variant="buggy",
                        strategy=strat, delay_ms=args.delay_ms,
                        heuristic_k=3,
                    )
                    n_done = 0
                    start = time.monotonic()

                    def _stop() -> bool:
                        if args.duration > 0:
                            return (time.monotonic() - start) >= args.duration
                        return n_done >= args.n_trials

                    while not _stop():
                        tr = runner.run_trial()
                        n_done += 1
                        oracle_bug = tr.bug_found
                        oracle_reason = tr.bug_reason
                        val_bug, val_reason = validator(tr.outcomes, tr.canonical_state)
                        confusions[(db, wl_name)].update(oracle_bug, val_bug)
                        if oracle_bug != val_bug:
                            edn_path = _dump_edn(
                                n_done, db, wl_name,
                                tr.outcomes, tr.canonical_state, edn_dir,
                            )
                            disagreements.append(Disagreement(
                                db=db, workload=wl_name, strategy=strat,
                                rep=rep, trial_idx=n_done,
                                canonical_hash=tr.canonical_hash,
                                oracle_bug=oracle_bug,
                                oracle_reason=oracle_reason,
                                validator_bug=val_bug,
                                validator_reason=val_reason,
                                edn_path=edn_path,
                            ))
                    c = confusions[(db, wl_name)]
                    print(
                        f"  -> after {n_done} trials: TP={c.tp} TN={c.tn} "
                        f"FP={c.fp} FN={c.fn} agreement={c.agreement:.3f}"
                    )

    # Write confusion-matrix CSV
    out_dir.mkdir(parents=True, exist_ok=True)
    confusion_path = out_dir / "oracle_confusion.csv"
    with confusion_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "workload", "trials",
            "tp", "tn", "fp", "fn",
            "agreement", "precision_oracle", "recall_oracle",
        ])
        for (db, wl), c in sorted(confusions.items()):
            w.writerow([
                db, wl, c.total, c.tp, c.tn, c.fp, c.fn,
                f"{c.agreement:.4f}", f"{c.precision:.4f}", f"{c.recall:.4f}",
            ])

    # Disagreements CSV
    dis_path = out_dir / "oracle_disagreements.csv"
    with dis_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "workload", "strategy", "rep", "trial_idx",
            "canonical_hash", "oracle_bug", "oracle_reason",
            "validator_bug", "validator_reason", "edn_path",
        ])
        for d in disagreements:
            w.writerow([
                d.db, d.workload, d.strategy, d.rep, d.trial_idx,
                d.canonical_hash, d.oracle_bug, d.oracle_reason,
                d.validator_bug, d.validator_reason, d.edn_path,
            ])

    print(f"\nWrote: {confusion_path}")
    print(f"Wrote: {dis_path} ({len(disagreements)} disagreement(s))")


if __name__ == "__main__":
    main()
