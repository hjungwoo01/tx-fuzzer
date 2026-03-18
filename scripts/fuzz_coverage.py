"""
Coverage-guided fuzzer for database litmus tests.

Explores the state space by retaining and mutating schedules that produce
unique output states (e.g. append-only list serialization orders).

Example:
  python scripts/fuzz_coverage.py --target mysql --test lost_update
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from adapters import DatabaseAdapter
from fuzz_utils import (
    DEFAULT_SCHEDULE_LEN,
    Mutator,
    Schedule,
    StateHasher,
)
from litmus_utils import LitmusTester

# Import adapter factory and defaults from run_litmus
from run_litmus import DEFAULT_DSN, get_adapter


def _rows_to_state_dict(raw: Union[List[Any], Dict[str, List[str]]]) -> Dict[str, List[str]]:
    """
    Convert run_test result to dict state for StateHasher.
    If already a dict (append-only list format), return as-is.
    If list of rows (k, v), convert to {str(k): [str(v)]}.
    """
    if isinstance(raw, dict):
        return raw
    # Rows e.g. [(1, 10), (2, 0)] -> {'1': ['10'], '2': ['0']}
    return {str(k): [str(v)] for k, v in (raw or [])}


class CoverageFuzzer:
    """
    Coverage-guided fuzzer: queue of interesting Schedule objects, seen_hashes
    of StateHasher outputs. Pop → Mutate → Inject → Observe → Hash → Retain or Discard.
    """

    def __init__(
        self,
        target: str,
        test_name: str,
        adapter: DatabaseAdapter,
        schedule_len: int = DEFAULT_SCHEDULE_LEN,
        seed: Optional[int] = None,
    ) -> None:
        self.target = target
        self.test_name = test_name
        self._adapter = adapter
        self.schedule_len = schedule_len
        self._rng = random.Random(seed)
        self._mutator = Mutator(
            min_delay=0.0,
            max_delay=0.2,
            schedule_len=schedule_len,
            seed=seed,
        )
        self._hasher = StateHasher()
        # Queue of "interesting" Schedule objects (we pop and mutate from these)
        self.queue: List[Schedule] = []
        # State hashes we have already discovered
        self.seen_hashes: set[str] = set()
        self._tester: Optional[LitmusTester] = None

    def _tester_or_create(self) -> LitmusTester:
        if self._tester is None:
            self._tester = LitmusTester(adapter=self._adapter)
        return self._tester

    def run_loop(self, steps: int = 100) -> None:
        """
        Step 1: Bootstrap — generate 10 random schedules and push to queue.
        Step 2: Loop — Pop parent, Mutate to child, Inject (run test with schedule),
        Observe (fetch final DB state), Hash, Decide: retain (add to queue + log) or discard.
        """
        # Step 1: Bootstrap (random workload)
        print(f"Coverage fuzzer: target={self.target} test={self.test_name} steps={steps}")
        print("Bootstrap: generating 10 random Schedule objects and pushing to queue...")
        for _ in range(10):
            s = Mutator.random_schedule(
                length=self.schedule_len,
                min_delay=0.0,
                max_delay=0.1,
                seed=self._rng.randint(0, 2**31 - 1),
            )
            self.queue.append(s)

        print(f"Starting loop (queue size={len(self.queue)})...")
        for step in range(steps):
            # Pop: take a parent_schedule from the queue
            if not self.queue:
                parent = Mutator.random_schedule(
                    length=self.schedule_len,
                    min_delay=0.0,
                    max_delay=0.1,
                    seed=self._rng.randint(0, 2**31 - 1),
                )
            else:
                parent = self.queue.pop(0)

            # Mutate: create child_schedule
            child_schedule = self._mutator.mutate(parent)

            # Inject: run the test logic using child_schedule (time.sleep before SQL)
            tester = self._tester_or_create()
            try:
                raw_state = tester.run_test(
                    self.test_name,
                    schedule=child_schedule.delays,
                )
            except Exception:
                continue

            # Observe: fetch the final database state (append-only list format)
            db_state = _rows_to_state_dict(raw_state)

            # Hash
            h = self._hasher.hash(db_state)

            # Decide
            if h not in self.seen_hashes:
                # RETAIN: add child_schedule to the queue
                self.queue.append(child_schedule)
                # Record: add h to seen_hashes
                self.seen_hashes.add(h)
                # Log
                print(f"🚀 NEW STATE FOUND! (Hash: {h}). Queue Size: {len(self.queue)}")
            # else: DISCARD

        print(f"\nDone. Unique states discovered: {len(self.seen_hashes)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Coverage-guided fuzzing for database litmus tests.",
    )
    parser.add_argument(
        "--target",
        default="mysql",
        choices=["postgres", "mysql", "redis"],
        help="Database backend to test",
    )
    parser.add_argument(
        "--test",
        default="lost_update",
        choices=["lost_update"],
        help="Litmus test to fuzz (only lost_update has schedule injection for now)",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Connection DSN (defaults from run_litmus).",
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Isolation level for SQL backends",
    )
    parser.add_argument(
        "--steps",
        type=int,
        default=100,
        metavar="N",
        help="Number of fuzzing steps (default 100)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    args = parser.parse_args()
    args.isolation = (args.isolation or "").strip().upper().replace("_", " ")
    if args.isolation == "SERIALIZATION":
        args.isolation = "SERIALIZABLE"

    dsn = args.dsn or DEFAULT_DSN.get(args.target)
    if not dsn:
        print(f"No default DSN for target {args.target!r}. Pass --dsn.", file=sys.stderr)
        return 2

    adapter = get_adapter(args.target, dsn, args.isolation, logger=None)
    try:
        adapter.connect()
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return 2

    try:
        fuzzer = CoverageFuzzer(
            target=args.target,
            test_name=args.test,
            adapter=adapter,
            seed=args.seed,
        )
        fuzzer.run_loop(steps=args.steps)
        return 0
    finally:
        adapter.teardown()


if __name__ == "__main__":
    sys.exit(main())
