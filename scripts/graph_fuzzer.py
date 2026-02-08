from __future__ import annotations

"""
Graph-based workload fuzzer using `LitmusTester`.

This module lets you:
  - Generate small transaction-dependency graphs (cycles, chains, dense graphs).
  - Compile those graphs into simple multi-transaction workloads over the `kv` table.
  - Run a fuzz loop that measures how often PostgreSQL raises `SerializationFailure`
    under a given isolation level.

The focus here is on exploring different dependency topologies and
seeing which shapes trigger more aborts or anomalies under load.
"""

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.litmus_utils import LitmusTester


Edge = Tuple[int, int]
WorkloadFn = Callable[[str, str, int], Tuple[int, bool]]


class GraphGenerator:
    """Helpers for generating small directed graphs used as dependency topologies."""

    @staticmethod
    def generate_cycle(nodes: int = 3) -> List[Edge]:
        """
        Generate a simple directed cycle: 0 -> 1 -> 2 -> ... -> (n-1) -> 0.
        """
        if nodes < 2:
            raise ValueError("Cycle requires at least 2 nodes")
        return [(i, (i + 1) % nodes) for i in range(nodes)]

    @staticmethod
    def generate_dense(nodes: int = 5, density: float = 0.8) -> List[Edge]:
        """
        Generate a random, highly connected directed graph (no self-loops).

        `density` is the probability of including each possible directed edge i -> j.
        """
        if not (0.0 <= density <= 1.0):
            raise ValueError("density must be between 0.0 and 1.0")
        edges: List[Edge] = []
        for i in range(nodes):
            for j in range(nodes):
                if i == j:
                    continue
                if random.random() <= density:
                    edges.append((i, j))
        return edges

    @staticmethod
    def generate_long_chain(length: int = 10) -> List[Edge]:
        """
        Generate a long linear chain: 0 -> 1 -> 2 -> ... -> (length-1).
        """
        if length < 2:
            raise ValueError("Chain requires length >= 2")
        return [(i, i + 1) for i in range(length - 1)]


def _assign_nodes_to_conns(nodes: Iterable[int]) -> Dict[int, int]:
    """
    Deterministically map abstract nodes to one of two `LitmusTester` connections.

    Returns a dict: node_id -> 1 or 2 (meaning conn1/cur1 vs conn2/cur2).
    """
    mapping: Dict[int, int] = {}
    for idx, n in enumerate(sorted(set(nodes))):
        mapping[n] = 1 if idx % 2 == 0 else 2
    return mapping


def compile_workload(dependencies: Sequence[Edge]) -> WorkloadFn:
    """
    Compile an edge list into a workload function that drives `LitmusTester`.

    The compiled workload simulates a simple rule:
      - Each abstract node corresponds to a logical transaction.
      - For an edge (A, B), we ensure that A performs a write before B performs
        a read on a shared key, possibly followed by its own write.

    The returned callable has the signature:
        workload(dsn: str, isolation: str, target_key: int)
            -> (serialization_failures: int, anomaly_detected: bool)
    """

    all_nodes = {a for (a, b) in dependencies} | {b for (a, b) in dependencies}
    node_to_conn = _assign_nodes_to_conns(all_nodes)

    def workload(dsn: str, isolation: str, target_key: int) -> Tuple[int, bool]:
        tester = LitmusTester(dsn, isolation_level=isolation)
        serialization_failures = 0
        anomaly_detected = False

        try:
            tester.setup_db()

            begun: Dict[int, bool] = {n: False for n in all_nodes}

            def _get_cursor(node: int):
                conn_id = node_to_conn[node]
                if conn_id == 1:
                    return tester.conn1, tester.cur1
                return tester.conn2, tester.cur2

            def _begin_if_needed(node: int) -> None:
                if begun[node]:
                    return
                conn, cur = _get_cursor(node)
                cur.execute("BEGIN")
                begun[node] = True

            for a, b in dependencies:
                _begin_if_needed(a)
                _begin_if_needed(b)

                conn_a, cur_a = _get_cursor(a)
                try:
                    cur_a.execute("UPDATE kv SET v = v + 1 WHERE k = %s", (target_key,))
                except psycopg2.errors.SerializationFailure:
                    serialization_failures += 1
                    conn_a.rollback()
                    begun[a] = False 
                
                conn_b, cur_b = _get_cursor(b)
                try:
                    cur_b.execute("SELECT v FROM kv WHERE k = %s", (target_key,))
                    _ = cur_b.fetchone()
                    cur_b.execute("UPDATE kv SET v = v + 1 WHERE k = %s", (target_key,))
                except psycopg2.errors.SerializationFailure:
                    serialization_failures += 1
                    conn_b.rollback()
                    begun[b] = False

            for n in all_nodes:
                if not begun[n]:
                    continue
                conn, _cur = _get_cursor(n)
                try:
                    conn.commit()
                except psycopg2.errors.SerializationFailure:
                    serialization_failures += 1
                    conn.rollback()

            return (serialization_failures, anomaly_detected)

        finally:
            tester.teardown()

    return workload


@dataclass
class FuzzResult:
    attempts: int = 0
    total_serialization_failures: int = 0
    interesting_attempts: int = 0


class FuzzLoop:
    """
    Run compiled graph workloads repeatedly and collect basic feedback:
      - serialization failure count
      - which attempts were "interesting"
    """

    def __init__(
        self,
        dsn: str,
        isolation: str,
        graph_type: str,
        nodes: int,
        density: float = 0.8,
        attempts: int = 50,
        target_key: int = 1,
        seeds_dir: Path | None = None,
    ) -> None:
        self.dsn = dsn
        self.isolation = isolation
        self.graph_type = graph_type
        self.nodes = nodes
        self.density = density
        self.attempts = attempts
        self.target_key = target_key
        self.seeds_dir = seeds_dir or Path("seeds")
        self.seeds_dir.mkdir(parents=True, exist_ok=True)

    def _generate_dependencies(self) -> List[Edge]:
        gg = GraphGenerator()
        gt = self.graph_type.lower()
        if gt == "cycle":
            return gg.generate_cycle(self.nodes)
        if gt == "dense":
            return gg.generate_dense(self.nodes, self.density)
        if gt in ("chain", "long_chain", "long-chain"):
            return gg.generate_long_chain(self.nodes)
        raise ValueError(f"Unsupported graph type: {self.graph_type!r}")

    def run(self) -> FuzzResult:
        deps = self._generate_dependencies()
        workload = compile_workload(deps)
        result = FuzzResult()

        for i in range(1, self.attempts + 1):
            ser_failures, anomaly = workload(self.dsn, self.isolation, self.target_key)

            result.attempts += 1
            result.total_serialization_failures += ser_failures

            interesting = ser_failures > 0 or anomaly
            if interesting:
                result.interesting_attempts += 1
                self._save_seed(i, deps, ser_failures, anomaly)

            print(
                f"[FuzzLoop] Attempt {i}/{self.attempts} - "
                f"serialization_failures={ser_failures}, anomaly={anomaly}",
                file=sys.stderr,
            )

        return result

    def _save_seed(
        self,
        attempt_idx: int,
        dependencies: Sequence[Edge],
        ser_failures: int,
        anomaly: bool,
    ) -> None:
        ts = int(time.time() * 1000)
        fname = (
            f"{self.graph_type}_nodes{self.nodes}_attempt{attempt_idx}_"
            f"{'critical' if anomaly else 'interesting'}_{ts}.json"
        )
        path = self.seeds_dir / fname
        payload = {
            "graph_type": self.graph_type,
            "nodes": self.nodes,
            "density": self.density,
            "dependencies": list(dependencies),
            "isolation": self.isolation,
            "target_key": self.target_key,
            "attempt": attempt_idx,
            "serialization_failures": ser_failures,
            "anomaly": anomaly,
            "timestamp_ms": ts,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Graph-based workload fuzzer using LitmusTester.",
    )
    parser.add_argument(
        "--dsn",
        default="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
        help=(
            "PostgreSQL DSN. "
            "Defaults to postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
        ),
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Transaction isolation level (READ COMMITTED, REPEATABLE READ, SERIALIZABLE).",
    )
    parser.add_argument(
        "--type",
        required=True,
        choices=["cycle", "dense", "chain"],
        help="Type of graph to generate (cycle, dense, chain).",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=3,
        help="Number of nodes (or chain length) in the graph.",
    )
    parser.add_argument(
        "--density",
        type=float,
        default=0.8,
        help="Edge density for dense graphs (ignored for cycle/chain).",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=50,
        help="Number of fuzz attempts to run.",
    )
    parser.add_argument(
        "--target-key",
        type=int,
        default=1,
        help="Shared key in `kv` that the workload will exercise.",
    )
    parser.add_argument(
        "--seeds-dir",
        type=str,
        default="seeds",
        help="Directory to store interesting workload seeds (default: seeds/).",
    )

    args = parser.parse_args()

    fuzz = FuzzLoop(
        dsn=args.dsn,
        isolation=args.isolation,
        graph_type=args.type,
        nodes=args.nodes,
        density=args.density,
        attempts=args.attempts,
        target_key=args.target_key,
        seeds_dir=Path(args.seeds_dir),
    )

    print(
        f"Starting graph fuzzing: type={args.type}, nodes={args.nodes}, "
        f"isolation={args.isolation}, attempts={args.attempts}, "
        f"target_key={args.target_key}",
        file=sys.stderr,
    )

    res = fuzz.run()

    if res.attempts > 0:
        ser_rate = res.total_serialization_failures / res.attempts
    else:
        ser_rate = 0.0

    print("\n=== Graph Fuzzing Summary ===")
    print(f"Graph type                 : {args.type}")
    print(f"Nodes                      : {args.nodes}")
    print(f"Isolation                  : {args.isolation}")
    print(f"Attempts                   : {res.attempts}")
    print(f"Total SerializationFailures: {res.total_serialization_failures}")
    print(f"Avg failures per attempt   : {ser_rate:.2f}")
    print(f"Interesting attempts saved : {res.interesting_attempts}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

