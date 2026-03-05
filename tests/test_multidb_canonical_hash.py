from __future__ import annotations

from pathlib import Path

from scripts.experiments.multidb_workloads import (
    MICRO_WORKLOADS,
    hash_canonical_state,
    run_micro_trial,
)


def test_hash_canonical_state_stable() -> None:
    state = {"z": [3, 2, 1], "a": {"k": "v"}, "n": 9}
    h1 = hash_canonical_state(state)
    h2 = hash_canonical_state({"n": 9, "a": {"k": "v"}, "z": [3, 2, 1]})
    assert h1 == h2
    assert len(h1) == 16


def test_micro_workloads_emit_canonical_hash_sqlite(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "micro.sqlite"
    for workload in MICRO_WORKLOADS:
        res = run_micro_trial(
            db="sqlite",
            dsn=str(sqlite_path),
            workload=workload,
            strategy="baseline",
            seed=1234,
            k=0,
            analysis_only=False,
        )
        assert isinstance(res.canonical_state, dict)
        assert isinstance(res.state_hash, str)
        assert len(res.state_hash) == 16


def test_list_append_micro_has_multiple_states_sqlite(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "append.sqlite"
    hashes = set()
    for seed in range(200, 208):
        res = run_micro_trial(
            db="sqlite",
            dsn=str(sqlite_path),
            workload="list_append_micro",
            strategy="random_delays",
            seed=seed,
            k=3,
            analysis_only=False,
        )
        hashes.add(res.state_hash)
    assert len(hashes) >= 2

