#!/usr/bin/env python3
"""Unit tests for repro_pipeline modules — seed_gen, slicer, minimizer."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_seed_gen() -> None:
    from scripts.repro_pipeline.seed_gen import (
        generate_seed_trace, setup_sql, TEMPLATES, TraceEvent,
    )

    # Check templates exist
    assert len(TEMPLATES) == 5, f"Expected 5 templates, got {len(TEMPLATES)}"
    expected = {"rmw_update", "unique_insert", "insert_update_delete",
                "multi_table_rw", "abort_after_write"}
    assert set(TEMPLATES.keys()) == expected

    # Generate traces for each DB
    for db in ("sqlite", "mysql", "postgres"):
        trace = generate_seed_trace(seed=42, db=db, n_sessions=3, txns_per_session=6)
        assert trace.n_sessions == 3
        assert len(trace.events) > 0, f"No events for db={db}"
        assert trace.db == db

        # Check event structure
        for e in trace.events:
            assert isinstance(e, TraceEvent)
            assert e.session is not None
            assert e.event_id is not None
            assert e.type in ("begin", "sql", "commit", "rollback")

        # JSON round-trip
        j = trace.to_json()
        parsed = json.loads(j)
        assert parsed["db"] == db
        assert parsed["n_sessions"] == 3
        assert len(parsed["events"]) == len(trace.events)

    # Setup SQL
    for db in ("sqlite", "mysql", "postgres"):
        stmts = setup_sql(db)
        assert len(stmts) > 0
        # Should have DROP, CREATE, INSERT statements
        assert any("DROP" in s for s in stmts)
        assert any("CREATE" in s for s in stmts)
        assert any("INSERT" in s for s in stmts)

    # Deterministic: same seed → same trace
    t1 = generate_seed_trace(seed=99, db="sqlite")
    t2 = generate_seed_trace(seed=99, db="sqlite")
    assert t1.to_json() == t2.to_json(), "Same seed should produce same trace"

    # Different seed → different trace
    t3 = generate_seed_trace(seed=100, db="sqlite")
    assert t1.to_json() != t3.to_json(), "Different seeds should differ"

    print("  seed_gen: PASS")


def test_slicer() -> None:
    from scripts.repro_pipeline.seed_gen import generate_seed_trace
    from scripts.repro_pipeline.slicer import (
        extract_slices, select_top_slices, score_slice,
        generate_delays_for_slice,
    )

    trace = generate_seed_trace(seed=42, db="sqlite", n_sessions=4, txns_per_session=12)

    # Score slice
    s = score_slice(trace.events[:20])
    assert s.total_score >= 0

    # Extract slices
    slices = extract_slices(trace, window_size=20, stride=5)
    assert len(slices) > 0, "Should have at least one slice"

    # Top slices
    top = select_top_slices(slices, top_n=3)
    assert len(top) <= 3
    assert all(top[i].total_score >= top[i+1].total_score for i in range(len(top)-1)), \
        "Top slices should be sorted by score"

    # Delay generation
    for strategy in ("stress", "random", "heuristic"):
        delays = generate_delays_for_slice(trace.events[:20], strategy=strategy)
        assert isinstance(delays, dict)
        if strategy == "stress":
            assert len(delays) == 0, "Stress should have no delays"
        # random and heuristic may or may not have delays depending on events

    print("  slicer: PASS")


def test_minimizer() -> None:
    from scripts.repro_pipeline.seed_gen import TraceEvent
    from scripts.repro_pipeline.minimizer import (
        minimize_events, minimize_delays, run_minimization,
        _is_fifo_valid, _complete_transactions, _remove_empty_txns,
    )

    # Build a simple trace with 2 sessions, 2 txns each
    events = [
        TraceEvent(session="s0", event_id="e0", type="begin"),
        TraceEvent(session="s0", event_id="e1", type="sql", sql="SELECT 1", key=0, op="read", table="kv"),
        TraceEvent(session="s0", event_id="e2", type="sql", sql="UPDATE kv SET v=1 WHERE k=0", key=0, op="write", table="kv"),
        TraceEvent(session="s0", event_id="e3", type="commit"),
        TraceEvent(session="s1", event_id="e4", type="begin"),
        TraceEvent(session="s1", event_id="e5", type="sql", sql="SELECT 1", key=0, op="read", table="kv"),
        TraceEvent(session="s1", event_id="e6", type="sql", sql="UPDATE kv SET v=2 WHERE k=0", key=0, op="write", table="kv"),
        TraceEvent(session="s1", event_id="e7", type="commit"),
    ]
    delays = {"e1": 0.005, "e5": 0.003}

    # FIFO valid
    assert _is_fifo_valid(events), "Should be FIFO valid"

    # Invalid: commit before begin
    bad = [TraceEvent(session="s0", event_id="x0", type="commit")]
    assert not _is_fifo_valid(bad)

    # Complete transactions
    incomplete = [
        TraceEvent(session="s0", event_id="e0", type="begin"),
        TraceEvent(session="s0", event_id="e1", type="sql", sql="SELECT 1"),
    ]
    completed = _complete_transactions(incomplete)
    assert any(e.type == "commit" for e in completed), "Should add missing commit"

    # Remove empty txns
    empty = [
        TraceEvent(session="s0", event_id="e0", type="begin"),
        TraceEvent(session="s0", event_id="e1", type="commit"),
        TraceEvent(session="s1", event_id="e2", type="begin"),
        TraceEvent(session="s1", event_id="e3", type="sql", sql="SELECT 1"),
        TraceEvent(session="s1", event_id="e4", type="commit"),
    ]
    cleaned = _remove_empty_txns(empty)
    # s0 txn should be removed (empty), s1 txn kept
    assert len(cleaned) == 3, f"Expected 3 events after removing empty txn, got {len(cleaned)}"
    assert cleaned[0].session == "s1"

    # Minimize events — use a test_fn that always returns True (bug always reproduces)
    def always_true(evts, dlys):
        return True

    min_evts, min_delays = minimize_events(events, delays, always_true, max_iterations=5)
    assert len(min_evts) <= len(events), "Minimized should be <= original"
    assert _is_fifo_valid(min_evts)

    # Minimize delays
    min_d = minimize_delays(min_evts, min_delays, always_true)
    assert isinstance(min_d, dict)

    # Full minimization
    result = run_minimization(events, delays, always_true, max_event_iters=5, replays_for_verify=1)
    assert result.minimized_event_count <= result.original_event_count
    assert result.reduction_ratio >= 0.0

    print("  minimizer: PASS")


def test_registry() -> None:
    from scripts.repro_pipeline.bug_pack import list_packs, get_pack
    import scripts.repro_pipeline.packs  # noqa: F401

    packs = list_packs()
    assert len(packs) == 3, f"Expected 3 packs, got {len(packs)}"
    ids = {p.id for p in packs}
    assert "pg_update_conflict_out" in ids
    assert "mysql_rr_phantom" in ids
    assert "sqlite_narrow_dirty" in ids

    # Filter by DB
    sqlite_packs = list_packs(db="sqlite")
    assert len(sqlite_packs) == 1
    assert sqlite_packs[0].id == "sqlite_narrow_dirty"

    # Get specific pack
    p = get_pack("sqlite_narrow_dirty")
    assert p.db == "sqlite"

    # Unknown pack
    try:
        get_pack("nonexistent")
        assert False, "Should raise KeyError"
    except KeyError:
        pass

    print("  registry: PASS")


if __name__ == "__main__":
    print("Running repro_pipeline module tests...")
    test_seed_gen()
    test_slicer()
    test_minimizer()
    test_registry()
    print("\nAll tests PASSED.")
