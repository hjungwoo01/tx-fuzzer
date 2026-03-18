"""
Automated tests for the schedule pool workflow.

Tests:
  - schedule serialization/deserialization
  - stable event ids
  - no delay stacking across unrelated runs
  - correct inherited mutation semantics
  - schedule pool retention / deduplication
  - exact schedule replay
  - canonical state novelty detection
"""

from __future__ import annotations

import json
import os
import random
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.schedule_pool.schedule import Schedule, ScheduleEntry, ScheduleRecord
from scripts.schedule_pool.event_id import (
    make_event_id,
    make_trial_event_ids,
    parse_event_id,
    get_workload_event_ids,
)
from scripts.schedule_pool.pool import PoolConfig, SchedulePool
from scripts.schedule_pool.mutator import MutationConfig, ScheduleMutator
from scripts.schedule_pool.retention import RetentionPolicy, RetentionReason

try:
    from scripts.schedule_pool.pool import SearchPhase
except ImportError:
    SearchPhase = None  # type: ignore[misc,assignment]


passed = 0
failed = 0
errors = []


def check(name: str, condition: bool, msg: str = "") -> None:
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS: {name}")
    else:
        failed += 1
        detail = f" — {msg}" if msg else ""
        errors.append(f"FAIL: {name}{detail}")
        print(f"  FAIL: {name}{detail}")


def section(name: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")


def test_schedule_serialization() -> None:
    section("Schedule Serialization / Deserialization")

    # Empty schedule
    s = Schedule.empty()
    check("empty schedule is_empty", s.is_empty)
    check("empty schedule delay_count=0", s.delay_count == 0)
    check("empty schedule total_delay=0", s.total_delay_ms == 0.0)

    # Basic schedule
    entries = [
        ScheduleEntry("s0_t0_q0_begin", 10.0),
        ScheduleEntry("s1_t0_q1_exec", 25.5),
        ScheduleEntry("s0_t0_q2_commit", 5.0),
    ]
    s = Schedule(entries)
    check("schedule delay_count=3", s.delay_count == 3)
    check("schedule total_delay=40.5", abs(s.total_delay_ms - 40.5) < 0.01)

    # Serialize to list
    lst = s.to_list()
    check("to_list returns list of dicts", isinstance(lst, list) and all(isinstance(d, dict) for d in lst))
    check("to_list preserves count", len(lst) == 3)

    # Deserialize from list
    s2 = Schedule.from_list(lst)
    check("from_list round-trip", s == s2)

    # JSON round-trip
    j = s.to_json()
    s3 = Schedule.from_json(j)
    check("json round-trip", s == s3)
    check("json is valid", json.loads(j) is not None)

    # Content hash determinism
    h1 = s.content_hash()
    h2 = s.content_hash()
    check("content_hash deterministic", h1 == h2)

    # Different schedule has different hash
    s4 = Schedule([ScheduleEntry("s0_t0_q0_begin", 11.0)])
    check("different schedule different hash", s.content_hash() != s4.content_hash())

    # ScheduleEntry negative delay clamped to 0
    e = ScheduleEntry("test", -5.0)
    check("negative delay clamped to 0", e.delay_ms == 0.0)

    # Deduplication: duplicate events keep last
    entries_dup = [
        ScheduleEntry("s0_t0_q0_begin", 10.0),
        ScheduleEntry("s0_t0_q0_begin", 20.0),
    ]
    s5 = Schedule(entries_dup)
    check("duplicate events deduplicated", s5.delay_count == 1)
    check("duplicate keeps last", s5.get_delay("s0_t0_q0_begin") == 20.0)


def test_schedule_record() -> None:
    section("ScheduleRecord Serialization")

    sched = Schedule([ScheduleEntry("s0_t0_q0_begin", 10.0)])
    rec = ScheduleRecord(
        db="postgres",
        workload="bank_overdraft",
        schedule=sched,
        bug_found=True,
        bug_reason="overdraft detected",
    )

    # to_dict
    d = rec.to_dict()
    check("record to_dict has schedule_id", "schedule_id" in d and len(d["schedule_id"]) > 0)
    check("record to_dict has schedule list", isinstance(d["schedule"], list))
    check("record to_dict total_delay_ms", d["total_delay_ms"] == 10.0)

    # JSONL round-trip
    line = rec.to_jsonl()
    rec2 = ScheduleRecord.from_jsonl(line)
    check("record jsonl round-trip db", rec2.db == "postgres")
    check("record jsonl round-trip bug_found", rec2.bug_found is True)
    check("record jsonl round-trip schedule", rec2.schedule == sched)


def test_stable_event_ids() -> None:
    section("Stable Event IDs")

    # Deterministic generation
    eid = make_event_id(0, 0, 0, "begin")
    check("event_id format", eid == "s0_t0_q0_begin")

    eid2 = make_event_id(2, 1, 3, "exec")
    check("event_id format complex", eid2 == "s2_t1_q3_exec")

    # Parse round-trip
    parsed = parse_event_id(eid2)
    check("parse session", parsed["session"] == 2)
    check("parse txn_idx", parsed["txn_idx"] == 1)
    check("parse query_idx", parsed["query_idx"] == 3)
    check("parse phase", parsed["phase"] == "exec")

    # Same seed = same IDs
    ids1 = make_trial_event_ids(4, 3)
    ids2 = make_trial_event_ids(4, 3)
    check("trial ids deterministic", ids1 == ids2)
    check("trial ids sorted", ids1 == sorted(ids1))

    # Different worker count = different IDs
    ids3 = make_trial_event_ids(2, 3)
    check("different workers different ids", ids1 != ids3)

    # Workload event IDs
    ids_jq = get_workload_event_ids("job_queue_claim", 4)
    check("workload event ids non-empty", len(ids_jq) > 0)
    check("workload event ids have begin", any("begin" in e for e in ids_jq))
    check("workload event ids have commit", any("commit" in e for e in ids_jq))


def test_no_cross_run_stacking() -> None:
    section("No Delay Stacking Across Unrelated Runs")

    # Two independent mutations from empty should be independent
    rng = random.Random(42)
    events = make_trial_event_ids(4, 3)
    mutator = ScheduleMutator(events, config=MutationConfig(), rng=rng)

    parent = Schedule.empty()
    child1 = mutator.mutate(parent)
    child2 = mutator.mutate(parent)

    # Both start from empty, not from each other
    # They should generally be different (probabilistic but with seed=42, reliable)
    check("independent mutations from empty exist", child1.delay_count > 0)
    check("independent mutations produce schedules", child2.delay_count > 0)

    # Crucially: child2 does NOT inherit child1's delays
    # (they both start from empty parent)
    child1_events = set(e.event for e in child1.entries)
    child2_events = set(e.event for e in child2.entries)
    # They may overlap by chance but should not be identical (different rng draws)
    check("independent mutations not identical (generally)",
          child1.content_hash() != child2.content_hash() or child1.delay_count == child2.delay_count == 0,
          "Both children from empty parent should generally differ")


def test_inherited_mutation() -> None:
    section("Inherited Mutation Semantics")

    rng = random.Random(123)
    events = make_trial_event_ids(4, 3)
    mutator = ScheduleMutator(events, config=MutationConfig(), rng=rng)

    # Create a parent with known delays
    parent = Schedule([
        ScheduleEntry("s0_t0_q0_begin", 50.0),
        ScheduleEntry("s1_t0_q1_exec", 30.0),
    ])

    # Mutate from parent multiple times
    children = [mutator.mutate(parent) for _ in range(10)]

    # Children should generally inherit some parent delays
    inherited_count = 0
    for child in children:
        child_events = set(e.event for e in child.entries)
        parent_events = set(e.event for e in parent.entries)
        if child_events & parent_events:
            inherited_count += 1

    check("children inherit parent delays", inherited_count > 0,
          f"{inherited_count}/10 children inherited at least one parent event")

    # Caps are respected
    cfg = MutationConfig(max_per_event_delay_ms=100.0, max_total_delay_ms=500.0, max_delay_count=8)
    strict_mutator = ScheduleMutator(events, config=cfg, rng=rng)
    for _ in range(20):
        child = strict_mutator.mutate(parent)
        check_cap = (
            child.delay_count <= cfg.max_delay_count and
            child.total_delay_ms <= cfg.max_total_delay_ms + 1.0 and
            all(e.delay_ms <= cfg.max_per_event_delay_ms + 0.1 for e in child.entries)
        )
        if not check_cap:
            check("caps respected (all children)", False,
                  f"count={child.delay_count}, total={child.total_delay_ms}")
            return
    check("caps respected (all 20 children)", True)


def test_pool_retention() -> None:
    section("Schedule Pool Retention / Deduplication")

    with tempfile.TemporaryDirectory() as tmpdir:
        pool_path = Path(tmpdir) / "test_pool.jsonl"
        pool = SchedulePool(path=pool_path, config=PoolConfig())

        # Add a record
        sched1 = Schedule([ScheduleEntry("s0_t0_q0_begin", 10.0)])
        rec1 = ScheduleRecord(
            db="postgres", workload="bank_overdraft",
            schedule=sched1, discovered_state_hash="abc123",
        )
        added = pool.add(rec1)
        check("first add succeeds", added)
        check("pool size is 1", pool.size == 1)

        # Duplicate rejected
        rec1_dup = ScheduleRecord(
            db="postgres", workload="bank_overdraft",
            schedule=sched1, discovered_state_hash="abc123",
        )
        added = pool.add(rec1_dup)
        check("duplicate rejected", not added)
        check("pool still size 1", pool.size == 1)

        # Different schedule accepted
        sched2 = Schedule([ScheduleEntry("s0_t0_q0_begin", 20.0)])
        rec2 = ScheduleRecord(
            db="postgres", workload="bank_overdraft",
            schedule=sched2, discovered_state_hash="def456",
        )
        added = pool.add(rec2)
        check("different schedule accepted", added)
        check("pool size is 2", pool.size == 2)

        # Persistence: save and reload
        pool.save()
        check("pool file exists", pool_path.exists())

        pool2 = SchedulePool(path=pool_path, config=PoolConfig())
        loaded = pool2.load()
        check("reload count", loaded == 2)
        check("reload pool size", pool2.size == 2)

        # Deduplication
        pool2._content_hashes.clear()
        pool2._records.append(rec1)  # force a dup
        removed = pool2.deduplicate()
        check("deduplicate removes dups", removed >= 1)

        # Sampling
        rng = random.Random(42)
        sampled = pool.sample(rng, workload="bank_overdraft", db="postgres")
        check("sample returns a record", sampled is not None)

        # Listing
        bug_list = pool.list(bug_only=True)
        check("list bug_only on non-bug records", len(bug_list) == 0)

        # Summary
        s = pool.summary()
        check("summary has pool_size", s["pool_size"] == 2)


def test_retention_policy() -> None:
    section("Retention Policy")

    pool = SchedulePool(config=PoolConfig())
    policy = RetentionPolicy(pool)

    # New state → retained
    sched = Schedule([ScheduleEntry("s0_t0_q0_begin", 10.0)])
    rec = ScheduleRecord(
        db="postgres", workload="bank_overdraft",
        schedule=sched, discovered_state_hash="new_state_1",
    )
    reason = policy.evaluate(rec)
    check("new state is retained", reason == RetentionReason.NEW_STATE)

    accepted = policy.accept(rec)
    check("accept returns True for new state", accepted)
    check("pool now has 1 entry", pool.size == 1)
    check("seen states updated", "new_state_1" in policy.seen_states)

    # Same state → not retained
    rec2 = ScheduleRecord(
        db="postgres", workload="bank_overdraft",
        schedule=Schedule([ScheduleEntry("s0_t0_q0_begin", 20.0)]),
        discovered_state_hash="new_state_1",
    )
    reason2 = policy.evaluate(rec2)
    check("same state not retained", reason2 == RetentionReason.NONE)

    # New bug → retained
    rec3 = ScheduleRecord(
        db="postgres", workload="bank_overdraft",
        schedule=Schedule([ScheduleEntry("s1_t0_q1_exec", 30.0)]),
        discovered_state_hash="new_state_1",
        bug_found=True,
        bug_reason="overdraft detected",
    )
    reason3 = policy.evaluate(rec3)
    check("new bug is retained", reason3 == RetentionReason.NEW_BUG)


def test_exact_replay() -> None:
    section("Exact Schedule Replay (unit-level)")

    # Verify that to_delay_map produces correct executor format
    sched = Schedule([
        ScheduleEntry("s0_t0_q0_begin", 15.0),
        ScheduleEntry("s1_t0_q1_exec", 42.5),
    ])
    dm = sched.to_delay_map()
    check("delay_map has correct keys", set(dm.keys()) == {"s0_t0_q0_begin", "s1_t0_q1_exec"})
    check("delay_map converts to seconds", abs(dm["s0_t0_q0_begin"] - 0.015) < 0.001)
    check("delay_map converts to seconds (2)", abs(dm["s1_t0_q1_exec"] - 0.0425) < 0.001)

    # Verify same schedule produces same delay map
    sched2 = Schedule.from_json(sched.to_json())
    dm2 = sched2.to_delay_map()
    check("replay uses exact same delays", dm == dm2)


def test_canonical_novelty() -> None:
    section("Canonical State Novelty Detection")

    pool = SchedulePool(config=PoolConfig())
    policy = RetentionPolicy(pool)

    # Add several states
    states = ["hash_a", "hash_b", "hash_c"]
    for i, h in enumerate(states):
        rec = ScheduleRecord(
            db="postgres", workload="test",
            schedule=Schedule([ScheduleEntry(f"s{i}_t0_q0_begin", float(i + 1))]),
            discovered_state_hash=h,
        )
        policy.accept(rec)

    check("3 unique states tracked", policy.unique_state_count == 3)

    # New state detected
    rec_new = ScheduleRecord(
        db="postgres", workload="test",
        schedule=Schedule([ScheduleEntry("s3_t0_q0_begin", 4.0)]),
        discovered_state_hash="hash_d",
    )
    reason = policy.evaluate(rec_new)
    check("new canonical state detected", reason == RetentionReason.NEW_STATE)

    # Existing state not retained
    rec_old = ScheduleRecord(
        db="postgres", workload="test",
        schedule=Schedule([ScheduleEntry("s4_t0_q0_begin", 5.0)]),
        discovered_state_hash="hash_a",
    )
    reason_old = policy.evaluate(rec_old)
    check("existing state not retained", reason_old == RetentionReason.NONE)


def test_pretty_print() -> None:
    section("Schedule Pretty Print")

    s_empty = Schedule.empty()
    check("empty pretty", "empty" in s_empty.pretty().lower())

    s = Schedule([
        ScheduleEntry("s0_t0_q0_begin", 10.0),
        ScheduleEntry("s1_t0_q1_exec", 25.5),
    ])
    pp = s.pretty()
    check("pretty has delay count", "2 delays" in pp)
    check("pretty has event names", "s0_t0_q0_begin" in pp)
    check("pretty has delay values", "25.50ms" in pp)


def test_case_pool_search() -> None:
    """Test the generic case study pool search with a synthetic trial fn."""
    section("Case Study Pool Search (synthetic)")

    from scripts.experiments.run_final_eval import run_case_pool_search

    call_count = 0
    states_seen: set = set()

    def fake_trial(delays: dict) -> dict:
        nonlocal call_count
        call_count += 1
        total = sum(delays.values())
        # Simulate: bug found when total delay crosses 50ms
        bug = total > 0.050
        state = {"bug": bug, "total_bin": round(total, 2)}
        h = hash(json.dumps(state, sort_keys=True))
        states_seen.add(h)
        return {
            "bug_found": bug,
            "bug_reason": "synthetic" if bug else "",
            "canonical_state": state,
            "canonical_hash": f"{h:016x}",
        }

    events = ["ev_a", "ev_b", "ev_c"]
    result = run_case_pool_search(
        trial_fn=fake_trial,
        event_ids=events,
        db="test",
        case_name="synthetic",
        duration_s=2.0,
        seed=42,
    )

    check("case_pool ran trials", result.total_trials > 0)
    check("case_pool found bug", result.bug_found,
          f"trials={result.total_trials}, bugs={result.bugs_found}")
    if result.bug_found:
        check("case_pool has ttfb", result.ttfb_s is not None and result.ttfb_s > 0)
        check("case_pool has first schedule",
              result.first_bug_schedule is not None)
    check("case_pool tracked unique states", result.unique_states > 0)
    check("case_pool has trial_log", len(result.trial_log) == result.total_trials)
    check("case_pool pool grew", result.pool_size > 0)


def test_dual_objective_search() -> None:
    """Test SearchPhase transitions and exploit bug-parent sampling."""
    section("Dual-Objective Search Phase")

    if SearchPhase is None:
        check("SearchPhase import", False, "SearchPhase not found")
        return

    # Pool starts in EXPLORE phase
    pool = SchedulePool(config=PoolConfig())
    check("initial phase is EXPLORE", pool.phase == SearchPhase.EXPLORE)
    check("has_bugs initially False", not pool.has_bugs)

    # Add non-bug record → stays EXPLORE
    entries = [ScheduleEntry("s0_t0_q0_begin", 10.0)]
    sched = Schedule(entries)
    rec = ScheduleRecord(
        db="test", workload="test", schedule=sched,
        discovered_state_hash="state1",
    )
    pool.add(rec)
    check("phase still EXPLORE after non-bug", pool.phase == SearchPhase.EXPLORE)

    # Add bug record → transitions to EXPLOIT
    bug_sched = Schedule([ScheduleEntry("s0_t0_q1_exec", 20.0)])
    bug_rec = ScheduleRecord(
        db="test", workload="test", schedule=bug_sched,
        discovered_state_hash="bugstate",
        bug_found=True, bug_reason="test bug",
    )
    pool.add(bug_rec)
    check("phase transitions to EXPLOIT on bug",
          pool.phase == SearchPhase.EXPLOIT)
    check("has_bugs now True", pool.has_bugs)


def test_bug_bonus_scoring() -> None:
    """Test that bug_bonus increases sampling weight for bug schedules."""
    section("Bug Bonus Scoring")

    cfg = PoolConfig(bug_bonus=3.0, novelty_bonus=1.0, replay_bonus=1.0,
                     compact_bonus=1.0, alpha=0.0, beta=0.0)
    pool = SchedulePool(config=cfg)

    # Non-bug schedule
    s1 = Schedule([ScheduleEntry("s0_t0_q0_begin", 10.0)])
    r1 = ScheduleRecord(db="t", workload="t", schedule=s1,
                        discovered_state_hash="s1")
    pool.add(r1)

    # Bug schedule
    s2 = Schedule([ScheduleEntry("s0_t0_q1_exec", 10.0)])
    r2 = ScheduleRecord(db="t", workload="t", schedule=s2,
                        discovered_state_hash="s2",
                        bug_found=True, bug_reason="test")
    pool.add(r2)

    w1 = pool._weight(r1)
    w2 = pool._weight(r2)
    check("bug schedule has higher weight", w2 > w1,
          f"w_bug={w2:.3f} vs w_normal={w1:.3f}")
    check("bug weight is 3x normal", abs(w2 / w1 - 3.0) < 0.01,
          f"ratio={w2/w1:.3f}")


def test_ttfsr_retention() -> None:
    """Test BEST_TTFSR retention for compact, high-replay bug schedules."""
    section("TTFSR Retention")

    pool = SchedulePool(config=PoolConfig())

    # Seed with a bug schedule (low replay, high delay count)
    s1 = Schedule([
        ScheduleEntry("s0_t0_q0_begin", 50.0),
        ScheduleEntry("s0_t0_q1_exec", 50.0),
        ScheduleEntry("s0_t0_q2_exec", 50.0),
        ScheduleEntry("s0_t0_q3_commit", 50.0),
        ScheduleEntry("s1_t0_q0_begin", 50.0),
        ScheduleEntry("s1_t0_q1_exec", 50.0),
    ])
    r1 = ScheduleRecord(
        db="t", workload="t", schedule=s1,
        discovered_state_hash="s1",
        bug_found=True, bug_reason="test",
        replay_rate_buggy=0.3,
    )
    pool.add(r1)

    retention = RetentionPolicy(pool)

    # Candidate: compact + high replay → should get BEST_TTFSR
    s2 = Schedule([
        ScheduleEntry("s0_t0_q0_begin", 15.0),
        ScheduleEntry("s0_t0_q1_exec", 20.0),
    ])
    r2 = ScheduleRecord(
        db="t", workload="t", schedule=s2,
        discovered_state_hash="s1",  # same state (not new)
        bug_found=True, bug_reason="test",  # same bug (not new)
        replay_rate_buggy=0.8,
    )
    reason = retention.evaluate(r2)
    check("TTFSR retention triggered",
          reason == RetentionReason.BEST_TTFSR,
          f"got {reason}")

    # Candidate: high delay count + low replay → should NOT get TTFSR
    s3 = Schedule([
        ScheduleEntry(f"s{i}_t0_q0_begin", 40.0) for i in range(6)
    ])
    r3 = ScheduleRecord(
        db="t", workload="t", schedule=s3,
        discovered_state_hash="s1",
        bug_found=True, bug_reason="test",
        replay_rate_buggy=0.5,
    )
    reason3 = retention.evaluate(r3)
    check("high delay_count not TTFSR", reason3 != RetentionReason.BEST_TTFSR,
          f"got {reason3}")


def test_compact_bonus() -> None:
    """Test compact_bonus for bug schedules with few delays."""
    section("Compact Bonus")

    cfg = PoolConfig(bug_bonus=1.0, novelty_bonus=1.0, replay_bonus=1.0,
                     compact_bonus=1.5, compact_threshold=4,
                     alpha=0.0, beta=0.0)
    pool = SchedulePool(config=cfg)

    # Compact bug schedule (2 delays)
    s1 = Schedule([
        ScheduleEntry("s0_t0_q0_begin", 10.0),
        ScheduleEntry("s0_t0_q1_exec", 10.0),
    ])
    r1 = ScheduleRecord(db="t", workload="t", schedule=s1,
                        bug_found=True, bug_reason="test")

    # Non-compact bug schedule (6 delays)
    s2 = Schedule([ScheduleEntry(f"s{i}_t0_q0_begin", 10.0) for i in range(6)])
    r2 = ScheduleRecord(db="t", workload="t", schedule=s2,
                        bug_found=True, bug_reason="test")

    w1 = pool._weight(r1)
    w2 = pool._weight(r2)
    check("compact bug gets higher weight", w1 > w2,
          f"w_compact={w1:.3f} vs w_large={w2:.3f}")


def test_final_eval_imports() -> None:
    """Verify all critical imports in run_final_eval work."""
    section("Final Eval Imports")
    try:
        from scripts.schedule_pool import (
            run_pool_search,
            run_random_pool_search,
            run_heuristic_pool_search,
            replay_schedule,
        )
        from scripts.experiments.run_final_eval import (
            run_case_pool_search,
            CasePoolResult,
            CaseStudyRow,
            DeterminismRow,
            ThroughputRow,
            STRATEGIES,
            SEEDS_10,
            PG_UCO_EVENTS,
            MYSQL_RR_EVENTS,
            SQLITE_DIRTY_EVENTS,
        )
        check("final_eval imports OK", True)
        check("schedule_pool runner exports", all([
            callable(run_pool_search),
            callable(run_random_pool_search),
            callable(run_heuristic_pool_search),
            callable(replay_schedule),
        ]))
        check("strategies has 4 entries", len(STRATEGIES) == 4)
        check("seeds has 10 entries", len(SEEDS_10) == 10)
        check("PG events count", len(PG_UCO_EVENTS) == 11)
        check("MySQL events count", len(MYSQL_RR_EVENTS) == 15)
        check("SQLite events count", len(SQLITE_DIRTY_EVENTS) == 9)
    except ImportError as e:
        check("final_eval imports OK", False, str(e))


def main() -> int:
    print("=" * 60)
    print("  Schedule Pool — Automated Test Suite")
    print("=" * 60)

    test_schedule_serialization()
    test_schedule_record()
    test_stable_event_ids()
    test_no_cross_run_stacking()
    test_inherited_mutation()
    test_pool_retention()
    test_retention_policy()
    test_exact_replay()
    test_canonical_novelty()
    test_pretty_print()
    test_case_pool_search()
    test_dual_objective_search()
    test_bug_bonus_scoring()
    test_ttfsr_retention()
    test_compact_bonus()
    test_final_eval_imports()

    print(f"\n{'='*60}")
    print(f"  Results: {passed} passed, {failed} failed")
    if errors:
        print("\n  Failures:")
        for e in errors:
            print(f"    {e}")
    print(f"{'='*60}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
