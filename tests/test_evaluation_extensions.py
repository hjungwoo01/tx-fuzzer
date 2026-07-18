"""
Tests for the conference-quality evaluation extensions:
  - external-baseline strategies (PCT, NaiveUniform, IIDBernoulli)
  - extended workload registration (8 new bug workloads)
  - statistical helpers (bootstrap CI, Mann–Whitney U, Welch, Cliff's delta)
  - oracle independent-validator wiring
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def test_pct_strategy_is_bounded():
    from scripts.schedule_pool.base_strategy import PCTStrategy

    s = PCTStrategy(max_depth=3, min_delay_ms=10.0, max_delay_ms=80.0)
    rng = random.Random(0)
    eids = [f"s{i}_t0_q0_exec" for i in range(20)]
    sched = s.seed(eids, rng)
    # PCT picks at most max_depth events
    assert 1 <= len(sched.entries) <= 3
    for e in sched.entries:
        assert 10.0 - 1e-6 <= e.delay_ms <= 80.0 + 1e-6


def test_naive_uniform_strategy_assigns_all():
    from scripts.schedule_pool.base_strategy import NaiveUniformStrategy

    s = NaiveUniformStrategy(delay_ms=42.0)
    rng = random.Random(0)
    eids = [f"s0_t0_q{i}_exec" for i in range(5)]
    sched = s.seed(eids, rng)
    assert len(sched.entries) == 5
    assert all(e.delay_ms == 42.0 for e in sched.entries)


def test_iid_bernoulli_strategy_respects_p():
    from scripts.schedule_pool.base_strategy import IIDBernoulliStrategy

    # Run with p=0.0 -> no entries.
    s = IIDBernoulliStrategy(p_delay=0.0)
    rng = random.Random(0)
    eids = [f"s0_t0_q{i}_exec" for i in range(10)]
    sched = s.seed(eids, rng)
    assert len(sched.entries) == 0


def test_extended_workloads_register_into_micro_workloads():
    import scripts.experiments.extended_workloads as ext  # registers
    from scripts.experiments.micro_workloads import MICRO_WORKLOADS

    expected = {
        "doctors_on_call", "phantom_insert", "lost_update_counter",
        "read_skew", "unique_violation", "inventory_oversell",
        "fk_phantom", "long_fork",
    }
    for name in expected:
        assert name in MICRO_WORKLOADS, f"missing {name}"
        assert name in ext.CORPUS_METADATA, f"missing metadata for {name}"


def test_corpus_metadata_covers_originals_too():
    import scripts.experiments.extended_workloads as ext

    assert "bank_overdraft" in ext.CORPUS_METADATA
    assert "smallbank" in ext.CORPUS_METADATA
    # All entries have non-empty references
    for name, meta in ext.CORPUS_METADATA.items():
        assert meta.bug_class
        assert meta.isolation_level
        assert meta.historical_ref


def test_stats_mann_whitney_extreme_separation():
    from scripts.stats_utils import mann_whitney_u

    a, b = [1, 2, 3, 4, 5], [10, 11, 12, 13, 14]
    U, z, p, r = mann_whitney_u(a, b)
    assert U == 0.0
    assert p < 0.05
    assert r > 0.5


def test_stats_bootstrap_ci_brackets_mean():
    from scripts.stats_utils import bootstrap_ci

    samples = [10.0, 12.0, 11.0, 9.0, 13.0]
    mean, lo, hi = bootstrap_ci(samples, n_resamples=2000, seed=1)
    assert lo <= mean <= hi
    assert hi - lo > 0


def test_stats_cliffs_delta_known_values():
    from scripts.stats_utils import cliffs_delta, interpret_cliffs

    assert cliffs_delta([5, 6, 7], [1, 2, 3]) == 1.0
    assert cliffs_delta([1, 2, 3], [5, 6, 7]) == -1.0
    assert cliffs_delta([1, 2, 3], [1, 2, 3]) == 0.0
    assert interpret_cliffs(0.6) == "large"
    assert interpret_cliffs(0.05) == "negligible"


def test_oracle_soundness_validators_basic():
    from scripts.validate_oracle_soundness import VALIDATORS

    # job_queue: 2 distinct workers in claims = bug
    bug, _r = VALIDATORS["job_queue_claim"](
        [], {"claims": [(1, "w1"), (1, "w2")]},
    )
    assert bug

    # list_append: missing token = bug
    bug, _r = VALIDATORS["list_append"](
        [
            {"outcome": "appended", "worker": "t1"},
            {"outcome": "appended", "worker": "t2"},
        ],
        {"tokens": ["t1"]},
    )
    assert bug

    # inventory: stock + sold should == initial; mismatch = bug
    bug, _r = VALIDATORS["inventory_oversell"](
        [{"outcome": "sold"}, {"outcome": "sold"}],
        {"sold_count": 2, "stock": 0},
    )
    # initial=5, 0+2 != 5 → bug
    assert bug


if __name__ == "__main__":
    # Plain assert-based runner so the file works without pytest too.
    suite = [
        test_pct_strategy_is_bounded,
        test_naive_uniform_strategy_assigns_all,
        test_iid_bernoulli_strategy_respects_p,
        test_extended_workloads_register_into_micro_workloads,
        test_corpus_metadata_covers_originals_too,
        test_stats_mann_whitney_extreme_separation,
        test_stats_bootstrap_ci_brackets_mean,
        test_stats_cliffs_delta_known_values,
        test_oracle_soundness_validators_basic,
    ]
    failed = 0
    for fn in suite:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {fn.__name__}: {e}")
    if failed:
        sys.exit(1)
    print(f"\n{len(suite)} tests passed.")
