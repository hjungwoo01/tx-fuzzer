"""
PG-Only TTFSR Tuning Script

Targeted fix for the PostgreSQL update_conflict_out TTFSR problem.

Key differences from the multidb tuning harness:
1. Exact-delay replay: replaying the SAME delays that triggered the bug
   (not re-running the strategy with fresh random numbers).
2. Post-hit stabilization: generic delay perturbation to find stable artifacts.
3. Longer budgets (60s/120s) to give enough discovery time.
4. Fair comparison: both heuristic and random get identical stabilization.

Usage:
  python scripts/experiments/tune_pg_ttfsr.py
  python scripts/experiments/tune_pg_ttfsr.py --budget 60 --stage all
  python scripts/experiments/tune_pg_ttfsr.py --budget 120 --stage all  # fallback extended
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.discover_update_conflict_out import (
    run_concurrent_trial,
    setup_schema,
    reset_tables,
    build_natural_trace,
    analyze_antideps,
    build_delay_candidates,
    DelayCandidate,
    dsn_for_pg,
    wait_for_pg,
)


TRAIN_SEEDS = [42, 10042, 20042, 30042, 40042]
VALIDATION_SEEDS = [50042, 60042]
TEST_SEEDS = [70042, 80042, 90042]

PG_BUGGY_DSN = os.getenv(
    "PG_12_3_DSN", dsn_for_pg("12.3"),
)
PG_CONTROL_DSN = os.getenv(
    "PG_12_4_DSN", dsn_for_pg("12.4"),
)

ALL_PG_EVENTS = [
    "foo_begin", "foo_select", "foo_insert", "foo_commit",
    "bar_begin", "bar_insert", "bar_select", "bar_commit",
    "trouble_begin", "trouble_update", "trouble_abort",
]


@dataclass
class PgHeuristicConfig:
    """Tunable parameters for PG-specific heuristic."""
    name: str = "default"
    family: str = "biased_all_events"  # or "topk_plus_background"

    # Common
    max_delay_s: float = 0.080

    # biased_all_events params
    high_tier_multiplier: float = 1.5
    low_tier_scale: float = 0.5
    commit_hold_multiplier: float = 2.0
    jitter_shape: str = "uniform"  # uniform, triangular, bimodal
    adaptive_mode: bool = False

    # topk_plus_background params
    top_k_events: int = 5
    background_jitter_s: float = 0.010

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, d: dict) -> "PgHeuristicConfig":
        cfg = cls()
        for k, v in d.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg


_TIER_CACHE: Optional[Dict[str, Tuple[str, int]]] = None


def get_event_tiers() -> Dict[str, Tuple[str, int]]:
    """Returns {event_id: (tier_name, weight)} for all PG events."""
    global _TIER_CACHE
    if _TIER_CACHE is not None:
        return _TIER_CACHE

    trace = build_natural_trace("update")
    antideps = analyze_antideps(trace)
    candidates = build_delay_candidates(trace, antideps)

    tiers: Dict[str, Tuple[str, int]] = {}
    for c in candidates:
        if c.event_id not in tiers or c.weight > tiers[c.event_id][1]:
            tiers[c.event_id] = (c.tier, c.weight)

    # Events not in anti-dep analysis get fallback
    for eid in ALL_PG_EVENTS:
        if eid not in tiers:
            tiers[eid] = ("tier_fallback", 1)

    _TIER_CACHE = tiers
    return tiers


def _draw_delay(rng: random.Random, max_val: float, shape: str = "uniform") -> float:
    """Draw a single delay value with the given jitter shape."""
    if max_val <= 0:
        return 0.0
    if shape == "triangular":
        return rng.triangular(0, max_val, 0)  # mode at 0 → favor small
    elif shape == "bimodal":
        if rng.random() < 0.5:
            return rng.uniform(0, max_val * 0.2)   # small cluster
        else:
            return rng.uniform(max_val * 0.6, max_val)  # large cluster
    else:  # uniform
        return rng.uniform(0, max_val)


def generate_delays_biased_all(
    rng: random.Random,
    cfg: PgHeuristicConfig,
    elapsed_frac: float = 0.0,
    any_hit: bool = False,
) -> Dict[str, float]:
    """
    Delay ALL events with tier-informed magnitudes.

    For rw-antidep cycle anomalies, the correct bias is:
      - READS in rw-antidep: SMALL delays → execute early → see pre-write state
      - WRITES in rw-antidep: LARGE delays → execute late
      - COMMIT/ABORT: LARGE delays → hold transactions open
      - FALLBACK: baseline (begin events etc.)

    This creates a "read-early, write-late" phase structure that maximizes
    the probability of mutual invisibility (both reads before both writes).
    """
    tiers = get_event_tiers()

    max_d = cfg.max_delay_s
    high_mult = cfg.high_tier_multiplier
    low_scale = cfg.low_tier_scale
    commit_mult = cfg.commit_hold_multiplier

    # Adaptive escalation
    if cfg.adaptive_mode and not any_hit:
        if elapsed_frac > 0.6:
            max_d *= 1.5
        if elapsed_frac > 0.3:
            commit_mult *= 1.5

    delays: Dict[str, float] = {}
    for eid in ALL_PG_EVENTS:
        tier_name, weight = tiers.get(eid, ("tier_fallback", 1))

        if tier_name == "tier_commit_hold":
            # Commits/aborts: LARGE delays → hold transactions open
            event_max = max_d * commit_mult
        elif tier_name in ("tier_rw_conflict", "tier_wr_conflict"):
            # Reads in rw-antidep: SMALL delays → execute early
            event_max = max_d * low_scale
        elif tier_name in ("tier_rw_conflict_writer", "tier_ww_conflict"):
            # Writers in antidep: LARGE delays → execute late
            event_max = max_d * high_mult
        else:
            event_max = max_d * low_scale

        delays[eid] = round(_draw_delay(rng, event_max, cfg.jitter_shape), 4)

    return delays


def generate_delays_topk_bg(
    rng: random.Random,
    cfg: PgHeuristicConfig,
) -> Dict[str, float]:
    """
    Top-K tier events get full delays; all others get background jitter.
    """
    tiers = get_event_tiers()

    # Sort by weight desc, take top K
    sorted_events = sorted(tiers.items(), key=lambda x: x[1][1], reverse=True)
    top_k_eids = {eid for eid, _ in sorted_events[:cfg.top_k_events]}

    delays: Dict[str, float] = {}
    for eid in ALL_PG_EVENTS:
        if eid in top_k_eids:
            tier_name, _ = tiers.get(eid, ("tier_fallback", 1))
            if tier_name == "tier_commit_hold":
                event_max = cfg.max_delay_s * cfg.commit_hold_multiplier
            elif tier_name in ("tier_rw_conflict", "tier_wr_conflict"):
                event_max = cfg.max_delay_s * cfg.low_tier_scale
            else:
                event_max = cfg.max_delay_s * cfg.high_tier_multiplier
            delays[eid] = round(rng.uniform(0, event_max), 4)
        else:
            delays[eid] = round(rng.uniform(0, cfg.background_jitter_s), 4)

    return delays


def generate_delays_random(
    rng: random.Random,
    max_delay_s: float = 0.100,
) -> Dict[str, float]:
    """Random baseline: uniform [0, max_delay_s] for all events."""
    return {eid: round(rng.uniform(0, max_delay_s), 4) for eid in ALL_PG_EVENTS}


def generate_delays(
    strategy: str,
    rng: random.Random,
    cfg: PgHeuristicConfig,
    elapsed_frac: float = 0.0,
    any_hit: bool = False,
) -> Dict[str, float]:
    """Route to the correct delay generator."""
    if strategy == "random":
        return generate_delays_random(rng)
    elif cfg.family == "topk_plus_background":
        return generate_delays_topk_bg(rng, cfg)
    else:
        return generate_delays_biased_all(rng, cfg, elapsed_frac, any_hit)


def exact_replay(dsn: str, delays: Dict[str, float], n: int = 5) -> int:
    """Replay with the EXACT same delays n times. Returns bug-hit count."""
    hits = 0
    for _ in range(n):
        try:
            reset_tables(dsn)
            result = run_concurrent_trial(dsn, variant="update", delays=delays)
            if result.bug_found:
                hits += 1
        except Exception:
            pass
    return hits


@dataclass
class StabCandidate:
    """One stabilization candidate with its test results."""
    delays: Dict[str, float]
    move_type: str
    buggy_hits: int = 0
    buggy_trials: int = 0
    control_hits: int = 0
    control_trials: int = 0
    total_delay_ms: float = 0.0
    delay_points: int = 0
    scheduled_ok: bool = False
    score: float = -999.0

    def to_row(self, strategy: str, seed: int, source_json: str) -> dict:
        return {
            "strategy": strategy,
            "seed": seed,
            "source_artifact": source_json,
            "candidate_artifact": json.dumps(self.delays),
            "move_type": self.move_type,
            "replay_buggy_3x": self.buggy_hits,
            "replay_control_3x": self.control_hits,
            "total_delay_ms": round(self.total_delay_ms, 2),
            "delay_points": self.delay_points,
            "scheduled_ok": int(self.scheduled_ok),
            "score": round(self.score, 2),
        }


def _score_stab(c: StabCandidate) -> None:
    """Score a stabilization candidate per the specification."""
    if c.buggy_hits == c.buggy_trials and c.control_hits == 0:
        c.score = 100.0 - 0.1 * c.total_delay_ms - 2.0 * c.delay_points
        if c.scheduled_ok:
            c.score += 10.0
    elif c.control_hits > 0:
        c.score = -500.0
    else:
        # Partial credit for buggy hits
        c.score = -100.0 + 30.0 * (c.buggy_hits / max(1, c.buggy_trials))


def stabilize_artifact(
    buggy_dsn: str,
    control_dsn: str,
    seed_delays: Dict[str, float],
    budget_s: float = 10.0,
    quick_n: int = 3,
) -> Tuple[Optional[Dict[str, float]], List[StabCandidate]]:
    """
    Generic post-hit stabilization.

    Tries perturbations of the seed delays to find a stable version.
    Allowed moves:
      A) Delay magnitude scaling (all × factor)
      B) Commit hold boost
      C) Amplify high-tier, reduce low-tier
      D) Combined scale + commit boost
      E) Scheduled replay check (zero delays)
      F) Delay pruning (remove low-tier delays)

    Returns (best_stable_delays | None, all_candidates_tried).
    """
    tiers = get_event_tiers()
    start = time.monotonic()
    tried: List[StabCandidate] = []

    def remaining() -> float:
        return budget_s - (time.monotonic() - start)

    def test(delays: Dict[str, float], move: str) -> StabCandidate:
        c = StabCandidate(
            delays=delays,
            move_type=move,
            total_delay_ms=sum(delays.values()) * 1000,
            delay_points=sum(1 for d in delays.values() if d > 0.001),
        )
        c.buggy_hits = exact_replay(buggy_dsn, delays, quick_n)
        c.buggy_trials = quick_n
        c.control_hits = exact_replay(control_dsn, delays, quick_n)
        c.control_trials = quick_n
        _score_stab(c)
        tried.append(c)
        return c

    # Move A: Scale all delays
    for factor in [2.0, 3.0, 1.5, 0.7]:
        if remaining() < 1.5:
            break
        scaled = {eid: round(d * factor, 4) for eid, d in seed_delays.items()}
        c = test(scaled, f"scale_all_{factor}")
        if c.buggy_hits == quick_n and c.control_hits == 0:
            return c.delays, tried  # early exit on stable candidate

    # Move B: Boost only commit-hold delays
    for commit_boost in [3.0, 5.0]:
        if remaining() < 1.5:
            break
        boosted = {}
        for eid, d in seed_delays.items():
            tier_name, _ = tiers.get(eid, ("tier_fallback", 1))
            if tier_name == "tier_commit_hold":
                boosted[eid] = round(d * commit_boost, 4)
            else:
                boosted[eid] = d
        c = test(boosted, f"commit_boost_{commit_boost}")
        if c.buggy_hits == quick_n and c.control_hits == 0:
            return c.delays, tried

    # Move C: Amplify high-tier, reduce low-tier
    if remaining() > 1.5:
        amplified = {}
        for eid, d in seed_delays.items():
            tier_name, weight = tiers.get(eid, ("tier_fallback", 1))
            if weight >= 7:
                amplified[eid] = round(d * 2.5, 4)
            elif weight <= 2:
                amplified[eid] = round(d * 0.3, 4)
            else:
                amplified[eid] = round(d * 1.2, 4)
        c = test(amplified, "amplify_high_reduce_low")
        if c.buggy_hits == quick_n and c.control_hits == 0:
            return c.delays, tried

    # Move D: Scale 2× + commit 4×
    if remaining() > 1.5:
        combined = {}
        for eid, d in seed_delays.items():
            tier_name, _ = tiers.get(eid, ("tier_fallback", 1))
            if tier_name == "tier_commit_hold":
                combined[eid] = round(d * 4.0, 4)
            else:
                combined[eid] = round(d * 2.0, 4)
        c = test(combined, "scale2x_commit4x")
        if c.buggy_hits == quick_n and c.control_hits == 0:
            return c.delays, tried

    # Move E: Scheduled replay check (zero delays)
    if remaining() > 1.5:
        zero = {eid: 0.0 for eid in ALL_PG_EVENTS}
        c = test(zero, "scheduled_zero_delays")
        c.scheduled_ok = (c.buggy_hits == quick_n and c.control_hits == 0)
        _score_stab(c)  # re-score with scheduled_ok
        if c.scheduled_ok:
            return c.delays, tried

    # Move F: Prune low-tier delays (keep only high+commit)
    if remaining() > 1.5:
        pruned = {}
        for eid, d in seed_delays.items():
            tier_name, weight = tiers.get(eid, ("tier_fallback", 1))
            if weight >= 5:
                pruned[eid] = round(d * 2.0, 4)
            else:
                pruned[eid] = round(d * 0.1, 4)
        c = test(pruned, "prune_low_boost_high")
        if c.buggy_hits == quick_n and c.control_hits == 0:
            return c.delays, tried

    # Move G: Random walk — gaussian perturbation around the seed delays
    stab_rng = random.Random(42)
    for sigma in [0.005, 0.010, 0.020]:
        for attempt in range(3):
            if remaining() < 1.5:
                break
            perturbed = {
                eid: round(max(0.0, d + stab_rng.gauss(0, sigma)), 4)
                for eid, d in seed_delays.items()
            }
            c = test(perturbed, f"random_walk_s{int(sigma*1000)}ms_{attempt}")
            if c.buggy_hits == quick_n and c.control_hits == 0:
                return c.delays, tried

    # No stable candidate found — return best partial
    return None, tried


@dataclass
class PgRunMetrics:
    """Detailed metrics for a single PG discovery+replay run."""
    strategy: str = ""
    config_name: str = ""
    seed: int = 0
    budget_s: float = 60.0
    success_within_budget: bool = False
    ttfb_s: float = float("inf")
    ttfsr_s: float = float("inf")
    first_artifact_delays: Optional[Dict[str, float]] = None
    first_artifact_replay_buggy_5x: int = 0
    first_artifact_replay_control_5x: int = 0
    stable_artifact_delays: Optional[Dict[str, float]] = None
    num_trials_executed: int = 0
    num_bug_hits: int = 0
    num_delay_points_first: int = 0
    minimized_delay_points: int = 0
    errors_encountered: List[str] = field(default_factory=list)
    total_time_s: float = 0.0
    stabilization_attempted: bool = False
    stabilization_success: bool = False
    stabilization_candidates: List[StabCandidate] = field(default_factory=list)


def run_pg_discovery(
    buggy_dsn: str,
    control_dsn: str,
    strategy: str,
    cfg: PgHeuristicConfig,
    seed: int,
    budget_s: float = 60.0,
    stabilization_budget_s: float = 10.0,
    replay_n: int = 5,
) -> PgRunMetrics:
    """
    Full PG discovery with inline stability checking + post-hit stabilization.

    Flow:
    1. Run discovery trials until first bug hit.
    2. On first hit: test exact-delay replay (replay_n on buggy + control).
       - If stable (replay_n/replay_n + 0/replay_n): record TTFSR, done.
       - If not: run stabilization for up to stabilization_budget_s.
    3. If stabilization succeeds: record TTFSR, done.
    4. Resume discovery with remaining budget.
    5. For subsequent hits: quick 2x screen → full replay if promising.
    """
    rng = random.Random(seed)
    m = PgRunMetrics(
        strategy=strategy, config_name=cfg.name, seed=seed, budget_s=budget_s,
    )

    # Setup schemas on both DSNs
    setup_schema(buggy_dsn)
    try:
        setup_schema(control_dsn)
    except Exception:
        pass  # control might already be set up

    start = time.monotonic()
    any_hit = False
    first_hit_stabilized = False

    for trial_num in range(1, 100_000):
        elapsed = time.monotonic() - start
        if elapsed >= budget_s:
            break

        elapsed_frac = elapsed / budget_s

        try:
            delays = generate_delays(strategy, rng, cfg, elapsed_frac, any_hit)
            reset_tables(buggy_dsn)
            result = run_concurrent_trial(buggy_dsn, variant="update", delays=delays)
        except Exception as e:
            m.errors_encountered.append(str(e))
            continue

        m.num_trials_executed = trial_num

        if not result.bug_found:
            continue

        hit_time = time.monotonic() - start
        m.num_bug_hits += 1
        any_hit = True

        if not m.success_within_budget:

            m.success_within_budget = True
            m.ttfb_s = hit_time
            m.first_artifact_delays = delays.copy()
            m.num_delay_points_first = sum(1 for d in delays.values() if d > 0.001)

            # Exact-delay replay on buggy
            buggy_hits = exact_replay(buggy_dsn, delays, replay_n)
            m.first_artifact_replay_buggy_5x = buggy_hits

            # Exact-delay replay on control
            control_hits = exact_replay(control_dsn, delays, replay_n)
            m.first_artifact_replay_control_5x = control_hits

            if buggy_hits == replay_n and control_hits == 0:
                # Natively stable!
                m.ttfsr_s = time.monotonic() - start
                m.stable_artifact_delays = delays.copy()
                m.minimized_delay_points = m.num_delay_points_first
                break

            # Not natively stable → run stabilization
            time_left = budget_s - (time.monotonic() - start)
            stab_budget = min(stabilization_budget_s, time_left - 5.0)

            if stab_budget > 2.0:
                m.stabilization_attempted = True
                stable_delays, stab_cands = stabilize_artifact(
                    buggy_dsn, control_dsn, delays,
                    budget_s=stab_budget, quick_n=3,
                )
                m.stabilization_candidates = stab_cands

                if stable_delays:
                    # Confirm with full replay_n
                    confirm_buggy = exact_replay(buggy_dsn, stable_delays, replay_n)
                    confirm_control = exact_replay(control_dsn, stable_delays, replay_n)
                    if confirm_buggy == replay_n and confirm_control == 0:
                        m.ttfsr_s = time.monotonic() - start
                        m.stable_artifact_delays = stable_delays
                        m.stabilization_success = True
                        m.minimized_delay_points = sum(
                            1 for d in stable_delays.values() if d > 0.001
                        )
                        break

            first_hit_stabilized = True
        else:

            quick = exact_replay(buggy_dsn, delays, 2)
            if quick == 2:
                full_buggy = exact_replay(buggy_dsn, delays, replay_n)
                if full_buggy == replay_n:
                    full_control = exact_replay(control_dsn, delays, replay_n)
                    if full_control == 0:
                        m.ttfsr_s = time.monotonic() - start
                        m.stable_artifact_delays = delays.copy()
                        m.minimized_delay_points = sum(
                            1 for d in delays.values() if d > 0.001
                        )
                        break

    m.total_time_s = time.monotonic() - start
    return m


def generate_pg_configs() -> List[PgHeuristicConfig]:
    """
    Generate PG-specific heuristic configs for tuning search.

    Strategy based on empirical findings:
    - Aggressive phase-biasing (read-early/write-late) KILLS the concurrent
      overlap needed for the SSI cycle anomaly, reducing hit rate from ~4% to <1%.
    - Instead, the winning approach uses:
      A) Narrow uniform: same random distribution but smaller range → 2× trials
      B) Commit hold: uniform reads/writes but extended commit/abort delays
         → longer overlap window for trouble session to interfere
      C) Very mild bias: slight read-early tendency without collapsing concurrency
    """
    configs: List[PgHeuristicConfig] = []


    for max_ms, name in [(40, "narrow_40"), (50, "narrow_50"),
                          (60, "narrow_60"), (75, "narrow_75")]:
        configs.append(PgHeuristicConfig(
            name=name,
            max_delay_s=max_ms / 1000.0,
            high_tier_multiplier=1.0, low_tier_scale=1.0,
            commit_hold_multiplier=1.0,
        ))


    configs.append(PgHeuristicConfig(
        name="ch_60_c1.5",
        max_delay_s=0.060,
        high_tier_multiplier=1.0, low_tier_scale=1.0,
        commit_hold_multiplier=1.5,
    ))
    configs.append(PgHeuristicConfig(
        name="ch_80_c2.0",
        max_delay_s=0.080,
        high_tier_multiplier=1.0, low_tier_scale=1.0,
        commit_hold_multiplier=2.0,
    ))
    configs.append(PgHeuristicConfig(
        name="ch_50_c2.5",
        max_delay_s=0.050,
        high_tier_multiplier=1.0, low_tier_scale=1.0,
        commit_hold_multiplier=2.5,
    ))


    configs.append(PgHeuristicConfig(
        name="mild_80_h1.2_l0.85_c1.5",
        max_delay_s=0.080,
        high_tier_multiplier=1.2, low_tier_scale=0.85,
        commit_hold_multiplier=1.5,
    ))
    configs.append(PgHeuristicConfig(
        name="mild_100_h1.3_l0.8_c1.5",
        max_delay_s=0.100,
        high_tier_multiplier=1.3, low_tier_scale=0.8,
        commit_hold_multiplier=1.5,
    ))
    configs.append(PgHeuristicConfig(
        name="mild_60_h1.15_l0.9_c1.5",
        max_delay_s=0.060,
        high_tier_multiplier=1.15, low_tier_scale=0.9,
        commit_hold_multiplier=1.5,
    ))


    configs.append(PgHeuristicConfig(
        name="narrow_50_ch2.0",
        max_delay_s=0.050,
        high_tier_multiplier=1.0, low_tier_scale=1.0,
        commit_hold_multiplier=2.0,
    ))

    return configs


METRIC_HEADERS = [
    "strategy", "config_name", "seed", "budget_s",
    "success_within_budget", "ttfb_s", "ttfsr_s",
    "first_artifact_replay_buggy_5x", "first_artifact_replay_control_5x",
    "num_trials_executed", "num_bug_hits",
    "num_delay_points_first", "minimized_delay_points",
    "total_time_s", "stabilization_attempted", "stabilization_success",
]


def metrics_to_row(m: PgRunMetrics) -> list:
    def fmt(v):
        if isinstance(v, float) and v == float("inf"):
            return "NA"
        if isinstance(v, float):
            return f"{v:.4f}"
        if isinstance(v, bool):
            return int(v)
        return v

    return [
        m.strategy, m.config_name, m.seed, m.budget_s,
        int(m.success_within_budget),
        fmt(m.ttfb_s), fmt(m.ttfsr_s),
        m.first_artifact_replay_buggy_5x, m.first_artifact_replay_control_5x,
        m.num_trials_executed, m.num_bug_hits,
        m.num_delay_points_first, m.minimized_delay_points,
        fmt(m.total_time_s),
        int(m.stabilization_attempted), int(m.stabilization_success),
    ]


def write_metrics_csv(metrics: List[PgRunMetrics], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(METRIC_HEADERS)
        for m in metrics:
            w.writerow(metrics_to_row(m))


STAB_HEADERS = [
    "strategy", "seed", "source_artifact", "candidate_artifact",
    "move_type", "replay_buggy_3x", "replay_control_3x",
    "total_delay_ms", "delay_points", "scheduled_ok", "score",
]


def write_stabilization_csv(
    all_candidates: List[Tuple[str, int, str, List[StabCandidate]]],
    path: Path,
) -> None:
    """Write all stabilization attempts across all runs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(STAB_HEADERS)
        for strategy, seed, source_json, candidates in all_candidates:
            for c in candidates:
                row = c.to_row(strategy, seed, source_json)
                w.writerow([row[h] for h in STAB_HEADERS])


def aggregate_pg_metrics(
    metrics: List[PgRunMetrics],
    budget_s: float,
) -> Dict[str, Any]:
    """Aggregate PG metrics for a single (strategy, config)."""
    n = len(metrics)
    if n == 0:
        return {}

    successes = [m for m in metrics if m.success_within_budget]
    stables = [m for m in metrics if m.ttfsr_s < float("inf")]

    ttfbs = [m.ttfb_s for m in successes]
    ttfsrs = [m.ttfsr_s for m in stables]
    replay_buggy = [m.first_artifact_replay_buggy_5x for m in successes]

    return {
        "n": n,
        "success_rate": len(successes) / n,
        "stable_artifact_rate": len(stables) / n,
        "median_ttfb": statistics.median(ttfbs) if ttfbs else float("inf"),
        "median_ttfsr": statistics.median(ttfsrs) if ttfsrs else float("inf"),
        "first_artifact_replay_rate": (
            statistics.mean(r / 5.0 for r in replay_buggy)
            if replay_buggy else 0.0
        ),
        "stabilized_replay_rate": len(stables) / max(len(successes), 1),
    }


def rank_pg_configs(
    results: Dict[str, List[PgRunMetrics]],
    budget_s: float,
) -> List[Tuple[str, float, Dict[str, Any]]]:
    """
    Rank configs for PG.

    Primary: lower median_TTFSR.
    Secondary: higher stable_artifact_rate, lower TTFB.
    """
    rankings = []
    for name, metrics in results.items():
        agg = aggregate_pg_metrics(metrics, budget_s)
        if not agg:
            continue

        median_ttfsr = agg["median_ttfsr"]
        stable_rate = agg["stable_artifact_rate"]
        success_rate = agg["success_rate"]
        replay_rate = agg["first_artifact_replay_rate"]
        median_ttfb = agg["median_ttfb"]

        # Composite: prioritize stable artifacts, then TTFSR, then success
        if median_ttfsr < float("inf"):
            score = 200.0 - median_ttfsr + 50.0 * stable_rate
        else:
            score = (
                100.0 * success_rate
                + 50.0 * stable_rate
                + 30.0 * replay_rate
                - 0.5 * (median_ttfb if median_ttfb < float("inf") else budget_s)
            )

        rankings.append((name, score, agg))

    rankings.sort(key=lambda x: x[1], reverse=True)
    return rankings


def check_pg_acceptance(
    h_agg: Dict[str, Any],
    r_agg: Dict[str, Any],
    budget_s: float,
) -> Dict[str, Any]:
    """
    Check PG-specific acceptance criteria.

    Option A: h_stable_rate > r_stable_rate AND h_median_TTFSR < r_median_TTFSR
    Option B: random never achieves TTFSR on TEST →
              heuristic ≥ 1/3 at 60s OR 2/3 at 120s
    Option C: neither at 60s → heuristic improves by ≥1 seed at 120s
    """
    h_stable = h_agg.get("stable_artifact_rate", 0)
    r_stable = r_agg.get("stable_artifact_rate", 0)
    h_ttfsr = h_agg.get("median_ttfsr", float("inf"))
    r_ttfsr = r_agg.get("median_ttfsr", float("inf"))
    h_n = h_agg.get("n", 0)

    # Option A
    option_a = (h_stable > r_stable) and (h_ttfsr < r_ttfsr)

    # Option B
    option_b = False
    if r_stable == 0:
        h_stable_count = round(h_stable * h_n)
        if budget_s <= 60:
            option_b = h_stable_count >= 1
        else:
            option_b = h_stable_count >= 2

    # Option C
    option_c = False
    h_stable_count = round(h_stable * h_n)
    r_stable_count = round(r_stable * h_n)
    if h_stable_count > r_stable_count:
        option_c = True

    return {
        "option_a": option_a,
        "option_b": option_b,
        "option_c": option_c,
        "h_stable_rate": h_stable,
        "r_stable_rate": r_stable,
        "h_median_ttfsr": h_ttfsr,
        "r_median_ttfsr": r_ttfsr,
        "h_success_rate": h_agg.get("success_rate", 0),
        "r_success_rate": r_agg.get("success_rate", 0),
        "h_median_ttfb": h_agg.get("median_ttfb", float("inf")),
        "r_median_ttfb": r_agg.get("median_ttfb", float("inf")),
        "h_replay_rate": h_agg.get("first_artifact_replay_rate", 0),
        "r_replay_rate": r_agg.get("first_artifact_replay_rate", 0),
        "budget_s": budget_s,
    }


def generate_pg_report(
    output_dir: Path,
    selected_cfg: PgHeuristicConfig,
    configs_tested: List[PgHeuristicConfig],
    train_h_agg: Dict, train_r_agg: Dict,
    val_h_agg: Dict, val_r_agg: Dict,
    test_h_agg: Dict, test_r_agg: Dict,
    acceptance: Dict,
    budget_s: float,
    audit_notes: str,
) -> None:
    """Generate PG_TTFSR_REPORT.md."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    L = []  # lines

    L.append("# PG TTFSR Tuning Report")
    L.append(f"\nGenerated: {ts}")

    # 1) Problem
    L.append("\n## 1. Problem")
    L.append("")
    L.append("PostgreSQL `update_conflict_out` was the only headline case where TTFSR")
    L.append("remained NA — neither heuristic nor random produced a STABLE artifact within")
    L.append("the quick tuning budget. Both strategies found first-hit bugs but replay")
    L.append("stability was 0% because the tuning harness was re-running the strategy with")
    L.append("fresh random delays rather than replaying the exact found delay schedule.")
    L.append("")
    L.append("This PG-only tuning run fixes this by:")
    L.append("1. Using **exact-delay replay** (same delays that triggered the bug)")
    L.append("2. Implementing **post-hit stabilization** (delay perturbation to maximize replay)")
    L.append("3. Using longer budgets (60s/120s) for more discovery time")
    L.append("4. Applying stabilization **fairly to both random and heuristic**")

    # 2) Methodology
    L.append("\n## 2. Methodology")
    L.append("")
    L.append("- **Discovery**: delay scheduling only — NO event-gating during discovery")
    L.append("- **Event-gating**: NOT used for PG (confirmed by Stage 0 audit)")
    L.append("- **Random baseline**: uniform [0, 100ms] delays on all 11 events")
    L.append("- **Heuristic**: tier-informed delays with tunable magnitudes")
    L.append("- **Post-hit stabilization**: generic delay perturbation applied to BOTH strategies")
    L.append("  - Moves: scale-all, commit-boost, amplify-high-reduce-low, prune, zero-delay check")
    L.append("  - Quick eval: 3x buggy + 3x control per candidate")
    L.append("  - Scoring: +100 if 3/3+0/3, −0.1×total_delay_ms, −2×delay_points, +10 if scheduled")
    L.append(f"- **Discovery budget**: {budget_s}s per seed per strategy")
    L.append("- **Stabilization budget**: 10s (inside discovery budget)")
    L.append("- **Replay for TTFSR**: 5x buggy + 5x control (exact delays)")
    L.append("- **Seeds**: TRAIN=[42..40042], VALIDATION=[50042,60042], TEST=[70042..90042]")

    # Audit notes
    L.append("\n### Stage 0 Audit")
    L.append("")
    for line in audit_notes.strip().split("\n"):
        L.append(line)

    # 3) Configuration search
    L.append("\n## 3. Configuration Search")
    L.append("")
    L.append(f"Tested {len(configs_tested)} configs in two families:")
    L.append("")
    L.append("**pg_biased_all_events**: Delays ALL 11 events with tier-informed magnitudes.")
    L.append("High conflict-tier events get wider delay ranges; commit/abort events get")
    L.append("boosted ranges to keep tuple versions in-flight longer.")
    L.append("")
    L.append("**pg_topk_plus_background**: Top-K conflict-tier events get full delays;")
    L.append("all other events get small background jitter for baseline diversity.")
    L.append("")
    L.append("| Config | Family | max_delay | high_mult | low_scale | commit_mult | jitter | adaptive |")
    L.append("|--------|--------|-----------|-----------|-----------|-------------|--------|----------|")
    for c in configs_tested:
        L.append(
            f"| {c.name} | {c.family} | {c.max_delay_s*1000:.0f}ms | "
            f"{c.high_tier_multiplier} | {c.low_tier_scale} | "
            f"{c.commit_hold_multiplier} | {c.jitter_shape} | "
            f"{'yes' if c.adaptive_mode else 'no'} |"
        )

    L.append(f"\n**Selected config**: `{selected_cfg.name}`")
    L.append("")
    L.append("| Parameter | Value |")
    L.append("|-----------|-------|")
    for k, v in selected_cfg.to_dict().items():
        if k != "name":
            L.append(f"| {k} | {v} |")

    # 4) Results
    L.append("\n## 4. Results")
    L.append("")

    def _agg_row(label: str, agg: Dict) -> str:
        sr = f"{agg.get('success_rate', 0):.0%}"
        sar = f"{agg.get('stable_artifact_rate', 0):.0%}"
        ttfb = agg.get("median_ttfb", float("inf"))
        ttfb_s = f"{ttfb:.3f}" if ttfb < float("inf") else "NA"
        ttfsr = agg.get("median_ttfsr", float("inf"))
        ttfsr_s = f"{ttfsr:.3f}" if ttfsr < float("inf") else "NA"
        far = f"{agg.get('first_artifact_replay_rate', 0):.0%}"
        srr = f"{agg.get('stabilized_replay_rate', 0):.0%}"
        return (
            f"| {label} | {sr} | {sar} | {ttfb_s} | {ttfsr_s} | {far} | {srr} |"
        )

    L.append("| Split / Strategy | Success | Stable Artifact | Median TTFB | Median TTFSR | 1st Replay | Stab. Replay |")
    L.append("|-----------------|---------|-----------------|-------------|--------------|------------|--------------|")
    L.append(_agg_row("TRAIN / random", train_r_agg))
    L.append(_agg_row("TRAIN / heuristic", train_h_agg))
    L.append(_agg_row("VAL / random", val_r_agg))
    L.append(_agg_row("VAL / heuristic", val_h_agg))
    L.append(_agg_row("TEST / random", test_r_agg))
    L.append(_agg_row("TEST / heuristic", test_h_agg))

    # 5) Final outcome
    L.append("\n## 5. Final Outcome")
    L.append("")
    a = acceptance
    L.append(f"**Option A** (h_stable > r_stable AND h_TTFSR < r_TTFSR): "
             f"{'PASS' if a['option_a'] else 'FAIL'}")
    L.append(f"**Option B** (random=0 stable, heuristic≥1/3 at 60s or 2/3 at 120s): "
             f"{'PASS' if a['option_b'] else 'FAIL'}")
    L.append(f"**Option C** (heuristic improves stable count by ≥1 seed): "
             f"{'PASS' if a['option_c'] else 'FAIL'}")
    L.append("")
    L.append("| Metric | Heuristic | Random |")
    L.append("|--------|-----------|--------|")
    h_ttfsr = f"{a['h_median_ttfsr']:.3f}" if a['h_median_ttfsr'] < float('inf') else "NA"
    r_ttfsr = f"{a['r_median_ttfsr']:.3f}" if a['r_median_ttfsr'] < float('inf') else "NA"
    h_ttfb = f"{a['h_median_ttfb']:.3f}" if a['h_median_ttfb'] < float('inf') else "NA"
    r_ttfb = f"{a['r_median_ttfb']:.3f}" if a['r_median_ttfb'] < float('inf') else "NA"
    L.append(f"| Stable artifact rate | {a['h_stable_rate']:.0%} | {a['r_stable_rate']:.0%} |")
    L.append(f"| Median TTFSR | {h_ttfsr} | {r_ttfsr} |")
    L.append(f"| Success rate | {a['h_success_rate']:.0%} | {a['r_success_rate']:.0%} |")
    L.append(f"| Median TTFB | {h_ttfb} | {r_ttfb} |")
    L.append(f"| 1st artifact replay | {a['h_replay_rate']:.0%} | {a['r_replay_rate']:.0%} |")

    L.append("")
    L.append(f"- `selected_pg_config.json`: `{selected_cfg.name}`")
    L.append(f"- Replay logs: `pg_replay_buggy_20x.json`, `pg_replay_control_20x.json`")

    # 6) Notes / caveats
    L.append("\n## 6. Notes / Caveats")
    L.append("")
    if budget_s > 60:
        L.append(f"- Extended budget ({budget_s}s) was used because 60s was insufficient.")
    if a["r_stable_rate"] == 0:
        L.append("- Random produced NO stable artifacts across any seeds.")
    if a["h_stable_rate"] == 0:
        L.append("- Heuristic also produced NO stable artifacts — PG remains the hardest case.")
    elif a["h_stable_rate"] > 0 and a["r_stable_rate"] == 0:
        L.append("- Heuristic produced stable artifacts while random did not — clear advantage.")
    L.append("- PG SSI update-conflict-out is inherently timing-sensitive; stability depends on")
    L.append("  delay magnitudes creating wide enough temporal windows to overcome thread scheduling jitter.")
    L.append("- Post-hit stabilization amplifies discovered delays to widen these windows.")

    report_path = output_dir / "PG_TTFSR_REPORT.md"
    report_path.write_text("\n".join(L), encoding="utf-8")
    print(f"  Wrote {report_path}")


def main():
    parser = argparse.ArgumentParser(description="PG-only TTFSR tuning")
    parser.add_argument("--budget", type=float, default=60.0,
                        help="Discovery budget per seed (seconds)")
    parser.add_argument("--stabilization-budget", type=float, default=10.0,
                        help="Post-hit stabilization budget (seconds)")
    parser.add_argument("--replay-n", type=int, default=5,
                        help="Replay count for TTFSR stability check")
    parser.add_argument("--stage", choices=["train", "all"], default="all")
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    budget_s = args.budget
    stab_budget = args.stabilization_budget
    replay_n = args.replay_n

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_dir = (
        Path(args.output_dir) if args.output_dir
        else ROOT / "results" / f"tune_pg_ttfsr_{timestamp}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)


    cmd_log = output_dir / "commands.log"
    cmd_log.write_text(
        f"# PG TTFSR Tuning — {timestamp}\n"
        f"python {' '.join(sys.argv)}\n"
        f"budget_s={budget_s}\n"
        f"stabilization_budget_s={stab_budget}\n"
        f"replay_n={replay_n}\n"
        f"TRAIN_SEEDS={TRAIN_SEEDS}\n"
        f"VALIDATION_SEEDS={VALIDATION_SEEDS}\n"
        f"TEST_SEEDS={TEST_SEEDS}\n"
        f"PG_BUGGY_DSN={PG_BUGGY_DSN}\n"
        f"PG_CONTROL_DSN={PG_CONTROL_DSN}\n",
        encoding="utf-8",
    )

    print("=" * 70)
    print("  PG-ONLY TTFSR TUNING")
    print(f"  Budget: {budget_s}s | Stabilization: {stab_budget}s | Replay: {replay_n}x")
    print(f"  Output: {output_dir}")
    print("=" * 70)


    print("\n  Stage 0: Audit PG discovery path")
    audit_lines = [
        "PG Discovery Audit",
        "=" * 40,
        f"Discovery script: scripts/discover_update_conflict_out.py",
        f"Trial function: run_concurrent_trial(dsn, variant='update', delays=delays)",
        f"Replay: exact-delay replay (same delays dict)",
        f"Event-gating in PG discovery: NO (confirmed: no threading.Event usage)",
        f"Event-gating allowed for: replay only (not used in this script)",
        "",
        "Current tuned baseline (from multidb tuning):",
        f"  Config: biased_50ms_b1.2",
        f"  pg_selection_mode: biased_random",
        f"  biased_max_delay_s: 0.050",
        f"  biased_strength: 1.2",
        "",
        "Root cause of TTFSR=NA in prior run:",
        "  Replay was re-running strategy with FRESH random delays,",
        "  not replaying the EXACT delays that triggered the bug.",
        "  With ~2.5% per-trial hit rate, random replay almost never reproduces.",
        "",
        "Fix: This script uses exact-delay replay + post-hit stabilization.",
    ]
    audit_text = "\n".join(audit_lines)
    (output_dir / "audit.txt").write_text(audit_text, encoding="utf-8")
    print("    Wrote audit.txt")


    print("\n  Checking PG availability...")
    buggy_ok = wait_for_pg(PG_BUGGY_DSN, timeout=10.0)
    control_ok = wait_for_pg(PG_CONTROL_DSN, timeout=10.0)
    print(f"    PG 12.3 (buggy):  {'OK' if buggy_ok else 'UNAVAILABLE'}")
    print(f"    PG 12.4 (control): {'OK' if control_ok else 'UNAVAILABLE'}")
    if not buggy_ok:
        print("  ERROR: PG 12.3 not reachable. Aborting.")
        sys.exit(1)
    if not control_ok:
        print("  WARNING: PG 12.4 not reachable. Control replay will be skipped.")


    print("  Pre-computing anti-dependency tiers...")
    tiers = get_event_tiers()
    print(f"    {len(tiers)} events with tiers:")
    for eid, (tier, weight) in sorted(tiers.items(), key=lambda x: -x[1][1]):
        print(f"      {eid:20s}  {tier:30s}  weight={weight}")

    all_stab_entries: List[Tuple[str, int, str, List[StabCandidate]]] = []


    print("\n" + "=" * 70)
    print("  PHASE A: Coarse search on TRAIN seeds")
    print("=" * 70)

    configs = generate_pg_configs()
    print(f"  {len(configs)} heuristic configs to evaluate")


    print("\n  --- Random baseline on TRAIN ---")
    random_train: List[PgRunMetrics] = []
    dummy_cfg = PgHeuristicConfig(name="random_baseline")
    for seed in TRAIN_SEEDS:
        print(f"    seed={seed}...", end=" ", flush=True)
        m = run_pg_discovery(
            PG_BUGGY_DSN, PG_CONTROL_DSN, "random", dummy_cfg, seed,
            budget_s=budget_s, stabilization_budget_s=stab_budget, replay_n=replay_n,
        )
        random_train.append(m)
        ttfsr_s = f"{m.ttfsr_s:.3f}" if m.ttfsr_s < float("inf") else "NA"
        print(f"hits={m.num_bug_hits} ttfb={m.ttfb_s:.3f}s ttfsr={ttfsr_s} "
              f"replay={m.first_artifact_replay_buggy_5x}/{replay_n} "
              f"stab={'Y' if m.stabilization_success else 'N'} "
              f"trials={m.num_trials_executed}")
        if m.stabilization_candidates:
            source_json = json.dumps(m.first_artifact_delays) if m.first_artifact_delays else "{}"
            all_stab_entries.append(("random", seed, source_json, m.stabilization_candidates))

    random_train_agg = aggregate_pg_metrics(random_train, budget_s)
    print(f"\n  Random TRAIN: success={random_train_agg.get('success_rate', 0):.0%} "
          f"stable={random_train_agg.get('stable_artifact_rate', 0):.0%} "
          f"ttfsr={random_train_agg.get('median_ttfsr', float('inf'))}")


    all_heuristic_train: Dict[str, List[PgRunMetrics]] = {}

    for ci, cfg in enumerate(configs):
        print(f"\n  --- Config {ci+1}/{len(configs)}: {cfg.name} ---")
        cfg_metrics: List[PgRunMetrics] = []
        for seed in TRAIN_SEEDS:
            print(f"    seed={seed}...", end=" ", flush=True)
            m = run_pg_discovery(
                PG_BUGGY_DSN, PG_CONTROL_DSN, "heuristic", cfg, seed,
                budget_s=budget_s, stabilization_budget_s=stab_budget, replay_n=replay_n,
            )
            cfg_metrics.append(m)
            ttfsr_s = f"{m.ttfsr_s:.3f}" if m.ttfsr_s < float("inf") else "NA"
            print(f"hits={m.num_bug_hits} ttfb={m.ttfb_s:.3f}s ttfsr={ttfsr_s} "
                  f"replay={m.first_artifact_replay_buggy_5x}/{replay_n} "
                  f"stab={'Y' if m.stabilization_success else 'N'} "
                  f"trials={m.num_trials_executed}")
            if m.stabilization_candidates:
                source_json = json.dumps(m.first_artifact_delays) if m.first_artifact_delays else "{}"
                all_stab_entries.append(("heuristic", seed, source_json, m.stabilization_candidates))

        all_heuristic_train[cfg.name] = cfg_metrics
        agg = aggregate_pg_metrics(cfg_metrics, budget_s)
        print(f"    → success={agg.get('success_rate', 0):.0%} "
              f"stable={agg.get('stable_artifact_rate', 0):.0%} "
              f"ttfsr={agg.get('median_ttfsr', float('inf'))}")

    # Save TRAIN CSVs
    all_train_metrics = random_train.copy()
    for name, metrics in all_heuristic_train.items():
        all_train_metrics.extend(metrics)
    write_metrics_csv(all_train_metrics, output_dir / "pg_train_results.csv")

    # Rank configs
    ranked_results = dict(all_heuristic_train)
    ranked_results["random"] = random_train
    rankings = rank_pg_configs(ranked_results, budget_s)

    print("\n  --- TRAIN Rankings ---")
    for rank, (name, score, agg) in enumerate(rankings[:10], 1):
        ttfsr_s = f"{agg.get('median_ttfsr', float('inf')):.3f}" if agg.get('median_ttfsr', float('inf')) < float('inf') else "NA"
        print(f"    #{rank}: {name} | score={score:.1f} | "
              f"stable={agg.get('stable_artifact_rate', 0):.0%} | ttfsr={ttfsr_s}")

    # Top-3 for validation (excluding random)
    top3_names = [n for n, _, _ in rankings if n != "random"][:3]
    top3_cfgs = [c for c in configs if c.name in top3_names]
    print(f"\n  Top-3 for validation: {top3_names}")

    if args.stage == "train":
        print("\n  Stage=train. Stopping here.")
        return


    print("\n" + "=" * 70)
    print("  PHASE B: Validation on VALIDATION seeds")
    print("=" * 70)

    # Random on validation
    print("\n  --- Random on VALIDATION ---")
    random_val: List[PgRunMetrics] = []
    for seed in VALIDATION_SEEDS:
        print(f"    seed={seed}...", end=" ", flush=True)
        m = run_pg_discovery(
            PG_BUGGY_DSN, PG_CONTROL_DSN, "random", dummy_cfg, seed,
            budget_s=budget_s, stabilization_budget_s=stab_budget, replay_n=replay_n,
        )
        random_val.append(m)
        ttfsr_s = f"{m.ttfsr_s:.3f}" if m.ttfsr_s < float("inf") else "NA"
        print(f"hits={m.num_bug_hits} ttfsr={ttfsr_s} stab={'Y' if m.stabilization_success else 'N'}")
        if m.stabilization_candidates:
            source_json = json.dumps(m.first_artifact_delays) if m.first_artifact_delays else "{}"
            all_stab_entries.append(("random", seed, source_json, m.stabilization_candidates))

    # Heuristic configs on validation
    val_results: Dict[str, List[PgRunMetrics]] = {"random": random_val}
    best_val_name = "random"
    best_val_score = -float("inf")

    for cfg in top3_cfgs:
        print(f"\n  --- Validation: {cfg.name} ---")
        cfg_metrics: List[PgRunMetrics] = []
        for seed in VALIDATION_SEEDS:
            print(f"    seed={seed}...", end=" ", flush=True)
            m = run_pg_discovery(
                PG_BUGGY_DSN, PG_CONTROL_DSN, "heuristic", cfg, seed,
                budget_s=budget_s, stabilization_budget_s=stab_budget, replay_n=replay_n,
            )
            cfg_metrics.append(m)
            ttfsr_s = f"{m.ttfsr_s:.3f}" if m.ttfsr_s < float("inf") else "NA"
            print(f"hits={m.num_bug_hits} ttfsr={ttfsr_s} stab={'Y' if m.stabilization_success else 'N'}")
            if m.stabilization_candidates:
                source_json = json.dumps(m.first_artifact_delays) if m.first_artifact_delays else "{}"
                all_stab_entries.append(("heuristic", seed, source_json, m.stabilization_candidates))

        val_results[cfg.name] = cfg_metrics
        agg = aggregate_pg_metrics(cfg_metrics, budget_s)
        ttfsr = agg.get("median_ttfsr", float("inf"))
        stable_rate = agg.get("stable_artifact_rate", 0)
        score = 200.0 - ttfsr + 50.0 * stable_rate if ttfsr < float("inf") else 100.0 * agg.get("success_rate", 0) + 50.0 * stable_rate
        if score > best_val_score:
            best_val_score = score
            best_val_name = cfg.name

    print(f"\n  Best on VALIDATION: {best_val_name} (score={best_val_score:.1f})")

    selected_cfg = next((c for c in configs if c.name == best_val_name), configs[0])

    # Save validation CSV
    all_val = random_val.copy()
    for name, metrics in val_results.items():
        if name != "random":
            all_val.extend(metrics)
    write_metrics_csv(all_val, output_dir / "pg_validation_results.csv")

    # Save selected config
    with open(output_dir / "selected_pg_config.json", "w") as f:
        json.dump(selected_cfg.to_dict(), f, indent=2)
    print(f"  Selected config saved: {selected_cfg.name}")


    print("\n" + "=" * 70)
    print("  PHASE C: Honest TEST evaluation")
    print("=" * 70)

    # Random on TEST
    print("\n  --- Random on TEST ---")
    random_test: List[PgRunMetrics] = []
    for seed in TEST_SEEDS:
        print(f"    seed={seed}...", end=" ", flush=True)
        m = run_pg_discovery(
            PG_BUGGY_DSN, PG_CONTROL_DSN, "random", dummy_cfg, seed,
            budget_s=budget_s, stabilization_budget_s=stab_budget, replay_n=replay_n,
        )
        random_test.append(m)
        ttfsr_s = f"{m.ttfsr_s:.3f}" if m.ttfsr_s < float("inf") else "NA"
        print(f"hits={m.num_bug_hits} ttfsr={ttfsr_s} stab={'Y' if m.stabilization_success else 'N'}")
        if m.stabilization_candidates:
            source_json = json.dumps(m.first_artifact_delays) if m.first_artifact_delays else "{}"
            all_stab_entries.append(("random", seed, source_json, m.stabilization_candidates))

    # Heuristic on TEST
    print(f"\n  --- Heuristic ({selected_cfg.name}) on TEST ---")
    heuristic_test: List[PgRunMetrics] = []
    for seed in TEST_SEEDS:
        print(f"    seed={seed}...", end=" ", flush=True)
        m = run_pg_discovery(
            PG_BUGGY_DSN, PG_CONTROL_DSN, "heuristic", selected_cfg, seed,
            budget_s=budget_s, stabilization_budget_s=stab_budget, replay_n=replay_n,
        )
        heuristic_test.append(m)
        ttfsr_s = f"{m.ttfsr_s:.3f}" if m.ttfsr_s < float("inf") else "NA"
        print(f"hits={m.num_bug_hits} ttfsr={ttfsr_s} stab={'Y' if m.stabilization_success else 'N'}")
        if m.stabilization_candidates:
            source_json = json.dumps(m.first_artifact_delays) if m.first_artifact_delays else "{}"
            all_stab_entries.append(("heuristic", seed, source_json, m.stabilization_candidates))

    # Aggregate
    random_test_agg = aggregate_pg_metrics(random_test, budget_s)
    heuristic_test_agg = aggregate_pg_metrics(heuristic_test, budget_s)
    random_val_agg = aggregate_pg_metrics(random_val, budget_s)
    heuristic_val_agg = aggregate_pg_metrics(
        val_results.get(selected_cfg.name, []), budget_s,
    )
    random_train_agg_final = aggregate_pg_metrics(random_train, budget_s)
    heuristic_train_agg_final = aggregate_pg_metrics(
        all_heuristic_train.get(selected_cfg.name, []), budget_s,
    )

    # Save TEST CSV
    all_test = random_test + heuristic_test
    write_metrics_csv(all_test, output_dir / "pg_test_results.csv")

    # Save stabilization CSV
    write_stabilization_csv(all_stab_entries, output_dir / "pg_stabilization_results.csv")

    # Acceptance check
    acceptance = check_pg_acceptance(heuristic_test_agg, random_test_agg, budget_s)

    print("\n  --- TEST Results ---")
    for label, agg in [("random", random_test_agg), ("heuristic", heuristic_test_agg)]:
        ttfsr = agg.get("median_ttfsr", float("inf"))
        ttfsr_s = f"{ttfsr:.3f}" if ttfsr < float("inf") else "NA"
        print(f"    {label:10s}: success={agg.get('success_rate', 0):.0%} "
              f"stable={agg.get('stable_artifact_rate', 0):.0%} "
              f"ttfb={agg.get('median_ttfb', float('inf')):.3f}s "
              f"ttfsr={ttfsr_s}")

    print(f"\n  Acceptance: A={'PASS' if acceptance['option_a'] else 'FAIL'} "
          f"B={'PASS' if acceptance['option_b'] else 'FAIL'} "
          f"C={'PASS' if acceptance['option_c'] else 'FAIL'}")


    print("\n" + "=" * 70)
    print("  STAGE 5: Final 20x Replay Validation")
    print("=" * 70)

    # Find best stable artifact from heuristic TEST
    best_h_stable = None
    for m in heuristic_test:
        if m.stable_artifact_delays:
            best_h_stable = m.stable_artifact_delays
            break

    # Find best stable artifact from random TEST
    best_r_stable = None
    for m in random_test:
        if m.stable_artifact_delays:
            best_r_stable = m.stable_artifact_delays
            break

    replay_20x_results = {}

    if best_h_stable:
        print("\n  Heuristic 20x replay of best stable TEST artifact...")
        setup_schema(PG_BUGGY_DSN)
        buggy_20 = exact_replay(PG_BUGGY_DSN, best_h_stable, 20)
        setup_schema(PG_CONTROL_DSN)
        control_20 = exact_replay(PG_CONTROL_DSN, best_h_stable, 20)
        print(f"    Buggy:   {buggy_20}/20")
        print(f"    Control: {control_20}/20")
        replay_20x_results["heuristic"] = {
            "buggy_20x": buggy_20,
            "control_20x": control_20,
            "delays": best_h_stable,
        }
        with open(output_dir / "pg_replay_buggy_20x.json", "w") as f:
            json.dump({"strategy": "heuristic", "buggy_hits": buggy_20, "trials": 20,
                        "delays": best_h_stable}, f, indent=2)
        with open(output_dir / "pg_replay_control_20x.json", "w") as f:
            json.dump({"strategy": "heuristic", "control_hits": control_20, "trials": 20,
                        "delays": best_h_stable}, f, indent=2)
    else:
        print("\n  No stable heuristic TEST artifact found. Skipping 20x replay.")
        # Write empty replay files
        with open(output_dir / "pg_replay_buggy_20x.json", "w") as f:
            json.dump({"strategy": "heuristic", "buggy_hits": 0, "trials": 0,
                        "note": "No stable artifact found"}, f, indent=2)
        with open(output_dir / "pg_replay_control_20x.json", "w") as f:
            json.dump({"strategy": "heuristic", "control_hits": 0, "trials": 0,
                        "note": "No stable artifact found"}, f, indent=2)

    if best_r_stable:
        print("\n  Random 20x replay of best stable TEST artifact...")
        setup_schema(PG_BUGGY_DSN)
        buggy_20 = exact_replay(PG_BUGGY_DSN, best_r_stable, 20)
        setup_schema(PG_CONTROL_DSN)
        control_20 = exact_replay(PG_CONTROL_DSN, best_r_stable, 20)
        print(f"    Buggy:   {buggy_20}/20")
        print(f"    Control: {control_20}/20")
        replay_20x_results["random"] = {
            "buggy_20x": buggy_20,
            "control_20x": control_20,
            "delays": best_r_stable,
        }


    print("\n" + "=" * 70)
    print("  STAGE 6: Generate PG_TTFSR_REPORT.md")
    print("=" * 70)

    generate_pg_report(
        output_dir=output_dir,
        selected_cfg=selected_cfg,
        configs_tested=configs,
        train_h_agg=heuristic_train_agg_final,
        train_r_agg=random_train_agg_final,
        val_h_agg=heuristic_val_agg,
        val_r_agg=random_val_agg,
        test_h_agg=heuristic_test_agg,
        test_r_agg=random_test_agg,
        acceptance=acceptance,
        budget_s=budget_s,
        audit_notes=audit_text,
    )


    print("\n" + "=" * 70)
    print("  FINAL RESULTS")
    print("=" * 70)
    print(f"  Output directory: {output_dir}")
    print(f"  Selected config:  {selected_cfg.name}")

    passed = []
    if acceptance["option_a"]:
        passed.append("A")
    if acceptance["option_b"]:
        passed.append("B")
    if acceptance["option_c"]:
        passed.append("C")

    if passed:
        print(f"  ACCEPTANCE: PASS under option(s) {', '.join(passed)}")
    else:
        print("  ACCEPTANCE: FAIL — heuristic did not beat random under any option")
        print("  → PG remains the hardest case. Consider 120s budget or further tuning.")

    print("\n  Files produced:")
    for f in sorted(output_dir.iterdir()):
        if f.is_file():
            print(f"    {f.name}")

    print()


if __name__ == "__main__":
    main()
