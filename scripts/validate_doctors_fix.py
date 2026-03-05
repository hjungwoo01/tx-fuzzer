#!/usr/bin/env python3
"""
Quick targeted validation of doctors_write_skew variants.
Tests that:
  - buggy RR: write skew triggers (bug rate > 0)
  - fixed_ordered_locks RR: no bugs AND near-zero deadlocks
  - fixed (naive FOR UPDATE) RR: no bugs but high deadlock rate (reference)
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from workloads import doctors_write_skew as wl


DSN = os.getenv("PG_DSN", "postgres://postgres:postgres@localhost:54326/postgres?sslmode=disable")
REPS = 3
TRIALS_PER_REP = 100
TRIALS_NAIVE_REP = 10  # Naive FOR UPDATE is too slow due to deadlock timeouts; small sample suffices
ISOLATION = "REPEATABLE READ"


def run_variant(variant: str, trials: int, seed_base: int):
    """Run trials for a variant, return summary stats."""
    bugs = 0
    deadlocks = 0
    serialization_failures = 0
    clean = 0
    errors_all = []

    for i in range(trials):
        result = wl.run_trial(
            dsn=DSN,
            isolation=ISOLATION,
            variant=variant,
            delays=None,
            seed=seed_base + i,
        )
        if result["bug_found"]:
            bugs += 1
        elif result.get("got_40P01"):
            deadlocks += 1
        elif result.get("got_40001"):
            serialization_failures += 1
        else:
            clean += 1
        errors_all.extend(result.get("errors", []))

    return {
        "variant": variant,
        "trials": trials,
        "bugs": bugs,
        "deadlocks": deadlocks,
        "serialization_failures": serialization_failures,
        "clean": clean,
        "bug_rate": bugs / max(1, trials),
        "deadlock_rate": deadlocks / max(1, trials),
    }


def main():
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = ROOT / "results" / f"patch_doctors_fixed_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    variants = [
        ("buggy", "Baseline buggy — should see write skew", TRIALS_PER_REP),
        ("fixed", "Naive FOR UPDATE — no bugs but high deadlocks", TRIALS_NAIVE_REP),
        ("fixed_ordered_locks", "Ordered locks — no bugs, no deadlocks", TRIALS_PER_REP),
    ]

    all_results = []
    for variant, desc, trials in variants:
        print(f"\n{'='*60}")
        print(f"Variant: {variant} — {desc}")
        print(f"{'='*60}")
        for rep in range(REPS):
            seed = 42 + rep * 10000
            print(f"  Rep {rep+1}/{REPS} (seed={seed})...", end=" ", flush=True)
            t0 = time.monotonic()
            stats = run_variant(variant, trials, seed)
            elapsed = time.monotonic() - t0
            stats["rep"] = rep
            stats["elapsed_s"] = round(elapsed, 2)
            all_results.append(stats)
            print(f"bugs={stats['bugs']}/{stats['trials']} "
                  f"deadlocks={stats['deadlocks']}/{stats['trials']} "
                  f"clean={stats['clean']} "
                  f"({elapsed:.1f}s)")

    # Write results
    results_path = out_dir / "doctors_fix_validation.json"
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults: {results_path}")

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"{'Variant':<30} {'Rep':>4} {'Bugs':>6} {'DLocks':>7} {'Clean':>6} {'BugRate':>10} {'DLRate':>10}")
    print("-"*70)
    for r in all_results:
        print(f"{r['variant']:<30} {r['rep']:>4} {r['bugs']:>6} {r['deadlocks']:>7} "
              f"{r['clean']:>6} {r['bug_rate']:>10.1%} {r['deadlock_rate']:>10.1%}")

    # Acceptance criteria checks
    print("\n" + "="*70)
    print("ACCEPTANCE CRITERIA")
    print("="*70)

    ordered_results = [r for r in all_results if r["variant"] == "fixed_ordered_locks"]
    total_ordered_bugs = sum(r["bugs"] for r in ordered_results)
    total_ordered_deadlocks = sum(r["deadlocks"] for r in ordered_results)
    total_ordered_trials = sum(r["trials"] for r in ordered_results)
    ordered_dl_rate = total_ordered_deadlocks / max(1, total_ordered_trials)

    buggy_results = [r for r in all_results if r["variant"] == "buggy"]
    total_buggy_bugs = sum(r["bugs"] for r in buggy_results)

    pass_all = True

    if total_buggy_bugs > 0:
        print(f"  [PASS] buggy variant triggers write skew ({total_buggy_bugs} bugs found)")
    else:
        print(f"  [FAIL] buggy variant did NOT trigger write skew!")
        pass_all = False

    if total_ordered_bugs == 0:
        print(f"  [PASS] fixed_ordered_locks: 0 bugs")
    else:
        print(f"  [FAIL] fixed_ordered_locks: {total_ordered_bugs} bugs!")
        pass_all = False

    if ordered_dl_rate <= 0.01:
        print(f"  [PASS] fixed_ordered_locks: deadlock rate = {ordered_dl_rate:.1%} (≤ 1%)")
    else:
        print(f"  [FAIL] fixed_ordered_locks: deadlock rate = {ordered_dl_rate:.1%} (> 1%)")
        pass_all = False

    if pass_all:
        print("\n  *** ALL ACCEPTANCE CRITERIA PASSED ***")
    else:
        print("\n  *** SOME CRITERIA FAILED — review above ***")

    # Write summary markdown
    md_path = out_dir / "summary.md"
    with open(md_path, "w") as f:
        f.write("# Doctors Write-Skew Fix Validation\n\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n")
        f.write(f"| Variant | Rep | Trials | Bugs | Deadlocks | Clean | Bug Rate | DL Rate |\n")
        f.write(f"|---------|-----|--------|------|-----------|-------|----------|----------|\n")
        for r in all_results:
            f.write(f"| {r['variant']} | {r['rep']} | {r['trials']} | {r['bugs']} | "
                    f"{r['deadlocks']} | {r['clean']} | {r['bug_rate']:.1%} | {r['deadlock_rate']:.1%} |\n")
        f.write(f"\n## Acceptance\n\n")
        f.write(f"- buggy: write skew triggered = {'YES' if total_buggy_bugs > 0 else 'NO'}\n")
        f.write(f"- fixed_ordered_locks: bugs = {total_ordered_bugs}, deadlock rate = {ordered_dl_rate:.1%}\n")
        f.write(f"- All criteria passed = {'YES' if pass_all else 'NO'}\n")
    print(f"\nSummary: {md_path}")

    return 0 if pass_all else 1


if __name__ == "__main__":
    sys.exit(main())
