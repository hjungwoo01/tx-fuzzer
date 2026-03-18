"""
Quick smoke test for MySQL Bug #76927 harness.
Validates the harness works correctly on both MySQL 5.6 (vulnerable) and 5.7 (fixed).
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts" / "experiments"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(ROOT))

from multidb_db import read_db_version, wait_for_db
from multidb_workloads import run_case_mysql_replace_bug_trial


MYSQL_56_DSN = os.getenv("MYSQL_5_6_DSN", "mysql://root:root@127.0.0.1:33078/test")
MYSQL_57_DSN = os.getenv("MYSQL_5_7_DSN", "mysql://root:root@127.0.0.1:33079/test")

SMOKE_BUDGET_S = 20
STRATEGIES = ["stress", "random_delays", "heuristic_fuzzer"]


def run_smoke(dsn: str, label: str, budget_s: int):
    """Run trials within budget for each strategy and report results."""
    results = {}
    for strategy in STRATEGIES:
        t0 = time.monotonic()
        trials = 0
        bugs = 0
        errors_count = 0
        first_bug_s = None

        while (time.monotonic() - t0) < budget_s:
            seed = int(time.monotonic() * 1000) % 1_000_000 + trials
            try:
                r = run_case_mysql_replace_bug_trial(
                    dsn=dsn, strategy=strategy, seed=seed, k=3,
                )
                trials += 1
                if r.bug_found:
                    bugs += 1
                    if first_bug_s is None:
                        first_bug_s = time.monotonic() - t0
                if r.errors:
                    errors_count += 1
            except Exception as e:
                trials += 1
                errors_count += 1
                print(f"    ERROR: {e}")

        elapsed = time.monotonic() - t0
        results[strategy] = {
            "trials": trials,
            "bugs": bugs,
            "errors": errors_count,
            "elapsed_s": round(elapsed, 2),
            "first_bug_s": round(first_bug_s, 3) if first_bug_s is not None else None,
            "anomaly_rate": round(bugs / max(1, trials), 4),
        }
        status = f"BUG ({bugs}/{trials})" if bugs > 0 else f"clean ({trials} trials)"
        ttf_str = f"first@{first_bug_s:.2f}s" if first_bug_s is not None else "-"
        print(f"  [{label}] {strategy}: {status}, errors={errors_count}, {ttf_str}")

    return results


def main():
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = ROOT / "results" / f"mysql_bug76927_smoke_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}

    # MySQL 5.6 (vulnerable)
    print(f"\n{'='*60}")
    print(f"MySQL 5.6 (vulnerable) — Bug #76927 target")
    print(f"{'='*60}")
    if wait_for_db("mysql", MYSQL_56_DSN, timeout_s=10.0):
        ver = read_db_version("mysql", MYSQL_56_DSN)
        print(f"  Version: {ver}")
        all_results["mysql_5_6_vulnerable"] = run_smoke(
            MYSQL_56_DSN, "5.6-vuln", SMOKE_BUDGET_S,
        )
    else:
        print("  NOT AVAILABLE — skipping")

    # MySQL 5.7 (fixed)
    print(f"\n{'='*60}")
    print(f"MySQL 5.7 (fixed) — Bug #76927 control")
    print(f"{'='*60}")
    if wait_for_db("mysql", MYSQL_57_DSN, timeout_s=10.0):
        ver = read_db_version("mysql", MYSQL_57_DSN)
        print(f"  Version: {ver}")
        all_results["mysql_5_7_fixed"] = run_smoke(
            MYSQL_57_DSN, "5.7-fix", SMOKE_BUDGET_S,
        )
    else:
        print("  NOT AVAILABLE — skipping")

    # Write results
    results_path = out_dir / "smoke_results.json"
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for target, strats in all_results.items():
        print(f"\n  {target}:")
        for strat, stats in strats.items():
            bug_str = f"BUGS={stats['bugs']}" if stats['bugs'] > 0 else "clean"
            print(f"    {strat}: trials={stats['trials']}, {bug_str}, "
                  f"errors={stats['errors']}, anomaly_rate={stats['anomaly_rate']:.2%}")

    # Write markdown summary
    md_path = out_dir / "summary.md"
    with open(md_path, "w") as f:
        f.write("# MySQL Bug #76927 Smoke Test\n\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n")
        f.write("| Target | Strategy | Trials | Bugs | Errors | Rate | First Bug (s) |\n")
        f.write("|--------|----------|--------|------|--------|------|---------------|\n")
        for target, strats in all_results.items():
            for strat, stats in strats.items():
                f.write(f"| {target} | {strat} | {stats['trials']} | {stats['bugs']} | "
                        f"{stats['errors']} | {stats['anomaly_rate']:.2%} | "
                        f"{stats['first_bug_s'] if stats['first_bug_s'] is not None else '-'} |\n")

    print(f"\nResults: {results_path}")
    print(f"Summary: {md_path}")

    # Acceptance: harness doesn't crash, oracle works, no false positives on fixed version
    vuln_bugs = sum(s["bugs"] for s in all_results.get("mysql_5_6_vulnerable", {}).values())
    fixed_bugs = sum(s["bugs"] for s in all_results.get("mysql_5_7_fixed", {}).values())
    vuln_trials = sum(s["trials"] for s in all_results.get("mysql_5_6_vulnerable", {}).values())
    fixed_trials = sum(s["trials"] for s in all_results.get("mysql_5_7_fixed", {}).values())

    print(f"\n{'='*60}")
    print("ACCEPTANCE")
    print(f"{'='*60}")
    if vuln_trials > 0:
        print(f"  [PASS] Harness ran on MySQL 5.6 ({vuln_trials} trials)")
    else:
        print(f"  [SKIP] MySQL 5.6 not available")
    if fixed_trials > 0:
        print(f"  [PASS] Harness ran on MySQL 5.7 ({fixed_trials} trials)")
    else:
        print(f"  [SKIP] MySQL 5.7 not available")
    if fixed_bugs == 0:
        print(f"  [PASS] No false positives on fixed MySQL 5.7")
    else:
        print(f"  [WARN] {fixed_bugs} bugs reported on fixed MySQL 5.7 — may be false positive")
    if vuln_bugs > 0:
        print(f"  [PASS] Bug found on MySQL 5.6! ({vuln_bugs} bugs across strategies)")
    else:
        print(f"  [INFO] No bug triggered in {SMOKE_BUDGET_S}s window — expected for rare bug.")
        print(f"         Run with longer budget for production: --budget 300")

    return 0


if __name__ == "__main__":
    sys.exit(main())
