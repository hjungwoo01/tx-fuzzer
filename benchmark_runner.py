"""
Benchmark runner for measuring Time-To-Failure (TTF) of Write Skew detection.

This script supports two modes:
1. Random: Loops diluted_skew.yaml until a cycle is detected
2. BEC-1: Uses BEC-1 analysis to generate a targeted attack workload

The benchmark uses a "mismatch strategy":
- Database: Configured as READ COMMITTED (Weak)
- Checker: Configured as SERIALIZABLE (Strict)
This simulates an application that requires strict consistency but was
accidentally deployed on a weak isolation level.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import yaml
except ImportError as exc:
    raise SystemExit("[ERROR] Missing dependency pyyaml. Install with `pip install yaml`.") from exc


def load_yaml(path: Path) -> Dict[str, Any]:
    """Load a YAML file."""
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def extract_elle_model(workload_path: Path) -> str:
    """Extract the Elle consistency model from a workload YAML file."""
    cfg = load_yaml(workload_path)
    
    # Check for top-level consistency_model first
    consistency_model = cfg.get("consistency_model")
    if consistency_model:
        model_str = str(consistency_model).strip().lower()
        model_aliases = {
            "serializable": "serializable",
            "strict-serializable": "strict-serializable",
            "snapshot-isolation": "snapshot-isolation",
            "si": "snapshot-isolation",
            "repeatable-read": "repeatable-read",
            "rr": "repeatable-read",
            "read-committed": "read-committed",
            "rc": "read-committed",
        }
        if model_str in model_aliases:
            return model_aliases[model_str]
        return model_str
    
    # Fall back to default
    return "read-committed"


def run_workload(workload_path: Path, history_path: Path) -> bool:
    """Run a workload and generate a history file. Returns True if successful."""
    print(f"Running workload: {workload_path}")
    cmd = ["go", "run", "./cmd/runner", "-config", str(workload_path), "-out", str(history_path)]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running workload: {result.stderr}", file=sys.stderr)
        return False
    return True


def check_validity(history_path: Path, elle_model: str) -> tuple[bool, Optional[int]]:
    """Check if a history is valid according to Elle.
    
    Returns:
        (is_valid, exit_code) where is_valid is True if valid, False if invalid,
        and exit_code is the Elle exit code (0=valid, 4=invalid, 3=unknown).
    """
    if not history_path.exists():
        return (False, None)
    
    cmd = [
        "clj",
        "-J-Djava.awt.headless=true",
        "-M:run",
        str(history_path),
        "--model",
        elle_model,
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    exit_code = result.returncode
    
    # Elle exit codes:
    # 0 = valid (no violations)
    # 3 = unknown
    # 4 = invalid (violations detected)
    is_valid = (exit_code == 0)
    
    if exit_code == 0:
        print("  ✓ History is VALID (no cycles detected)")
    elif exit_code == 4:
        print("  ✗ History is INVALID (cycle detected - FAILURE!)")
    else:
        print(f"  ? History validity UNKNOWN (exit code {exit_code})")
    
    return (is_valid, exit_code)


def run_mode_random(workload_path: Path, max_iterations: int = 100) -> Optional[float]:
    """Run Random mode: loop diluted_skew.yaml until failure is detected.
    
    Returns the time-to-failure in seconds, or None if max_iterations reached.
    """
    print("\n" + "="*60)
    print("MODE: Random")
    print("="*60)
    
    elle_model = extract_elle_model(workload_path)
    print(f"Elle model: {elle_model}")
    print(f"Max iterations: {max_iterations}")
    
    start_time = time.time()
    history_path = Path("benchmark_history.edn")
    
    for iteration in range(1, max_iterations + 1):
        print(f"\n--- Iteration {iteration} ---")
        iter_start = time.time()
        
        # Run workload
        if not run_workload(workload_path, history_path):
            print(f"Error: Failed to run workload at iteration {iteration}", file=sys.stderr)
            return None
        
        # Check validity
        is_valid, exit_code = check_validity(history_path, elle_model)
        
        iter_duration = time.time() - iter_start
        print(f"  Duration: {iter_duration:.2f}s")
        
        if not is_valid:
            # Failure detected
            total_time = time.time() - start_time
            print(f"\n{'='*60}")
            print(f"FAILURE DETECTED at iteration {iteration}")
            print(f"Time-to-Failure: {total_time:.2f} seconds")
            print(f"{'='*60}\n")
            return total_time
    
    # No failure detected within max_iterations
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"No failure detected after {max_iterations} iterations")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"{'='*60}\n")
    return None


def run_mode_bec1(workload_path: Path) -> Optional[float]:
    """Run BEC-1 mode: exploration -> analysis -> retargeting -> attack.
    
    Returns the total duration in seconds, or None if any step fails.
    """
    print("\n" + "="*60)
    print("MODE: BEC-1")
    print("="*60)
    
    elle_model = extract_elle_model(workload_path)
    print(f"Elle model: {elle_model}")
    
    start_time = time.time()
    exploration_history = Path("exploration_history.edn")
    attack_history = Path("attack_history.edn")
    next_workload = Path("workloads/next.yaml")
    bec1_profile = Path("bec1_target.json")
    
    # Step 1: Run exploration workload (diluted_skew.yaml)
    print("\n--- Step 1: Exploration (diluted_skew.yaml) ---")
    if not run_workload(workload_path, exploration_history):
        print("Error: Failed to run exploration workload", file=sys.stderr)
        return None
    
    is_valid, exit_code = check_validity(exploration_history, elle_model)
    if not is_valid:
        print("\n⚠️  WARNING: Exploration workload failed immediately!")
        print("   This suggests the search space is too small.")
        print("   Aborting BEC-1 benchmark.")
        return None
    
    print("  ✓ Exploration passed (as expected)")
    
    # Step 2: Run BEC-1 analysis
    print("\n--- Step 2: BEC-1 Analysis ---")
    print("  (This may take a while for large histories...)")
    
    # Check history file size
    if exploration_history.exists():
        size_mb = exploration_history.stat().st_size / (1024 * 1024)
        print(f"  History file size: {size_mb:.1f} MB")
        if size_mb > 50:
            print("  Warning: Large history file - analysis may be slow")
    
    bec1_cmd = ["python", "scripts/analyze_bec1.py", "--history", str(exploration_history)]
    if size_mb > 50:
        # limit to 5000 transactions
        bec1_cmd.extend(["--max-transactions", "5000"])
        print("  Using --max-transactions=5000 to speed up analysis")
    
    try:
        result = subprocess.run(
            bec1_cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes
        )
    except subprocess.TimeoutExpired:
        print("Error: BEC-1 analysis timed out after 5 minutes", file=sys.stderr)
        print("  Consider reducing workload duration or keyspace size", file=sys.stderr)
        return None
    
    if result.returncode != 0:
        error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
        if not error_msg:
            error_msg = f"Exit code {result.returncode}"
        print(f"Error: BEC-1 analysis failed: {error_msg}", file=sys.stderr)
        if result.stdout:
            print(f"stdout: {result.stdout}", file=sys.stderr)
        return None
    
    if not bec1_profile.exists():
        print("Error: BEC-1 profile not generated", file=sys.stderr)
        return None
    
    # Check if profile has a target
    profile_data = json.loads(bec1_profile.read_text())
    if not profile_data.get("target_key"):
        print("Warning: BEC-1 profile has no target_key", file=sys.stderr)
    
    print("  ✓ BEC-1 analysis complete")
    
    # Step 3: Generate retargeted workload
    print("\n--- Step 3: Retargeting (generate next.yaml) ---")
    retarget_cmd = [
        "python",
        "scripts/next_workload_from_profile.py",
        "--base",
        str(workload_path),
        "--profile",
        str(bec1_profile),
        "--out",
        str(next_workload),
    ]
    result = subprocess.run(retarget_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
        if not error_msg:
            error_msg = f"Exit code {result.returncode}"
        print(f"Error: Retargeting failed: {error_msg}", file=sys.stderr)
        if result.stdout:
            print(f"stdout: {result.stdout}", file=sys.stderr)
        return None
    
    if not next_workload.exists():
        print("Error: Retargeted workload not generated", file=sys.stderr)
        return None
    
    print("  ✓ Retargeted workload generated")
    
    # Step 4: Run attack workload (next.yaml)
    print("\n--- Step 4: Attack (next.yaml) ---")
    if not run_workload(next_workload, attack_history):
        print("Error: Failed to run attack workload", file=sys.stderr)
        return None
    
    is_valid, exit_code = check_validity(attack_history, elle_model)
    
    if is_valid:
        print("\n⚠️  WARNING: Attack workload passed (unexpected)")
        print("   BEC-1 targeting may not have been effective.")
    else:
        print("\n  ✓ Attack failed (as expected - cycle detected)")
    
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"BEC-1 Benchmark Complete")
    print(f"Total duration: {total_time:.2f} seconds")
    print(f"{'='*60}\n")
    return total_time


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark runner for Time-To-Failure measurement"
    )
    parser.add_argument(
        "--mode",
        choices=["random", "bec1", "both"],
        default="both",
        help="Benchmark mode: random, bec1, or both (default: both)",
    )
    parser.add_argument(
        "--workload",
        type=Path,
        default=Path("workloads/diluted_skew.yaml"),
        help="Path to diluted_skew.yaml workload (default: workloads/diluted_skew.yaml)",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=100,
        help="Maximum iterations for Random mode (default: 100)",
    )
    
    args = parser.parse_args()
    
    if not args.workload.exists():
        print(f"Error: Workload file not found: {args.workload}", file=sys.stderr)
        return 1
    
    results = {}
    
    if args.mode in ("random", "both"):
        ttf = run_mode_random(args.workload, args.max_iterations)
        results["random"] = ttf
    
    if args.mode in ("bec1", "both"):
        duration = run_mode_bec1(args.workload)
        results["bec1"] = duration
    
    if len(results) > 1:
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        for mode, value in results.items():
            if value is not None:
                print(f"{mode.upper()}: {value:.2f} seconds")
            else:
                print(f"{mode.upper()}: No result (timeout or error)")
        print("="*60 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
