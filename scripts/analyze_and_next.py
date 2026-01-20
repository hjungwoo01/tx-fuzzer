"""
Orchestrate the full analysis and next workload generation workflow.

This script:
1. Extracts isolation level from workload YAML
2. Runs Elle checker with the correct model
3. Runs analyze_bec1.py and analyze_beck.py
4. Generates the next workload using the selected profile
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import yaml
except ImportError as exc:
    raise SystemExit("[ERROR] Missing dependency pyyaml. Install with `pip install pyyaml`.") from exc


def load_yaml(path: Path) -> Dict[str, Any]:
    """Load a YAML file."""
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def extract_isolation_level(workload_path: Path) -> Optional[str]:
    """Extract the isolation level from a workload YAML file."""
    cfg = load_yaml(workload_path)
    workload = cfg.get("workload", {})
    transactions = workload.get("transactions", [])
    
    if not transactions:
        return None
    
    # Get the first transaction's isolation level
    for txn in transactions:
        isolation = txn.get("isolation")
        if isolation:
            return str(isolation).strip().upper()
    
    return None


def isolation_to_elle_model(isolation: Optional[str]) -> str:
    """Map PostgreSQL isolation level to Elle model name."""
    if not isolation:
        return "read-committed"
    
    isolation_upper = isolation.upper()
    
    # Map PostgreSQL isolation levels to Elle models
    mapping = {
        "REPEATABLE READ": "snapshot-isolation",
        "READ COMMITTED": "read-committed",
        "READ UNCOMMITTED": "read-uncommitted",
        "SERIALIZABLE": "serializable",
    }
    
    # Also handle aliases
    alias_mapping = {
        "RR": "snapshot-isolation",
        "RC": "read-committed",
        "RU": "read-uncommitted",
        "SERIALIZABLE": "serializable",
    }
    
    # Try exact match first
    if isolation_upper in mapping:
        return mapping[isolation_upper]
    
    # Try alias
    if isolation_upper in alias_mapping:
        return alias_mapping[isolation_upper]
    
    # Default fallback
    print(f"Warning: Unknown isolation level '{isolation}', defaulting to 'read-committed'", file=sys.stderr)
    return "read-committed"


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return True if successful."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"Error: {description} failed with exit code {result.returncode}", file=sys.stderr)
        return False
    return True


def load_profile(path: Path) -> Dict[str, Any]:
    """Load a JSON profile file."""
    if not path.exists():
        return {}
    try:
        content = path.read_text(encoding="utf-8").strip()
        if not content or content == "null":
            return {}
        return json.loads(content)
    except (json.JSONDecodeError, Exception) as e:
        print(f"Warning: Could not load profile from {path}: {e}", file=sys.stderr)
        return {}


def select_profile(bec1_path: Path, beck_path: Path, strategy: Optional[str]) -> tuple[Path, str]:
    """Select which profile to use based on strategy or availability."""
    bec1_profile = load_profile(bec1_path)
    beck_profile = load_profile(beck_path)
    
    has_bec1 = bool(bec1_profile.get("target_key") or bec1_profile.get("strategy"))
    has_beck = bool(beck_profile.get("targets") or beck_profile.get("strategy"))
    
    if strategy:
        if strategy.lower() == "bec1" or strategy.lower() == "bec-1":
            if has_bec1:
                return bec1_path, "bec1"
            print("Warning: BEC-1 profile requested but not available, trying BEC-k", file=sys.stderr)
        elif strategy.lower() in ("beck", "bec-k", "k"):
            if has_beck:
                return beck_path, "beck"
            print("Warning: BEC-k profile requested but not available, trying BEC-1", file=sys.stderr)
    
    # Auto-select: prefer BEC-1 if available, otherwise BEC-k
    if has_bec1:
        return bec1_path, "bec1"
    if has_beck:
        return beck_path, "beck"
    
    # Neither available
    print("Warning: No valid profiles found. Using BEC-1 as fallback.", file=sys.stderr)
    return bec1_path, "bec1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrate analysis and next workload generation"
    )
    parser.add_argument(
        "--workload",
        type=Path,
        default=Path("workloads/next.yaml"),
        help="Path to the workload YAML file that was run (default: workloads/next.yaml)",
    )
    parser.add_argument(
        "--history",
        type=Path,
        default=Path("history.edn"),
        help="Path to the history EDN file (default: history.edn)",
    )
    parser.add_argument(
        "--base",
        type=Path,
        default=Path("workloads/next.yaml"),
        help="Base workload YAML for generating next workload (default: workloads/next.yaml)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("workloads/next.yaml"),
        help="Output path for next workload (default: workloads/next.yaml)",
    )
    parser.add_argument(
        "--strategy",
        choices=["bec1", "beck"],
        help="Force use of BEC-1 or BEC-k profile (default: auto-select)",
    )
    parser.add_argument(
        "--skip-check",
        action="store_true",
        help="Skip running Elle checker",
    )
    parser.add_argument(
        "--skip-analyze",
        action="store_true",
        help="Skip running analysis scripts (use existing profiles)",
    )
    parser.add_argument(
        "--skip-next",
        action="store_true",
        help="Skip generating next workload (only run analysis)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    
    # Step 1: Extract isolation level from workload
    print(f"\n{'='*60}")
    print("Step 1: Extracting isolation level from workload")
    print(f"{'='*60}")
    
    if not args.workload.exists():
        print(f"Error: Workload file not found: {args.workload}", file=sys.stderr)
        return 1
    
    isolation = extract_isolation_level(args.workload)
    elle_model = isolation_to_elle_model(isolation)
    
    print(f"Workload: {args.workload}")
    print(f"Isolation level: {isolation or 'N/A (defaulting to read-committed)'}")
    print(f"Elle model: {elle_model}")
    
    # Step 2: Run Elle checker
    if not args.skip_check:
        if not args.history.exists():
            print(f"Error: History file not found: {args.history}", file=sys.stderr)
            return 1
        
        elle_cmd = [
            "clj",
            "-J-Djava.awt.headless=true",
            "-M:run",
            str(args.history),
            "--model",
            elle_model,
        ]
        
        if not run_command(elle_cmd, "Elle consistency checker"):
            print("Warning: Elle checker failed, continuing anyway...", file=sys.stderr)
    else:
        print("\nSkipping Elle checker (--skip-check)")
    
    # Step 3: Run analysis scripts
    if not args.skip_analyze:
        if not args.history.exists():
            print(f"Error: History file not found: {args.history}", file=sys.stderr)
            return 1
        
        bec1_cmd = ["python", "scripts/analyze_bec1.py", "--history", str(args.history)]
        beck_cmd = ["python", "scripts/analyze_beck.py", "--history", str(args.history)]
        
        if not run_command(bec1_cmd, "BEC-1 analysis"):
            print("Warning: BEC-1 analysis failed", file=sys.stderr)
        
        if not run_command(beck_cmd, "BEC-k analysis"):
            print("Warning: BEC-k analysis failed", file=sys.stderr)
    else:
        print("\nSkipping analysis scripts (--skip-analyze)")
    
    # Step 4: Select profile and generate next workload
    if args.skip_next:
        print(f"\n{'='*60}")
        print("Skipping next workload generation (--skip-next)")
        print(f"{'='*60}\n")
        return 0
    
    print(f"\n{'='*60}")
    print("Step 4: Generating next workload")
    print(f"{'='*60}")
    
    bec1_profile_path = Path("bec1_target.json")
    beck_profile_path = Path("beck_target.json")
    
    profile_path, strategy_used = select_profile(bec1_profile_path, beck_profile_path, args.strategy)
    
    print(f"Selected profile: {profile_path} (strategy: {strategy_used})")
    
    next_cmd = [
        "python",
        "scripts/next_workload_from_profile.py",
        "--base",
        str(args.base),
        "--profile",
        str(profile_path),
        "--out",
        str(args.out),
    ]
    
    if not run_command(next_cmd, "Next workload generation"):
        print("Error: Failed to generate next workload", file=sys.stderr)
        return 1
    
    print(f"\n{'='*60}")
    print("Success! Next workload generated:")
    print(f"  {args.out}")
    print(f"{'='*60}\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
