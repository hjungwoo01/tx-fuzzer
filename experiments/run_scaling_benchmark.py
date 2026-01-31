from __future__ import annotations

import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError as exc:
    raise SystemExit("[ERROR] Missing pyyaml. pip install pyyaml") from exc

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("[ERROR] Missing matplotlib. pip install matplotlib") from exc

REPO_ROOT = Path(__file__).resolve().parent.parent
KEY_COUNTS = [1000, 5000, 10000, 25000, 50000]
STRATEGIES = ["random", "bec-1"]
N_TRIALS = 5
RANDOM_TIMEOUT_S = 120
BEC1_TIMEOUT_S = 300
BEC1_ANALYZE_TIMEOUT_S = 180 
TEMPLATE_PATH = REPO_ROOT / "workloads" / "templates" / "blind_write.yaml"
RETARGET_SCRIPT = REPO_ROOT / "scripts" / "next_workload_from_profile.py"
GENERATE_REPLAY_SCRIPT = REPO_ROOT / "scripts" / "generate_replay.py"
ANALYZE_SCRIPT = REPO_ROOT / "scripts" / "analyze_bec1.py"
OUTPUT_PNG = Path(__file__).resolve().parent / "benchmark_results.png"


def verify_retarget_patches_pivot() -> Tuple[bool, str]:
    """
    Verify that the retargeting script (next_workload_from_profile.py) exists
    and correctly patches the $1 pivot key in retargetable transactions.

    Returns:
        (ok, message)
    """
    if not RETARGET_SCRIPT.exists():
        return False, f"Retarget script not found: {RETARGET_SCRIPT}"

    text = RETARGET_SCRIPT.read_text()
    if 'key_spec != "$1"' not in text and 'key_spec != \'$1\'' not in text:
        return False, "Retarget script does not contain $1 pivot check (key_spec != \"$1\")"

    
    with tempfile.TemporaryDirectory() as td:
        base_path = Path(td) / "base.yaml"
        profile_path = Path(td) / "profile.json"
        out_path = Path(td) / "next.yaml"

        base = {
            "database": {"kind": "postgres", "dsn": "postgres://local/dummy"},
            "workload": {
                "datatype": "register",
                "transactions": [
                    {
                        "name": "pivot_txn",
                        "steps": [
                            {
                                "sql": "SELECT 1",
                                "args": [1],
                                "elle": {"f": ":read", "key": "$1"},
                            }
                        ],
                    }
                ],
                "mix": [{"txn": "pivot_txn", "weight": 1.0}],
            },
            "scheduling": {"duration": "1s", "clients": 1},
        }
        with base_path.open("w") as f:
            yaml.safe_dump(base, f, default_flow_style=False, sort_keys=False)

        profile_path.write_text(json.dumps({"target_key": 7, "weight": 0.9}) + "\n")

        cmd = [
            sys.executable,
            str(RETARGET_SCRIPT),
            "--base", str(base_path),
            "--profile", str(profile_path),
            "--out", str(out_path),
        ]
        r = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
        if r.returncode != 0:
            return False, f"Retarget dry-run failed: {r.stderr or r.stdout}"

        if not out_path.exists():
            return False, "Retarget did not produce output file"

        out_cfg = yaml.safe_load(out_path.read_text()) or {}
        txn_list = out_cfg.get("workload", {}).get("transactions", [])
        hot_txn = next((t for t in txn_list if t.get("name", "").startswith("hot_")), None)
        if not hot_txn:
            return False, "Retarget did not create hot_ transaction"

        for step in hot_txn.get("steps", []):
            elle = step.get("elle") or {}
            if elle.get("key") == "$1" or "key" in elle:
                args = step.get("args", [])
                if args and args[0] == 7:
                    return True, "Retarget correctly patches $1 pivot to target_key"
                return False, f"Retarget did not patch $1: args={args}, expected [7]"

    return False, "No retargetable step with $1 found in output"


def _set_keyspace(obj: Any, key_max: int) -> None:
    """Recursively set rand_int {min:1, max:?} to max=key_max."""
    if isinstance(obj, dict):
        ri = obj.get("rand_int")
        if isinstance(ri, dict) and ri.get("min") == 1 and "max" in ri:
            ri["max"] = key_max
        for v in obj.values():
            _set_keyspace(v, key_max)
    elif isinstance(obj, list):
        for v in obj:
            _set_keyspace(v, key_max)


def load_yaml(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(data: Dict[str, Any], p: Path) -> None:
    with p.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)


EXPLORATION_TXN_LIMIT = 20000


def generate_diluted(workload_path: Path, key_count: int, use_limit: bool = False) -> None:
    """Generate workload YAML with keyspace [1, key_count] from template.
    If use_limit=True, uses a strict transaction limit for BEC-1 exploration so the
    scout phase produces ~2k txns and analysis stays under ~1s.
    If use_limit=False, uses duration=30s (for Random testing - run until timeout/bug)."""
    cfg = load_yaml(TEMPLATE_PATH)
    _set_keyspace(cfg, key_count)
    sched = cfg.setdefault("scheduling", {})
    
    if use_limit:
        sched["limit"] = EXPLORATION_TXN_LIMIT
        sched["duration"] = "30s"
        
    else:
        sched.pop("limit", None)
        sched["duration"] = "30s"
        
    if "clients" not in sched:
        sched["clients"] = 4
    save_yaml(cfg, workload_path)


def extract_elle_model(workload_path: Path) -> str:
    cfg = load_yaml(workload_path)
    model = (cfg.get("consistency_model") or "read-committed")
    return str(model).strip().lower()


_DEVNULL = subprocess.DEVNULL


def run_workload(workload_path: Path, history_path: Path) -> bool:
    cmd = ["go", "run", "./cmd/runner", "-config", str(workload_path), "-out", str(history_path)]
    env = {**os.environ, "TXFUZZER_QUIET": "1"}
    r = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=_DEVNULL, stderr=_DEVNULL, env=env)
    return r.returncode == 0


def check_validity(history_path: Path, elle_model: str) -> Tuple[bool, Optional[int]]:
    """Check if history has consistency violations.
    
    Returns:
        (is_valid, exit_code) where:
        - is_valid=True: No violations (exit 0) or unknown (exit 3) - not counted as failure
        - is_valid=False: Actual violation detected (exit 4) - this is the failure
    """
    if not history_path.exists():
        return False, None
    cmd = [
        "clj", "-J-Djava.awt.headless=true", "-M:run",
        str(history_path), "--model", elle_model,
    ]
    r = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=_DEVNULL, stderr=_DEVNULL)
    # Elle exit codes:
    # 0 = valid (no violations)
    # 3 = unknown (analysis issue, not a violation - treat as valid for TTFF)
    # 4 = invalid (actual violation detected = failure)
    is_valid = (r.returncode != 4)  # Valid if not exit 4 (violation)
    return (is_valid, r.returncode)


def run_random(workload_path: Path, history_path: Path, elle_model: str) -> Optional[float]:
    """Run random loop until cycle or RANDOM_TIMEOUT_S. Returns TTFF in seconds or None."""
    start = time.time()
    it = 0
    print("Random searching: ", end="",flush=True)
    while (time.time() - start) < RANDOM_TIMEOUT_S:
        it += 1
        print(".", end="",flush=True)
        if not run_workload(workload_path, history_path):
            return None
        valid, _ = check_validity(history_path, elle_model)
        if not valid:
            return time.time() - start
    return None


def run_bec1(
    workload_path: Path,
    exploration_path: Path,
    profile_path: Path,
    next_attack_path: Path,
    attack_history_path: Path,
    elle_model: str,
    key_count: int,
) -> Tuple[Optional[float], bool]:
    """Exploration -> Analyze -> Retarget -> Attack. Returns (TTFF or None, timed_out).
    Aborts if wall-clock exceeds BEC1_TIMEOUT_S to avoid multi‑hour runs on large keys."""
    start = time.time()
    timed_out = False

    def elapsed() -> float:
        return time.time() - start

    def over() -> bool:
        if elapsed() > BEC1_TIMEOUT_S:
            nonlocal timed_out
            timed_out = True
            return True
        return False

    
    if over():
        print(f"[BEC-1] Timeout before exploration (elapsed: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)
    expl_start = time.time()
    if not run_workload(workload_path, exploration_path):
        print(f"[BEC-1] Exploration failed (elapsed: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)
    expl_time = time.time() - expl_start
    print(f"[BEC-1] Exploration complete in {expl_time:.1f}s (total: {elapsed():.1f}s)", file=sys.stderr)
  
    
    if over():
        print(f"[BEC-1] Timeout before analysis (elapsed: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)
    cmd = [sys.executable, str(ANALYZE_SCRIPT), "--history", str(exploration_path), "--out", str(profile_path)]
    
    analyze_start = time.time()
    try:
        r = subprocess.run(
            cmd, cwd=str(REPO_ROOT), stdout=_DEVNULL, stderr=None, timeout=BEC1_ANALYZE_TIMEOUT_S
        )
    except subprocess.TimeoutExpired:
        analyze_time = time.time() - analyze_start
        print(f"[BEC-1] Analysis TIMED OUT after {analyze_time:.1f}s (limit: {BEC1_ANALYZE_TIMEOUT_S}s, total: {elapsed():.1f}s)", file=sys.stderr)
        timed_out = True
        return (None, timed_out)
    analyze_time = time.time() - analyze_start
    print(f"[BEC-1] Analysis complete in {analyze_time:.1f}s (total: {elapsed():.1f}s)", file=sys.stderr)
    if r.returncode != 0 or not profile_path.exists():
        print(f"[BEC-1] Analysis failed (exit: {r.returncode}, profile exists: {profile_path.exists()}, total: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)

    try:
        with profile_path.open() as f:
            profile_data = json.load(f)
        target_key = profile_data.get("target_key")
        strategy = (profile_data.get("strategy") or "").strip().lower()
    except Exception as e:
        print(f"[BEC-1] Failed to load profile: {e}", file=sys.stderr)
        return (None, timed_out)
    if target_key is None:
        print(f"[BEC-1] No target key in profile (0 candidates found), skipping attack", file=sys.stderr)
        return (None, timed_out)

    if strategy == "interleave":
        # Deterministic replay: generate replay YAML; runner uses replay mode when config has replay.enabled: true
        if over():
            print(f"[BEC-1] Timeout before replay config (elapsed: {elapsed():.1f}s)", file=sys.stderr)
            return (None, timed_out)
        cmd = [
            sys.executable, str(GENERATE_REPLAY_SCRIPT),
            "--base", str(workload_path),
            "--profile", str(profile_path),
            "--out", str(next_attack_path),
        ]
        r = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
        if r.returncode != 0:
            print(f"[BEC-1] Generate replay failed (exit: {r.returncode}, total: {elapsed():.1f}s)", file=sys.stderr)
            if r.stderr:
                print(r.stderr, file=sys.stderr)
            return (None, timed_out)
        if not next_attack_path.exists():
            # Replay not applicable (e.g. reader/writer are read-only or write-only) → fall back to key-flood
            if over():
                print(f"[BEC-1] Timeout before retarget fallback (elapsed: {elapsed():.1f}s)", file=sys.stderr)
                return (None, timed_out)
            if r.stderr:
                print(r.stderr.strip(), file=sys.stderr)
            cmd = [
                sys.executable, str(RETARGET_SCRIPT),
                "--base", str(workload_path),
                "--profile", str(profile_path),
                "--out", str(next_attack_path),
            ]
            r2 = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=_DEVNULL, stderr=_DEVNULL)
            if r2.returncode != 0 or not next_attack_path.exists():
                print(f"[BEC-1] Retarget fallback failed (exit: {r2.returncode}, total: {elapsed():.1f}s)", file=sys.stderr)
                return (None, timed_out)
            attack_cfg = load_yaml(next_attack_path)
            attack_sched = attack_cfg.setdefault("scheduling", {})
            attack_sched.pop("limit", None)
            attack_sched["duration"] = "10s"
            if "clients" not in attack_sched:
                attack_sched["clients"] = 4
            save_yaml(attack_cfg, next_attack_path)
    else:
        # Legacy key fuzzing: weighted mix retarget and run Go in normal mode
        if over():
            print(f"[BEC-1] Timeout before retarget (elapsed: {elapsed():.1f}s)", file=sys.stderr)
            return (None, timed_out)
        cmd = [
            sys.executable, str(RETARGET_SCRIPT),
            "--base", str(workload_path),
            "--profile", str(profile_path),
            "--out", str(next_attack_path),
        ]
        r = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=_DEVNULL, stderr=_DEVNULL)
        if r.returncode != 0 or not next_attack_path.exists():
            print(f"[BEC-1] Retarget failed (exit: {r.returncode}, total: {elapsed():.1f}s)", file=sys.stderr)
            return (None, timed_out)
        attack_cfg = load_yaml(next_attack_path)
        attack_sched = attack_cfg.setdefault("scheduling", {})
        attack_sched.pop("limit", None)
        attack_sched["duration"] = "10s"
        if "clients" not in attack_sched:
            attack_sched["clients"] = 4
        save_yaml(attack_cfg, next_attack_path)

    if over():
        print(f"[BEC-1] Timeout before attack (elapsed: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)
    t_attack_start = time.time()
    if not run_workload(next_attack_path, attack_history_path):
        print(f"[BEC-1] Attack workload failed (total: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)
    t_attack = time.time() - t_attack_start
    t_validate_start = time.time()
    valid, exit_code = check_validity(attack_history_path, elle_model)
    t_validate = time.time() - t_validate_start
    print(
        f"[BEC-1] Attack (runner): {t_attack:.2f}s | Validation (Elle): {t_validate:.2f}s (total: {elapsed():.1f}s)",
        file=sys.stderr,
    )
    if valid:
        print(f"[BEC-1] Attack found no bug (exit: {exit_code}, total: {elapsed():.1f}s)", file=sys.stderr)
        return (None, timed_out)
    print(f"[BEC-1] SUCCESS: Bug found! (exit: {exit_code}, total: {elapsed():.1f}s)", file=sys.stderr)
    return (elapsed(), timed_out)


def main() -> int:
    ok, msg = verify_retarget_patches_pivot()
    if not ok:
        print(f"[ERROR] {msg}", file=sys.stderr)
        return 1
    print(f"[OK] {msg}")

    if not TEMPLATE_PATH.exists():
        print(f"[ERROR] Template not found: {TEMPLATE_PATH}", file=sys.stderr)
        return 1

    work_dir = REPO_ROOT / "experiments"
    work_dir.mkdir(exist_ok=True)
    random_workload_path = work_dir / "random_workload.yaml"
    bec1_workload_path = work_dir / "bec1_workload.yaml"
    exploration_path = work_dir / "exploration_history.edn"
    profile_path = work_dir / "bec1_target.json"
    next_attack_path = work_dir / "next_attack.yaml"
    attack_history_path = work_dir / "attack_history.edn"
    history_path = work_dir / "history.edn"

    results: Dict[str, Dict[int, Tuple[Optional[float], float]]] = {"random": {}, "bec-1": {}}

    for i, key_count in enumerate(KEY_COUNTS):
        print(f"--- Key count {key_count} ---")
        generate_diluted(random_workload_path, key_count, use_limit=False)
        generate_diluted(bec1_workload_path, key_count, use_limit=True)
        elle_model = extract_elle_model(random_workload_path)

        for strategy in STRATEGIES:
            values: List[Optional[float]] = []
            for _ in range(N_TRIALS):
                if strategy == "random":
                    values.append(run_random(random_workload_path, history_path, elle_model))
                else:
                    ttff, _ = run_bec1(
                        bec1_workload_path, exploration_path, profile_path,
                        next_attack_path, attack_history_path, elle_model, key_count,
                    )
                    values.append(ttff)
            successes = [v for v in values if v is not None]
            n_ok, n_to = len(successes), len(values) - len(successes)
            if n_ok == 0:
                results[strategy][key_count] = (None, 0.0)
                print(f"  {strategy}: timeout ({n_to}/{len(values)} trials)")
            else:
                mean_val = sum(successes) / n_ok
                std_val = statistics.stdev(successes) if n_ok >= 2 else 0.0
                results[strategy][key_count] = (mean_val, std_val)
                extra = f", {n_to} timeout" if n_to else ""
                print(f"  {strategy}: {mean_val:.1f}±{std_val:.1f}s ({len(values)} trials{extra})")

    key_counts = KEY_COUNTS
    r_vals = []
    r_errs = []
    b_vals = []
    b_errs = []
    for k in key_counts:
        rm, rs = results["random"].get(k, (None, 0.0))
        bm, bs = results["bec-1"].get(k, (None, 0.0))
        r_vals.append(float(rm) if rm is not None else math.nan)
        r_errs.append(rs)
        b_vals.append(float(bm) if bm is not None else math.nan)
        b_errs.append(bs)

    fig, ax = plt.subplots(figsize=(8, 5))
    r_lower = [min(err, val) if not math.isnan(val) else 0 for val, err in zip(r_vals, r_errs)]
    r_upper = r_errs
    b_lower = [min(err, val) if not math.isnan(val) else 0 for val, err in zip(b_vals, b_errs)]
    b_upper = b_errs
    ax.errorbar(key_counts, r_vals, yerr=(r_lower, r_upper), fmt="o-", color="red", label="Random", linewidth=2, markersize=8, capsize=3)
    ax.errorbar(key_counts, b_vals, yerr=(b_lower, b_upper), fmt="s-", color="blue", label="BEC-1", linewidth=2, markersize=8, capsize=3)
    ax.set_xlabel("Key Space Size", fontsize=12)
    ax.set_ylabel("Time to Bug (seconds)", fontsize=12)
    ax.set_title("Time-to-Bug vs Key Space Size: Random vs BEC-1 (mean ± std, 5 trials)")
    ax.legend(loc="upper left", fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    ax.set_xticks(key_counts)
    ax.get_xaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    fig.tight_layout()
    fig.savefig(OUTPUT_PNG, dpi=150)
    plt.close(fig)
    print()
    print(f"Plot saved: {OUTPUT_PNG}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
