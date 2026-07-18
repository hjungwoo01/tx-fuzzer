#!/usr/bin/env python3
"""
Elle / AWDIT Verification Pipeline — Python Wrapper

Programmatic interface for running IsoTest .edn history files through
the Clojure-based Elle checker.  Can be used standalone or imported as
a library from other evaluation scripts.

This wrapper:
  1. Validates that Java 11+ and Clojure CLI are available
  2. Generates or locates a deps.edn with the Elle dependency
  3. Invokes Elle's list-append or rw-register checker via subprocess
  4. Parses the output into a structured ElleReport dataclass
  5. Writes a summary CSV when run in batch mode

Usage (CLI):
  python scripts/verify_elle.py results/histories/bank_overdraft.edn
  python scripts/verify_elle.py results/histories/*.edn --output-dir results/elle/
  python scripts/verify_elle.py --generate-edn --workload bank_overdraft --db sqlite

Usage (library):
  from scripts.verify_elle import verify_edn_file, ElleReport
  report = verify_edn_file("history.edn", models=["snapshot-isolation"])
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class ElleReport:
    """Structured result from an Elle verification run."""
    history_path: str
    models_checked: List[str]
    valid: Optional[bool]        # True = pass, False = fail, None = unknown/error
    anomaly_types: List[str]
    operation_count: int
    raw_output: str
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.valid is True

    def summary_line(self) -> str:
        status = "PASS" if self.passed else ("FAIL" if self.valid is False else "UNKNOWN")
        anomalies = ", ".join(self.anomaly_types) if self.anomaly_types else "none"
        return (
            f"{status} | {self.history_path} | "
            f"models={self.models_checked} | anomalies=[{anomalies}] | "
            f"ops={self.operation_count}"
        )


# ---------------------------------------------------------------------------
# Prerequisite checks
# ---------------------------------------------------------------------------

def check_java() -> str:
    """Verify Java 11+ is available. Returns the java path."""
    java = shutil.which("java")
    if not java:
        raise EnvironmentError(
            "Java not found on PATH. Elle requires Java 11+.\n"
            "Install: brew install openjdk@11 (macOS) or apt install openjdk-11-jre (Linux)"
        )
    result = subprocess.run(
        [java, "-version"], capture_output=True, text=True, timeout=10
    )
    version_line = result.stderr.split("\n")[0] if result.stderr else ""
    # Extract major version number
    import re
    match = re.search(r'"(\d+)', version_line)
    if match:
        major = int(match.group(1))
        if major < 11:
            raise EnvironmentError(f"Java {major} found, but Elle requires 11+. Got: {version_line}")
    return java


def check_clojure() -> str:
    """Verify Clojure CLI is available. Returns the clj/clojure path."""
    for cmd in ["clj", "clojure"]:
        path = shutil.which(cmd)
        if path:
            return path
    raise EnvironmentError(
        "Clojure CLI (clj) not found on PATH.\n"
        "Install: brew install clojure/tools/clojure (macOS)\n"
        "See: https://clojure.org/guides/install"
    )


def resolve_elle_deps(elle_dir: Optional[str] = None) -> Path:
    """Find or generate a deps.edn with the Elle dependency.

    Resolution order:
      1. Explicit elle_dir argument
      2. ELLE_DIR environment variable
      3. elle-deps.edn in the project root
      4. Auto-generated temporary deps.edn
    """
    # 1. Explicit
    if elle_dir:
        p = Path(elle_dir)
        if (p / "deps.edn").exists():
            return p
        raise FileNotFoundError(f"No deps.edn found in {p}")

    # 2. Environment
    env_dir = os.environ.get("ELLE_DIR", "")
    if env_dir and (Path(env_dir) / "deps.edn").exists():
        return Path(env_dir)

    # 3. Project-local
    local_deps = ROOT / "elle-deps.edn"
    if local_deps.exists():
        return ROOT

    # 4. Auto-generate
    tmp = Path(tempfile.mkdtemp(prefix="isotest_elle_"))
    deps_content = '{:deps {elle/elle {:mvn/version "0.2.1"}}}\n'
    (tmp / "deps.edn").write_text(deps_content)
    print(f"  Auto-generated Elle deps.edn at {tmp}", file=sys.stderr)
    return tmp


# ---------------------------------------------------------------------------
# Elle invocation
# ---------------------------------------------------------------------------

# Clojure script that loads a history and runs Elle's checker.
ELLE_CHECK_CLJ = r"""
(require '[clojure.java.io :as io]
         '[clojure.edn :as edn])

;; Attempt to require Elle checkers; exit gracefully if not found
(def la-check
  (try (require '[elle.list-append :as la])
       (resolve 'elle.list-append/check)
   (catch Exception _ nil)))

(def rw-check
  (try (require '[elle.rw-register :as rw])
       (resolve 'elle.rw-register/check)
   (catch Exception _ nil)))

(defn parse-history [path]
  (with-open [rdr (java.io.PushbackReader. (io/reader path))]
    (loop [acc []]
      (let [v (edn/read {:eof ::done} rdr)]
        (if (= v ::done) acc (recur (conj acc v)))))))

(defn run-check [history models]
  (let [opts {:consistency-models models :directory "/tmp/elle-out"}]
    (or
      (when la-check
        (try (@la-check opts history)
             (catch Exception _ nil)))
      (when rw-check
        (try (@rw-check opts history)
             (catch Exception _ nil)))
      {:valid? :unknown :error "No applicable Elle checker succeeded"})))

(let [args *command-line-args*
      path (first args)
      model-strs (rest args)
      models (set (map keyword model-strs))]
  (when (nil? path)
    (binding [*out* *err*] (println "Usage: <script> <file.edn> [model ...]"))
    (System/exit 2))
  (let [history (parse-history path)
        _ (binding [*out* *err*]
            (println (str "  Loaded " (count history) " operations from " path)))
        result (run-check history models)]
    ;; Emit machine-readable JSON to stdout
    (println
      (str "{\"valid\":"
           (cond (true? (:valid? result)) "true"
                 (false? (:valid? result)) "false"
                 :else "null")
           ",\"anomaly_types\":"
           (str "[" (clojure.string/join ","
                      (map #(str "\"" (name %) "\"")
                           (or (:anomaly-types result) []))) "]")
           ",\"ops\":" (count history)
           (when (:error result)
             (str ",\"error\":\"" (clojure.string/replace (str (:error result)) "\"" "\\\"") "\""))
           "}"))
    (System/exit (if (true? (:valid? result)) 0 1))))
"""


def verify_edn_file(
    edn_path: str,
    *,
    models: Optional[List[str]] = None,
    elle_dir: Optional[str] = None,
    timeout_s: int = 120,
    verbose: bool = False,
) -> ElleReport:
    """Run Elle checker on a single .edn history file.

    Args:
        edn_path: Path to the .edn history file.
        models: Consistency models to check (default: ["snapshot-isolation"]).
        elle_dir: Optional path to Elle checkout.
        timeout_s: Subprocess timeout in seconds.
        verbose: Print detailed output.

    Returns:
        ElleReport with structured results.
    """
    edn_path = str(Path(edn_path).resolve())
    if not Path(edn_path).exists():
        return ElleReport(
            history_path=edn_path,
            models_checked=models or ["snapshot-isolation"],
            valid=None,
            anomaly_types=[],
            operation_count=0,
            raw_output="",
            error=f"File not found: {edn_path}",
        )

    models = models or ["snapshot-isolation"]
    deps_dir = resolve_elle_deps(elle_dir)
    clj_cmd = check_clojure()

    cmd = [
        clj_cmd,
        "-e", ELLE_CHECK_CLJ,
        "--",
        edn_path,
    ] + models

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            cwd=str(deps_dir),
        )
    except subprocess.TimeoutExpired:
        return ElleReport(
            history_path=edn_path,
            models_checked=models,
            valid=None,
            anomaly_types=[],
            operation_count=0,
            raw_output="",
            error=f"Elle timed out after {timeout_s}s",
        )
    except FileNotFoundError as e:
        return ElleReport(
            history_path=edn_path,
            models_checked=models,
            valid=None,
            anomaly_types=[],
            operation_count=0,
            raw_output="",
            error=str(e),
        )

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    raw = f"STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"

    if verbose:
        print(raw, file=sys.stderr)

    # Parse the JSON output line from stdout
    valid: Optional[bool] = None
    anomaly_types: List[str] = []
    op_count = 0
    error: Optional[str] = None

    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                parsed = json.loads(line)
                valid = parsed.get("valid")
                anomaly_types = parsed.get("anomaly_types", [])
                op_count = parsed.get("ops", 0)
                error = parsed.get("error")
            except json.JSONDecodeError:
                pass

    if valid is None and result.returncode != 0 and not error:
        error = f"Elle exited with code {result.returncode}"

    return ElleReport(
        history_path=edn_path,
        models_checked=models,
        valid=valid,
        anomaly_types=anomaly_types,
        operation_count=op_count,
        raw_output=raw,
        error=error,
    )


# ---------------------------------------------------------------------------
# Batch verification
# ---------------------------------------------------------------------------

def verify_batch(
    edn_files: List[str],
    *,
    models: Optional[List[str]] = None,
    elle_dir: Optional[str] = None,
    verbose: bool = False,
) -> List[ElleReport]:
    """Run Elle checker on multiple .edn files."""
    reports = []
    for f in edn_files:
        print(f"  Verifying: {f}")
        report = verify_edn_file(f, models=models, elle_dir=elle_dir, verbose=verbose)
        print(f"    {report.summary_line()}")
        reports.append(report)
    return reports


def write_reports_csv(reports: List[ElleReport], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "history_path", "models_checked", "valid",
            "anomaly_types", "operation_count", "error",
        ])
        for r in reports:
            writer.writerow([
                r.history_path,
                ";".join(r.models_checked),
                r.valid,
                ";".join(r.anomaly_types),
                r.operation_count,
                r.error or "",
            ])


# ---------------------------------------------------------------------------
# EDN generation helper (for testing the pipeline)
# ---------------------------------------------------------------------------

def generate_sample_edn(
    workload_name: str,
    db: str,
    output_path: str,
    n_trials: int = 10,
    n_workers: int = 4,
) -> str:
    """Generate a sample .edn history using IsoTest's ElleLogger.

    Useful for testing the verification pipeline end-to-end.
    """
    from scripts.schedule_pool.elle_logger import ElleLogger
    from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
    from scripts.experiments.micro_workloads import MICRO_WORKLOADS
    from scripts.schedule_pool.base_strategy import RandomStrategy
    from scripts.schedule_pool.event_id import get_workload_event_ids
    from scripts.schedule_pool.runner import SchedulePoolRunner

    dsn = os.environ.get(f"{db.upper()}_DSN", DEFAULT_DSNS.get(db, ""))
    wl = MICRO_WORKLOADS[workload_name]
    strategy = RandomStrategy(max_delay_ms=25.0)
    all_events = get_workload_event_ids(workload_name, n_workers)

    import random
    rng = random.Random(42)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with ElleLogger(out) as logger:
        runner = SchedulePoolRunner(
            workload=wl,
            db=db,
            dsn=dsn,
            n_workers=n_workers,
            variant="buggy",
            elle_logger=logger,
        )
        for _ in range(n_trials):
            schedule = strategy.seed(all_events, rng)
            runner.run_trial_with_schedule(schedule)

    print(f"  Generated {out} ({n_trials} trials)")
    return str(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Elle/AWDIT Verification Pipeline for IsoTest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Verify a single .edn history
  python scripts/verify_elle.py results/histories/bank_overdraft.edn

  # Verify all histories in a directory
  python scripts/verify_elle.py results/histories/*.edn --output-dir results/elle/

  # Generate a sample history then verify it
  python scripts/verify_elle.py --generate-edn --workload bank_overdraft --db sqlite

  # Check against serializable isolation
  python scripts/verify_elle.py history.edn --models serializable
        """,
    )
    parser.add_argument(
        "edn_files", nargs="*", default=[],
        help=".edn history files to verify",
    )
    parser.add_argument(
        "--models", nargs="+", default=["snapshot-isolation"],
        help="Consistency models to check (default: snapshot-isolation)",
    )
    parser.add_argument(
        "--elle-dir", type=str, default=None,
        help="Path to Elle checkout (must contain deps.edn)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Directory for report CSV output",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print detailed Elle output",
    )
    parser.add_argument(
        "--generate-edn", action="store_true",
        help="Generate a sample .edn history before verification",
    )
    parser.add_argument(
        "--workload", type=str, default="bank_overdraft",
        help="Workload for --generate-edn (default: bank_overdraft)",
    )
    parser.add_argument(
        "--db", type=str, default="sqlite",
        help="Database for --generate-edn (default: sqlite)",
    )
    parser.add_argument(
        "--n-trials", type=int, default=10,
        help="Number of trials for --generate-edn (default: 10)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("IsoTest Elle Verification Pipeline")
    print("=" * 60)

    edn_files = list(args.edn_files)

    # Optionally generate a sample history
    if args.generate_edn:
        out_dir = Path(args.output_dir) if args.output_dir else ROOT / "results" / "histories"
        out_path = out_dir / f"{args.workload}_{args.db}.edn"
        print(f"\nGenerating sample history: {out_path}")
        generated = generate_sample_edn(
            workload_name=args.workload,
            db=args.db,
            output_path=str(out_path),
            n_trials=args.n_trials,
        )
        edn_files.append(generated)

    if not edn_files:
        print("No .edn files to verify. Use --generate-edn or pass files as arguments.")
        parser.print_help()
        sys.exit(1)

    # Check prerequisites
    try:
        check_java()
        check_clojure()
    except EnvironmentError as e:
        print(f"\nPrerequisite check failed:\n  {e}", file=sys.stderr)
        print("\nTo generate .edn histories without Elle, use --generate-edn only.")
        sys.exit(1)

    # Run verification
    print(f"\nVerifying {len(edn_files)} file(s) against models: {args.models}\n")
    reports = verify_batch(
        edn_files,
        models=args.models,
        elle_dir=args.elle_dir,
        verbose=args.verbose,
    )

    # Write CSV if output-dir specified
    if args.output_dir:
        csv_path = Path(args.output_dir) / "elle_verification_results.csv"
        write_reports_csv(reports, csv_path)
        print(f"\nReport CSV: {csv_path}")

    # Write individual report files
    for report in reports:
        report_path = Path(report.history_path).with_suffix(".elle_report.txt")
        if args.output_dir:
            report_path = Path(args.output_dir) / report_path.name
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report.raw_output, encoding="utf-8")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    passed = sum(1 for r in reports if r.passed)
    failed = sum(1 for r in reports if r.valid is False)
    unknown = sum(1 for r in reports if r.valid is None)
    print(f"  Total:   {len(reports)}")
    print(f"  Pass:    {passed}")
    print(f"  Fail:    {failed}")
    print(f"  Unknown: {unknown}")

    all_anomalies = set()
    for r in reports:
        all_anomalies.update(r.anomaly_types)
    if all_anomalies:
        print(f"  Anomaly types found: {sorted(all_anomalies)}")

    print("=" * 60)

    sys.exit(0 if (failed == 0 and unknown == 0) else 1)


if __name__ == "__main__":
    main()
