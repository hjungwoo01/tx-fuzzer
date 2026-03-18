"""Merge results from separate PG+SQLite, PG-case, and MySQL runs into a single 3-DB report."""
import sys, csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.run_dbtest_artifact import (
    step4_master, CommandLogger, CaseDiscoveryRow,
)


# 1. PG + SQLite micro/overhead + SQLite case study
PG_SQLITE_DIR = ROOT / "results" / "dbtest_2026_artifact_20260306_093120"
# 2. PG case study (update_conflict_out)
PG_CASE_DIR   = ROOT / "results" / "dbtest_pg_case_20260307_123319"
# 3. MySQL micro/overhead/case study (will be filled in once run completes)
MYSQL_DIR     = None  # set below from argv or glob

if len(sys.argv) > 1:
    MYSQL_DIR = Path(sys.argv[1])
else:
    candidates = sorted(ROOT.glob("results/dbtest_mysql_*"), reverse=True)
    if candidates:
        MYSQL_DIR = candidates[0]

if not MYSQL_DIR or not MYSQL_DIR.exists():
    sys.exit(f"MySQL results dir not found. Pass as argv[1].")


OUT = ROOT / "results" / "dbtest_3db_merged"
OUT.mkdir(parents=True, exist_ok=True)

print(f"Merging:")
print(f"  PG+SQLite base: {PG_SQLITE_DIR}")
print(f"  PG case study:  {PG_CASE_DIR}")
print(f"  MySQL run:      {MYSQL_DIR}")
print(f"  Output:         {OUT}")

logger = CommandLogger(OUT / "commands_merge.log")


def load_csv_dicts(path: Path):
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def load_auc(path: Path):
    rows = load_csv_dicts(path)
    for r in rows:
        r["auc"] = float(r["auc"])
        r["final_states"] = int(r["final_states"])
        r["bugs"] = int(r["bugs"])
        r["txns"] = int(r["txns"])
        r["rep"] = int(r["rep"])
        r["seed"] = int(r["seed"])
    return rows


def load_discovery(path: Path):
    rows = []
    for r in load_csv_dicts(path):
        rows.append(CaseDiscoveryRow(
            db=r["db"], case_name=r["case_name"], variant=r["variant"],
            strategy=r["strategy"], rep_idx=int(r["rep_idx"]), seed=int(r["seed"]),
            duration_s=float(r["duration_s"]), txns_executed=int(r["txns_executed"]),
            bugs_found=int(r["bugs_found"]), success=r["success"] == "True",
            ttfb_s=float(r["ttfb_s"]) if r["ttfb_s"] != "NA" else None,
        ))
    return rows


def load_replay(path: Path):
    rows = load_csv_dicts(path)
    for r in rows:
        r["replays"] = int(r["replays"])
        r["bug_hits"] = int(r["bug_hits"])
        r["bug_rate"] = float(r["bug_rate"])
    return rows


auc_rows = load_auc(PG_SQLITE_DIR / "micro_coverage" / "auc.csv")
auc_rows += load_auc(MYSQL_DIR / "micro_coverage" / "auc.csv")
print(f"  AUC rows: {len(auc_rows)}")


# SQLite from base run, PG from PG case run, MySQL from MySQL run
discovery_rows = load_discovery(PG_SQLITE_DIR / "case_studies" / "discovery_runs.csv")
discovery_rows += load_discovery(PG_CASE_DIR / "case_studies" / "discovery_runs.csv")
discovery_rows += load_discovery(MYSQL_DIR / "case_studies" / "discovery_runs.csv")
print(f"  Discovery rows: {len(discovery_rows)}")


replay_rows = load_replay(PG_SQLITE_DIR / "case_studies" / "replay_summary.csv")
replay_rows += load_replay(PG_CASE_DIR / "case_studies" / "replay_summary.csv")
replay_rows += load_replay(MYSQL_DIR / "case_studies" / "replay_summary.csv")
print(f"  Replay rows: {len(replay_rows)}")


minimization_rows = load_csv_dicts(PG_SQLITE_DIR / "case_studies" / "minimization_summary.csv")
minimization_rows += load_csv_dicts(PG_CASE_DIR / "case_studies" / "minimization_summary.csv")
minimization_rows += load_csv_dicts(MYSQL_DIR / "case_studies" / "minimization_summary.csv")
print(f"  Minimization rows: {len(minimization_rows)}")


overhead_detail = load_csv_dicts(PG_SQLITE_DIR / "overhead" / "overhead_detail.csv")
overhead_detail += load_csv_dicts(MYSQL_DIR / "overhead" / "overhead_detail.csv")
print(f"  Overhead detail rows: {len(overhead_detail)}")


import shutil
for fn in ["versions.txt", "hardware.txt"]:
    src = PG_SQLITE_DIR / fn
    if src.exists():
        shutil.copy2(src, OUT / fn)


step4_master(OUT, logger, auc_rows, discovery_rows, minimization_rows,
             replay_rows, [], overhead_detail)

logger.close()
print(f"\nDone! Merged 3-DB report in {OUT}")
