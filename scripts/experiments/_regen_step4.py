"""Re-run only step4 (MASTER_REPORT + MASTER_SUMMARY) from existing CSVs."""
import sys, csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.run_dbtest_artifact import (
    step4_master, CommandLogger, CaseDiscoveryRow,
)
from scripts.experiments.overhead_bench import OverheadRow

# Find latest results dir or use argv[1]
if len(sys.argv) > 1:
    out = Path(sys.argv[1])
else:
    candidates = sorted(ROOT.glob("results/dbtest_2026_artifact_*"), reverse=True)
    if not candidates:
        sys.exit("No results directory found")
    out = candidates[0]

print(f"Regenerating step4 for: {out}")
logger = CommandLogger(out / "commands_step4.log")

# Load auc_rows
auc_rows = []
with (out / "micro_coverage/auc.csv").open() as f:
    for r in csv.DictReader(f):
        r["auc"] = float(r["auc"])
        r["final_states"] = int(r["final_states"])
        r["bugs"] = int(r["bugs"])
        r["txns"] = int(r["txns"])
        r["rep"] = int(r["rep"])
        r["seed"] = int(r["seed"])
        auc_rows.append(r)

# Load discovery_rows
discovery_rows = []
with (out / "case_studies/discovery_runs.csv").open() as f:
    for r in csv.DictReader(f):
        discovery_rows.append(CaseDiscoveryRow(
            db=r["db"], case_name=r["case_name"], variant=r["variant"],
            strategy=r["strategy"], rep_idx=int(r["rep_idx"]), seed=int(r["seed"]),
            duration_s=float(r["duration_s"]), txns_executed=int(r["txns_executed"]),
            bugs_found=int(r["bugs_found"]), success=r["success"] == "True",
            ttfb_s=float(r["ttfb_s"]) if r["ttfb_s"] != "NA" else None,
        ))

# Load replay
replay_rows = []
rp = out / "case_studies/replay_summary.csv"
if rp.exists():
    with rp.open() as f:
        for r in csv.DictReader(f):
            r["replays"] = int(r["replays"])
            r["bug_hits"] = int(r["bug_hits"])
            r["bug_rate"] = float(r["bug_rate"])
            replay_rows.append(r)

# Load minimization
minimization_rows = []
mp = out / "case_studies/minimization_summary.csv"
if mp.exists():
    with mp.open() as f:
        minimization_rows = list(csv.DictReader(f))

# Load overhead detail
overhead_detail = []
odp = out / "overhead/overhead_detail.csv"
if odp.exists():
    with odp.open() as f:
        overhead_detail = list(csv.DictReader(f))

step4_master(out, logger, auc_rows, discovery_rows, minimization_rows,
             replay_rows, [], overhead_detail)
logger.close()
print("Done! Generated MASTER_REPORT.md and MASTER_SUMMARY.csv")
