SEED ?= workloads/seed.yaml
WORKLOAD ?= workloads/next.yaml

OUT ?= history.edn
HIST ?= history.edn
BASE ?= workloads/next.yaml
NEXT_OUT ?= workloads/next.yaml

init:
	go run ./cmd/runner -config $(SEED) -out seed_history.edn

run:
	go run ./cmd/runner -config $(WORKLOAD) -out $(OUT)

analyse:
	clj -M:run $(HIST)

check:
	clj -J-Djava.awt.headless=true -M:run $(HIST)

# Full workflow: run workload, analyze, check, and generate next workload
full: run
	python scripts/analyze_and_next.py \
		--workload $(WORKLOAD) \
		--history $(OUT) \
		--base $(BASE) \
		--out $(NEXT_OUT)

# Run analysis and generate next workload (skip running the workload)
analyze-next:
	python scripts/analyze_and_next.py \
		--workload $(WORKLOAD) \
		--history $(HIST) \
		--base $(BASE) \
		--out $(NEXT_OUT)

# Run analysis only (skip generating next workload)
analyze-only:
	python scripts/analyze_and_next.py \
		--workload $(WORKLOAD) \
		--history $(HIST) \
		--skip-next