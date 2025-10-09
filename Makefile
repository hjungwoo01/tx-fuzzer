SEED ?= workloads/seed.yaml
WORKLOAD ?= workloads/rw_mix.yaml

OUT ?= history.edn
HIST ?= history.edn

init:
	go run ./cmd/runner -config $(SEED) -out seed_history.edn

run:
	go run ./cmd/runner -config $(WORKLOAD) -out $(OUT)

analyse:
	clj -M:run $(HIST)

clj:
	clj -M:run $(HIST)