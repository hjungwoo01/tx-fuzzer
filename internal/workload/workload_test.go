package workload

import (
	"testing"
	"time"

	"github.com/hjungwoo01/tx-fuzzer/internal/config"
)

func TestFlattenTxnQueries(t *testing.T) {
	tpl := config.TxnTemplate{
		Name: "test",
		Queries: []config.Query{
			{SQL: "SELECT 1"},
		},
		Steps: []config.Step{
			{
				SQL:   "UPDATE kv SET v=v+1 WHERE k=$1",
				Args:  []any{1},
				Sleep: 50 * time.Millisecond,
			},
			{
				Sleep: 100 * time.Millisecond,
				Queries: []config.Query{
					{SQL: "INSERT INTO kv(k,v) VALUES($1,$2)", Sleep: 10 * time.Millisecond},
					{SQL: "DELETE FROM kv WHERE k=$1"},
				},
			},
		},
	}

	plans := BuildPlans(config.Workload{Transactions: []config.TxnTemplate{tpl}})
	plan, ok := plans["test"]
	if !ok {
		t.Fatalf("plan for txn 'test' not found")
	}
	if len(plan.Queries) != 4 {
		t.Fatalf("expected 4 queries, got %d", len(plan.Queries))
	}
	if plan.Queries[0].Template.SQL != "SELECT 1" {
		t.Fatalf("unexpected first query: %s", plan.Queries[0].Template.SQL)
	}
	if plan.Queries[1].Template.Sleep != 50*time.Millisecond {
		t.Fatalf("expected step sleep to propagate (got %s)", plan.Queries[1].Template.Sleep)
	}
	if plan.Queries[2].Template.Sleep != 10*time.Millisecond {
		t.Fatalf("expected nested query sleep preserved (got %s)", plan.Queries[2].Template.Sleep)
	}
	if plan.Queries[3].Template.Sleep != 100*time.Millisecond {
		t.Fatalf("expected parent step sleep appended to final nested query (got %s)", plan.Queries[3].Template.Sleep)
	}
	if len(plan.Queries[1].ArgsSpec) != 1 {
		t.Fatalf("expected args to carry over (got %d)", len(plan.Queries[1].ArgsSpec))
	}
}

func TestMaterializeArgsTxWriteID(t *testing.T) {
	spec := ParseArg(map[string]any{"tx_write_id": map[string]any{}})
	if spec.Kind != TxWriteID {
		t.Fatalf("expected TxWriteID spec, got kind=%v", spec.Kind)
	}

	specs := []GenSpec{spec}
	ctx := MaterializeContext{Process: 3, TxnID: "tx-17"}
	a1 := MaterializeArgs(specs, nil, ctx)
	a2 := MaterializeArgs(specs, nil, ctx)
	if len(a1) != 1 || len(a2) != 1 {
		t.Fatalf("expected one materialized arg, got %d and %d", len(a1), len(a2))
	}

	v1, ok := a1[0].(int64)
	if !ok {
		t.Fatalf("expected int64 tx_write_id, got %T", a1[0])
	}
	v2, ok := a2[0].(int64)
	if !ok {
		t.Fatalf("expected int64 tx_write_id, got %T", a2[0])
	}
	if v1 != v2 {
		t.Fatalf("expected stable tx_write_id within a txn, got %d and %d", v1, v2)
	}
	if gotProcess := int(v1 >> 48); gotProcess != 3 {
		t.Fatalf("expected encoded process=3, got %d (value=%d)", gotProcess, v1)
	}

	nextTxn := MaterializeArgs(specs, nil, MaterializeContext{Process: 3, TxnID: "tx-18"})
	nextVal := nextTxn[0].(int64)
	if nextVal == v1 {
		t.Fatalf("expected unique value for different txn IDs, both were %d", v1)
	}

	otherClient := MaterializeArgs(specs, nil, MaterializeContext{Process: 4, TxnID: "tx-17"})
	otherVal := otherClient[0].(int64)
	if otherVal == v1 {
		t.Fatalf("expected unique value across processes, both were %d", v1)
	}
}
