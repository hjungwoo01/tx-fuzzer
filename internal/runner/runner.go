package runner

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/hjungwoo01/tx-fuzzer/internal/config"
	"github.com/hjungwoo01/tx-fuzzer/internal/driver"
	"github.com/hjungwoo01/tx-fuzzer/internal/history"
	"github.com/hjungwoo01/tx-fuzzer/internal/workload"
)

type Env struct {
	Drv   driver.Driver
	Cfg   *config.Root
	Hist  *history.Writer
	Start time.Time
}

func Run(ctx context.Context, env *Env) error {
	wl := env.Cfg.Workload
	plans := workload.BuildPlans(wl)
	chooser := workload.NewChooser(wl.Mix)
	env.Start = time.Now()
	end := env.Start.Add(env.Cfg.Scheduling.Duration)
	clients := env.Cfg.Scheduling.Clients

	errCh := make(chan error, clients)
	barrierEvery := env.Cfg.Scheduling.BarrierEvery
	bar := NewCyclicBarrier(env.Cfg.Scheduling.Clients)

	if env.Hist == nil {
		fmt.Printf("[diag] env.Hist is nil -- history writer not initialized!\n")
	} else {
		fmt.Printf("[diag] env.Hist is ready (ok)\n")
	}
	fmt.Printf("[diag] clients=%d, duration=%s\n", clients, env.Cfg.Scheduling.Duration)

	for pid := 0; pid < clients; pid++ {
		go func(process int) {
			fmt.Printf("[diag] starting client %d\n", process)
			defer func() { errCh <- nil }()

			iter := 0
			for time.Now().Before(end) {
				iter++
				name := chooser.Pick()
				plan := plans[name]
				iso := mapIso(plan.Template.Isolation)

				tctx, cancel := workload.ContextWithTimeout(ctx, plan.Template.Timeout)
				tx, err := env.Drv.Begin(tctx, iso)
				if err != nil {
					cancel()
					continue
				}

				if len(plan.Queries) == 0 {
					_ = tx.Rollback(tctx)
					cancel()
					continue
				}

				txnID := history.NextTxnID()
				rollbackOnError := true
				if plan.Template.RollbackOnError != nil {
					rollbackOnError = *plan.Template.RollbackOnError
				}
				interDelay := time.Duration(plan.Template.InterQueryDelayMS) * time.Millisecond
				commitStrategy := strings.ToLower(strings.TrimSpace(plan.Template.CommitStrategy))

				prevArgs := []any{}
				invokeMops := make([]history.Mop, 0, len(plan.Queries))
				okMops := make([]history.Mop, 0, len(plan.Queries))
				var firstElleMono, firstElleWall int64
				var lastElleMono, lastElleWall int64

				fail := false
				queryFailed := false
				rolledBack := false
				committed := false

				var finalEventF string
				var finalEventMeta map[string]any
				var finalEventErr string
				var finalEventType history.OpType

				for qi, qp := range plan.Queries {
					if barrierEvery > 0 && iter%barrierEvery == 0 && qi == 0 {
						_ = bar.Await(200 * time.Millisecond)
					}

					args := workload.MaterializeArgs(qp.ArgsSpec, prevArgs)
					overrideLiteralArgs(args, qp.Template.Args)

					stepIndex := qi + 1
					stepStartMono := time.Since(env.Start).Nanoseconds()
					stepStartWall := time.Now().UnixNano()

					haveElle := qp.Template.Elle != nil
					var elleOpKind, elleMopKind, elleKeyStr string
					var elleInvokeVal any
					var elleOkVal any
					elleIdx := -1

					if haveElle {
						elleOpKind, elleMopKind = elleOpKinds(qp.Template.Elle.F)
						elleKeyStr = subRef(qp.Template.Elle.Key, args)
						if len(invokeMops) == 0 {
							firstElleMono = stepStartMono
							firstElleWall = stepStartWall
						}
						if elleOpKind == ":read" {
							elleInvokeVal = nil
						} else {
							val := qp.Template.Elle.Value
							if val == nil {
								val = valueArgGuess(args)
							}
							elleInvokeVal = val
							elleOkVal = val
						}
						invokeVal := elleInvokeVal
						if elleMopKind == ":r" {
							invokeVal = nil
						}
						okVal := elleOkVal
						if elleMopKind == ":r" {
							okVal = nil
						}
						invokeMops = append(invokeMops, history.Mop{Kind: elleMopKind, Key: elleKeyStr, Value: invokeVal})
						okMops = append(okMops, history.Mop{Kind: elleMopKind, Key: elleKeyStr, Value: okVal})
					}

					invokeDetails := map[string]any{}
					if haveElle {
						invokeDetails["elle_f"] = elleOpKind
						invokeDetails["elle_key"] = elleKeyStr
						if elleInvokeVal != nil {
							invokeDetails["elle_value"] = elleInvokeVal
						}
					}

					recordQueryOp(env.Hist, history.Invoke, process, txnID, stepIndex, qp.Template.SQL, args, invokeDetails, "", stepStartMono, stepStartWall)

					var rowsAffected int64 = -1
					var selectVal any
					var execErr error

					if hasSelect(qp.Template.SQL) {
						row, err := tx.QueryRow(tctx, qp.Template.SQL, args...)
						if err != nil {
							execErr = err
						} else {
							if qp.Template.Elle != nil && qp.Template.Elle.ValueFromResultCol != nil {
								if err := row.Scan(&selectVal); err != nil {
									execErr = err
								}
							} else {
								var tmp any
								if err := row.Scan(&tmp); err != nil {
									execErr = err
								} else {
									selectVal = tmp
								}
							}
						}
						if execErr == nil && haveElle {
							val := selectVal
							if qp.Template.Elle.Value != nil {
								val = qp.Template.Elle.Value
							} else if qp.Template.Elle.ValueFromResultCol == nil && val == nil {
								val = valueArgGuess(args)
							}
							elleOkVal = val
							if elleIdx >= 0 {
								okMops[elleIdx].Value = val
							}
							selectVal = val
						}
					} else {
						rowsAffected, execErr = tx.Exec(tctx, qp.Template.SQL, args...)
						if execErr == nil && haveElle {
							val := qp.Template.Elle.Value
							if val == nil {
								val = valueArgGuess(args)
							}
							elleOkVal = val
							if elleIdx >= 0 {
								okMops[elleIdx].Value = val
							}
						}
					}

					opEndMono := time.Since(env.Start).Nanoseconds()
					opEndWall := time.Now().UnixNano()

					okDetails := cloneStringAnyMap(invokeDetails)
					if rowsAffected >= 0 {
						if okDetails == nil {
							okDetails = map[string]any{}
						}
						okDetails["rows_affected"] = rowsAffected
					}
					if selectVal != nil {
						if okDetails == nil {
							okDetails = map[string]any{}
						}
						okDetails["result"] = selectVal
					}
					if haveElle && elleOkVal != nil {
						if okDetails == nil {
							okDetails = map[string]any{}
						}
						okDetails["elle_value"] = elleOkVal
					}

					if execErr != nil {
						fail = true
						queryFailed = true
						lastElleMono = opEndMono
						lastElleWall = opEndWall
						fmt.Printf("[warn] process %d txn %s step %d error: %v (sql=%s)\n", process, txnID, qi, execErr, qp.Template.SQL)
						recordQueryOp(env.Hist, history.Fail, process, txnID, stepIndex, qp.Template.SQL, args, okDetails, execErr.Error(), opEndMono, opEndWall)

						if rollbackOnError {
							rolledBack = true
							if finalEventF == "" {
								finalEventF = ":rollback"
								finalEventType = history.Fail
								finalEventErr = execErr.Error()
								finalEventMeta = map[string]any{
									"sql":  qp.Template.SQL,
									"step": stepIndex,
								}
							}
							_ = tx.Rollback(tctx)
							break
						}
					} else {
						lastElleMono = opEndMono
						lastElleWall = opEndWall
						recordQueryOp(env.Hist, history.Ok, process, txnID, stepIndex, qp.Template.SQL, args, okDetails, "", opEndMono, opEndWall)
					}

					prevArgs = args

					if execErr == nil && qp.Template.Sleep > 0 {
						time.Sleep(qp.Template.Sleep)
					}
					if execErr == nil && interDelay > 0 && qi < len(plan.Queries)-1 {
						time.Sleep(interDelay)
					}

					if execErr == nil && env.Cfg.Scheduling.RandomKillPct > 0 && rand.IntN(100) < env.Cfg.Scheduling.RandomKillPct {
						fail = true
						queryFailed = true
						rolledBack = true
						lastElleMono = time.Since(env.Start).Nanoseconds()
						lastElleWall = time.Now().UnixNano()
						finalEventF = ":rollback"
						finalEventType = history.Fail
						finalEventErr = "random kill"
						finalEventMeta = map[string]any{"reason": "random_kill"}
						_ = tx.Rollback(tctx)
						break
					}
				}

				if !rolledBack {
					switch chooseCommitAction(commitStrategy) {
					case commitAction:
						if err := tx.Commit(tctx); err != nil {
							fail = true
							rolledBack = true
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
							finalEventF = ":commit"
							finalEventType = history.Fail
							finalEventErr = err.Error()
							if commitStrategy != "" {
								finalEventMeta = map[string]any{"strategy": commitStrategy}
							}
							fmt.Printf("[warn] process %d txn %s commit error: %v\n", process, txnID, err)
							_ = tx.Rollback(tctx)
						} else {
							committed = true
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
							finalEventF = ":commit"
							finalEventType = history.Ok
							if commitStrategy != "" {
								finalEventMeta = map[string]any{"strategy": commitStrategy}
							}
						}
					case rollbackAction:
						if err := tx.Rollback(tctx); err != nil {
							fail = true
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
							finalEventF = ":rollback"
							finalEventType = history.Fail
							finalEventErr = err.Error()
						} else {
							fail = true
							rolledBack = true
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
							finalEventF = ":rollback"
							finalEventType = history.Ok
							if commitStrategy != "" {
								finalEventMeta = map[string]any{"strategy": commitStrategy}
							}
						}
					}
				} else {
					if finalEventF == "" {
						finalEventF = ":rollback"
					}
					if lastElleMono == 0 {
						lastElleMono = time.Since(env.Start).Nanoseconds()
						lastElleWall = time.Now().UnixNano()
					}
					if finalEventType == "" {
						if fail {
							finalEventType = history.Fail
						} else {
							finalEventType = history.Ok
						}
					}
				}

				if finalEventF != "" {
					recordTxnBoundary(env.Hist, finalEventType, process, txnID, finalEventF, finalEventMeta, finalEventErr, lastElleMono, lastElleWall)
				}

				cancel()

				if len(invokeMops) > 0 {
					if firstElleMono == 0 {
						firstElleMono = time.Since(env.Start).Nanoseconds()
						firstElleWall = time.Now().UnixNano()
					}
					if lastElleMono == 0 {
						lastElleMono = time.Since(env.Start).Nanoseconds()
						lastElleWall = time.Now().UnixNano()
					}
					metaName := plan.Template.Name
					if committed {
						recordTxnOp(env.Hist, history.Invoke, process, firstElleMono, firstElleWall, invokeMops, metaName, txnID)
						recordTxnOp(env.Hist, history.Ok, process, lastElleMono, lastElleWall, okMops, metaName, txnID)
					} else {
						recordTxnOp(env.Hist, history.Invoke, process, firstElleMono, firstElleWall, invokeMops, metaName, txnID)
						recordTxnOp(env.Hist, history.Fail, process, lastElleMono, lastElleWall, okMops, metaName, txnID)
					}
				}

				// Avoid tight loop when failures happen instantly.
				if queryFailed && env.Cfg.Scheduling.JitterMaxMs > 0 {
					delay := time.Duration(env.Cfg.Scheduling.JitterMinMs) * time.Millisecond
					if env.Cfg.Scheduling.JitterMaxMs > env.Cfg.Scheduling.JitterMinMs {
						jitter := env.Cfg.Scheduling.JitterMaxMs - env.Cfg.Scheduling.JitterMinMs
						delay += time.Duration(rand.IntN(jitter+1)) * time.Millisecond
					}
					time.Sleep(delay)
				}
			}
		}(pid)
	}

	var runErr error
	for i := 0; i < clients; i++ {
		if err := <-errCh; err != nil && runErr == nil {
			runErr = err
		}
	}
	if env.Hist != nil {
		if err := env.Hist.Close(); err != nil {
			if runErr == nil {
				runErr = err
			} else {
				fmt.Printf("history close error: %v\n", err)
			}
		}
	}
	return runErr
}

func hasSelect(sql string) bool {
	s := strings.TrimSpace(sql)
	return len(s) >= 6 && strings.EqualFold(s[:6], "select")
}

func valueArgGuess(args []any) any {
	if len(args) > 0 {
		return args[0]
	}
	return nil
}

func subRef(expr string, args []any) string {
	if expr == "" || expr[0] != '$' {
		return expr
	}
	var i int
	fmt.Sscanf(expr, "$%d", &i)
	if i > 0 && i <= len(args) {
		if s, ok := args[i-1].(string); ok {
			return s
		}
		return fmt.Sprintf("%v", args[i-1])
	}
	return expr
}

func overrideLiteralArgs(args []any, raw []any) {
	for i := range raw {
		if i >= len(args) {
			break
		}
		if args[i] != nil {
			continue
		}
		switch v := raw[i].(type) {
		case string:
			if v != "" && v[0] != '$' {
				args[i] = v
			}
		default:
			args[i] = v
		}
	}
}

func cloneAnySlice(src []any) []any {
	if len(src) == 0 {
		return nil
	}
	dst := make([]any, len(src))
	copy(dst, src)
	return dst
}

func cloneStringAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func recordQueryOp(hw *history.Writer, opType history.OpType, process int, txnID string, step int, sql string, args []any, payload map[string]any, errMsg string, timeNS, wallNS int64) {
	if hw == nil {
		return
	}
	value := map[string]any{
		"sql": sql,
	}
	if len(args) > 0 {
		value["args"] = cloneAnySlice(args)
	}
	if len(payload) > 0 {
		for k, v := range payload {
			value[k] = v
		}
	}
	op := history.Op{
		Type:    opType,
		F:       ":query",
		Process: process,
		TimeNS:  timeNS,
		WallNS:  wallNS,
		TxnID:   txnID,
		Step:    step,
		Value:   value,
	}
	if errMsg != "" {
		op.Err = errMsg
	}
	writeHistoryOp(hw, op)
}

func recordTxnBoundary(hw *history.Writer, opType history.OpType, process int, txnID string, f string, payload map[string]any, errMsg string, timeNS, wallNS int64) {
	if hw == nil {
		return
	}
	val := map[string]any{}
	if len(payload) > 0 {
		for k, v := range payload {
			val[k] = v
		}
	}
	op := history.Op{
		Type:    opType,
		F:       f,
		Process: process,
		TimeNS:  timeNS,
		WallNS:  wallNS,
		TxnID:   txnID,
		Value:   val,
	}
	if errMsg != "" {
		op.Err = errMsg
	}
	writeHistoryOp(hw, op)
}

func recordTxnOp(hw *history.Writer, opType history.OpType, process int, timeNS, wallNS int64, mops []history.Mop, txnName, txnID string) {
	if hw == nil || len(mops) == 0 {
		return
	}
	meta := map[string]string{}
	if txnName != "" {
		meta["txn_name"] = txnName
	}
	if txnID != "" {
		meta["txn_id"] = txnID
	}
	if err := hw.WriteTxn(opType, process, timeNS, wallNS, mops, meta); err != nil {
		fmt.Printf("history txn write error: %v\n", err)
	}
}

type commitDecision int

const (
	commitAction commitDecision = iota
	rollbackAction
)

func chooseCommitAction(strategy string) commitDecision {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "rollback":
		return rollbackAction
	case "random":
		if rand.IntN(2) == 0 {
			return commitAction
		}
		return rollbackAction
	case "commit", "":
		return commitAction
	default:
		return commitAction
	}
}

func writeHistoryOp(hw *history.Writer, op history.Op) {
	if hw == nil {
		return
	}
	if op.Value == nil {
		op.Value = map[string]any{}
	}
	if err := hw.Write(op); err != nil {
		fmt.Printf("history op write error: %v\n", err)
	}
}

func elleOpKinds(f string) (string, string) {
	base := strings.TrimPrefix(f, ":")
	switch base {
	case "read", "r":
		return ":read", ":r"
	case "write", "w":
		return ":write", ":w"
	default:
		return ":" + base, ""
	}
}

func mapIso(s string) driver.Isolation {
	switch s {
	case "SERIALIZABLE":
		return driver.IsoSerializable
	case "REPEATABLE READ":
		return driver.IsoRepeatable
	case "READ COMMITTED":
		return driver.IsoReadCommitted
	default:
		return driver.IsoDefault
	}
}
