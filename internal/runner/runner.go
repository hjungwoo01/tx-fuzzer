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

	// start clients
	for pid := 0; pid < clients; pid++ {
		go func(process int) {
			fmt.Printf("[diag] starting client %d\n", process)
			defer func() { errCh <- nil }()
			iter := 0
			for time.Now().Before(end) {
				iter++
				name := chooser.Pick()
				plan := plans[name]
				// fmt.Printf("[diag] picked plan %s -> %d steps\n", name, len(plan.Template.Steps))
				// for i, st := range plan.Template.Steps {
				// 	fmt.Printf("[diag] step %d elle? %#v\n", i, st.Elle)
				// }
				iso := mapIso(plan.Template.Isolation)

				tctx, cancel := workload.ContextWithTimeout(ctx, plan.Template.Timeout)
				tx, err := env.Drv.Begin(tctx, iso)
				if err != nil {
					cancel()
					continue
				}

				// txnID := history.NextTxnID()
				stepVals := []any{}
				fail := false
				rolledBack := false
				// Per-transaction Elle mop collection
				invokeMops := make([]history.Mop, 0, 8)
				okMops := make([]history.Mop, 0, 8)
				var firstElleMono int64
				var firstElleWall int64
				var lastElleMono int64
				var lastElleWall int64
				firstWriteDiagDone := false

				for si, st := range plan.Template.Steps {
					// Barrier (optional): align starts
					if barrierEvery > 0 && iter%barrierEvery == 0 && si == 0 {
						// Align starts; don't block forever -- use a small timeout
						_ = bar.Await(200 * time.Millisecond)
					}

					// materialize args
					gen := plan.ArgsPerStep[si]
					args := workload.MaterializeArgs(gen, stepVals)

					// allow simple literals in YAML to override nils
					for i, raw := range st.Args {
						switch v := raw.(type) {
						case string:
							if v != "" && v[0] != '$' {
								args[i] = v
							}
						case int:
							if args[i] == nil {
								args[i] = v
							}
						}
					}

					// track Elle operations for transactional history
					stepStartMono := time.Since(env.Start).Nanoseconds()
					stepStartWall := time.Now().UnixNano()
					haveElle := st.Elle != nil
					elleOpKind := ""
					elleMopKind := ""
					elleKeyStr := ""
					var elleInvokeVal any
					var elleOkVal any
					elleIdx := -1

					if haveElle {
						elleOpKind, elleMopKind = elleOpKinds(st.Elle.F)
						elleKeyStr = subRef(st.Elle.Key, args)
						if len(invokeMops) == 0 {
							firstElleMono = stepStartMono
							firstElleWall = stepStartWall
						}
						if elleOpKind == ":read" {
							elleInvokeVal = nil
						} else {
							val := st.Elle.Value
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
						invokeMops = append(invokeMops, history.Mop{
							Kind:  elleMopKind,
							Key:   elleKeyStr,
							Value: invokeVal,
						})
						okMops = append(okMops, history.Mop{
							Kind:  elleMopKind,
							Key:   elleKeyStr,
							Value: okVal,
						})
						elleIdx = len(okMops) - 1
						if !firstWriteDiagDone {
							// if env.Hist == nil {
							// 	fmt.Printf("[diag] process %d has nil Hist!\n", process)
							// } else {
							// 	fmt.Printf("[diag] process %d about to write txn %s\n", process, txnID)
							// }
							firstWriteDiagDone = true
						}
					}

					// exec
					if hasSelect(st.SQL) {
						row, err := tx.QueryRow(tctx, st.SQL, args...)
						if err != nil {
							fmt.Printf("[warn] process %d step %d query error: %v (sql=%s)\n", process, si, err, st.SQL)
							_ = tx.Rollback(tctx)
							cancel()
							fail = true
							rolledBack = true
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
							break
						}
						var scanVal any
						if st.Elle != nil && st.Elle.ValueFromResultCol != nil {
							if err := row.Scan(&scanVal); err != nil {
								fmt.Printf("[warn] process %d step %d scan error: %v (sql=%s)\n", process, si, err, st.SQL)
								_ = tx.Rollback(tctx)
								cancel()
								fail = true
								rolledBack = true
								lastElleMono = time.Since(env.Start).Nanoseconds()
								lastElleWall = time.Now().UnixNano()
								break
							}
						} else {
							var dummy any
							if err := row.Scan(&dummy); err != nil {
								fmt.Printf("[warn] process %d step %d scan error: %v (sql=%s)\n", process, si, err, st.SQL)
								_ = tx.Rollback(tctx)
								cancel()
								fail = true
								rolledBack = true
								lastElleMono = time.Since(env.Start).Nanoseconds()
								lastElleWall = time.Now().UnixNano()
								break
							}
							scanVal = dummy
						}
						if haveElle {
							val := scanVal
							if st.Elle.Value != nil {
								val = st.Elle.Value
							} else if st.Elle.ValueFromResultCol == nil && val == nil {
								val = valueArgGuess(args)
							}
							elleOkVal = val
							if elleIdx >= 0 {
								okMops[elleIdx].Value = val
							}
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
						}
					} else {
						if _, err := tx.Exec(tctx, st.SQL, args...); err != nil {
							fmt.Printf("[warn] process %d step %d exec error: %v (sql=%s)\n", process, si, err, st.SQL)
							_ = tx.Rollback(tctx)
							cancel()
							fail = true
							rolledBack = true
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
							break
						}
						if haveElle {
							val := st.Elle.Value
							if val == nil {
								val = valueArgGuess(args)
							}
							elleOkVal = val
							if elleIdx >= 0 {
								okMops[elleIdx].Value = val
							}
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
						}
					}

					if st.Sleep > 0 {
						time.Sleep(st.Sleep)
					}

					stepVals = args
					if env.Cfg.Scheduling.RandomKillPct > 0 && rand.IntN(100) < env.Cfg.Scheduling.RandomKillPct {
						_ = tx.Rollback(tctx)
						fail = true
						rolledBack = true
						lastElleMono = time.Since(env.Start).Nanoseconds()
						lastElleWall = time.Now().UnixNano()
						break
					}
				}

				if !fail {
					if err := tx.Commit(tctx); err != nil {
						_ = tx.Rollback(tctx)
						fail = true
						rolledBack = true
					} else {
						if len(invokeMops) > 0 {
							if firstElleMono == 0 {
								firstElleMono = time.Since(env.Start).Nanoseconds()
								firstElleWall = time.Now().UnixNano()
							}
							if lastElleMono == 0 {
								lastElleMono = time.Since(env.Start).Nanoseconds()
								lastElleWall = time.Now().UnixNano()
							}
							recordTxnOp(env.Hist, history.Invoke, process, firstElleMono, firstElleWall, invokeMops)
							recordTxnOp(env.Hist, history.Ok, process, lastElleMono, lastElleWall, okMops)
						}
						cancel()
					}
				}
				if fail {
					if !rolledBack {
						_ = tx.Rollback(tctx)
					}
					if len(invokeMops) > 0 {
						if firstElleMono == 0 {
							firstElleMono = time.Since(env.Start).Nanoseconds()
							firstElleWall = time.Now().UnixNano()
						}
						if lastElleMono == 0 {
							lastElleMono = time.Since(env.Start).Nanoseconds()
							lastElleWall = time.Now().UnixNano()
						}
						recordTxnOp(env.Hist, history.Invoke, process, firstElleMono, firstElleWall, invokeMops)
						recordTxnOp(env.Hist, history.Fail, process, lastElleMono, lastElleWall, okMops)
					}
					cancel()
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

func recordTxnOp(hw *history.Writer, opType history.OpType, process int, timeNS, wallNS int64, mops []history.Mop) {
	if hw == nil || len(mops) == 0 {
		return
	}
	if err := hw.WriteTxn(opType, process, timeNS, wallNS, mops); err != nil {
		fmt.Printf("history txn write error: %v\n", err)
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
