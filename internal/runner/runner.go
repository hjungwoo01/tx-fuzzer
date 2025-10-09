package runner

import (
	"context"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hjungwoo01/elle-runner/internal/config"
	"github.com/hjungwoo01/elle-runner/internal/driver"
	"github.com/hjungwoo01/elle-runner/internal/history"
	"github.com/hjungwoo01/elle-runner/internal/workload"
)

type Env struct {
	Drv   driver.Driver
	Cfg   *config.Root
	Hist  *history.Writer
	Start time.Time
}

var (
	keyMapMu  sync.Mutex
	keyMap    = make(map[string]int)
	nextKeyID int
)

func mapKeyStable(s string) int {
	keyMapMu.Lock()
	defer keyMapMu.Unlock()
	if id, ok := keyMap[s]; ok {
		return id
	}
	id := nextKeyID
	nextKeyID++
	keyMap[s] = id
	return id
}

var reKDigits = regexp.MustCompile(`^k(\d+)$`)

func toIntKey(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case uint:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	case string:
		// numeric string
		if n, err := strconv.Atoi(x); err == nil {
			return n
		}
		// pattern kNNN
		if m := reKDigits.FindStringSubmatch(x); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil {
				return n
			}
		}
		// fallback: stable map
		return mapKeyStable(x)
	default:
		return mapKeyStable(fmt.Sprintf("%v", v))
	}
}

func subRefInt(expr string, args []any) int {
	if expr == "" || expr[0] != '$' {
		// literal key expression
		return toIntKey(expr)
	}
	var i int
	fmt.Sscanf(expr, "$%d", &i)
	if i > 0 && i <= len(args) {
		return toIntKey(args[i-1])
	}
	return toIntKey(expr)
}

func Run(ctx context.Context, env *Env) error {
	wl := env.Cfg.Workload
	plans := workload.BuildPlans(wl)
	chooser := workload.NewChooser(wl.Mix)
	start := time.Now()
	end := start.Add(env.Cfg.Scheduling.Duration)
	clients := env.Cfg.Scheduling.Clients

	errCh := make(chan error, clients)
	barrierEvery := env.Cfg.Scheduling.BarrierEvery
	bar := NewCyclicBarrier(env.Cfg.Scheduling.Clients)

	// start clients
	for pid := 0; pid < clients; pid++ {
		go func(process int) {
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

				stepVals := []any{}
				fail := false
				// Per-transaction Elle mop collection
				mops := make([]history.Mop, 0, 8)
				var lastMono int64

				for si, st := range plan.Template.Steps {
					// Barrier (optional): align starts
					if barrierEvery > 0 && iter%barrierEvery == 0 && si == 0 {
						// Align starts; don’t block forever—use a small timeout
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

					// exec
					if hasSelect(st.SQL) {
						row, err := tx.QueryRow(tctx, st.SQL, args...)
						if err != nil {
							_ = tx.Rollback(tctx)
							cancel()
							fail = true
							break
						}
						var v any
						if st.Elle != nil && st.Elle.ValueFromResultCol != nil {
							// assume single column result for simplicity
							if err := row.Scan(&v); err != nil {
								_ = tx.Rollback(tctx)
								cancel()
								fail = true
								break
							}
						} else {
							// ignore row value
							var dummy any
							_ = row.Scan(&dummy)
						}
						if st.Elle != nil {
							val := v
							if st.Elle.Value != nil {
								val = st.Elle.Value
							} else if st.Elle.ValueFromResultCol == nil && val == nil {
								val = valueArgGuess(args)
							}
							kind := st.Elle.F
							if kind == ":read" {
								kind = ":r"
							} else {
								kind = ":w"
							}

							// OUTPUT CAPTURED
							mops = append(mops, history.Mop{Kind: kind, Key: subRefInt(st.Elle.Key, args), Value: val})
							lastMono = time.Since(env.Start).Nanoseconds()
						}
					} else {
						_, err := tx.Exec(tctx, st.SQL, args...)
						if err != nil {
							_ = tx.Rollback(tctx)
							cancel()
							fail = true
							break
						}
						if st.Elle != nil {
							val := st.Elle.Value
							if val == nil {
								val = valueArgGuess(args)
							}
							kind := st.Elle.F
							if kind == ":read" {
								kind = ":r"
							} else {
								kind = ":w"
							}
							mops = append(mops, history.Mop{Kind: kind, Key: subRefInt(st.Elle.Key, args), Value: val})
							lastMono = time.Since(env.Start).Nanoseconds()
						}
					}

					if st.Sleep > 0 {
						time.Sleep(st.Sleep)
					}

					// optional random cancels
					if env.Cfg.Scheduling.RandomKillPct > 0 && rand.IntN(100) < env.Cfg.Scheduling.RandomKillPct {
						_ = tx.Rollback(tctx)
						cancel()
						fail = true
						break
					}

					stepVals = args
				}

				if !fail {
					if err := tx.Commit(tctx); err != nil {
						_ = tx.Rollback(tctx)
					} else {
						if len(mops) > 0 {
							_ = env.Hist.WriteTxn(process, lastMono, time.Now().UnixNano(), mops)
						}
					}
					cancel()
				}
			}
		}(pid)
	}

	for i := 0; i < clients; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
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
func getF(st config.Step) string {
	if st.Elle != nil {
		return st.Elle.F
	}
	return ":exec"
}
func subRefKey(st config.Step, args []any) string {
	if st.Elle != nil {
		return subRef(st.Elle.Key, args)
	}
	return ""
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
