package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hjungwoo01/tx-fuzzer/internal/config"
	"github.com/hjungwoo01/tx-fuzzer/internal/driver"
	"github.com/hjungwoo01/tx-fuzzer/internal/history"
	"github.com/hjungwoo01/tx-fuzzer/internal/workload"
)

type replayTxnState struct {
	alias      string
	spec       config.ReplayTxn
	plan       workload.TxnPlan
	process    int
	txnID      string
	ctx        context.Context
	cancel     context.CancelFunc
	tx         driver.Tx
	stepVals   []any
	invokeMops []history.Mop
	okMops     []history.Mop
	stepIndex  int
	firstMono  int64
	firstWall  int64
	lastMono   int64
	lastWall   int64
	committed  bool
	rolledBack bool
}

func RunReplay(ctx context.Context, env *Env) error {
	replayCfg := env.Cfg.Replay
	if !replayCfg.Enabled {
		return errors.New("replay requested without replay.enabled=true")
	}
	plans := workload.BuildPlans(env.Cfg.Workload)
	env.Start = time.Now()

	states := make(map[string]*replayTxnState, len(replayCfg.Transactions))
	usedProcess := map[int]struct{}{}
	processCounter := 0

	for _, spec := range replayCfg.Transactions {
		plan, ok := plans[spec.Txn]
		if !ok {
			return fmt.Errorf("replay: unknown txn template %q", spec.Txn)
		}
		alias := spec.Alias
		if alias == "" {
			return fmt.Errorf("replay: transaction entry missing alias for template %q", spec.Txn)
		}
		if _, exists := states[alias]; exists {
			return fmt.Errorf("replay: duplicate alias %q", alias)
		}
		proc := spec.Process
		if proc == 0 {
			for {
				if _, taken := usedProcess[processCounter]; !taken {
					proc = processCounter
					processCounter++
					break
				}
				processCounter++
			}
		} else {
			if _, taken := usedProcess[proc]; taken {
				return fmt.Errorf("replay: process id %d reused", proc)
			}
		}
		usedProcess[proc] = struct{}{}
		states[alias] = &replayTxnState{
			alias:   alias,
			spec:    spec,
			plan:    plan,
			process: proc,
		}
	}

	for idx, step := range replayCfg.Schedule {
		action := step.Action
		if action == "" {
			action = "step"
		}
		switch action {
		case "sleep":
			if step.Sleep <= 0 {
				return fmt.Errorf("replay: schedule[%d] sleep with non-positive duration", idx)
			}
			time.Sleep(step.Sleep)
		case "step":
			alias := step.Alias
			state, ok := states[alias]
			if !ok {
				return fmt.Errorf("replay: schedule[%d] references unknown alias %q", idx, alias)
			}
			if err := state.ensureStarted(ctx, env); err != nil {
				return fmt.Errorf("replay: schedule[%d] start %q: %w", idx, alias, err)
			}
			count := step.Steps
			if count <= 0 {
				count = len(state.plan.Queries) - state.stepIndex
			}
			if count <= 0 {
				continue
			}
			if err := state.runSteps(env, count); err != nil {
				return fmt.Errorf("replay: schedule[%d] step %q: %w", idx, alias, err)
			}
		case "commit":
			alias := step.Alias
			state, ok := states[alias]
			if !ok {
				return fmt.Errorf("replay: schedule[%d] commit unknown alias %q", idx, alias)
			}
			if err := state.ensureStarted(ctx, env); err != nil {
				return fmt.Errorf("replay: schedule[%d] commit start %q: %w", idx, alias, err)
			}
			if err := state.commit(env); err != nil {
				return fmt.Errorf("replay: schedule[%d] commit %q: %w", idx, alias, err)
			}
		case "rollback":
			alias := step.Alias
			state, ok := states[alias]
			if !ok {
				return fmt.Errorf("replay: schedule[%d] rollback unknown alias %q", idx, alias)
			}
			if err := state.ensureStarted(ctx, env); err != nil {
				return fmt.Errorf("replay: schedule[%d] rollback start %q: %w", idx, alias, err)
			}
			if err := state.rollback(env); err != nil {
				return fmt.Errorf("replay: schedule[%d] rollback %q: %w", idx, alias, err)
			}
		default:
			return fmt.Errorf("replay: schedule[%d] has unsupported action %q", idx, action)
		}
	}

	// Auto-finalize any transactions left open.
	for alias, state := range states {
		if state.tx == nil || state.committed || state.rolledBack {
			continue
		}
		if state.stepIndex >= len(state.plan.Queries) {
			if err := state.commit(env); err != nil {
				return fmt.Errorf("replay: finalize commit %q: %w", alias, err)
			}
			continue
		}
		if err := state.rollback(env); err != nil {
			return fmt.Errorf("replay: finalize rollback %q: %w", alias, err)
		}
	}
	return nil
}

func (s *replayTxnState) ensureStarted(parent context.Context, env *Env) error {
	if s.tx != nil {
		return nil
	}
	if s.committed || s.rolledBack {
		return fmt.Errorf("transaction %s already finalized", s.alias)
	}
	iso := mapIso(s.plan.Template.Isolation)
	var timeout time.Duration
	if s.spec.Timeout > 0 {
		timeout = s.spec.Timeout
	} else {
		timeout = s.plan.Template.Timeout
	}
	tctx, cancel := workload.ContextWithTimeout(parent, timeout)
	tx, err := env.Drv.Begin(tctx, iso)
	if err != nil {
		cancel()
		return err
	}
	s.ctx = tctx
	s.cancel = cancel
	s.tx = tx
	s.stepVals = nil
	s.txnID = history.NextTxnID()
	s.invokeMops = make([]history.Mop, 0, len(s.plan.Queries))
	s.okMops = make([]history.Mop, 0, len(s.plan.Queries))
	s.stepIndex = 0
	s.firstMono = 0
	s.firstWall = 0
	s.lastMono = 0
	s.lastWall = 0
	s.committed = false
	s.rolledBack = false
	return nil
}

func (s *replayTxnState) runSteps(env *Env, count int) error {
	for i := 0; i < count && s.stepIndex < len(s.plan.Queries); i++ {
		if err := s.runSingleStep(env); err != nil {
			return err
		}
	}
	return nil
}

func (s *replayTxnState) runSingleStep(env *Env) error {
	if s.tx == nil {
		return errors.New("transaction not started")
	}
	qp := s.plan.Queries[s.stepIndex]
	st := qp.Template
	gen := qp.ArgsSpec
	args := workload.MaterializeArgs(gen, s.stepVals)
	overrideLiteralArgs(args, st.Args)

	stepIndex := s.stepIndex + 1
	stepStartMono := time.Since(env.Start).Nanoseconds()
	stepStartWall := time.Now().UnixNano()

	haveElle := st.Elle != nil
	var elleOpKind, elleMopKind, elleKeyStr string
	var elleInvokeVal any
	var elleOkVal any
	elleIdx := -1

	if haveElle {
		elleOpKind, elleMopKind = elleOpKinds(st.Elle.F)
		elleKeyStr = subRef(st.Elle.Key, args)
		if len(s.invokeMops) == 0 {
			s.firstMono = stepStartMono
			s.firstWall = stepStartWall
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
		s.invokeMops = append(s.invokeMops, history.Mop{
			Kind:  elleMopKind,
			Key:   elleKeyStr,
			Value: invokeVal,
		})
		s.okMops = append(s.okMops, history.Mop{
			Kind:  elleMopKind,
			Key:   elleKeyStr,
			Value: okVal,
		})
		elleIdx = len(s.okMops) - 1
	}

	invokeDetails := map[string]any{}
	if haveElle {
		invokeDetails["elle_f"] = elleOpKind
		invokeDetails["elle_key"] = elleKeyStr
		if elleInvokeVal != nil {
			invokeDetails["elle_value"] = elleInvokeVal
		}
	}
	recordQueryOp(env.Hist, history.Invoke, s.process, s.txnID, stepIndex, st.SQL, args, invokeDetails, "", stepStartMono, stepStartWall)

	var rowsAffected int64 = -1
	var selectVal any
	var execErr error

	if hasSelect(st.SQL) {
		row, err := s.tx.QueryRow(s.ctx, st.SQL, args...)
		if err != nil {
			execErr = err
		} else {
			if st.Elle != nil && st.Elle.ValueFromResultCol != nil {
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
			if st.Elle.Value != nil {
				val = st.Elle.Value
			} else if st.Elle.ValueFromResultCol == nil && val == nil {
				val = valueArgGuess(args)
			}
			elleOkVal = val
			if elleIdx >= 0 {
				s.okMops[elleIdx].Value = val
			}
			selectVal = val
		}
	} else {
		rowsAffected, execErr = s.tx.Exec(s.ctx, st.SQL, args...)
		if execErr == nil && haveElle {
			val := st.Elle.Value
			if val == nil {
				val = valueArgGuess(args)
			}
			elleOkVal = val
			if elleIdx >= 0 {
				s.okMops[elleIdx].Value = val
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
		errWrapped := fmt.Errorf("step %d failed: %w", s.stepIndex, execErr)
		recordQueryOp(env.Hist, history.Fail, s.process, s.txnID, stepIndex, st.SQL, args, okDetails, execErr.Error(), opEndMono, opEndWall)
		recordTxnBoundary(env.Hist, history.Fail, s.process, s.txnID, ":rollback", map[string]any{
			"sql":  st.SQL,
			"step": stepIndex,
		}, execErr.Error(), opEndMono, opEndWall)
		return s.fail(env, opEndMono, opEndWall, errWrapped)
	}

	s.lastMono = opEndMono
	s.lastWall = opEndWall
	recordQueryOp(env.Hist, history.Ok, s.process, s.txnID, stepIndex, st.SQL, args, okDetails, "", opEndMono, opEndWall)

	if st.Sleep > 0 {
		time.Sleep(st.Sleep)
	}

	s.stepVals = args
	s.stepIndex++
	return nil
}

func (s *replayTxnState) fail(env *Env, failMono, failWall int64, cause error) error {
	if s.tx != nil {
		_ = s.tx.Rollback(s.ctx)
	}
	s.rolledBack = true
	if len(s.invokeMops) > 0 {
		if s.firstMono == 0 {
			s.firstMono = failMono
			s.firstWall = failWall
		}
	}
	if failMono != 0 {
		s.lastMono = failMono
	} else {
		s.lastMono = time.Since(env.Start).Nanoseconds()
	}
	if failWall != 0 {
		s.lastWall = failWall
	} else {
		s.lastWall = time.Now().UnixNano()
	}
	s.emitHistory(env, history.Fail)
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.tx = nil
	return cause
}

func (s *replayTxnState) commit(env *Env) error {
	if s.tx == nil {
		if s.committed {
			return nil
		}
		if s.rolledBack {
			return errors.New("transaction already rolled back")
		}
		return errors.New("commit called before transaction start")
	}
	if s.committed {
		return nil
	}
	if s.rolledBack {
		return errors.New("transaction already rolled back")
	}
	if err := s.tx.Commit(s.ctx); err != nil {
		_ = s.tx.Rollback(s.ctx)
		s.rolledBack = true
		s.lastMono = time.Since(env.Start).Nanoseconds()
		s.lastWall = time.Now().UnixNano()
		recordTxnBoundary(env.Hist, history.Fail, s.process, s.txnID, ":commit", nil, err.Error(), s.lastMono, s.lastWall)
		s.emitHistory(env, history.Fail)
		s.cancel()
		s.tx = nil
		return err
	}
	if len(s.invokeMops) > 0 {
		if s.firstMono == 0 {
			s.firstMono = time.Since(env.Start).Nanoseconds()
			s.firstWall = time.Now().UnixNano()
		}
	}
	s.lastMono = time.Since(env.Start).Nanoseconds()
	s.lastWall = time.Now().UnixNano()
	recordTxnBoundary(env.Hist, history.Ok, s.process, s.txnID, ":commit", nil, "", s.lastMono, s.lastWall)
	if len(s.invokeMops) > 0 {
		if s.firstMono == 0 {
			s.firstMono = s.lastMono
			s.firstWall = s.lastWall
		}
		s.emitHistory(env, history.Ok)
	}
	s.committed = true
	s.cancel()
	s.cancel = nil
	s.tx = nil
	return nil
}

func (s *replayTxnState) rollback(env *Env) error {
	if s.tx == nil {
		if s.rolledBack {
			return nil
		}
		return errors.New("rollback called before transaction start")
	}
	if s.rolledBack {
		return nil
	}
	if err := s.tx.Rollback(s.ctx); err != nil {
		recordTxnBoundary(env.Hist, history.Fail, s.process, s.txnID, ":rollback", nil, err.Error(), time.Since(env.Start).Nanoseconds(), time.Now().UnixNano())
		return err
	}
	s.rolledBack = true
	if s.lastMono == 0 {
		s.lastMono = time.Since(env.Start).Nanoseconds()
		s.lastWall = time.Now().UnixNano()
	}
	recordTxnBoundary(env.Hist, history.Ok, s.process, s.txnID, ":rollback", nil, "", s.lastMono, s.lastWall)
	s.emitHistory(env, history.Fail)
	s.cancel()
	s.cancel = nil
	s.tx = nil
	return nil
}

func (s *replayTxnState) emitHistory(env *Env, result history.OpType) {
	if env.Hist == nil || len(s.invokeMops) == 0 {
		return
	}
	if s.firstMono == 0 {
		s.firstMono = time.Since(env.Start).Nanoseconds()
	}
	if s.firstWall == 0 {
		s.firstWall = time.Now().UnixNano()
	}
	if s.lastMono == 0 {
		s.lastMono = time.Since(env.Start).Nanoseconds()
	}
	if s.lastWall == 0 {
		s.lastWall = time.Now().UnixNano()
	}
	recordTxnOp(env.Hist, history.Invoke, s.process, s.firstMono, s.firstWall, s.invokeMops, s.plan.Template.Name, s.txnID)
	recordTxnOp(env.Hist, result, s.process, s.lastMono, s.lastWall, s.okMops, s.plan.Template.Name, s.txnID)
}
