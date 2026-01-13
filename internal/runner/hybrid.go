package runner

import (
	"context"
	"time"
)

// RunHybrid executes the deterministic replay schedule alongside a background
// workload run. This preserves existing edges while attempting to force a
// specific missing edge via replay.
func RunHybrid(ctx context.Context, env *Env) error {
	replayCfg := env.Cfg.Replay
	start := env.Start
	if start.IsZero() {
		start = time.Now()
	}

	bgCfg := *env.Cfg
	bgCfg.Replay.Enabled = false
	if replayCfg.BackgroundClients > 0 {
		bgCfg.Scheduling.Clients = replayCfg.BackgroundClients
	}
	if replayCfg.BackgroundDuration > 0 {
		bgCfg.Scheduling.Duration = replayCfg.BackgroundDuration
	}

	bgEnv := &Env{
		Drv:              env.Drv,
		Cfg:              &bgCfg,
		Hist:             env.Hist,
		Start:            start,
		SkipHistoryClose: true,
	}
	replayRootCfg := *env.Cfg
	if replayRootCfg.Replay.ProcessOffset <= 0 && replayRootCfg.Replay.BackgroundClients > 0 {
		replayRootCfg.Replay.ProcessOffset = replayRootCfg.Replay.BackgroundClients
	}
	replayEnv := &Env{
		Drv:   env.Drv,
		Cfg:   &replayRootCfg,
		Hist:  env.Hist,
		Start: start,
	}

	errCh := make(chan error, 2)
	go func() { errCh <- Run(ctx, bgEnv) }()
	go func() { errCh <- RunReplay(ctx, replayEnv) }()

	var runErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && runErr == nil {
			runErr = err
		}
	}
	return runErr
}
