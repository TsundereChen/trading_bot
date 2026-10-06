package trader

import (
	"context"
	"sync"
	"time"
)

const decisionIntervalSeconds = 5

func decisionCycleSeconds(pairs int) int {
	if pairs < 1 {
		pairs = 1
	}
	return decisionIntervalSeconds * pairs
}

func (a *App) Run(ctx context.Context) {
	var services sync.WaitGroup
	services.Add(3)
	go func() { defer services.Done(); a.feeds.Run(ctx, a.binance) }()
	go func() {
		defer services.Done()
		var workers sync.WaitGroup
		defer workers.Wait()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.dispatchPoll(ctx, &workers)
			}
		}
	}()
	go func() {
		defer services.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pruneCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err := a.repo.Prune(pruneCtx, time.Now().UTC().AddDate(0, 0, -a.cfg.AuditDays), a.cfg.AuditMaxEvents)
				cancel()
				if err != nil && ctx.Err() == nil {
					a.metrics.Failures.WithLabelValues("retention").Inc()
				}
			}
		}
	}()
	defer services.Wait()
	ticker := time.NewTicker(decisionIntervalSeconds * time.Second)
	defer ticker.Stop()
	index := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pairs := a.snapshot().symbols()
			if len(pairs) == 0 {
				continue
			}
			pair := pairs[index%len(pairs)]
			index++
			a.decide(ctx, pair)
		}
	}
}
