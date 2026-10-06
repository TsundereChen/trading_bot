package trader

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const decisionIntervalSeconds = 30

func decisionCycleSeconds(pairs int) int {
	return decisionIntervalSeconds * max(1, pairs)
}

func (a *App) Run(ctx context.Context) {
	var services sync.WaitGroup
	services.Add(3)
	go func() { defer services.Done(); a.feeds.Run(ctx, a.binance) }()
	go func() {
		defer services.Done()
		var workers sync.WaitGroup
		defer workers.Wait()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		lastHeartbeat := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.dispatchPoll(ctx, &workers)
				if time.Since(lastHeartbeat) >= 30*time.Second {
					for pair, p := range a.snapshot().Pairs {
						m := a.feeds.Snapshot(pair)
						slog.Info("monitor heartbeat", "pair", pair, "quote_fresh", freshMarket(m, pair), "paused", p.Paused, "quantity", p.Qty.String(), "pending_order", p.Pending != nil, "native_stop", p.Protection != nil, "last_decision_candle", p.LastDecisionCandle)
					}
					lastHeartbeat = time.Now()
				}
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
	interval := time.Duration(a.cfg.decisionSeconds()) * time.Second
	timer := time.NewTimer(interval)
	defer timer.Stop()
	index := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pairs := a.snapshot().symbols()
			if len(pairs) == 0 {
				timer.Reset(interval)
				continue
			}
			pair := pairs[index%len(pairs)]
			index++
			nextRequest := time.Now().Add(interval)
			a.decide(ctx, pair)
			delay := time.Until(nextRequest)
			if delay < 0 {
				delay = 0
			}
			timer.Reset(delay)
		}
	}
}
