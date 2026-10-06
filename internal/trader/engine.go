package trader

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const decisionIntervalSeconds = 30

// decisionPairs preserves configuration order and includes retained exposure
// not in the current configuration after the configured pairs.
func (a *App) decisionPairs() []string {
	s := a.snapshot()
	pairs := make([]string, 0, len(s.Pairs))
	for _, pair := range a.cfg.Pairs {
		if s.Pairs[pair] != nil && !containsString(pairs, pair) {
			pairs = append(pairs, pair)
		}
	}
	for _, pair := range s.symbols() {
		if !containsString(pairs, pair) {
			pairs = append(pairs, pair)
		}
	}
	return pairs
}

// Each round evaluates every pair serially. Slow rounds never overlap or queue
// catch-up rounds: after an overrun the next round starts once this one finishes.
func runDecisionRounds(ctx context.Context, interval time.Duration, pairs func() []string, decide func(context.Context, string)) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if ctx.Err() != nil {
				return
			}
			nextRound := time.Now().Add(interval)
			for _, pair := range pairs() {
				if ctx.Err() != nil {
					return
				}
				decide(ctx, pair)
			}
			timer.Reset(max(time.Until(nextRound), 0))
		}
	}
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
						slog.Info("monitor heartbeat", "pair", pair, "quote_fresh", freshMarket(m, pair), "paused", p.Paused, "quantity", p.Qty.String(), "pending_order", p.Pending != nil, "native_stop", p.Protection != nil, "last_decision_candle", p.LastDecisionCandle, "context_interval", a.cfg.ContextInterval, "context_ready", a.cfg.ContextInterval != "" && contextReady(m, a.cfg.ContextInterval), "context_close_time", m.ContextCloseTime)
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
	runDecisionRounds(ctx, interval, a.decisionPairs, a.decide)
}
