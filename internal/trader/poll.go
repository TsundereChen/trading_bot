package trader

import (
	"context"
	"sync"
	"time"
)

func (a *App) dispatchPoll(ctx context.Context, wg *sync.WaitGroup) {
	a.refreshMetrics()
	if !a.executionAllowed() {
		return
	}
	for _, pair := range a.snapshot().symbols() {
		gate := a.pairLock(pair)
		if !gate.TryLock() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer gate.Unlock()
			a.pollPair(ctx, pair)
			a.refreshMetrics()
		}()
	}
}

// Synchronous wrapper used by tests; Run dispatches independently so a slow
// pair does not stretch another pair's five-second reconciliation schedule.
func (a *App) poll(ctx context.Context) {
	var wg sync.WaitGroup
	a.dispatchPoll(ctx, &wg)
	wg.Wait()
}

func (a *App) pollPair(ctx context.Context, pair string) {
	workCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if !a.executionAllowed() {
		return
	}
	p := a.snapshot().Pairs[pair]
	if p == nil {
		return
	}
	if p.Pending != nil {
		order, err := a.binance.FindPending(workCtx, pair, p.Pending)
		if err == nil {
			err = a.applyOrder(workCtx, pair, order, false)
		}
		if err != nil {
			a.fail(context.WithoutCancel(ctx), pair, "reconciliation", err)
			return
		}
	}
	p = a.snapshot().Pairs[pair]
	if p == nil {
		return
	}
	if p.Protection != nil {
		order, err := a.binance.FindPending(workCtx, pair, p.Protection)
		if err == nil {
			err = a.applyOrder(workCtx, pair, order, true)
		}
		if err != nil {
			a.fail(context.WithoutCancel(ctx), pair, "native_stop", err)
			return
		}
	}
	p = a.snapshot().Pairs[pair]
	if p == nil || p.Pending != nil {
		return
	}
	m := a.feeds.Snapshot(pair)
	if freshMarket(m, pair) {
		if len(p.UnvaluedFees) == 0 {
			if err := a.update(workCtx, "performance_mark", map[string]string{"pair": pair}, func(s State) error {
				pos := s.Pairs[pair]
				if pos == nil {
					return errNoChange
				}
				initialized := pos.Performance == nil
				if !markPerformance(pos, m.Bid) && !initialized {
					return errNoChange
				}
				return nil
			}); err != nil {
				return
			}
		}
		day := time.Now().UTC().Format("2006-01-02")
		if p.Day != day && len(p.UnvaluedFees) == 0 {
			if err := a.update(workCtx, "day_rollover", map[string]any{"pair": pair}, func(s State) error {
				pos := s.Pairs[pair]
				if pos == nil || pos.Day == day {
					return errNoChange
				}
				pos.Day, pos.DayEquity = day, pos.equity(m.Bid)
				return nil
			}); err != nil {
				return
			}
		}
		p = a.snapshot().Pairs[pair]
		if p == nil {
			return
		}
		dailyHit := p.Day == day && p.DayEquity.Sub(p.equity(m.Bid)).GreaterThanOrEqual(a.cfg.DailyLoss)
		if dailyHit && (!p.Paused || p.Starting) {
			if err := a.update(workCtx, "daily_loss_limit", map[string]string{"pair": pair}, func(s State) error {
				pos := s.Pairs[pair]
				if pos == nil {
					return errNoChange
				}
				pos.Paused = true
				pos.Starting = false
				return nil
			}); err != nil {
				return
			}
		}
		if p.Qty.IsPositive() && (m.Bid.LessThanOrEqual(p.Stop) || (p.Target.IsPositive() && m.Bid.GreaterThanOrEqual(p.Target)) || dailyHit) {
			if err := a.closeUnderGate(workCtx, pair, "protective_exit"); err != nil {
				a.fail(context.WithoutCancel(ctx), pair, "execution", err)
			}
			return
		}
	}
	// Native protection is established even when the local quote stream is down.
	if err := a.ensureProtection(workCtx, pair); err != nil {
		a.fail(context.WithoutCancel(ctx), pair, "native_stop", err)
		pos := a.snapshot().Pairs[pair]
		if pos != nil && pos.Protection == nil && pos.Pending == nil && pos.Qty.IsPositive() && freshMarket(a.feeds.Snapshot(pair), pair) {
			if closeErr := a.closeUnderGate(workCtx, pair, "native_stop_failed"); closeErr != nil {
				a.fail(context.WithoutCancel(ctx), pair, "execution", closeErr)
			}
		}
	}
	if err := a.valueExternalFees(workCtx, pair); err != nil {
		a.fail(context.WithoutCancel(ctx), pair, "fee_valuation", err)
		return
	}
	if err := a.autoEnableEntries(workCtx, pair); err != nil {
		a.fail(context.WithoutCancel(ctx), pair, "startup_reconciliation", err)
	}
}
