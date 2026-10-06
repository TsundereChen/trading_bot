package trader

import (
	"context"
	"fmt"
	"time"

	"automated-trader/internal/strategy"

	"github.com/shopspring/decimal"
)

func features(cs []strategy.Candle) (strategy.Features, error) {
	if len(cs) > 0 && time.Now().UnixMilli()-cs[len(cs)-1].CloseTime > 90000 {
		return strategy.Features{}, fmt.Errorf("completed candles stale")
	}
	return strategy.CalculateFeatures(cs)
}

func (a *App) decisionSnapshot(pair string, p *Position, f strategy.Features, market MarketSnapshot) map[string]any {
	bid, ask := market.Bid, market.Ask
	spread := ask.Sub(bid).Div(bid).Mul(decimal.NewFromInt(10000)).InexactFloat64()
	eligible := !p.Paused && p.Qty.IsZero() && p.Protection == nil && len(p.UnvaluedFees) == 0 && f.Uptrend && f.Pullback && spread <= 10 && time.Now().After(p.CooldownUntil) && f.SetupID != p.LastSetup
	allowed := []string{"HOLD"}
	if eligible {
		allowed = append(allowed, "ENTER_LONG")
	}
	if p.Qty.IsPositive() {
		allowed = append(allowed, "EXIT_LONG")
	}
	return map[string]any{"pair": pair, "venue": a.cfg.Venue.label(), "timestamp": time.Now().UTC(), "market": map[string]any{"bid": bid, "ask": ask, "spread_bps": spread}, "features": f, "position": *p, "entry_eligible": eligible, "allowed_actions": allowed, "strategy_version": "trend-pullback-v1", "prompt_version": "v1"}
}

func (a *App) decide(ctx context.Context, pair string) {
	p := a.snapshot().Pairs[pair]
	if p == nil || p.Pending != nil {
		return
	}
	m := a.feeds.Snapshot(pair)
	if !freshMarket(m, pair) {
		return
	}
	f, err := features(m.Candles)
	if err != nil {
		a.metrics.Failures.WithLabelValues("signals").Inc()
		return
	}
	input := a.decisionSnapshot(pair, p, f, m)
	result, err := a.askOllaya(ctx, pair, input)
	if err != nil {
		a.metrics.Failures.WithLabelValues("ollaya").Inc()
		_ = a.update(ctx, "decision_error", map[string]any{"pair": pair, "input": input, "error": err.Error()}, func(State) error { return nil })
		return
	}
	action, valid := validatedAction(result)
	if !valid {
		a.metrics.Failures.WithLabelValues("ollaya_validation").Inc()
		return
	}
	a.mu.Lock()
	if a.state.Pairs[pair] == nil {
		a.mu.Unlock()
		return
	}
	a.lastDecisions[pair] = result
	a.metrics.Decisions.WithLabelValues(pair, action).Inc()
	a.mu.Unlock()
	if err := a.update(ctx, "decision", map[string]any{"pair": pair, "input": input, "output": result}, func(s State) error {
		if s.Pairs[pair] == nil {
			return errNoChange
		}
		return nil
	}); err != nil || !a.executionAllowed() {
		return
	}
	gate := a.pairLock(pair)
	if !gate.TryLock() {
		return
	}
	defer gate.Unlock()
	current := a.snapshot().Pairs[pair]
	if current == nil || current.Version != p.Version || current.Pending != nil {
		return
	}
	m = a.feeds.Snapshot(pair)
	if !freshMarket(m, pair) {
		return
	}
	latest, err := features(m.Candles)
	if err != nil || latest.SetupID != f.SetupID {
		return
	}
	if action == "EXIT_LONG" && current.Qty.IsPositive() && latest.Reversal {
		if err := a.closeUnderGate(ctx, pair, "model_exit"); err != nil {
			a.fail(context.WithoutCancel(ctx), pair, "execution", err)
		}
		return
	}
	if action != "ENTER_LONG" || a.decisionSnapshot(pair, current, latest, m)["entry_eligible"] != true {
		return
	}
	distance := decimal.NewFromFloat(latest.ATR * 1.5)
	stop := m.Bid.Sub(distance)
	if symbol, ok := a.symbolFor(pair); ok && symbol.Tick.IsPositive() {
		stop = strategy.FloorStep(stop, symbol.Tick)
	}
	if !stop.IsPositive() {
		return
	}
	qty := decimal.Min(a.cfg.MaxPosition.Div(m.Ask), a.cfg.RiskPerTrade.Div(m.Ask.Sub(stop)))
	qty = decimal.Min(qty, current.Cash.Sub(current.ExternalFeeQuote).Div(m.Ask.Mul(decimal.NewFromFloat(1.01))))
	if !qty.IsPositive() {
		return
	}
	if err := a.update(ctx, "setup_consumed", map[string]any{"pair": pair, "features": latest}, func(s State) error {
		pos := s.Pairs[pair]
		if pos == nil || pos.Version != current.Version || pos.Paused || pos.Pending != nil || !pos.Qty.IsZero() {
			return fmt.Errorf("entry state changed")
		}
		pos.LastSetup, pos.Stop, pos.Target = latest.SetupID, stop, m.Ask.Add(distance.Mul(decimal.NewFromInt(2)))
		return nil
	}); err != nil {
		return
	}
	if err := a.submitMarket(ctx, pair, "BUY", qty, "model_entry"); err != nil {
		a.fail(context.WithoutCancel(ctx), pair, "execution", err)
	}
}
