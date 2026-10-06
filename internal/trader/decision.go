package trader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"automated-trader/internal/strategy"

	"github.com/shopspring/decimal"
)

func features(cs []strategy.Candle, interval ...string) (strategy.Features, error) {
	value := "1m"
	if len(interval) > 0 {
		value = interval[0]
	}
	if len(cs) > 0 && time.Now().UnixMilli()-cs[len(cs)-1].CloseTime > (candleDuration(value)+30*time.Second).Milliseconds() {
		return strategy.Features{}, fmt.Errorf("completed candles stale")
	}
	return strategy.CalculateFeatures(cs)
}

func (a *App) decisionSnapshot(pair string, p *Position, f strategy.Features, market MarketSnapshot) map[string]any {
	modelPosition := *p
	modelPosition.Performance = nil // Analytics must not inflate inference context.
	bid, ask := market.Bid, market.Ask
	spread := ask.Sub(bid).Div(bid).Mul(decimal.NewFromInt(10000)).InexactFloat64()
	eligible := !p.Paused && p.Qty.IsZero() && p.Pending == nil && p.Protection == nil && len(p.UnvaluedFees) == 0 && spread <= 10 && time.Now().After(p.CooldownUntil)
	if a.cfg.ContextInterval != "" {
		eligible = eligible && contextReady(market, a.cfg.ContextInterval)
	}
	if a.cfg.EntryPolicy != "ollaya" {
		eligible = eligible && f.Uptrend && f.Pullback && f.SetupID != p.LastSetup
	}
	allowed := []string{"HOLD"}
	if eligible {
		allowed = append(allowed, "ENTER_LONG")
	}
	if p.Qty.IsPositive() {
		allowed = append(allowed, "EXIT_LONG")
	}
	return map[string]any{"pair": pair, "venue": a.cfg.Venue.label(), "timestamp": time.Now().UTC(), "market": map[string]any{"bid": bid, "ask": ask, "spread_bps": spread}, "features": f, "position": modelPosition, "entry_eligible": eligible, "allowed_actions": allowed, "strategy_version": "trend-pullback-v1", "prompt_version": "v1"}
}

func (a *App) decide(ctx context.Context, pair string) {
	evaluation := Evaluation{At: time.Now().UTC(), Outcome: "skipped", Reason: "state_changed"}
	observation := &submissionObservation{}
	defer func() {
		evaluation.OrderClientID, evaluation.OrderAccepted, evaluation.OrderUncertain = observation.ClientID, observation.Accepted, observation.Uncertain
		a.recordEvaluation(pair, evaluation)
	}()
	p := a.snapshot().Pairs[pair]
	if p == nil || p.Pending != nil {
		evaluation.Reason = "unresolved_order"
		slog.Info("decision skipped", "pair", pair, "reason", "untracked pair or unresolved order")
		return
	}
	m := a.feeds.Snapshot(pair)
	if !freshMarket(m, pair) {
		evaluation.Reason = "stale_quote"
		slog.Warn("decision skipped", "pair", pair, "reason", "market quote unavailable or stale", "connected", m.Connected)
		return
	}
	f, err := features(m.Candles, a.cfg.SignalInterval)
	if err != nil {
		a.metrics.Failures.WithLabelValues("signals").Inc()
		evaluation.Reason = "invalid_candles"
		slog.Warn("decision skipped", "pair", pair, "reason", "completed candles unavailable or invalid")
		return
	}
	var contextSignals strategy.Features
	if a.cfg.ContextInterval != "" {
		contextSignals, err = contextFeatures(m, a.cfg.ContextInterval)
		if err != nil {
			evaluation.Reason = "context_unavailable"
			a.metrics.Failures.WithLabelValues("context_signals").Inc()
			slog.Warn("decision skipped", "pair", pair, "reason", "context candles unavailable, invalid or stale")
			return
		}
	}
	// Track the candle for diagnostics, but never use it to throttle inference.
	// Quotes and positions can change many times within one signal candle.
	claimed := false
	if err := a.update(ctx, "decision_candle", map[string]any{"pair": pair, "close_time": f.SetupID}, func(s State) error {
		pos := s.Pairs[pair]
		if pos == nil || pos.Version != p.Version {
			return errNoChange
		}
		claimed = true
		if pos.LastDecisionCandle == f.SetupID {
			return errNoChange
		}
		pos.LastDecisionCandle = f.SetupID
		return nil
	}); err != nil || !claimed {
		return
	}
	p = a.snapshot().Pairs[pair]
	if p == nil || p.Pending != nil {
		return
	}
	input := a.decisionSnapshot(pair, p, f, m)
	input["signal_interval"] = signalInterval(a.cfg.SignalInterval)
	input["entry_policy"] = a.cfg.EntryPolicy
	if a.cfg.ContextInterval != "" {
		input["market_context"] = map[string]any{"interval": a.cfg.ContextInterval, "completed_candle_close_time": m.ContextCandles[len(m.ContextCandles)-1].CloseTime, "features": contextSignals}
	}
	if symbol, ok := a.symbolFor(pair); ok {
		input["quote_asset"] = symbol.Quote
	}
	input["estimated_round_trip_fee_bps"] = 20
	evaluation.ModelAt = time.Now().UTC()
	result, err := a.askOllaya(ctx, pair, input)
	evaluation.Latency = time.Since(evaluation.ModelAt).Seconds()
	if err != nil {
		evaluation.Outcome, evaluation.Reason = "failed", "model_error"
		if errors.Is(err, context.DeadlineExceeded) {
			evaluation.Reason = "model_timeout"
		}
		a.metrics.Failures.WithLabelValues("ollaya").Inc()
		slog.Warn("model request failed", "pair", pair, "model", a.cfg.OllayaModel)
		_ = a.update(ctx, "decision_error", map[string]any{"pair": pair, "input": input, "error": err.Error()}, func(State) error { return nil })
		return
	}
	action, valid := validatedAction(result, a.cfg.OllayaModel)
	if !valid {
		evaluation.Outcome, evaluation.Reason = "failed", "model_invalid"
		a.metrics.Failures.WithLabelValues("ollaya_validation").Inc()
		slog.Warn("model response rejected", "pair", pair, "model", a.cfg.OllayaModel)
		return
	}
	evaluation.Action = action
	slog.Info("model decision", "pair", pair, "model", a.cfg.OllayaModel, "action", action, "entry_eligible", input["entry_eligible"], "paused", p.Paused, "uptrend", f.Uptrend, "pullback", f.Pullback, "quantity", p.Qty.String())
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
	}); err != nil {
		evaluation.Reason = "storage_error"
		return
	}
	if action == "HOLD" {
		evaluation.Outcome, evaluation.Reason = "hold", "model_hold"
		return
	}
	if !a.executionAllowed() {
		evaluation.Reason = "execution_disabled"
		return
	}
	gate := a.pairLock(pair)
	if !gate.TryLock() {
		evaluation.Reason = "pair_busy"
		return
	}
	defer gate.Unlock()
	current := a.snapshot().Pairs[pair]
	if current == nil || current.Version != p.Version || current.Pending != nil {
		return
	}
	m = a.feeds.Snapshot(pair)
	if !freshMarket(m, pair) {
		evaluation.Reason = "stale_quote"
		return
	}
	latest, err := features(m.Candles, a.cfg.SignalInterval)
	if err != nil || latest.SetupID != f.SetupID {
		evaluation.Reason = "signal_changed"
		return
	}
	if action == "ENTER_LONG" && a.cfg.ContextInterval != "" {
		latestContext, err := contextFeatures(m, a.cfg.ContextInterval)
		if err != nil {
			evaluation.Reason = "context_unavailable"
			return
		}
		if latestContext.SetupID != contextSignals.SetupID {
			evaluation.Reason = "context_changed"
			return
		}
	}
	if action == "EXIT_LONG" && current.Qty.IsPositive() && (a.cfg.EntryPolicy == "ollaya" || latest.Reversal) {
		if err := a.closeUnderGate(ctx, pair, "model_exit", observation); err != nil {
			evaluation.Outcome, evaluation.Reason = "failed", executionReason(err)
			if observation.Uncertain {
				evaluation.Outcome, evaluation.Reason = "uncertain", "submission_uncertain"
			}
			a.fail(context.WithoutCancel(ctx), pair, "execution", err)
		} else {
			evaluation.Outcome, evaluation.Reason = "executed", "none"
			if !observation.Accepted {
				evaluation.Outcome, evaluation.Reason = "no_order", "position_already_closed"
			}
		}
		return
	}
	if action != "ENTER_LONG" || a.decisionSnapshot(pair, current, latest, m)["entry_eligible"] != true {
		evaluation.Outcome = "blocked"
		if action == "ENTER_LONG" {
			evaluation.Reason = entryBlockReason(current, a.cfg, m, latest, true)
		} else {
			evaluation.Reason = "exit_ineligible"
		}
		return
	}
	distance := decimal.NewFromFloat(latest.ATR * 1.5)
	stop := m.Bid.Sub(distance)
	if symbol, ok := a.symbolFor(pair); ok && symbol.Tick.IsPositive() {
		stop = strategy.FloorStep(stop, symbol.Tick)
	}
	if !stop.IsPositive() {
		evaluation.Reason = "invalid_stop"
		return
	}
	qty := decimal.Min(a.cfg.MaxPosition.Div(m.Ask).Sub(current.DustQty), a.cfg.RiskPerTrade.Div(m.Ask.Sub(stop)).Sub(current.DustQty))
	qty = decimal.Min(qty, current.Cash.Sub(current.ExternalFeeQuote).Div(m.Ask.Mul(decimal.NewFromFloat(1.01))))
	if !qty.IsPositive() {
		evaluation.Reason = "insufficient_allocation"
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
	observation.ContextSetupID = contextSignals.SetupID
	if err := a.submitMarket(ctx, pair, "BUY", qty, "model_entry", observation); err != nil {
		evaluation.Outcome, evaluation.Reason = "failed", executionReason(err)
		if observation.Uncertain {
			evaluation.Outcome, evaluation.Reason = "uncertain", "submission_uncertain"
		}
		a.fail(context.WithoutCancel(ctx), pair, "execution", err)
	} else {
		evaluation.Outcome, evaluation.Reason = "executed", "none"
	}
}
