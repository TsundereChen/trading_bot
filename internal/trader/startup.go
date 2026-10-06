package trader

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/shopspring/decimal"
)

func prepareEntryStartup(p *Position, trading bool) {
	p.Paused, p.Starting = trading, trading
}

// Startup is automatic, but it never bypasses reconciliation, account holdings,
// native protection, feed readiness, or the persisted daily loss baseline.
// The caller holds the pair gate. Faults disable Starting until a process restart.
func (a *App) autoEnableEntries(ctx context.Context, pair string) error {
	if !a.executionAllowed() {
		return nil
	}
	p := a.snapshot().Pairs[pair]
	if p == nil || !p.Starting {
		return nil
	}
	m := a.feeds.Snapshot(pair)
	if p.Pending != nil || len(p.UnvaluedFees) > 0 || !freshMarket(m, pair) {
		return nil
	}
	if _, err := features(m.Candles, a.cfg.SignalInterval); err != nil {
		return nil
	}
	if a.cfg.ContextInterval != "" && !contextReady(m, a.cfg.ContextInterval) {
		return nil
	}
	if p.Qty.IsPositive() && (p.Protection == nil || p.Protection.OrderID == 0) {
		return nil
	}
	if p.Protection != nil && p.Protection.OrderID == 0 {
		return nil
	}
	holdings, err := a.binance.Holdings(ctx)
	if err != nil {
		return fmt.Errorf("account holdings reconciliation: %w", err)
	}
	enabled := false
	err = a.update(ctx, "entries_auto_enabled", map[string]string{"pair": pair}, func(s State) error {
		pos := s.Pairs[pair]
		if pos == nil || !pos.Starting || pos.Version != p.Version {
			return errNoChange
		}
		market := a.feeds.Snapshot(pair)
		if !freshMarket(market, pair) || (a.cfg.ContextInterval != "" && !contextReady(market, a.cfg.ContextInterval)) {
			return errNoChange
		}
		if _, err := features(market.Candles, a.cfg.SignalInterval); err != nil {
			return errNoChange
		}
		day := time.Now().UTC().Format("2006-01-02")
		if pos.Day == day && pos.DayEquity.Sub(pos.equity(market.Bid)).GreaterThanOrEqual(a.cfg.DailyLoss) {
			return errNoChange
		}
		symbol, ok := a.symbolFor(pair)
		if !ok || !symbol.StopAllowed {
			return fmt.Errorf("missing native stop rules on %s", pair)
		}
		// Two quote pairs can own the same base asset. Reconcile their combined
		// holdings, not each allocation against the full account independently.
		required := decimal.Zero
		for name, other := range s.Pairs {
			rules, ok := a.symbolFor(name)
			if !ok {
				return fmt.Errorf("missing exchange rules on %s", name)
			}
			if rules.Base == symbol.Base {
				required = required.Add(other.Qty).Add(other.DustQty)
			}
		}
		if holdings[symbol.Base].LessThan(required) {
			return fmt.Errorf("tracked %s holdings exceed account holdings", symbol.Base)
		}
		if pos.Day != day {
			pos.Day, pos.DayEquity = day, pos.equity(market.Bid)
		}
		pos.Paused, pos.Starting, pos.Error = false, false, ""
		enabled = true
		return nil
	})
	if err == nil && enabled {
		slog.Info("entries automatically enabled", "pair", pair)
	}
	return err
}
