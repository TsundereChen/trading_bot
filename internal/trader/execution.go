package trader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"automated-trader/internal/strategy"

	"github.com/shopspring/decimal"
)

func newOrderID() (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "at-" + hex.EncodeToString(random[:]), nil
}

func checkQuantity(symbol Symbol, qty, bid, ask decimal.Decimal) error {
	if !qty.IsPositive() || qty.LessThan(symbol.MinQty) || (symbol.MaxQty.IsPositive() && qty.GreaterThan(symbol.MaxQty)) {
		return fmt.Errorf("quantity outside exchange limits; position may be dust")
	}
	if qty.Mul(bid).LessThan(symbol.MinNotional) || (symbol.MaxNotional.IsPositive() && qty.Mul(ask).GreaterThan(symbol.MaxNotional)) {
		return fmt.Errorf("notional outside exchange limits")
	}
	return nil
}

func (a *App) checkEntryAllocation(p *Position, qty decimal.Decimal, m MarketSnapshot, freeQuote decimal.Decimal) error {
	cost := qty.Mul(m.Ask).Mul(decimal.NewFromFloat(1.01))
	if freeQuote.LessThan(cost) || p.Cash.Sub(p.ExternalFeeQuote).LessThan(cost) || qty.Mul(m.Ask).GreaterThan(a.cfg.MaxPosition) {
		return fmt.Errorf("insufficient balance or position allocation exceeded")
	}
	if !p.Stop.LessThan(m.Bid) || qty.Mul(m.Ask.Sub(p.Stop)).GreaterThan(a.cfg.RiskPerTrade) {
		return fmt.Errorf("entry-to-stop risk allocation exceeded")
	}
	return nil
}

func (a *App) submitLocked(ctx context.Context, pair, side string, qty decimal.Decimal, reason string) error {
	gate := a.pairLock(pair)
	gate.Lock()
	defer gate.Unlock()
	return a.submitMarket(ctx, pair, side, qty, reason)
}

// The pair gate is held, but no application-wide lock spans exchange I/O.
func (a *App) submitMarket(ctx context.Context, pair, side string, qty decimal.Decimal, reason string) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if !a.executionAllowed() {
		return fmt.Errorf("execution disabled or identity unverified")
	}
	p := a.snapshot().Pairs[pair]
	symbol, ok := a.symbolFor(pair)
	if p == nil || !ok || p.Pending != nil || p.Protection != nil || (side != "BUY" && side != "SELL") {
		return fmt.Errorf("unknown pair or unresolved order")
	}
	if side == "BUY" && (p.Paused || !p.Qty.IsZero() || !p.Stop.IsPositive() || !symbol.StopAllowed || len(p.UnvaluedFees) > 0) {
		return fmt.Errorf("entry paused or missing protection rules")
	}
	version := p.Version
	balances, err := a.binance.Balances(ctx)
	if err != nil {
		return err
	}
	m := a.feeds.Snapshot(pair)
	if !freshMarket(m, pair) {
		return fmt.Errorf("market stream unavailable or stale after balance check")
	}
	qty = strategy.FloorStep(qty, symbol.Step)
	if err := checkQuantity(symbol, qty, m.Bid, m.Ask); err != nil {
		return err
	}
	if side == "BUY" {
		if err := a.checkEntryAllocation(p, qty, m, balances[symbol.Quote]); err != nil {
			return err
		}
	} else if balances[symbol.Base].LessThan(qty) || qty.GreaterThan(p.Qty) {
		return fmt.Errorf("tracked position exceeds free balance; reconcile account")
	}
	id, err := newOrderID()
	if err != nil {
		return err
	}
	if err := a.update(ctx, "order_intent", map[string]any{"pair": pair, "id": id, "side": side, "quantity": qty, "reason": reason}, func(s State) error {
		pos := s.Pairs[pair]
		if pos == nil || pos.Version != version || pos.Pending != nil || pos.Protection != nil || (side == "BUY" && pos.Paused) {
			return fmt.Errorf("position changed during order preflight")
		}
		pos.Pending = &Pending{ID: id, Side: side, Qty: qty}
		return nil
	}); err != nil {
		return err
	}
	order, err := a.binance.PlaceChecked(ctx, pair, side, qty.String(), id, func() error {
		currentMarket := a.feeds.Snapshot(pair)
		if !a.executionAllowed() || !freshMarket(currentMarket, pair) {
			return fmt.Errorf("execution disabled or quote became stale")
		}
		pos := a.snapshot().Pairs[pair]
		if pos == nil || pos.Pending == nil || pos.Pending.ID != id || (side == "BUY" && pos.Paused) {
			return fmt.Errorf("order preflight invalidated")
		}
		if err := checkQuantity(symbol, qty, currentMarket.Bid, currentMarket.Ask); err != nil {
			return err
		}
		if side == "BUY" {
			if err := a.checkEntryAllocation(pos, qty, currentMarket, balances[symbol.Quote]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		var local *preflightError
		if definitiveRejection(err) || errors.As(err, &local) {
			a.metrics.Orders.WithLabelValues(pair, side, "rejected").Inc()
			if commitErr := a.clearRejected(ctx, pair, id, false, err); commitErr != nil {
				return commitErr
			}
			return err
		}
		a.metrics.Orders.WithLabelValues(pair, side, "uncertain").Inc()
		return fmt.Errorf("submission unresolved; reconciliation required: %w", err)
	}
	a.metrics.Orders.WithLabelValues(pair, side, "accepted").Inc()
	settleCtx, settleCancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer settleCancel()
	if err := a.applyOrder(settleCtx, pair, order, false); err != nil {
		return err
	}
	if side == "BUY" {
		// Protect an accepted buy even when its HTTP caller disconnected.
		protectCtx, protectCancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
		defer protectCancel()
		if err := a.ensureProtection(protectCtx, pair); err != nil {
			if pos := a.snapshot().Pairs[pair]; pos != nil && pos.Protection == nil && pos.Pending == nil && pos.Qty.IsPositive() {
				if closeErr := a.closeUnderGate(protectCtx, pair, "native_stop_failed"); closeErr != nil {
					return fmt.Errorf("%v; emergency close: %w", err, closeErr)
				}
			}
			return err
		}
	}
	return a.valueExternalFees(settleCtx, pair)
}
