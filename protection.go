package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Native STOP_LOSS orders reserve the sellable quantity at Binance. Their
// durable intent and execution accounting are separate from market orders.
func (a *App) ensureProtection(ctx context.Context, pair string) error {
	if !a.executionAllowed() {
		return fmt.Errorf("execution disabled or identity unverified")
	}
	p := a.snapshot().Pairs[pair]
	if p == nil || !p.Qty.IsPositive() || p.Pending != nil || p.Protection != nil {
		return nil
	}
	symbol, ok := a.symbolFor(pair)
	if !ok || !symbol.StopAllowed || !p.Stop.IsPositive() {
		return fmt.Errorf("native STOP_LOSS is unavailable or stop is missing")
	}
	qty, stop := floorStep(p.Qty, symbol.Step), p.Stop
	if symbol.Tick.IsPositive() {
		stop = floorStep(stop, symbol.Tick)
	}
	if !stop.IsPositive() || stop.LessThan(symbol.MinPrice) || (symbol.MaxPrice.IsPositive() && stop.GreaterThan(symbol.MaxPrice)) {
		return fmt.Errorf("stop outside exchange price limits")
	}
	if err := checkQuantity(symbol, qty, stop, stop); err != nil {
		return err
	}
	id, err := newOrderID()
	if err != nil {
		return err
	}
	if err := a.update(ctx, "native_stop_intent", map[string]any{"pair": pair, "id": id, "quantity": qty, "stop": stop}, func(s State) error {
		pos := s.Pairs[pair]
		if pos == nil || pos.Pending != nil || pos.Protection != nil || !pos.Qty.Equal(p.Qty) {
			return fmt.Errorf("position changed before native stop")
		}
		pos.Protection = &Pending{ID: id, Side: "SELL", Qty: qty}
		pos.Stop = stop
		return nil
	}); err != nil {
		return err
	}
	order, err := a.binance.PlaceStop(ctx, pair, qty.String(), stop.String(), id)
	if err != nil {
		var local *preflightError
		if definitiveRejection(err) || errors.As(err, &local) {
			if commitErr := a.clearRejected(ctx, pair, id, true, err); commitErr != nil {
				return commitErr
			}
		}
		return fmt.Errorf("native stop submission requires reconciliation: %w", err)
	}
	return a.applyOrder(ctx, pair, order, true)
}

// Never issue a market sell until cancellation is confirmed and any stop fills
// are accounted for. A timeout or incomplete fill history keeps protection
// pending; a cancel/fill race can reduce the quantity to zero instead of selling
// someone else's holdings a second time.
func (a *App) cancelProtection(ctx context.Context, pair string) error {
	p := a.snapshot().Pairs[pair]
	if p == nil || p.Protection == nil {
		return nil
	}
	order, err := a.binance.FindPending(ctx, pair, p.Protection)
	if err != nil {
		return err
	}
	if err := a.applyOrder(ctx, pair, order, true); err != nil {
		return err
	}
	p = a.snapshot().Pairs[pair]
	if p == nil || p.Protection == nil {
		return nil
	}
	order, err = a.binance.Cancel(ctx, pair, p.Protection)
	if err != nil {
		// The stop may have filled while cancellation reached the engine.
		order, err = a.binance.FindPending(ctx, pair, p.Protection)
		if err != nil {
			return err
		}
	}
	if err := a.applyOrder(ctx, pair, order, true); err != nil {
		return err
	}
	if pos := a.snapshot().Pairs[pair]; pos != nil && pos.Protection != nil {
		return fmt.Errorf("native stop cancellation remains unresolved")
	}
	return nil
}

func (a *App) closeUnderGate(ctx context.Context, pair, reason string) error {
	if !a.executionAllowed() {
		return fmt.Errorf("execution disabled or identity unverified")
	}
	p := a.snapshot().Pairs[pair]
	if p == nil || p.Pending != nil {
		return fmt.Errorf("unresolved market order")
	}
	if err := a.cancelProtection(ctx, pair); err != nil {
		return err
	}
	p = a.snapshot().Pairs[pair]
	if p == nil || !p.Qty.IsPositive() {
		return nil
	}
	if err := a.submitMarket(ctx, pair, "SELL", p.Qty, reason); err != nil {
		// If no sell was sent, restore protection even if the quote feed failed.
		// Uncertain sells retain Pending and must not get a second sell order.
		if pos := a.snapshot().Pairs[pair]; pos != nil && pos.Pending == nil && pos.Protection == nil && pos.Qty.IsPositive() {
			protectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
			_ = a.ensureProtection(protectCtx, pair)
			cancel()
		}
		return err
	}
	// A rounded residual may be dust. Do not silently declare it closed.
	remaining := a.snapshot().Pairs[pair]
	if remaining == nil {
		return nil
	}
	if remaining.Pending != nil {
		return fmt.Errorf("close order is still pending reconciliation")
	}
	if remaining.Qty.IsPositive() {
		return fmt.Errorf("residual quantity %s remains; it may be below exchange limits", remaining.Qty)
	}
	return nil
}
