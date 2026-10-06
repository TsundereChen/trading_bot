package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
)

func freshMarket(m MarketSnapshot, pair string) bool {
	age := time.Since(m.QuoteAt)
	return m.Pair == pair && m.Connected && !m.QuoteAt.IsZero() && age >= 0 && age <= 3*time.Second && m.Bid.IsPositive() && !m.Ask.LessThan(m.Bid)
}

func newOrderID() (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "at-" + hex.EncodeToString(random[:]), nil
}

func terminalOrder(status string) bool {
	return status == "FILLED" || status == "CANCELED" || status == "EXPIRED" || status == "REJECTED" || status == "EXPIRED_IN_MATCH"
}

func (a *App) refreshMetrics() {
	// Serialize metric publication with pair removal, but do no I/O here.
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.reconnectCounts == nil {
		a.reconnectCounts = map[string]uint64{}
	}
	totalEquity, totalExposure := 0.0, 0.0
	for pair, p := range a.state.Pairs {
		m := a.feeds.Snapshot(pair)
		fresh := freshMarket(m, pair)
		connected, running, age := 0.0, 0.0, math.Inf(1)
		if m.Connected {
			connected = 1
		}
		if !m.QuoteAt.IsZero() {
			age = time.Since(m.QuoteAt).Seconds()
		}
		if fresh && !p.Paused && p.Pending == nil && len(p.UnvaluedFees) == 0 && !a.fatal && a.cfg.Trading {
			running = 1
		}
		equity, exposure := p.equity(decimal.Zero).InexactFloat64(), 0.0
		if p.Qty.IsPositive() {
			if fresh {
				equity, exposure = p.equity(m.Bid).InexactFloat64(), p.Qty.Mul(m.Bid).InexactFloat64()
			} else {
				equity, exposure = math.NaN(), math.NaN()
			}
		}
		if len(p.UnvaluedFees) > 0 {
			equity = math.NaN()
		}
		a.metrics.WSConnected.WithLabelValues(pair).Set(connected)
		previous := a.reconnectCounts[pair]
		delta := m.Reconnects
		if m.Reconnects >= previous {
			delta -= previous
		}
		a.metrics.WSReconnects.WithLabelValues(pair).Add(float64(delta))
		a.reconnectCounts[pair] = m.Reconnects
		a.metrics.FeedAge.WithLabelValues(pair).Set(age)
		a.metrics.Running.WithLabelValues(pair).Set(running)
		a.metrics.Equity.WithLabelValues(pair).Set(equity)
		a.metrics.Exposure.WithLabelValues(pair).Set(exposure)
		for asset, fee := range p.ExternalFees {
			a.metrics.ExternalFees.WithLabelValues(pair, asset).Set(fee.InexactFloat64())
		}
		totalEquity, totalExposure = totalEquity+equity, totalExposure+exposure
	}
	a.metrics.PortfolioEquity.Set(totalEquity)
	a.metrics.PortfolioExposure.Set(totalExposure)
}

func (a *App) removeMetrics(pair string) {
	for _, gauge := range []*prometheus.GaugeVec{a.metrics.Equity, a.metrics.Exposure, a.metrics.Running, a.metrics.FeedAge, a.metrics.WSConnected, a.metrics.ExternalFees} {
		gauge.DeletePartialMatch(prometheus.Labels{"pair": pair})
	}
	a.metrics.Decisions.DeletePartialMatch(prometheus.Labels{"pair": pair})
	a.metrics.Orders.DeletePartialMatch(prometheus.Labels{"pair": pair})
	a.metrics.WSReconnects.DeletePartialMatch(prometheus.Labels{"pair": pair})
	delete(a.reconnectCounts, pair)
	delete(a.lastDecisions, pair)
}

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
// pair does not stretch another pair's one-second reconciliation schedule.
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
		if dailyHit && !p.Paused {
			if err := a.update(workCtx, "daily_loss_limit", map[string]string{"pair": pair}, func(s State) error {
				pos := s.Pairs[pair]
				if pos == nil {
					return errNoChange
				}
				pos.Paused = true
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
	}
}

// applyOrder performs all exchange I/O before the durable state transaction.
// The caller owns the pair gate. Fees must describe exactly the same execution
// snapshot; partial/ahead fill history must never finalize an order.
func (a *App) applyOrder(ctx context.Context, pair string, order Order, protection bool) error {
	qty, e1 := decimal.NewFromString(order.Executed)
	quote, e2 := decimal.NewFromString(order.Quote)
	if e1 != nil || e2 != nil || qty.IsNegative() || quote.IsNegative() || order.OrderID <= 0 {
		return fmt.Errorf("invalid order totals or identity")
	}
	if !terminalOrder(order.Status) && order.Status != "NEW" && order.Status != "PARTIALLY_FILLED" && order.Status != "PENDING_CANCEL" {
		return fmt.Errorf("unknown order status %q", order.Status)
	}
	symbol, ok := a.symbolFor(pair)
	if !ok {
		return fmt.Errorf("no exchange rules for %s", pair)
	}
	totals := FillTotals{Fees: map[string]decimal.Decimal{}}
	if qty.IsPositive() {
		var err error
		totals, err = a.binance.Fills(ctx, pair, order.OrderID)
		if err != nil {
			return err
		}
		if !totals.Qty.Equal(qty) || !totals.Quote.Equal(quote) {
			return fmt.Errorf("fill history does not match order totals; keep order pending")
		}
	} else if !quote.IsZero() {
		return fmt.Errorf("quote without executed quantity")
	}
	return a.update(context.WithoutCancel(ctx), "order_update", map[string]any{"pair": pair, "order": order, "fees": totals.Fees, "native_stop": protection}, func(s State) error {
		p := s.Pairs[pair]
		if p == nil {
			return fmt.Errorf("pair no longer tracked")
		}
		pending := p.Pending
		if protection {
			pending = p.Protection
		}
		if pending == nil || (pending.OrderID > 0 && pending.OrderID != order.OrderID) || (pending.OrderID == 0 && pending.ID != order.ClientID) {
			return fmt.Errorf("order identity mismatch")
		}
		if qty.GreaterThan(pending.Qty) {
			return fmt.Errorf("execution exceeds requested quantity")
		}
		if !terminalOrder(order.Status) && pending.OrderID == order.OrderID && pending.AppliedQty.Equal(qty) && pending.AppliedQuote.Equal(quote) && reflect.DeepEqual(pending.AppliedFees, totals.Fees) {
			return errNoChange
		}
		previous := pending.AppliedFees
		if previous == nil {
			previous = map[string]decimal.Decimal{symbol.Base: pending.AppliedBaseFee, symbol.Quote: pending.AppliedQuoteFee}
		}
		deltas := map[string]decimal.Decimal{}
		for asset, fee := range totals.Fees {
			deltas[asset] = fee.Sub(previous[asset])
			if deltas[asset].IsNegative() {
				return fmt.Errorf("fee totals moved backwards")
			}
		}
		for asset, fee := range previous {
			if totals.Fees[asset].LessThan(fee) {
				return fmt.Errorf("fee totals moved backwards")
			}
		}
		dq, dc := qty.Sub(pending.AppliedQty), quote.Sub(pending.AppliedQuote)
		bf, qf := deltas[symbol.Base], deltas[symbol.Quote]
		if dq.IsNegative() || dc.IsNegative() {
			return fmt.Errorf("order totals moved backwards")
		}
		if pending.Side == "BUY" {
			if bf.GreaterThan(p.Qty.Add(dq)) {
				return fmt.Errorf("base fee exceeds acquired quantity")
			}
			p.Qty, p.Cost, p.Cash = p.Qty.Add(dq).Sub(bf), p.Cost.Add(dc).Add(qf), p.Cash.Sub(dc).Sub(qf)
		} else if pending.Side == "SELL" {
			sold := dq.Add(bf)
			if sold.GreaterThan(p.Qty) {
				return fmt.Errorf("sell exceeds tracked position")
			}
			if p.Qty.IsPositive() {
				p.Cost = p.Cost.Mul(p.Qty.Sub(sold)).Div(p.Qty)
			}
			p.Qty, p.Cash = p.Qty.Sub(sold), p.Cash.Add(dc).Sub(qf)
		} else {
			return fmt.Errorf("invalid pending side")
		}
		if p.ExternalFees == nil {
			p.ExternalFees = map[string]decimal.Decimal{}
		}
		if p.UnvaluedFees == nil {
			p.UnvaluedFees = map[string]decimal.Decimal{}
		}
		for asset, fee := range deltas {
			if asset != symbol.Base && asset != symbol.Quote && fee.IsPositive() {
				p.ExternalFees[asset] = p.ExternalFees[asset].Add(fee)
				p.UnvaluedFees[asset] = p.UnvaluedFees[asset].Add(fee)
			}
		}
		pending.OrderID, pending.AppliedQty, pending.AppliedQuote = order.OrderID, qty, quote
		pending.AppliedFees = copyDecimals(totals.Fees)
		pending.AppliedBaseFee, pending.AppliedQuoteFee = totals.Fees[symbol.Base], totals.Fees[symbol.Quote]
		if terminalOrder(order.Status) {
			if protection {
				p.Protection = nil
			} else {
				p.Pending = nil
			}
			p.CooldownUntil = time.Now().Add(time.Minute)
		}
		// A BUY accepted with zero fills still needs its planned stop/target
		// when later reconciliation discovers the execution.
		if p.Qty.IsZero() && (p.Pending == nil || p.Pending.Side != "BUY") {
			p.Cost, p.Stop, p.Target = decimal.Zero, decimal.Zero, decimal.Zero
		}
		return nil
	})
}

func (a *App) valueExternalFees(ctx context.Context, pair string) error {
	p := a.snapshot().Pairs[pair]
	if p == nil {
		return nil
	}
	for asset, amount := range p.UnvaluedFees {
		book, err := a.binance.Book(ctx, asset+"USDT")
		if err != nil {
			return err
		}
		ask, err := decimal.NewFromString(book.Ask)
		if err != nil || !ask.IsPositive() {
			return fmt.Errorf("invalid external fee valuation")
		}
		if err := a.update(ctx, "fee_valuation", map[string]any{"pair": pair, "asset": asset, "units": amount, "ask": ask}, func(s State) error {
			pos := s.Pairs[pair]
			if pos == nil || !pos.UnvaluedFees[asset].Equal(amount) {
				return fmt.Errorf("fee valuation state changed")
			}
			pos.ExternalFeeQuote = pos.ExternalFeeQuote.Add(amount.Mul(ask))
			delete(pos.UnvaluedFees, asset)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) clearRejected(ctx context.Context, pair, id string, protection bool, err error) error {
	return a.update(context.WithoutCancel(ctx), "order_rejected", map[string]any{"pair": pair, "id": id, "error": err.Error(), "native_stop": protection}, func(s State) error {
		p := s.Pairs[pair]
		if p == nil {
			return fmt.Errorf("pair no longer tracked")
		}
		pending := p.Pending
		if protection {
			pending = p.Protection
		}
		if pending == nil || pending.ID != id {
			return fmt.Errorf("rejection identity mismatch")
		}
		if protection {
			p.Protection = nil
		} else {
			p.Pending = nil
		}
		p.Paused, p.Error = true, err.Error()
		return nil
	})
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
	qty = floorStep(qty, symbol.Step)
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
	input := a.snapshotLocked(pair, p, f, m)
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
	if action != "ENTER_LONG" || a.snapshotLocked(pair, current, latest, m)["entry_eligible"] != true {
		return
	}
	distance := decimal.NewFromFloat(latest.ATR * 1.5)
	stop := m.Bid.Sub(distance)
	if symbol, ok := a.symbolFor(pair); ok && symbol.Tick.IsPositive() {
		stop = floorStep(stop, symbol.Tick)
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
