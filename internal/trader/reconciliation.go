package trader

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/shopspring/decimal"
)

func terminalOrder(status string) bool {
	return status == "FILLED" || status == "CANCELED" || status == "EXPIRED" || status == "REJECTED" || status == "EXPIRED_IN_MATCH"
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
		e := ensurePerformance(p)
		for asset, fee := range deltas {
			e.Fees[asset] = e.Fees[asset].Add(fee)
		}
		e.QuoteFees = e.QuoteFees.Add(qf)
		if bf.IsPositive() && qty.IsPositive() {
			e.BaseFeeEstimate = e.BaseFeeEstimate.Add(bf.Mul(quote.Div(qty)))
		}
		if pending.Side == "BUY" {
			if dq.IsPositive() && e.Cycle == nil {
				e.Cycle = &TradeCycle{CompleteHistory: p.Qty.IsZero(), RealizedStart: e.RealizedExecution, ExternalFeeStart: p.ExternalFeeQuote}
			}
			if bf.GreaterThan(p.Qty.Add(dq)) {
				return fmt.Errorf("base fee exceeds acquired quantity")
			}
			p.Qty, p.Cost, p.Cash = p.Qty.Add(dq).Sub(bf), p.Cost.Add(dc).Add(qf), p.Cash.Sub(dc).Sub(qf)
			if dq.IsPositive() && p.DustQty.IsPositive() {
				p.Qty, p.Cost = p.Qty.Add(p.DustQty), p.Cost.Add(p.DustCost)
				p.DustQty, p.DustCost = decimal.Zero, decimal.Zero
			}
		} else if pending.Side == "SELL" {
			sold := dq.Add(bf)
			if sold.GreaterThan(p.Qty) {
				return fmt.Errorf("sell exceeds tracked position")
			}
			beforeCost := p.Cost
			if p.Qty.IsPositive() {
				p.Cost = p.Cost.Mul(p.Qty.Sub(sold)).Div(p.Qty)
			}
			e.RealizedExecution = e.RealizedExecution.Add(dc.Sub(qf).Sub(beforeCost.Sub(p.Cost)))
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
			p.CooldownUntil = time.Now().Add(a.cfg.cooldown())
			if pending.Side == "SELL" {
				retainDust(p, symbol)
			}
		}
		// A BUY accepted with zero fills still needs its planned stop/target
		// when later reconciliation discovers the execution.
		if p.Qty.IsZero() && (p.Pending == nil || p.Pending.Side != "BUY") {
			p.Cost, p.Stop, p.Target = decimal.Zero, decimal.Zero, decimal.Zero
		}
		finishTrade(p)
		return nil
	})
}

func (a *App) valueExternalFees(ctx context.Context, pair string) error {
	p := a.snapshot().Pairs[pair]
	if p == nil {
		return nil
	}
	for asset, amount := range p.UnvaluedFees {
		symbol, ok := a.symbolFor(pair)
		if !ok {
			return fmt.Errorf("missing fee valuation quote asset")
		}
		book, err := a.binance.Book(ctx, asset+symbol.Quote)
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
			finishTrade(pos)
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
