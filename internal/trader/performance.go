package trader

import (
	"reflect"
	"time"

	"github.com/shopspring/decimal"
)

// Performance is durable; it never fabricates pre-upgrade trade statistics.
type Performance struct {
	Since                                   time.Time                  `json:"since"`
	Baseline                                decimal.Decimal            `json:"baseline_quote"`
	ExternalFeeBaseline                     decimal.Decimal            `json:"external_fee_baseline"`
	RealizedExecution                       decimal.Decimal            `json:"realized_execution_quote"`
	QuoteFees                               decimal.Decimal            `json:"quote_fees"`
	BaseFeeEstimate                         decimal.Decimal            `json:"base_fee_quote_estimate"`
	Fees                                    map[string]decimal.Decimal `json:"fee_units"`
	Completed, Wins, Losses, Excluded       uint64
	WinningPnL, LosingPnL                   decimal.Decimal
	PeakEquity, MaxDrawdown, MaxDrawdownPct decimal.Decimal
	Cycle                                   *TradeCycle `json:"cycle,omitempty"`
	Marked                                  bool        `json:"marked"`
}

type TradeCycle struct {
	CompleteHistory                 bool
	RealizedStart, ExternalFeeStart decimal.Decimal
}

func ensurePerformance(p *Position) *Performance {
	if p.Performance == nil {
		e := &Performance{Since: time.Now().UTC(), Baseline: p.Cash.Add(p.Cost).Add(p.DustCost).Sub(p.ExternalFeeQuote), ExternalFeeBaseline: p.ExternalFeeQuote, Fees: map[string]decimal.Decimal{}}
		e.PeakEquity = e.Baseline
		e.Marked = p.Qty.Add(p.DustQty).IsZero()
		if p.Qty.IsPositive() || (p.Pending != nil && p.Pending.AppliedQty.IsPositive()) {
			e.Cycle = &TradeCycle{ExternalFeeStart: p.ExternalFeeQuote}
		}
		p.Performance = e
	}
	if p.Performance.Fees == nil {
		p.Performance.Fees = map[string]decimal.Decimal{}
	}
	return p.Performance
}

func finishTrade(p *Position) {
	e := ensurePerformance(p)
	if e.Cycle == nil || !p.Qty.IsZero() || p.Pending != nil || p.Protection != nil || len(p.UnvaluedFees) > 0 {
		return
	}
	if e.Cycle.CompleteHistory {
		pnl := e.RealizedExecution.Sub(e.Cycle.RealizedStart).Sub(p.ExternalFeeQuote.Sub(e.Cycle.ExternalFeeStart))
		e.Completed++
		if pnl.IsPositive() {
			e.Wins++
			e.WinningPnL = e.WinningPnL.Add(pnl)
		}
		if pnl.IsNegative() {
			e.Losses++
			e.LosingPnL = e.LosingPnL.Add(pnl)
		}
	} else {
		e.Excluded++
	}
	e.Cycle = nil
}

func markPerformance(p *Position, bid decimal.Decimal) bool {
	e := ensurePerformance(p)
	equity := p.equity(bid)
	if !e.Marked {
		e.PeakEquity, e.Marked = equity, true
		return true
	}
	changed := false
	if equity.GreaterThan(e.PeakEquity) {
		e.PeakEquity = equity
		changed = true
	}
	drawdown := e.PeakEquity.Sub(equity)
	if drawdown.GreaterThan(e.MaxDrawdown) {
		e.MaxDrawdown = drawdown
		changed = true
	}
	if e.PeakEquity.IsPositive() {
		pct := drawdown.Div(e.PeakEquity).Mul(decimal.NewFromInt(100))
		if pct.GreaterThan(e.MaxDrawdownPct) {
			e.MaxDrawdownPct = pct
			changed = true
		}
	}
	return changed
}

// Observability changes must not invalidate otherwise unchanged execution state.
func executionPositionEqual(a, b *Position) bool {
	if a == nil || b == nil {
		return a == b
	}
	x, y := *a, *b
	x.Performance, y.Performance = nil, nil
	return reflect.DeepEqual(x, y)
}
