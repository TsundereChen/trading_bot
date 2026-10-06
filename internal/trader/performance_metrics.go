package trader

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
)

func (m *Metrics) registerPerformance(reg *prometheus.Registry) {
	m.Performance = map[string]*prometheus.GaugeVec{}
	definitions := map[string]string{
		"context_ready":                        "One if higher-timeframe completed-candle features are available and fresh",
		"context_candle_age_seconds":           "Age of latest completed context candle; +Inf before bootstrap",
		"realized_pnl_quote":                   "Cost-basis realized PnL after quote/base fees and estimated third-asset fees since tracking began; NaN with unvalued fees",
		"unrealized_pnl_quote":                 "Mark-to-bid minus remaining position/dust cost basis; NaN with stale valuation",
		"net_pnl_quote":                        "Realized plus unrealized PnL since tracking began, after accounted fees",
		"daily_pnl_quote":                      "Current equity minus persisted UTC day-start equity; NaN if unavailable",
		"fees_paid_quote_estimate":             "Quote fees plus estimated base/third-asset fee value since tracking began; not subtracted twice from PnL",
		"position_entry_cost_quote":            "Average entry cost per base unit including allocated quote fees; zero when flat",
		"position_quantity_base":               "Active position quantity, excluding separate dust",
		"dust_quantity_base":                   "Retained unprotected sub-step quantity",
		"position_stop_price":                  "Planned stop price; zero when flat",
		"position_target_price":                "Planned target price; zero when flat",
		"native_stop_active":                   "One if a native stop has a verified exchange order ID",
		"pending_order":                        "One if a market order remains unresolved",
		"completed_trades":                     "Durable completed cycles with fully observed entry/exit history and valued fees",
		"winning_trades":                       "Durable positive-PnL completed cycles",
		"losing_trades":                        "Durable negative-PnL completed cycles",
		"trade_win_rate":                       "Winning/completed cycles, from zero to one; NaN before any complete cycle",
		"average_win_quote":                    "Average net profitable cycle; NaN without wins",
		"average_loss_quote":                   "Average net losing cycle, negative; NaN without losses",
		"max_drawdown_quote":                   "Maximum observed equity peak-to-trough decrease, sampled on fresh quotes since tracking began",
		"max_drawdown_percent":                 "Maximum observed percentage equity drawdown since tracking began",
		"performance_since_timestamp_seconds":  "Start of durable performance tracking; zero if not initialized",
		"excluded_legacy_trades":               "Closed inherited cycles excluded from win/loss statistics because entry history predates tracking",
		"last_evaluation_timestamp_seconds":    "Latest scheduled evaluation time, including skipped/failed evaluations",
		"last_model_request_timestamp_seconds": "Latest model request time; zero before any request",
		"last_model_latency_seconds":           "Latency of latest model request, including failures",
		"last_decision_order_accepted":         "One if the latest evaluation led to an acknowledged market order; not a fill confirmation",
		"last_decision_order_uncertain":        "One if the latest evaluation led to an uncertain market submission",
	}
	for name, help := range definitions {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_" + name, Help: help}, []string{"pair", "quote_asset"})
		m.Performance[name] = g
		reg.MustRegister(g)
	}
	makeInfo := func(name, help, label string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_" + name, Help: help}, []string{"pair", "quote_asset", label})
		reg.MustRegister(g)
		return g
	}
	m.LatestAction = makeInfo("last_model_action", "Latest model action; one-hot, UNKNOWN for a failed response", "action")
	m.LatestOutcome = makeInfo("last_evaluation_outcome", "Latest evaluation outcome; one-hot", "outcome")
	m.LatestReason = makeInfo("last_no_trade_reason", "Latest evaluation reason; none after execution, one-hot bounded labels", "reason")
	m.EntryBlocker = makeInfo("entry_block_reason", "Current local entry blocker; none if entry filters pass, not a guarantee of exchange acceptance", "reason")
	m.FeeUnits = makeInfo("fees_paid_units", "Durable commissions since tracking began in original asset units", "asset")
	m.NoTrades = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_no_trade_total", Help: "Evaluations without acknowledged or uncertain market submission by bounded reason; process lifetime"}, []string{"pair", "reason"})
	reg.MustRegister(m.NoTrades)
}

func (a *App) publishPerformance(pair, quote string, p *Position, market MarketSnapshot, fresh bool, equity float64) {
	set := func(name string, value float64) { a.metrics.Performance[name].WithLabelValues(pair, quote).Set(value) }
	ready, contextAge := 0.0, math.Inf(1)
	if a.cfg.ContextInterval != "" {
		if contextReady(market, a.cfg.ContextInterval) {
			ready = 1
		}
		if len(market.ContextCandles) > 0 {
			contextAge = time.Since(time.UnixMilli(market.ContextCandles[len(market.ContextCandles)-1].CloseTime)).Seconds()
		}
	}
	set("context_ready", ready)
	set("context_candle_age_seconds", contextAge)
	float := func(d decimal.Decimal) float64 { return d.InexactFloat64() }
	entry := 0.0
	if p.Qty.IsPositive() {
		entry = float(p.Cost.Div(p.Qty))
	}
	set("position_entry_cost_quote", entry)
	set("position_quantity_base", float(p.Qty))
	set("dust_quantity_base", float(p.DustQty))
	set("position_stop_price", float(p.Stop))
	set("position_target_price", float(p.Target))
	stop, pending := 0.0, 0.0
	if p.Protection != nil && p.Protection.OrderID > 0 {
		stop = 1
	}
	if p.Pending != nil {
		pending = 1
	}
	set("native_stop_active", stop)
	set("pending_order", pending)
	unrealized := 0.0
	if p.Qty.Add(p.DustQty).IsPositive() {
		unrealized = math.NaN()
		if fresh {
			unrealized = float(p.Qty.Add(p.DustQty).Mul(market.Bid).Sub(p.Cost).Sub(p.DustCost))
		}
	}
	set("unrealized_pnl_quote", unrealized)
	daily := math.NaN()
	if p.Day == time.Now().UTC().Format("2006-01-02") {
		daily = equity - float(p.DayEquity)
	}
	set("daily_pnl_quote", daily)
	if e := p.Performance; e != nil {
		external := p.ExternalFeeQuote.Sub(e.ExternalFeeBaseline)
		realized, fees := float(e.RealizedExecution.Sub(external)), float(e.QuoteFees.Add(e.BaseFeeEstimate).Add(external))
		if len(p.UnvaluedFees) > 0 {
			realized, fees = math.NaN(), math.NaN()
		}
		set("realized_pnl_quote", realized)
		set("net_pnl_quote", realized+unrealized)
		set("fees_paid_quote_estimate", fees)
		set("performance_since_timestamp_seconds", float64(e.Since.Unix()))
		set("completed_trades", float64(e.Completed))
		set("winning_trades", float64(e.Wins))
		set("losing_trades", float64(e.Losses))
		set("excluded_legacy_trades", float64(e.Excluded))
		winRate, win, loss := math.NaN(), math.NaN(), math.NaN()
		if e.Completed > 0 {
			winRate = float64(e.Wins) / float64(e.Completed)
		}
		if e.Wins > 0 {
			win = float(e.WinningPnL.Div(decimal.NewFromUint64(e.Wins)))
		}
		if e.Losses > 0 {
			loss = float(e.LosingPnL.Div(decimal.NewFromUint64(e.Losses)))
		}
		set("trade_win_rate", winRate)
		set("average_win_quote", win)
		set("average_loss_quote", loss)
		set("max_drawdown_quote", float(e.MaxDrawdown))
		set("max_drawdown_percent", float(e.MaxDrawdownPct))
		for asset, fee := range e.Fees {
			a.metrics.FeeUnits.WithLabelValues(pair, quote, asset).Set(float(fee))
		}
	}
	d := a.state.Evaluations[pair]
	at, modelAt := 0.0, 0.0
	if !d.At.IsZero() {
		at = float64(d.At.Unix())
	}
	if !d.ModelAt.IsZero() {
		modelAt = float64(d.ModelAt.Unix())
	}
	set("last_evaluation_timestamp_seconds", at)
	set("last_model_request_timestamp_seconds", modelAt)
	set("last_model_latency_seconds", d.Latency)
	accepted, uncertain := 0.0, 0.0
	if d.OrderAccepted {
		accepted = 1
	}
	if d.OrderUncertain {
		uncertain = 1
	}
	set("last_decision_order_accepted", accepted)
	set("last_decision_order_uncertain", uncertain)
	action, outcome, reason := d.Action, d.Outcome, d.Reason
	if action == "" {
		action = "UNKNOWN"
	}
	if outcome == "" {
		outcome = "not_evaluated"
	}
	if reason == "" {
		reason = "not_evaluated"
	}
	features, err := features(market.Candles, a.cfg.SignalInterval)
	blocker := entryBlockReason(p, a.cfg, market, features, err == nil)
	for _, info := range []struct {
		g     *prometheus.GaugeVec
		value string
	}{{a.metrics.LatestAction, action}, {a.metrics.LatestOutcome, outcome}, {a.metrics.LatestReason, reason}, {a.metrics.EntryBlocker, blocker}} {
		info.g.DeletePartialMatch(prometheus.Labels{"pair": pair})
		info.g.WithLabelValues(pair, quote, info.value).Set(1)
	}
}
