package trader

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
)

type Metrics struct {
	Latency                                                       *prometheus.HistogramVec
	Decisions, Orders, Failures                                   *prometheus.CounterVec
	Equity, Exposure, Running, FeedAge, WSConnected, ExternalFees *prometheus.GaugeVec
	PortfolioEquity, PortfolioExposure                            prometheus.Gauge
	WSReconnects                                                  *prometheus.CounterVec
}

func newMetrics(reg *prometheus.Registry) Metrics {
	m := Metrics{
		Latency:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "trader_ollaya_request_duration_seconds", Help: "End-to-end decision latency", Buckets: []float64{.25, .5, 1, 2, 3, 4, 5, 10}}, []string{"outcome"}),
		Decisions:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_decisions_total", Help: "Model decisions by pair and action"}, []string{"pair", "action"}),
		Orders:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_orders_total", Help: "Order submission outcomes"}, []string{"pair", "side", "outcome"}),
		Failures:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_errors_total", Help: "Failures by bounded component name"}, []string{"component"}),
		Equity:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_equity_quote", Help: "Allocated equity; NaN when valuation is unavailable"}, []string{"pair"}),
		Exposure:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_exposure_quote", Help: "Position mark-to-bid value; NaN when unavailable"}, []string{"pair"}),
		Running:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_entries_enabled", Help: "1 when entries are enabled and quotes are fresh"}, []string{"pair"}),
		FeedAge:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_market_quote_age_seconds", Help: "Age of last quote; +Inf before first quote"}, []string{"pair"}),
		WSConnected:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_websocket_connected", Help: "Whether market stream is connected"}, []string{"pair"}),
		WSReconnects:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_websocket_reconnects_total", Help: "Market stream reconnect count"}, []string{"pair"}),
		ExternalFees:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_external_fee_units", Help: "Cumulative fees paid in third assets"}, []string{"pair", "asset"}),
		PortfolioEquity:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_portfolio_equity_quote", Help: "Total equity; NaN if any valuation is unavailable"}),
		PortfolioExposure: prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_portfolio_exposure_quote", Help: "Total exposure; NaN if any held pair lacks a fresh quote"}),
	}
	reg.MustRegister(m.Latency, m.Decisions, m.Orders, m.Failures, m.Equity, m.Exposure, m.Running, m.FeedAge, m.WSConnected, m.WSReconnects, m.ExternalFees, m.PortfolioEquity, m.PortfolioExposure)
	return m
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
