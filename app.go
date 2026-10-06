package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
)

type Pending struct {
	ID              string          `json:"id"`
	Side            string          `json:"side"`
	Qty             decimal.Decimal `json:"quantity"`
	AppliedQty      decimal.Decimal `json:"applied_quantity"`
	AppliedQuote    decimal.Decimal `json:"applied_quote"`
	AppliedBaseFee  decimal.Decimal `json:"applied_base_fee"`
	AppliedQuoteFee decimal.Decimal `json:"applied_quote_fee"`
}

// Position is per-pair trading state. Each pair is independent: its own cash,
// quantity, stop, setup tracking, and daily-loss baseline.
type Position struct {
	Pair          string          `json:"pair"`
	Paused        bool            `json:"paused"`
	Cash          decimal.Decimal `json:"cash"`
	Qty           decimal.Decimal `json:"quantity"`
	Cost          decimal.Decimal `json:"cost"`
	Stop          decimal.Decimal `json:"stop"`
	Target        decimal.Decimal `json:"target"`
	Pending       *Pending        `json:"pending"`
	LastSetup     int64           `json:"last_setup"`
	Day           string          `json:"day"`
	DayEquity     decimal.Decimal `json:"day_start_equity"`
	CooldownUntil time.Time       `json:"cooldown_until"`
	Error         string          `json:"error"`
}

func (p Position) equity(bid decimal.Decimal) decimal.Decimal { return p.Cash.Add(p.Qty.Mul(bid)) }

type State struct {
	Pairs map[string]*Position `json:"pairs"`
}

func (s State) copy() State {
	out := State{Pairs: make(map[string]*Position, len(s.Pairs))}
	for pair, p := range s.Pairs {
		if p == nil {
			continue
		}
		clone := *p
		if p.Pending != nil {
			pending := *p.Pending
			clone.Pending = &pending
		}
		out.Pairs[pair] = &clone
	}
	return out
}

func (s State) symbols() []string {
	out := make([]string, 0, len(s.Pairs))
	for pair := range s.Pairs {
		out = append(out, pair)
	}
	sort.Strings(out)
	return out
}

type Config struct {
	Database, Listen, OllayaURL, OllayaKey, ControlToken string
	DatabaseURL, MetricsToken                            string
	Trading                                              bool
	PaperMode                                            bool
	PerPairBudget, MaxPosition, RiskPerTrade, DailyLoss  decimal.Decimal
	MaxPairs                                             int
	Venue                                                Venue
	// Pairs is the pair list resolved from TRADING_PAIRS.
	Pairs []string
}

type Features struct {
	EMA20          float64 `json:"ema20"`
	EMA50          float64 `json:"ema50"`
	ATR            float64 `json:"atr14"`
	RSI            float64 `json:"rsi14"`
	RelativeVolume float64 `json:"relative_volume"`
	Uptrend        bool    `json:"uptrend"`
	Pullback       bool    `json:"pullback_recovered"`
	Reversal       bool    `json:"trend_reversal"`
	SetupID        int64   `json:"setup_id"`
}

func ema(cs []Candle, period int) float64 {
	v := cs[0].Close
	alpha := 2.0 / float64(period+1)
	for _, c := range cs[1:] {
		v += alpha * (c.Close - v)
	}
	return v
}

func features(cs []Candle) (Features, error) {
	if len(cs) > 0 && time.Now().UnixMilli()-cs[len(cs)-1].CloseTime > 90000 {
		return Features{}, fmt.Errorf("completed candles stale")
	}
	return calculateFeatures(cs)
}

// calculateFeatures is pure so backtests share live indicator semantics.
func calculateFeatures(cs []Candle) (Features, error) {
	if len(cs) < 60 {
		return Features{}, fmt.Errorf("not enough completed candles")
	}
	n := len(cs)
	last, prev := cs[n-1], cs[n-2]
	f := Features{EMA20: ema(cs, 20), EMA50: ema(cs, 50), SetupID: last.CloseTime}
	prevEMA := ema(cs[:n-1], 20)
	f.Uptrend = f.EMA20 > f.EMA50
	f.Reversal = f.EMA20 < f.EMA50
	// Previous completed candle touched and closed at/below its EMA20;
	// the latest completed candle closed back above it.
	f.Pullback = prev.Low <= prevEMA && prev.Close <= prevEMA && last.Close > f.EMA20
	gain, loss := 0.0, 0.0
	for i := n - 14; i < n; i++ {
		c, p := cs[i], cs[i-1]
		f.ATR += math.Max(c.High-c.Low, math.Max(math.Abs(c.High-p.Close), math.Abs(c.Low-p.Close))) / 14
		delta := c.Close - p.Close
		if delta > 0 {
			gain += delta
		} else {
			loss -= delta
		}
	}
	if loss == 0 {
		f.RSI = 100
		if gain == 0 {
			f.RSI = 50
		}
	} else {
		f.RSI = 100 - 100/(1+gain/loss)
	}
	vol := 0.0
	for _, c := range cs[n-21 : n-1] {
		vol += c.Volume / 20
	}
	if vol > 0 {
		f.RelativeVolume = last.Volume / vol
	}
	if f.ATR <= 0 {
		return f, fmt.Errorf("invalid volatility")
	}
	return f, nil
}

type Metrics struct {
	Latency                                         *prometheus.HistogramVec
	Decisions                                       *prometheus.CounterVec
	Orders                                          *prometheus.CounterVec
	Failures                                        *prometheus.CounterVec
	Equity, Exposure, Running, FeedAge, WSConnected *prometheus.GaugeVec
	PortfolioEquity, PortfolioExposure              prometheus.Gauge
	WSReconnects                                    *prometheus.CounterVec
}

func newMetrics(reg *prometheus.Registry) Metrics {
	m := Metrics{
		Latency:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "trader_ollaya_request_duration_seconds", Help: "End-to-end decision latency", Buckets: []float64{.25, .5, 1, 2, 3, 4, 5, 10}}, []string{"outcome"}),
		Decisions:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_decisions_total", Help: "Model decisions by pair and action"}, []string{"pair", "action"}),
		Orders:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_orders_total", Help: "Order submission outcomes"}, []string{"pair", "side", "outcome"}),
		Failures:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_errors_total", Help: "Failures by bounded component name"}, []string{"component"}),
		Equity:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_equity_quote", Help: "Per-pair allocated equity including unrealized PnL"}, []string{"pair"}),
		Exposure:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_exposure_quote", Help: "Per-pair position mark-to-bid value"}, []string{"pair"}),
		Running:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_entries_enabled", Help: "1 when entries are enabled for the pair"}, []string{"pair"}),
		FeedAge:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_market_quote_age_seconds", Help: "Age of last received quote for the pair"}, []string{"pair"}),
		WSConnected:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "trader_websocket_connected", Help: "Whether the pair's market stream is connected"}, []string{"pair"}),
		WSReconnects:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_websocket_reconnects_total", Help: "Market stream reconnect attempts"}, []string{"pair"}),
		PortfolioEquity:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_portfolio_equity_quote", Help: "Sum of per-pair equity"}),
		PortfolioExposure: prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_portfolio_exposure_quote", Help: "Sum of per-pair exposure"}),
	}
	reg.MustRegister(m.Latency, m.Decisions, m.Orders, m.Failures, m.Equity, m.Exposure, m.Running, m.FeedAge, m.WSConnected, m.WSReconnects, m.PortfolioEquity, m.PortfolioExposure)
	return m
}

type App struct {
	mu            sync.Mutex
	cfg           Config
	repo          Repository
	binance       *Binance
	feeds         *Feeds
	client        *http.Client
	state         State
	symbols       map[string]Symbol
	lastDecisions map[string]any
	metrics       Metrics
	fatal         bool // Persistence failure locks all order submission until restart.
}

func (a *App) commit(ctx context.Context, s State, kind string, data any) error {
	if err := a.repo.Commit(ctx, s, kind, data); err != nil {
		a.fatal = true
		for _, p := range a.state.Pairs {
			p.Paused = true
			p.Error = "Persistence failure: " + err.Error()
		}
		a.metrics.Failures.WithLabelValues("storage").Inc()
		return err
	}
	a.state = s
	return nil
}

func (a *App) positions(s State) []*Position {
	out := make([]*Position, 0, len(s.Pairs))
	for _, pair := range s.symbols() {
		out = append(out, s.Pairs[pair])
	}
	return out
}

// fail pauses one pair, or every pair when pair is empty (component-level fault).
func (a *App) fail(ctx context.Context, pair, component string, err error) {
	a.metrics.Failures.WithLabelValues(component).Inc()
	s := a.state
	var targets []*Position
	if pair == "" {
		targets = a.positions(s)
	} else if p := s.Pairs[pair]; p != nil {
		targets = []*Position{p}
	}
	for _, p := range targets {
		p.Paused = true
		p.Error = component + ": " + err.Error()
	}
	_ = a.commit(ctx, s, "fault", map[string]string{"pair": pair, "component": component, "error": err.Error()})
}

func (a *App) symbolFor(pair string) (Symbol, bool) {
	symbol, ok := a.symbols[pair]
	return symbol, ok
}

// poll services protective exits, order reconciliation, and portfolio metrics
// for every pair. It never waits for inference.
func (a *App) poll(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fatal {
		return
	}
	s := a.state
	totalEquity, totalExposure := decimal.Zero, decimal.Zero
	for _, pair := range s.symbols() {
		p := s.Pairs[pair]
		market := a.feeds.Snapshot(pair)
		connected := market.Connected && market.Pair == pair
		a.metrics.WSConnected.WithLabelValues(pair).Set(0)
		if connected {
			a.metrics.WSConnected.WithLabelValues(pair).Set(1)
		}
		if connected && !market.QuoteAt.IsZero() {
			a.metrics.FeedAge.WithLabelValues(pair).Set(time.Since(market.QuoteAt).Seconds())
		}
		a.metrics.Running.WithLabelValues(pair).Set(0)

		// Reconciliation does not depend on a healthy price feed.
		if p.Pending != nil {
			order, err := a.binance.Find(ctx, pair, p.Pending.ID)
			if err == nil {
				err = a.applyOrderLocked(ctx, pair, order)
			}
			if err != nil {
				a.fail(ctx, pair, "reconciliation", err)
				continue
			}
		}
		if !connected || time.Since(market.QuoteAt) > 3*time.Second {
			continue
		}
		bid := market.Bid
		equity := p.equity(bid)
		totalEquity = totalEquity.Add(equity)
		exposure := p.Qty.Mul(bid)
		totalExposure = totalExposure.Add(exposure)
		a.metrics.Equity.WithLabelValues(pair).Set(equity.InexactFloat64())
		a.metrics.Exposure.WithLabelValues(pair).Set(exposure.InexactFloat64())

		if !p.Paused && a.cfg.Trading {
			a.metrics.Running.WithLabelValues(pair).Set(1)
		}
		day := time.Now().UTC().Format("2006-01-02")
		if p.Day != day {
			p.Day = day
			p.DayEquity = equity
			if a.commit(ctx, s, "day_rollover", map[string]any{"pair": pair, "equity": equity}) != nil {
				return
			}
		}
		dailyHit := p.DayEquity.Sub(equity).GreaterThanOrEqual(a.cfg.DailyLoss)
		if dailyHit && !p.Paused {
			p.Paused = true
			if a.commit(ctx, s, "daily_loss_limit", map[string]any{"pair": pair, "equity": equity}) != nil {
				return
			}
		}
		if p.Qty.IsPositive() && p.Pending == nil && a.cfg.Trading {
			if bid.LessThanOrEqual(p.Stop) || bid.GreaterThanOrEqual(p.Target) || dailyHit {
				if err := a.submitLocked(ctx, pair, "SELL", p.Qty, "protective_exit"); err != nil {
					a.fail(ctx, pair, "execution", err)
				}
			}
		}
	}
	a.metrics.PortfolioEquity.Set(totalEquity.InexactFloat64())
	a.metrics.PortfolioExposure.Set(totalExposure.InexactFloat64())
}

func (a *App) applyOrderLocked(ctx context.Context, pair string, order Order) error {
	p := a.state.Pairs[pair]
	if p == nil || p.Pending == nil || order.ClientID != p.Pending.ID {
		return fmt.Errorf("order identity mismatch")
	}
	qty, err := decimal.NewFromString(order.Executed)
	if err != nil {
		return err
	}
	quote, err := decimal.NewFromString(order.Quote)
	if err != nil {
		return err
	}
	fees, err := a.binance.Fees(ctx, pair, order.OrderID)
	if err != nil {
		return err
	}
	if qty.IsPositive() && len(fees) == 0 {
		return fmt.Errorf("fills not yet available; keep order pending for reconciliation")
	}
	symbol, known := a.symbolFor(pair)
	if !known {
		return fmt.Errorf("no exchange rules for pair %s", pair)
	}
	base, quoteAsset := symbol.Base, symbol.Quote
	for asset, fee := range fees {
		if asset != base && asset != quoteAsset && fee.IsPositive() {
			return fmt.Errorf("unsupported fee asset %s; manual reconciliation required", asset)
		}
	}
	pending := *p.Pending
	dq := qty.Sub(pending.AppliedQty)
	dc := quote.Sub(pending.AppliedQuote)
	bf := fees[base].Sub(pending.AppliedBaseFee)
	qf := fees[quoteAsset].Sub(pending.AppliedQuoteFee)
	if dq.IsNegative() || dc.IsNegative() || bf.IsNegative() || qf.IsNegative() {
		return fmt.Errorf("order totals moved backwards; possible exchange reset")
	}
	if pending.Side == "BUY" {
		p.Qty = p.Qty.Add(dq).Sub(bf)
		p.Cost = p.Cost.Add(dc).Add(qf)
		p.Cash = p.Cash.Sub(dc).Sub(qf)
	} else {
		sold := dq.Add(bf)
		if sold.GreaterThan(p.Qty) {
			return fmt.Errorf("sell exceeds tracked position")
		}
		if p.Qty.IsPositive() {
			p.Cost = p.Cost.Mul(p.Qty.Sub(sold)).Div(p.Qty)
		}
		p.Qty = p.Qty.Sub(sold)
		p.Cash = p.Cash.Add(dc).Sub(qf)
	}
	pending.AppliedQty = qty
	pending.AppliedQuote = quote
	pending.AppliedBaseFee = fees[base]
	pending.AppliedQuoteFee = fees[quoteAsset]
	p.Pending = &pending
	terminal := order.Status == "FILLED" || order.Status == "CANCELED" || order.Status == "EXPIRED" || order.Status == "REJECTED" || order.Status == "EXPIRED_IN_MATCH"
	if terminal {
		p.Pending = nil
		p.CooldownUntil = time.Now().Add(time.Minute)
	}
	if p.Qty.IsZero() {
		p.Cost = decimal.Zero
		p.Stop = decimal.Zero
		p.Target = decimal.Zero
	}
	return a.commit(ctx, a.state, "order_update", map[string]any{"pair": pair, "order": order, "fees": fees})
}

func (a *App) submitLocked(ctx context.Context, pair, side string, qty decimal.Decimal, reason string) error {
	p := a.state.Pairs[pair]
	symbol, ok := a.symbolFor(pair)
	if p == nil || !ok {
		return fmt.Errorf("unknown pair")
	}
	if !a.cfg.Trading || a.fatal || p.Pending != nil {
		return fmt.Errorf("execution disabled or unresolved order")
	}
	market := a.feeds.Snapshot(pair)
	if !market.Connected || market.Pair != pair || time.Since(market.QuoteAt) > 3*time.Second {
		return fmt.Errorf("market stream unavailable")
	}
	bid, ask := market.Bid, market.Ask
	if side == "BUY" && p.Paused {
		return fmt.Errorf("entries paused")
	}
	qty = floorStep(qty, symbol.Step)
	if qty.LessThan(symbol.MinQty) || qty.IsZero() || (symbol.MaxQty.IsPositive() && qty.GreaterThan(symbol.MaxQty)) {
		return fmt.Errorf("quantity outside exchange limits; position may be dust")
	}
	notional := qty.Mul(bid)
	if notional.LessThan(symbol.MinNotional) || (symbol.MaxNotional.IsPositive() && qty.Mul(ask).GreaterThan(symbol.MaxNotional)) {
		return fmt.Errorf("notional outside exchange limits")
	}
	balances, err := a.binance.Balances(ctx)
	if err != nil {
		return err
	}
	if side == "BUY" && balances[symbol.Quote].LessThan(qty.Mul(ask).Mul(decimal.NewFromFloat(1.01))) {
		return fmt.Errorf("insufficient free quote balance")
	}
	if side == "SELL" && balances[symbol.Base].LessThan(qty) {
		return fmt.Errorf("tracked position exceeds free balance; reconcile account")
	}
	var idBytes [12]byte
	if _, err = rand.Read(idBytes[:]); err != nil {
		return err
	}
	id := "at-" + hex.EncodeToString(idBytes[:])
	p.Pending = &Pending{ID: id, Side: side, Qty: qty}
	// Persist intent before network I/O. Never blindly resubmit after uncertainty.
	if err = a.commit(ctx, a.state, "order_intent", map[string]any{"pair": pair, "id": id, "side": side, "quantity": qty, "reason": reason}); err != nil {
		p.Pending = nil
		return err
	}
	order, err := a.binance.Place(ctx, pair, side, qty.String(), id)
	if err != nil {
		a.metrics.Orders.WithLabelValues(pair, side, "uncertain").Inc()
		return fmt.Errorf("submission unresolved; reconciliation required: %w", err)
	}
	a.metrics.Orders.WithLabelValues(pair, side, "accepted").Inc()
	return a.applyOrderLocked(ctx, pair, order)
}

// decide evaluates one pair. The caller guarantees exclusive scheduling per pair.
func (a *App) decide(ctx context.Context, pair string) {
	a.mu.Lock()
	p, tracked := a.state.Pairs[pair]
	if !tracked || a.fatal || p.Pending != nil {
		a.mu.Unlock()
		return
	}
	market := a.feeds.Snapshot(pair)
	if market.Pair != pair || !market.Connected {
		a.mu.Unlock()
		return
	}
	f, err := features(market.Candles)
	if err != nil {
		a.metrics.Failures.WithLabelValues("signals").Inc()
		a.mu.Unlock()
		return
	}
	if time.Since(market.QuoteAt) > 3*time.Second {
		a.mu.Unlock()
		return
	}
	snapshot := a.snapshotLocked(pair, p, f, market)
	a.mu.Unlock()

	result, err := a.askOllaya(ctx, pair, snapshot)
	a.mu.Lock()
	defer a.mu.Unlock()
	p2, tracked := a.state.Pairs[pair]
	if !tracked || a.fatal || p2.Pending != nil || a.state.Pairs[pair] != p {
		return
	}
	if err != nil {
		a.metrics.Failures.WithLabelValues("ollaya").Inc()
		_ = a.commit(ctx, a.state, "decision_error", map[string]any{"pair": pair, "input": snapshot, "error": err.Error()})
		return
	}
	action, ok := validatedAction(result)
	if !ok {
		a.metrics.Failures.WithLabelValues("ollaya_validation").Inc()
		return
	}
	a.lastDecisions[pair] = result
	a.metrics.Decisions.WithLabelValues(pair, action).Inc()
	if a.commit(ctx, a.state, "decision", map[string]any{"pair": pair, "input": snapshot, "output": result}) != nil {
		return
	}
	if !a.cfg.Trading {
		return // Observe-only mode must never create order intents.
	}
	if p2 != a.state.Pairs[pair] || a.state.Pairs[pair].Pending != nil {
		return
	}
	market = a.feeds.Snapshot(pair)
	if !market.Connected || time.Since(market.QuoteAt) > 3*time.Second {
		return
	}
	bid, ask := market.Bid, market.Ask
	spreadBps := ask.Sub(bid).Div(bid).Mul(decimal.NewFromInt(10000))
	pos := a.state.Pairs[pair]

	switch {
	case action == "ENTER_LONG" && snapshot["entry_eligible"] == true && !pos.Paused && pos.Qty.IsZero() && f.SetupID != pos.LastSetup:
		if spreadBps.GreaterThan(decimal.NewFromInt(10)) {
			return
		}
		distance := decimal.NewFromFloat(f.ATR * 1.5)
		qty := decimal.Min(a.cfg.MaxPosition.Div(ask), a.cfg.RiskPerTrade.Div(distance))
		qty = decimal.Min(qty, pos.Cash.Div(ask.Mul(decimal.NewFromFloat(1.01))))
		stop := bid.Sub(distance)
		if !qty.IsPositive() || !stop.IsPositive() {
			return
		}
		pos.LastSetup = f.SetupID
		pos.Stop = stop
		pos.Target = ask.Add(distance.Mul(decimal.NewFromInt(2)))
		if a.commit(ctx, a.state, "setup_consumed", map[string]any{"pair": pair, "features": f}) != nil {
			return
		}
		if err := a.submitLocked(ctx, pair, "BUY", qty, "model_entry"); err != nil {
			a.fail(ctx, pair, "execution", err)
		}
	case action == "EXIT_LONG" && pos.Qty.IsPositive() && f.Reversal:
		if err := a.submitLocked(ctx, pair, "SELL", pos.Qty, "model_exit"); err != nil {
			a.fail(ctx, pair, "execution", err)
		}
	}
}

// snapshotLocked builds the model input from computed values only.
func (a *App) snapshotLocked(pair string, p *Position, f Features, market MarketSnapshot) map[string]any {
	bid, ask := market.Bid, market.Ask
	spread := ask.Sub(bid).Div(bid).Mul(decimal.NewFromInt(10000)).InexactFloat64()
	eligible := !p.Paused && !p.Qty.IsPositive() && f.Uptrend && f.Pullback && spread <= 10 && time.Now().After(p.CooldownUntil) && f.SetupID != p.LastSetup
	allowed := []string{"HOLD"}
	if eligible {
		allowed = append(allowed, "ENTER_LONG")
	}
	if p.Qty.IsPositive() {
		allowed = append(allowed, "EXIT_LONG")
	}
	return map[string]any{
		"pair":             pair,
		"venue":            a.cfg.Venue.label(),
		"timestamp":        time.Now().UTC(),
		"market":           map[string]any{"bid": bid, "ask": ask, "spread_bps": spread},
		"features":         f,
		"position":         *p,
		"entry_eligible":   eligible,
		"allowed_actions":  allowed,
		"strategy_version": "trend-pullback-v1",
		"prompt_version":   "v1",
	}
}

func (a *App) askOllaya(ctx context.Context, pair string, snapshot map[string]any) (*ollayaAnswer, error) {
	questions := map[string]any{
		"action":        map[string]any{"type": "choice", "instructions": "Choose only from allowed_actions. ENTER_LONG requires entry_eligible=true. EXIT_LONG requires an existing position and trend_reversal=true. Otherwise HOLD. Model probabilities are not probabilities of trading profit.", "criteria": map[string]string{"ENTER_LONG": "Eligible long-only entry", "EXIT_LONG": "Close tracked long on trend reversal", "HOLD": "No order"}},
		"regime":        map[string]any{"type": "choice", "instructions": "Classify market regime from supplied features.", "criteria": map[string]string{"UPTREND": "EMA20 above EMA50", "DOWNTREND": "EMA20 below EMA50", "RANGE": "No clear trend"}},
		"setup_quality": map[string]any{"type": "score", "instructions": "Rate the match to the stated trend-pullback entry rules, not future profitability.", "criteria": []string{"Not eligible", "Weak", "Moderate", "Strong"}},
	}
	request := map[string]any{"model": "winnow:e4b", "state": snapshot, "questions": questions, "keep_alive": "10m"}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	inferCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(inferCtx, "POST", strings.TrimRight(a.cfg.OllayaURL, "/")+"/api/decide", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.OllayaKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.OllayaKey)
	}
	start := time.Now()
	outcome := "error"
	defer func() { a.metrics.Latency.WithLabelValues(outcome).Observe(time.Since(start).Seconds()) }()
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Ollaya HTTP %d", resp.StatusCode)
	}
	var result ollayaAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return nil, err
	}
	outcome = "success"
	return &result, nil
}

type ollayaAnswer struct {
	Model     string                     `json:"model"`
	Truncated bool                       `json:"state_truncated"`
	Reason    string                     `json:"done_reason"`
	Answers   map[string]json.RawMessage `json:"answers"`
}

func validatedAction(result *ollayaAnswer) (string, bool) {
	if result == nil || result.Truncated || result.Reason != "decide" || result.Model != "winnow:e4b" {
		return "", false
	}
	var action struct {
		Type          string             `json:"type"`
		Choice        string             `json:"choice"`
		Probabilities map[string]float64 `json:"probabilities"`
	}
	if json.Unmarshal(result.Answers["action"], &action) != nil || action.Type != "choice" {
		return "", false
	}
	if action.Choice != "ENTER_LONG" && action.Choice != "EXIT_LONG" && action.Choice != "HOLD" {
		return "", false
	}
	if !validProbabilities(action.Choice, action.Probabilities) {
		return "", false
	}
	return action.Choice, true
}

func validProbabilities(choice string, probabilities map[string]float64) bool {
	if len(probabilities) != 3 {
		return false
	}
	sum := 0.0
	for _, label := range []string{"ENTER_LONG", "EXIT_LONG", "HOLD"} {
		p, ok := probabilities[label]
		if !ok || math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 || p > probabilities[choice] {
			return false
		}
		sum += p
	}
	return math.Abs(sum-1) <= 0.001
}

func (a *App) Run(ctx context.Context) {
	feedDone := make(chan struct{})
	go func() { defer close(feedDone); a.feeds.Run(ctx, a.binance) }()
	defer func() { <-feedDone }()

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.poll(ctx)
			}
		}
	}()

	// Decisions are scheduled round-robin: inference for one pair blocks the
	// next, so each pair's interval is (pair count x per-call latency).
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	index := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			pairs := a.state.symbols()
			a.mu.Unlock()
			if len(pairs) == 0 {
				continue
			}
			pair := pairs[index%len(pairs)]
			index++
			a.decide(ctx, pair)
		}
	}
}
