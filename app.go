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
type State struct {
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
type Config struct {
	Pair, Database, Listen, OllayaURL, OllayaKey, ControlToken string
	DatabaseURL, MetricsToken                                  string
	Trading                                                    bool
	Budget, MaxPosition, RiskPerTrade, DailyLoss               decimal.Decimal
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

// Pure feature calculation is shared by live and historical evaluation.
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
	// A completed candle touches its EMA, followed by a completed recovery candle.
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
	Equity, Exposure, Running, FeedAge, WSConnected prometheus.Gauge
	WSReconnects                                    prometheus.Counter
}

func newMetrics(reg *prometheus.Registry) Metrics {
	m := Metrics{
		Latency:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "trader_ollaya_request_duration_seconds", Help: "End-to-end decision latency", Buckets: []float64{.25, .5, 1, 2, 3, 4, 5, 10}}, []string{"outcome"}),
		Decisions:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_decisions_total", Help: "Model decisions"}, []string{"action"}),
		Orders:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_orders_total", Help: "Order submission outcomes"}, []string{"side", "outcome"}),
		Failures:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "trader_errors_total", Help: "Failures by bounded component name"}, []string{"component"}),
		Equity:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_equity_quote", Help: "Bot allocated equity including unrealized PnL"}),
		Exposure:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_exposure_quote", Help: "Bot position mark-to-bid value"}),
		Running:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_entries_enabled", Help: "1 when entries are enabled"}),
		FeedAge:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_market_quote_age_seconds", Help: "Age of last received WebSocket quote; not exchange event age"}),
		WSConnected:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "trader_websocket_connected", Help: "Whether the market stream is connected"}),
		WSReconnects: prometheus.NewCounter(prometheus.CounterOpts{Name: "trader_websocket_reconnects_total", Help: "Market stream reconnect attempts"}),
	}
	reg.MustRegister(m.Latency, m.Decisions, m.Orders, m.Failures, m.Equity, m.Exposure, m.Running, m.FeedAge, m.WSConnected, m.WSReconnects)
	return m
}

type App struct {
	mu            sync.Mutex
	cfg           Config
	repo          Repository
	binance       *Binance
	market        *Market
	client        *http.Client
	state         State
	symbol        Symbol
	bid, ask      decimal.Decimal
	bookAt        time.Time
	lastReconnect uint64
	lastDecision  any
	metrics       Metrics
	fatal         bool // Persistence failure locks all order submission until restart.
}

func (a *App) commit(ctx context.Context, s State, kind string, data any) error {
	if err := a.repo.Commit(ctx, s, kind, data); err != nil {
		a.fatal = true
		a.state.Paused = true
		a.state.Error = "Persistence failure: " + err.Error()
		a.metrics.Failures.WithLabelValues("storage").Inc()
		return err
	}
	a.state = s
	return nil
}
func (a *App) fail(ctx context.Context, component string, err error) {
	a.metrics.Failures.WithLabelValues(component).Inc()
	s := a.state
	s.Error = component + ": " + err.Error()
	s.Paused = true
	_ = a.commit(ctx, s, "fault", map[string]string{"component": component, "error": err.Error()})
}

// poll also services protective exits and pending-order reconciliation. It never waits for inference.
func (a *App) poll(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.metrics.FeedAge.Set(time.Since(a.bookAt).Seconds())
	market := a.market.Snapshot()
	a.metrics.WSConnected.Set(0)
	if market.Connected {
		a.metrics.WSConnected.Set(1)
	}
	if market.Reconnects > a.lastReconnect {
		a.metrics.WSReconnects.Add(float64(market.Reconnects - a.lastReconnect))
		a.lastReconnect = market.Reconnects
	}
	if !market.QuoteAt.IsZero() {
		a.metrics.FeedAge.Set(time.Since(market.QuoteAt).Seconds())
	}
	if market.Pair != a.state.Pair || !market.Connected || time.Since(market.QuoteAt) > 3*time.Second {
		a.metrics.Running.Set(0)
		// Order reconciliation does not depend on a functioning price feed.
		if a.state.Pending != nil {
			order, err := a.binance.Find(ctx, a.state.Pair, a.state.Pending.ID)
			if err == nil {
				err = a.applyOrder(ctx, order)
			}
			if err != nil {
				a.fail(ctx, "reconciliation", err)
			}
		}
		return
	}
	bid, ask := market.Bid, market.Ask
	a.bid, a.ask, a.bookAt = bid, ask, market.QuoteAt
	a.metrics.FeedAge.Set(time.Since(market.QuoteAt).Seconds())
	if a.state.Pending != nil {
		order, err := a.binance.Find(ctx, a.state.Pair, a.state.Pending.ID)
		if err != nil {
			a.fail(ctx, "reconciliation", err)
			return
		}
		if err = a.applyOrder(ctx, order); err != nil {
			a.fail(ctx, "reconciliation", err)
			return
		}
	}
	equity := a.state.Cash.Add(a.state.Qty.Mul(bid))
	a.metrics.Equity.Set(equity.InexactFloat64())
	a.metrics.Exposure.Set(a.state.Qty.Mul(bid).InexactFloat64())
	a.metrics.Running.Set(0)
	if !a.state.Paused && a.cfg.Trading && !a.fatal {
		a.metrics.Running.Set(1)
	}
	day := time.Now().UTC().Format("2006-01-02")
	if a.state.Day != day {
		s := a.state
		s.Day = day
		s.DayEquity = equity
		if a.commit(ctx, s, "day_rollover", equity) != nil {
			return
		}
	}
	dailyHit := a.state.DayEquity.Sub(equity).GreaterThanOrEqual(a.cfg.DailyLoss)
	if dailyHit && !a.state.Paused {
		s := a.state
		s.Paused = true
		if a.commit(ctx, s, "daily_loss_limit", equity) != nil {
			return
		}
	}
	if a.state.Qty.IsPositive() && a.state.Pending == nil && !a.fatal && a.cfg.Trading {
		if bid.LessThanOrEqual(a.state.Stop) || bid.GreaterThanOrEqual(a.state.Target) || dailyHit {
			if err := a.submit(ctx, "SELL", a.state.Qty, "protective_exit"); err != nil {
				a.fail(ctx, "execution", err)
			}
		}
	}
}

func (a *App) applyOrder(ctx context.Context, order Order) error {
	if a.state.Pending == nil || order.ClientID != a.state.Pending.ID {
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
	fees, err := a.binance.Fees(ctx, a.state.Pair, order.OrderID)
	if err != nil {
		return err
	}
	if qty.IsPositive() && len(fees) == 0 {
		return fmt.Errorf("fills not yet available; keep order pending for reconciliation")
	}
	for asset, fee := range fees {
		if asset != a.symbol.Base && asset != a.symbol.Quote && fee.IsPositive() {
			return fmt.Errorf("unsupported fee asset %s; manual reconciliation required", asset)
		}
	}
	p := *a.state.Pending
	s := a.state
	dq, dc := qty.Sub(p.AppliedQty), quote.Sub(p.AppliedQuote)
	bf, qf := fees[a.symbol.Base].Sub(p.AppliedBaseFee), fees[a.symbol.Quote].Sub(p.AppliedQuoteFee)
	if dq.IsNegative() || dc.IsNegative() || bf.IsNegative() || qf.IsNegative() {
		return fmt.Errorf("order totals moved backwards; possible testnet reset")
	}
	if p.Side == "BUY" {
		s.Qty = s.Qty.Add(dq).Sub(bf)
		s.Cost = s.Cost.Add(dc).Add(qf)
		s.Cash = s.Cash.Sub(dc).Sub(qf)
	} else {
		sold := dq.Add(bf)
		if sold.GreaterThan(s.Qty) {
			return fmt.Errorf("sell exceeds tracked position")
		}
		if s.Qty.IsPositive() {
			s.Cost = s.Cost.Mul(s.Qty.Sub(sold)).Div(s.Qty)
		}
		s.Qty = s.Qty.Sub(sold)
		s.Cash = s.Cash.Add(dc).Sub(qf)
	}
	p.AppliedQty = qty
	p.AppliedQuote = quote
	p.AppliedBaseFee = fees[a.symbol.Base]
	p.AppliedQuoteFee = fees[a.symbol.Quote]
	s.Pending = &p
	terminal := order.Status == "FILLED" || order.Status == "CANCELED" || order.Status == "EXPIRED" || order.Status == "REJECTED" || order.Status == "EXPIRED_IN_MATCH"
	if terminal {
		s.Pending = nil
		s.CooldownUntil = time.Now().Add(time.Minute)
	}
	if s.Qty.IsZero() {
		s.Cost = decimal.Zero
		s.Stop = decimal.Zero
		s.Target = decimal.Zero
	}
	return a.commit(ctx, s, "order_update", map[string]any{"order": order, "fees": fees})
}

func (a *App) submit(ctx context.Context, side string, qty decimal.Decimal, reason string) error {
	if !a.cfg.Trading || a.fatal || a.state.Pending != nil {
		return fmt.Errorf("execution disabled or unresolved order")
	}
	if time.Since(a.bookAt) > 3*time.Second {
		return fmt.Errorf("stale quote")
	}
	market := a.market.Snapshot()
	if !market.Connected || market.Pair != a.state.Pair || time.Since(market.QuoteAt) > 3*time.Second {
		return fmt.Errorf("market stream unavailable")
	}
	a.bid, a.ask, a.bookAt = market.Bid, market.Ask, market.QuoteAt
	if side == "BUY" && a.state.Paused {
		return fmt.Errorf("entries paused")
	}
	qty = floorStep(qty, a.symbol.Step)
	if qty.LessThan(a.symbol.MinQty) || qty.IsZero() || (a.symbol.MaxQty.IsPositive() && qty.GreaterThan(a.symbol.MaxQty)) {
		return fmt.Errorf("quantity outside exchange limits; position may be dust")
	}
	notional := qty.Mul(a.bid)
	if notional.LessThan(a.symbol.MinNotional) || (a.symbol.MaxNotional.IsPositive() && qty.Mul(a.ask).GreaterThan(a.symbol.MaxNotional)) {
		return fmt.Errorf("notional outside exchange limits")
	}
	balances, err := a.binance.Balances(ctx)
	if err != nil {
		return err
	}
	if side == "BUY" && balances[a.symbol.Quote].LessThan(qty.Mul(a.ask).Mul(decimal.NewFromFloat(1.01))) {
		return fmt.Errorf("insufficient free quote balance")
	}
	if side == "SELL" && balances[a.symbol.Base].LessThan(qty) {
		return fmt.Errorf("tracked position exceeds free balance; reconcile account")
	}
	var idBytes [12]byte
	if _, err = rand.Read(idBytes[:]); err != nil {
		return err
	}
	id := "at-" + hex.EncodeToString(idBytes[:])
	s := a.state
	s.Pending = &Pending{ID: id, Side: side, Qty: qty}
	// Persist intent before network I/O. Never blindly resubmit after uncertain outcomes.
	if err = a.commit(ctx, s, "order_intent", map[string]any{"id": id, "side": side, "quantity": qty, "reason": reason}); err != nil {
		return err
	}
	order, err := a.binance.Place(ctx, s.Pair, side, qty.String(), id)
	if err != nil {
		a.metrics.Orders.WithLabelValues(side, "uncertain").Inc()
		return fmt.Errorf("submission unresolved; reconciliation required: %w", err)
	}
	a.metrics.Orders.WithLabelValues(side, "accepted").Inc()
	return a.applyOrder(ctx, order)
}

func (a *App) decide(ctx context.Context) {
	a.mu.Lock()
	pair := a.state.Pair
	a.mu.Unlock()
	market := a.market.Snapshot()
	if market.Pair != pair || !market.Connected {
		return
	}
	f, err := features(market.Candles)
	if err != nil {
		a.metrics.Failures.WithLabelValues("signals").Inc()
		return
	}
	a.mu.Lock()
	if pair != a.state.Pair || time.Since(a.bookAt) > 3*time.Second || a.state.Pending != nil || a.fatal {
		a.mu.Unlock()
		return
	}
	s := a.state
	bid, ask := a.bid, a.ask
	spread := ask.Sub(bid).Div(bid).Mul(decimal.NewFromInt(10000)).InexactFloat64()
	eligible := !s.Paused && !s.Qty.IsPositive() && f.Uptrend && f.Pullback && spread <= 10 && time.Now().After(s.CooldownUntil) && f.SetupID != s.LastSetup
	allowed := []string{"HOLD"}
	if eligible {
		allowed = append(allowed, "ENTER_LONG")
	}
	if s.Qty.IsPositive() {
		allowed = append(allowed, "EXIT_LONG")
	}
	snapshot := map[string]any{"pair": pair, "timestamp": time.Now().UTC(), "market": map[string]any{"bid": bid, "ask": ask, "spread_bps": spread}, "features": f, "position": s, "entry_eligible": eligible, "allowed_actions": allowed, "strategy_version": "trend-pullback-v1", "prompt_version": "v1"}
	a.mu.Unlock()
	questions := map[string]any{
		"action":        map[string]any{"type": "choice", "instructions": "Choose only from allowed_actions. ENTER_LONG requires entry_eligible=true. EXIT_LONG requires an existing position and trend_reversal=true. Otherwise HOLD. Model probabilities are not probabilities of trading profit.", "criteria": map[string]string{"ENTER_LONG": "Eligible long-only entry", "EXIT_LONG": "Close tracked long on trend reversal", "HOLD": "No order"}},
		"regime":        map[string]any{"type": "choice", "instructions": "Classify market regime from supplied features.", "criteria": map[string]string{"UPTREND": "EMA20 above EMA50", "DOWNTREND": "EMA20 below EMA50", "RANGE": "No clear trend"}},
		"setup_quality": map[string]any{"type": "score", "instructions": "Rate the match to the stated trend-pullback entry rules, not future profitability.", "criteria": []string{"Not eligible", "Weak", "Moderate", "Strong"}},
	}
	request := map[string]any{"model": "winnow:e4b", "state": snapshot, "questions": questions, "keep_alive": "10m"}
	body, _ := json.Marshal(request)
	inferCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(inferCtx, "POST", strings.TrimRight(a.cfg.OllayaURL, "/")+"/api/decide", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.OllayaKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.OllayaKey)
	}
	start := time.Now()
	outcome := "error"
	defer func() { a.metrics.Latency.WithLabelValues(outcome).Observe(time.Since(start).Seconds()) }()
	resp, err := a.client.Do(req)
	var result struct {
		Model     string                     `json:"model"`
		Truncated bool                       `json:"state_truncated"`
		Reason    string                     `json:"done_reason"`
		Answers   map[string]json.RawMessage `json:"answers"`
	}
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			err = fmt.Errorf("Ollaya HTTP %d", resp.StatusCode)
		} else {
			err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.metrics.Failures.WithLabelValues("ollaya").Inc()
		_ = a.commit(ctx, a.state, "decision_error", map[string]any{"input": request, "error": err.Error()})
		return
	}
	if result.Truncated || result.Reason != "decide" || result.Model != "winnow:e4b" {
		a.metrics.Failures.WithLabelValues("ollaya_validation").Inc()
		return
	}
	var action struct {
		Type          string             `json:"type"`
		Choice        string             `json:"choice"`
		Probabilities map[string]float64 `json:"probabilities"`
	}
	if json.Unmarshal(result.Answers["action"], &action) != nil || action.Type != "choice" || (action.Choice != "ENTER_LONG" && action.Choice != "EXIT_LONG" && action.Choice != "HOLD") || !validProbabilities(action.Choice, action.Probabilities) {
		a.metrics.Failures.WithLabelValues("ollaya_validation").Inc()
		return
	}
	outcome = "success"
	a.metrics.Decisions.WithLabelValues(action.Choice).Inc()
	a.lastDecision = result
	if a.commit(ctx, a.state, "decision", map[string]any{"input": request, "output": result, "latency_seconds": time.Since(start).Seconds()}) != nil {
		return
	}
	if !a.cfg.Trading {
		return
	} // Observe-only mode must never create order intents.
	if pair != a.state.Pair || a.state.Pending != nil || time.Since(a.bookAt) > 3*time.Second || a.fatal {
		return
	}
	if action.Choice == "ENTER_LONG" && eligible && !a.state.Paused && a.state.Qty.IsZero() && f.SetupID != a.state.LastSetup {
		// Recheck spread and equity at order time; budget a 1% fee/slippage buffer.
		if a.ask.Sub(a.bid).Div(a.bid).Mul(decimal.NewFromInt(10000)).GreaterThan(decimal.NewFromInt(10)) {
			return
		}
		distance := decimal.NewFromFloat(f.ATR * 1.5)
		qty := decimal.Min(a.cfg.MaxPosition.Div(a.ask), a.cfg.RiskPerTrade.Div(distance))
		qty = decimal.Min(qty, a.state.Cash.Div(a.ask.Mul(decimal.NewFromFloat(1.01))))
		if !qty.IsPositive() {
			return
		}
		s := a.state
		s.LastSetup = f.SetupID
		s.Stop = a.bid.Sub(distance)
		s.Target = a.ask.Add(distance.Mul(decimal.NewFromInt(2)))
		if !s.Stop.IsPositive() {
			return
		}
		if a.commit(ctx, s, "setup_consumed", f) != nil {
			return
		}
		if err := a.submit(ctx, "BUY", qty, "model_entry"); err != nil {
			a.fail(ctx, "execution", err)
		}
	} else if action.Choice == "EXIT_LONG" && a.state.Qty.IsPositive() && f.Reversal && a.cfg.Trading {
		if err := a.submit(ctx, "SELL", a.state.Qty, "model_exit"); err != nil {
			a.fail(ctx, "execution", err)
		}
	}
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
	go func() { defer close(feedDone); a.market.Run(ctx, a.binance) }()
	defer func() { <-feedDone }()
	pollDone := make(chan struct{})
	defer func() { <-pollDone }()
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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.decide(ctx)
		}
	}
}
