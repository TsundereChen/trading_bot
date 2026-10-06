package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
)

type Pending struct {
	ID              string                     `json:"id"`
	OrderID         int64                      `json:"order_id,omitempty"`
	Side            string                     `json:"side"`
	Qty             decimal.Decimal            `json:"quantity"`
	AppliedQty      decimal.Decimal            `json:"applied_quantity"`
	AppliedQuote    decimal.Decimal            `json:"applied_quote"`
	AppliedBaseFee  decimal.Decimal            `json:"applied_base_fee"`
	AppliedQuoteFee decimal.Decimal            `json:"applied_quote_fee"`
	AppliedFees     map[string]decimal.Decimal `json:"applied_fees,omitempty"`
}

type Position struct {
	Pair             string                     `json:"pair"`
	Version          uint64                     `json:"version"`
	Paused           bool                       `json:"paused"`
	Cash             decimal.Decimal            `json:"cash"`
	Qty              decimal.Decimal            `json:"quantity"`
	Cost             decimal.Decimal            `json:"cost"`
	Stop             decimal.Decimal            `json:"stop"`
	Target           decimal.Decimal            `json:"target"`
	Pending          *Pending                   `json:"pending"`
	Protection       *Pending                   `json:"native_stop"`
	ExternalFees     map[string]decimal.Decimal `json:"external_fees,omitempty"`
	UnvaluedFees     map[string]decimal.Decimal `json:"unvalued_fees,omitempty"`
	ExternalFeeQuote decimal.Decimal            `json:"external_fee_quote_estimate"`
	LastSetup        int64                      `json:"last_setup"`
	Day              string                     `json:"day"`
	DayEquity        decimal.Decimal            `json:"day_start_equity"`
	CooldownUntil    time.Time                  `json:"cooldown_until"`
	Error            string                     `json:"error"`
}

func (p Position) equity(bid decimal.Decimal) decimal.Decimal {
	return p.Cash.Add(p.Qty.Mul(bid)).Sub(p.ExternalFeeQuote)
}

type State struct {
	Venue     string               `json:"venue"`
	AccountID string               `json:"account_id,omitempty"`
	Version   uint64               `json:"version"`
	Pairs     map[string]*Position `json:"pairs"`
}

func copyDecimals(in map[string]decimal.Decimal) map[string]decimal.Decimal {
	if in == nil {
		return nil
	}
	out := make(map[string]decimal.Decimal, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyPending(p *Pending) *Pending {
	if p == nil {
		return nil
	}
	out := *p
	out.AppliedFees = copyDecimals(p.AppliedFees)
	return &out
}

func (s State) copy() State {
	out := s
	out.Pairs = make(map[string]*Position, len(s.Pairs))
	for pair, p := range s.Pairs {
		if p == nil {
			continue
		}
		clone := *p
		clone.Pending, clone.Protection = copyPending(p.Pending), copyPending(p.Protection)
		clone.ExternalFees, clone.UnvaluedFees = copyDecimals(p.ExternalFees), copyDecimals(p.UnvaluedFees)
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

// Identity is bound before feeds or execution start. Adopting an old database
// requires an explicit attestation; known venue/account mismatches never do.
func bindState(s State, venue, accountID, confirmation string) (State, error) {
	if s.Venue != "" && s.Venue != venue {
		return s, fmt.Errorf("state belongs to venue %s, not %s; use a separate database", s.Venue, venue)
	}
	if s.AccountID != "" && accountID != "" && s.AccountID != accountID {
		return s, fmt.Errorf("state belongs to a different exchange account; use a separate database")
	}
	if s.Venue == "" && len(s.Pairs) > 0 && confirmation != venue+":"+accountID {
		return s, fmt.Errorf("unbound legacy state: verify its original venue/account, then set STATE_BINDING_CONFIRM=%s:%s", venue, accountID)
	}
	if accountID != "" && s.AccountID == "" {
		for _, p := range s.Pairs {
			if p != nil && (p.Qty.IsPositive() || p.Pending != nil || p.Protection != nil) && confirmation != venue+":"+accountID {
				return s, fmt.Errorf("existing exposure has no account binding; operator verification is required")
			}
		}
		s.AccountID = accountID
	}
	s.Venue = venue
	return s, nil
}

type Config struct {
	Database, Listen, OllayaURL, OllayaKey, ControlToken string
	DatabaseURL, MetricsToken, BindingConfirmation       string
	Trading                                              bool
	PerPairBudget, MaxPosition, RiskPerTrade, DailyLoss  decimal.Decimal
	MaxPairs                                             int
	AuditDays, AuditMaxEvents                            int
	Venue                                                Venue
	Pairs                                                []string
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

func calculateFeatures(cs []Candle) (Features, error) {
	if len(cs) < 60 {
		return Features{}, fmt.Errorf("not enough completed candles")
	}
	n := len(cs)
	last, prev := cs[n-1], cs[n-2]
	f := Features{EMA20: ema(cs, 20), EMA50: ema(cs, 50), SetupID: last.CloseTime}
	prevEMA := ema(cs[:n-1], 20)
	f.Uptrend, f.Reversal = f.EMA20 > f.EMA50, f.EMA20 < f.EMA50
	f.Pullback = prev.Low <= prevEMA && prev.Close <= prevEMA && last.Close > f.EMA20
	gain, loss := 0.0, 0.0
	for i := n - 14; i < n; i++ {
		c, p := cs[i], cs[i-1]
		f.ATR += math.Max(c.High-c.Low, math.Max(math.Abs(c.High-p.Close), math.Abs(c.Low-p.Close))) / 14
		if delta := c.Close - p.Close; delta > 0 {
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

type App struct {
	mu              sync.Mutex // Only in-memory snapshots/publication; never exchange or storage I/O.
	writeMu         sync.Mutex // Serializes durable state transactions, not exchange execution.
	pairLocks       sync.Map
	backtestMu      sync.Mutex
	requests        sync.WaitGroup
	closing         bool
	cfg             Config
	repo            Repository
	binance         *Binance
	feeds           *Feeds
	client          *http.Client
	state           State
	symbols         map[string]Symbol
	lastDecisions   map[string]any
	reconnectCounts map[string]uint64
	accountID       string
	metrics         Metrics
	fatal           bool
}

var errNoChange = errors.New("no state change")

func (a *App) snapshot() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state.copy()
}

func (a *App) symbolFor(pair string) (Symbol, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	symbol, ok := a.symbols[pair]
	return symbol, ok
}

func (a *App) pairLock(pair string) *sync.Mutex {
	value, _ := a.pairLocks.LoadOrStore(pair, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func (a *App) executionAllowed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.Trading && !a.fatal && a.binance != nil && a.binance.venue == a.cfg.Venue && a.accountID != "" && a.state.AccountID == a.accountID && a.state.Venue == a.cfg.Venue.Name
}

func (a *App) persist(ctx context.Context, next State, kind string, data any, publish func()) error {
	storageCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := a.repo.Commit(storageCtx, next, kind, data); err != nil {
		a.mu.Lock()
		a.fatal = true
		for _, p := range a.state.Pairs {
			p.Paused = true
			p.Error = "Persistence failure: " + err.Error()
		}
		a.mu.Unlock()
		a.metrics.Failures.WithLabelValues("storage").Inc()
		return err
	}
	a.mu.Lock()
	a.state = next
	if publish != nil {
		publish()
	}
	a.mu.Unlock()
	return nil
}

// commit is for initialization/tests. Runtime callers use update so they cannot
// overwrite another pair's state using an old whole-portfolio snapshot.
func (a *App) commit(ctx context.Context, s State, kind string, data any) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.persist(ctx, s.copy(), kind, data, nil)
}

func (a *App) update(ctx context.Context, kind string, data any, change func(State) error) error {
	return a.updateWith(ctx, kind, data, change, nil)
}

func (a *App) updateWith(ctx context.Context, kind string, data any, change func(State) error, publish func()) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	if a.fatal {
		a.mu.Unlock()
		return fmt.Errorf("storage fault; restart after repair")
	}
	old := a.state.copy()
	a.mu.Unlock()
	next := old.copy()
	if err := change(next); err != nil {
		if errors.Is(err, errNoChange) {
			return nil
		}
		return err
	}
	next.Version = old.Version + 1
	for pair, p := range next.Pairs {
		if previous := old.Pairs[pair]; previous == nil || !reflect.DeepEqual(previous, p) {
			p.Version = next.Version
		}
	}
	return a.persist(ctx, next, kind, data, publish)
}

func (a *App) fail(ctx context.Context, pair, component string, err error) {
	a.metrics.Failures.WithLabelValues(component).Inc()
	_ = a.update(ctx, "fault", map[string]string{"pair": pair, "component": component, "error": err.Error()}, func(s State) error {
		changed := false
		for name, p := range s.Pairs {
			if pair != "" && pair != name {
				continue
			}
			message := component + ": " + err.Error()
			if !p.Paused || p.Error != message {
				changed = true
			}
			p.Paused, p.Error = true, message
		}
		if !changed {
			return errNoChange
		}
		return nil
	})
}

func (a *App) snapshotLocked(pair string, p *Position, f Features, market MarketSnapshot) map[string]any {
	bid, ask := market.Bid, market.Ask
	spread := ask.Sub(bid).Div(bid).Mul(decimal.NewFromInt(10000)).InexactFloat64()
	eligible := !p.Paused && p.Qty.IsZero() && p.Protection == nil && len(p.UnvaluedFees) == 0 && f.Uptrend && f.Pullback && spread <= 10 && time.Now().After(p.CooldownUntil) && f.SetupID != p.LastSetup
	allowed := []string{"HOLD"}
	if eligible {
		allowed = append(allowed, "ENTER_LONG")
	}
	if p.Qty.IsPositive() {
		allowed = append(allowed, "EXIT_LONG")
	}
	return map[string]any{"pair": pair, "venue": a.cfg.Venue.label(), "timestamp": time.Now().UTC(), "market": map[string]any{"bid": bid, "ask": ask, "spread_bps": spread}, "features": f, "position": *p, "entry_eligible": eligible, "allowed_actions": allowed, "strategy_version": "trend-pullback-v1", "prompt_version": "v1"}
}

func (a *App) askOllaya(ctx context.Context, pair string, snapshot map[string]any) (*ollayaAnswer, error) {
	questions := map[string]any{
		"action":        map[string]any{"type": "choice", "instructions": "Choose only from allowed_actions. ENTER_LONG requires entry_eligible=true. EXIT_LONG requires an existing position and trend_reversal=true. Otherwise HOLD. Model probabilities are not probabilities of trading profit.", "criteria": map[string]string{"ENTER_LONG": "Eligible long-only entry", "EXIT_LONG": "Close tracked long on trend reversal", "HOLD": "No order"}},
		"regime":        map[string]any{"type": "choice", "instructions": "Classify market regime from supplied features.", "criteria": map[string]string{"UPTREND": "EMA20 above EMA50", "DOWNTREND": "EMA20 below EMA50", "RANGE": "No clear trend"}},
		"setup_quality": map[string]any{"type": "score", "instructions": "Rate the match to the stated trend-pullback entry rules, not future profitability.", "criteria": []string{"Not eligible", "Weak", "Moderate", "Strong"}},
	}
	body, err := json.Marshal(map[string]any{"model": "winnow:e4b", "state": snapshot, "questions": questions, "keep_alive": "10m"})
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
	start, outcome := time.Now(), "error"
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
	return math.Abs(sum-1) <= .001
}
