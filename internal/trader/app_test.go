package trader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"automated-trader/internal/strategy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(body string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}
func dec(v string) decimal.Decimal { return decimal.RequireFromString(v) }

func testApp(t *testing.T) *App {
	t.Helper()
	repo, err := OpenSQLite(t.TempDir() + "/test.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repo.Close() })
	a := &App{
		// Explicit model mirrors the configured runtime default.
		repo:          repo,
		cfg:           Config{ControlToken: strings.Repeat("a", 24), MetricsToken: strings.Repeat("m", 24), MaxPosition: dec("100"), RiskPerTrade: dec("2.5"), DailyLoss: dec("10"), MaxPairs: 8, PerPairBudget: dec("1000"), Venue: demo(), AuditDays: 30, AuditMaxEvents: 100000},
		state:         State{Venue: "demo", AccountID: "42", Pairs: map[string]*Position{"BTCUSDT": {Pair: "BTCUSDT", Paused: true, Cash: dec("1000")}}},
		accountID:     "42",
		symbols:       map[string]Symbol{"BTCUSDT": {Symbol: "BTCUSDT", Base: "BTC", Quote: "USDT", Step: dec("0.001"), MinQty: dec("0.001"), MinNotional: dec("5"), Tick: dec("0.01"), StopAllowed: true}},
		feeds:         NewFeeds([]string{"BTCUSDT"}),
		lastDecisions: map[string]any{},
		metrics:       newMetrics(prometheus.NewRegistry()),
	}
	a.cfg.OllayaModel = "winnow:e4b"
	market := a.feeds.markets["BTCUSDT"]
	market.bid, market.ask, market.quoteAt, market.connected = dec("100"), dec("101"), time.Now(), true
	return a
}

func TestPersistenceAndAudit(t *testing.T) {
	a := testApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Pending = &Pending{ID: "durable-intent", Side: "BUY", Qty: dec("0.01")}
	if err := a.commit(context.Background(), a.state, "intent", p.Pending); err != nil {
		t.Fatal(err)
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Pending.ID != "durable-intent" {
		t.Fatalf("load: %+v %v", loaded, err)
	}
	events, err := a.repo.Events(context.Background())
	if err != nil || len(events) != 1 || events[0].Kind != "intent" {
		t.Fatalf("events: %v %v", events, err)
	}
}

func TestOrderAccountingIdempotentAndFees(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["BTCUSDT"].Pending = &Pending{ID: "buy", Side: "BUY", Qty: dec("1")}
	a.binance = &Binance{venue: demo(), key: "test", secret: "test", client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		return response(`[{"id":1,"orderId":1,"qty":"1","quoteQty":"100","commission":"0.001","commissionAsset":"BTC"}]`), nil
	})}}
	o := Order{OrderID: 1, ClientID: "buy", Status: "PARTIALLY_FILLED", Executed: "1", Quote: "100"}
	for i := 0; i < 2; i++ {
		if err := a.applyOrder(context.Background(), "BTCUSDT", o, false); err != nil {
			t.Fatal(err)
		}
	}
	p := a.state.Pairs["BTCUSDT"]
	if !p.Qty.Equal(dec("0.999")) || !p.Cash.Equal(dec("900")) {
		t.Fatalf("duplicate accounting or missing fees: %+v", p)
	}
	o.Status = "FILLED"
	if err := a.applyOrder(context.Background(), "BTCUSDT", o, false); err != nil {
		t.Fatal(err)
	}
	if a.state.Pairs["BTCUSDT"].Pending != nil {
		t.Fatal("terminal order remained pending")
	}
}

func TestSubmissionPersistsBeforeNetworkAndNeverRetries(t *testing.T) {
	a := testApp(t)
	a.cfg.Trading = true
	a.state.Pairs["BTCUSDT"].Paused = false
	a.state.Pairs["BTCUSDT"].Stop = dec("99")
	orders := 0
	a.binance = &Binance{venue: demo(), key: "test", secret: "test", client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			return response(`{"canTrade":true,"balances":[{"asset":"USDT","free":"10000"}]}`), nil
		case "/api/v3/order":
			orders++
			s, err := a.repo.Load(context.Background())
			if err != nil || s.Pairs["BTCUSDT"].Pending == nil || s.Pairs["BTCUSDT"].Pending.ID != r.URL.Query().Get("newClientOrderId") {
				t.Error("missing durable intent")
			}
			return nil, errors.New("network outcome uncertain")
		}
		return nil, fmt.Errorf("unexpected request")
	})}}
	if a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test") == nil {
		t.Fatal("expected uncertain submission")
	}
	if a.state.Pairs["BTCUSDT"].Pending == nil {
		t.Fatal("uncertain intent discarded")
	}
	if a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test") == nil {
		t.Fatal("duplicate allowed")
	}
	if orders != 1 {
		t.Fatal("order retried")
	}
}

func TestControlAuthAndPairGuards(t *testing.T) {
	a := testApp(t)
	h := a.routes(prometheus.NewRegistry())
	call := func(body string, auth bool) int {
		r := httptest.NewRequest("POST", "/api/control", strings.NewReader(body))
		if auth {
			r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if code := call(`{"action":"start"}`, false); code != 401 {
		t.Fatal("unauthenticated control accepted:", code)
	}
	// Removing a pair that still holds a position is refused.
	a.state.Pairs["BTCUSDT"].Qty = dec("1")
	a.state.Pairs["BTCUSDT"].Paused = true
	if code := call(`{"action":"pairs","pairs":["ETHUSDT"]}`, true); code != 409 {
		t.Fatal("open position pair removed:", code)
	}
	if code := call(`{"action":"pause","pair":"NOPE"}`, true); code != 404 {
		t.Fatal("unknown pair accepted:", code)
	}
	if code := call(`{"action":"frobnicate"}`, true); code != 400 {
		t.Fatal("unknown action accepted:", code)
	}
}

// Each pair must be independently pausable and startable.
func TestPairsAreIndependent(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["ETHUSDT"] = &Position{Pair: "ETHUSDT", Paused: true, Cash: dec("1000")}
	a.feeds.SetPairs([]string{"BTCUSDT", "ETHUSDT"})
	market := a.feeds.markets["ETHUSDT"]
	market.bid, market.ask, market.quoteAt, market.connected = dec("50"), dec("51"), time.Now(), true
	a.symbols["ETHUSDT"] = Symbol{Symbol: "ETHUSDT", Base: "ETH", Quote: "USDT", Step: dec("0.01"), MinQty: dec("0.01"), MinNotional: dec("5")}

	h := a.routes(prometheus.NewRegistry())
	post := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/control", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := post(`{"action":"start","pair":"ETHUSDT"}`); w.Code != 200 {
		t.Fatal("start failed:", w.Code, w.Body.String())
	}
	if a.state.Pairs["ETHUSDT"].Paused || !a.state.Pairs["BTCUSDT"].Paused {
		t.Fatal("pause state leaked across pairs")
	}
	if w := post(`{"action":"pause"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, pair := range a.state.symbols() {
		if !a.state.Pairs[pair].Paused {
			t.Fatal("pause-all missed", pair)
		}
	}
	// Positions and cash stay isolated per pair.
	if a.state.Pairs["ETHUSDT"].Cash.String() != "1000" || a.state.Pairs["BTCUSDT"].Cash.String() != "1000" {
		t.Fatal("cash not isolated")
	}
}

func TestObserveOnlyAndTruncatedResponse(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(fmt.Sprint(truncated), func(t *testing.T) {
			a := testApp(t)
			a.state.Pairs["BTCUSDT"].Paused = false
			a.client = &http.Client{Timeout: time.Second}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"model": "winnow:e4b", "done_reason": "decide", "state_truncated": truncated, "answers": map[string]any{"action": map[string]any{"type": "choice", "choice": "ENTER_LONG", "probabilities": map[string]float64{"ENTER_LONG": 0.8, "EXIT_LONG": 0.1, "HOLD": 0.1}}}})
			}))
			defer server.Close()
			a.cfg.OllayaURL = server.URL
			cs := risingCandles(65)
			a.feeds.markets["BTCUSDT"].candles = cs
			a.binance = &Binance{venue: demo(), client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				t.Fatal("unexpected trading call")
				return nil, nil
			})}}
			a.decide(context.Background(), "BTCUSDT")
			if a.state.Pairs["BTCUSDT"].Pending != nil || a.state.Pairs["BTCUSDT"].Paused {
				t.Fatal("observe mode changed execution state")
			}
			events, err := a.repo.Events(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !truncated && (len(events) != 3 || events[0].Kind != "evaluation_outcome" || events[1].Kind != "decision" || events[2].Kind != "decision_candle") {
				t.Fatalf("expected decision and outcome audits, got %d events", len(events))
			}
			if truncated && (len(events) != 2 || events[0].Kind != "evaluation_outcome" || events[1].Kind != "decision_candle") {
				t.Fatal("truncated decision accepted")
			}
		})
	}
}

func risingCandles(n int) []strategy.Candle {
	cs := []strategy.Candle{}
	now := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		price := 100 + float64(i)
		cs = append(cs, strategy.Candle{CloseTime: now - int64(n-i)*60000, Open: price, High: price + 2, Low: price - 2, Close: price, Volume: 10})
	}
	return cs
}

func TestMetricsEndpointRequiresToken(t *testing.T) {
	a := testApp(t)
	reg := prometheus.NewRegistry()
	a.metrics = newMetrics(reg)
	a.metrics.Equity.WithLabelValues("BTCUSDT").Set(1000)
	a.metrics.WSConnected.WithLabelValues("BTCUSDT").Set(1)
	h := a.routes(reg)
	unauthenticated := httptest.NewRequest("GET", "/internal/metrics", nil)
	unauthRecorder := httptest.NewRecorder()
	h.ServeHTTP(unauthRecorder, unauthenticated)
	if unauthRecorder.Code != 401 {
		t.Fatal("metrics endpoint served without token")
	}
	r := httptest.NewRequest("GET", "/internal/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+a.cfg.MetricsToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("metrics status:", w.Code)
	}
	// Per-pair series must be labelled so Grafana can separate portfolios.
	for _, want := range []string{"trader_equity_quote", `pair="BTCUSDT"`, "trader_portfolio_equity_quote", "trader_websocket_connected"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
}

func TestProtectiveExitWhilePaused(t *testing.T) {
	a := testApp(t)
	a.cfg.Trading = true
	p := a.state.Pairs["BTCUSDT"]
	p.Qty, p.Cost, p.Cash = dec("1"), dec("100"), dec("900")
	p.Stop, p.Target = dec("95"), dec("120")
	market := a.feeds.markets["BTCUSDT"]
	market.bid, market.ask = dec("90"), dec("91")
	orders := 0
	a.binance = &Binance{venue: demo(), key: "test", secret: "test", client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			return response(`{"canTrade":true,"balances":[{"asset":"BTC","free":"1"}]}`), nil
		case "/api/v3/order":
			orders++
			if r.URL.Query().Get("side") != "SELL" {
				t.Error("protective order not a sell")
			}
			return response(fmt.Sprintf(`{"orderId":1,"clientOrderId":%q,"status":"FILLED","executedQty":"1","cummulativeQuoteQty":"90"}`, r.URL.Query().Get("newClientOrderId"))), nil
		case "/api/v3/myTrades":
			return response(`[{"id":1,"orderId":1,"qty":"1","quoteQty":"90","commission":"0.1","commissionAsset":"USDT"}]`), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", r.URL.Path)
		}
	})}}
	a.poll(context.Background())
	p = a.snapshot().Pairs["BTCUSDT"]
	if orders != 1 || !p.Qty.IsZero() || !p.Cash.Equal(dec("989.9")) || !p.Paused {
		t.Fatalf("protective exit failed: %+v", p)
	}
}

func TestRejectMalformedProbabilities(t *testing.T) {
	if validProbabilities("ENTER_LONG", map[string]float64{"ENTER_LONG": 0.2, "EXIT_LONG": 0.1, "HOLD": 0.7}) {
		t.Fatal("choice not top probability accepted")
	}
	if validProbabilities("HOLD", nil) {
		t.Fatal("missing probabilities accepted")
	}
	if !validProbabilities("HOLD", map[string]float64{"ENTER_LONG": 0.1, "EXIT_LONG": 0.1, "HOLD": 0.8}) {
		t.Fatal("valid distribution rejected")
	}
}

func TestParsePairsAndVenue(t *testing.T) {
	pairs, err := parsePairs("btcusdt, ETHUSDT ,btcusdt")
	if err != nil || len(pairs) != 2 || pairs[0] != "BTCUSDT" || pairs[1] != "ETHUSDT" {
		t.Fatalf("pairs %v %v", pairs, err)
	}
	for _, bad := range []string{"", "BTCUSDT,BTCBUSD", "   "} {
		if _, err := parsePairs(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	if v, err := resolveVenue(""); err != nil || v.Live {
		t.Fatal("unset endpoint must default to paper:", v, err)
	}
	if v, err := resolveVenue(liveBaseURL); err != nil || !v.Live || v.Name != "live" {
		t.Fatal("live endpoint must select live venue:", v, err)
	}
	// Trading permission is a separate switch from venue selection.
	if v, err := resolveVenue(liveBaseURL); err != nil || !v.Live {
		t.Fatal(err)
	}
	if _, err := resolveVenue("https://example.invalid"); err == nil {
		t.Fatal("custom venue accepted")
	}
	// A trailing slash still selects live.
	if v, err := resolveVenue("https://api.binance.com/"); err != nil || !v.Live {
		t.Fatal("trailing slash must still select live:", v, err)
	}
}

func TestReconcilePairsGivesNewPairsABudget(t *testing.T) {
	cfg := Config{PerPairBudget: dec("1000")}
	// Fresh database: configured pairs must receive the per-pair budget.
	_, fresh := reconcilePairs(State{}, []string{"BTCUSDT", "ETHUSDT"}, cfg)
	for _, pair := range []string{"BTCUSDT", "ETHUSDT"} {
		p := fresh.Pairs[pair]
		if p == nil || !p.Cash.Equal(dec("1000")) || !p.Paused {
			t.Fatalf("%s: %+v", pair, p)
		}
	}
	// An existing pair keeps its own cash and is not reset.
	existing := State{Pairs: map[string]*Position{"BTCUSDT": {Pair: "BTCUSDT", Cash: dec("412.5"), Qty: dec("1")}}}
	retired, kept := reconcilePairs(existing, []string{"BTCUSDT", "SOLUSDT"}, cfg)
	if len(retired) != 0 {
		t.Fatal("unexpected retired:", retired)
	}
	if !kept.Pairs["BTCUSDT"].Cash.Equal(dec("412.5")) || !kept.Pairs["BTCUSDT"].Qty.Equal(dec("1")) {
		t.Fatalf("existing pair was reset: %+v", kept.Pairs["BTCUSDT"])
	}
	if !kept.Pairs["SOLUSDT"].Cash.Equal(dec("1000")) {
		t.Fatal("new pair missing budget")
	}
	// A persisted pair dropped from config is retained and reported, never lost.
	orphan := State{Pairs: map[string]*Position{"DOGEUSDT": {Pair: "DOGEUSDT", Qty: dec("5")}}}
	retired, kept = reconcilePairs(orphan, []string{"BTCUSDT"}, cfg)
	if len(retired) != 1 || retired[0] != "DOGEUSDT" || kept.Pairs["DOGEUSDT"] == nil {
		t.Fatalf("persisted pair dropped: %v %+v", retired, kept.Pairs)
	}
}

func TestStateCopyIsDeep(t *testing.T) {
	s := State{Pairs: map[string]*Position{"BTCUSDT": {Pair: "BTCUSDT", Cash: dec("10"), Pending: &Pending{ID: "x"}}}}
	clone := s.copy()
	clone.Pairs["BTCUSDT"].Cash = dec("999")
	clone.Pairs["BTCUSDT"].Pending.ID = "y"
	if !s.Pairs["BTCUSDT"].Cash.Equal(dec("10")) || s.Pairs["BTCUSDT"].Pending.ID != "x" {
		t.Fatal("copy aliased the original state")
	}
}
