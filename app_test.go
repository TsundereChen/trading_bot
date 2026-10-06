package main

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
	return &App{repo: repo, cfg: Config{ControlToken: strings.Repeat("a", 24), MetricsToken: strings.Repeat("m", 24), MaxPosition: dec("100"), RiskPerTrade: dec("2.5"), DailyLoss: dec("10")}, state: State{Pair: "BTCUSDT", Cash: dec("1000"), Paused: true}, symbol: Symbol{Symbol: "BTCUSDT", Base: "BTC", Quote: "USDT", Step: dec("0.001"), MinQty: dec("0.001"), MinNotional: dec("5")}, market: &Market{pair: "BTCUSDT", bid: dec("100"), ask: dec("101"), quoteAt: time.Now(), connected: true}, bid: dec("100"), ask: dec("101"), bookAt: time.Now(), metrics: newMetrics(prometheus.NewRegistry())}
}

func TestFloorStep(t *testing.T) {
	for _, c := range []struct{ q, step, want string }{{"1.239", "0.01", "1.23"}, {"1.239", "0.05", "1.2"}, {"0.009", "0.01", "0"}} {
		if got := floorStep(dec(c.q), dec(c.step)); !got.Equal(dec(c.want)) {
			t.Fatalf("%s / %s: %s", c.q, c.step, got)
		}
	}
}
func TestPersistenceAndAudit(t *testing.T) {
	a := testApp(t)
	s := a.state
	s.Pending = &Pending{ID: "durable-intent", Side: "BUY", Qty: dec("0.01")}
	if err := a.commit(context.Background(), s, "intent", s.Pending); err != nil {
		t.Fatal(err)
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pending.ID != "durable-intent" {
		t.Fatalf("load: %+v %v", loaded, err)
	}
	events, err := a.repo.Events(context.Background())
	if err != nil || len(events) != 1 || events[0].Kind != "intent" {
		t.Fatalf("events: %v %v", events, err)
	}
}
func TestOrderAccountingIdempotentAndFees(t *testing.T) {
	a := testApp(t)
	a.state.Pending = &Pending{ID: "buy", Side: "BUY", Qty: dec("1")}
	a.binance = &Binance{key: "test", secret: "test", client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		return response(`[{"commission":"0.001","commissionAsset":"BTC"}]`), nil
	})}}
	o := Order{OrderID: 1, ClientID: "buy", Status: "PARTIALLY_FILLED", Executed: "1", Quote: "100"}
	for i := 0; i < 2; i++ {
		if err := a.applyOrder(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
	if !a.state.Qty.Equal(dec("0.999")) || !a.state.Cash.Equal(dec("900")) {
		t.Fatalf("duplicate accounting or missing fees: %+v", a.state)
	}
	o.Status = "FILLED"
	if err := a.applyOrder(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if a.state.Pending != nil {
		t.Fatal("terminal order remained pending")
	}
}
func TestSubmissionPersistsBeforeNetworkAndNeverRetries(t *testing.T) {
	a := testApp(t)
	a.cfg.Trading = true
	a.state.Paused = false
	orders := 0
	a.binance = &Binance{key: "test", secret: "test", client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			return response(`{"canTrade":true,"balances":[{"asset":"USDT","free":"10000"}]}`), nil
		case "/api/v3/order":
			orders++
			s, err := a.repo.Load(context.Background())
			if err != nil || s.Pending == nil || s.Pending.ID != r.URL.Query().Get("newClientOrderId") {
				t.Error("missing durable intent")
			}
			return nil, errors.New("network outcome uncertain")
		}
		return nil, fmt.Errorf("unexpected request")
	})}}
	if a.submit(context.Background(), "BUY", dec("1"), "test") == nil {
		t.Fatal("expected uncertain submission")
	}
	if a.state.Pending == nil {
		t.Fatal("uncertain intent discarded")
	}
	if a.submit(context.Background(), "BUY", dec("1"), "test") == nil {
		t.Fatal("duplicate allowed")
	}
	if orders != 1 {
		t.Fatal("order retried")
	}
}
func TestControlAuthAndPairGuard(t *testing.T) {
	a := testApp(t)
	h := a.routes(prometheus.NewRegistry())
	r := httptest.NewRequest("POST", "/api/control", strings.NewReader(`{"action":"start"}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	a.state.Qty = dec("1")
	r = httptest.NewRequest("POST", "/api/control", strings.NewReader(`{"action":"pair","pair":"ETHUSDT"}`))
	r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 409 {
		t.Fatal(w.Code)
	}
}
func TestObserveOnlyAndTruncatedResponse(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(fmt.Sprint(truncated), func(t *testing.T) {
			a := testApp(t)
			a.state.Paused = false
			a.client = &http.Client{Timeout: time.Second}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"model": "winnow:e4b", "done_reason": "decide", "state_truncated": truncated, "answers": map[string]any{"action": map[string]any{"type": "choice", "choice": "ENTER_LONG", "probabilities": map[string]float64{"ENTER_LONG": 0.8, "EXIT_LONG": 0.1, "HOLD": 0.1}}}})
			}))
			defer server.Close()
			a.cfg.OllayaURL = server.URL
			cs := []Candle{}
			now := time.Now().UnixMilli()
			for i := 0; i < 65; i++ {
				price := 100 + float64(i)
				cs = append(cs, Candle{CloseTime: now - int64(65-i)*60000, Open: price, High: price + 2, Low: price - 2, Close: price, Volume: 10})
			}
			a.market.candles = cs
			a.binance = &Binance{client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/api/v3/klines" {
					t.Fatal("unexpected trading call")
				}
				raw := [][]any{}
				for _, c := range cs {
					raw = append(raw, []any{c.CloseTime - 59999, fmt.Sprint(c.Open), fmt.Sprint(c.High), fmt.Sprint(c.Low), fmt.Sprint(c.Close), fmt.Sprint(c.Volume), c.CloseTime})
				}
				b, _ := json.Marshal(raw)
				return response(string(b)), nil
			})}}
			a.decide(context.Background())
			if a.state.Pending != nil || a.state.Paused {
				t.Fatal("observe mode changed execution state")
			}
			events, err := a.repo.Events(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !truncated && len(events) != 1 {
				t.Fatalf("expected audited decision, got %v", events)
			}
			if truncated && len(events) != 0 {
				t.Fatal("truncated decision accepted")
			}
		})
	}
}
func TestMetricsEndpoint(t *testing.T) {
	a := testApp(t)
	reg := prometheus.NewRegistry()
	a.metrics = newMetrics(reg)
	r := httptest.NewRequest("GET", "/internal/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+a.cfg.MetricsToken)
	w := httptest.NewRecorder()
	a.routes(reg).ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "trader_equity_quote") {
		t.Fatal("missing metrics")
	}
}

func TestProtectiveExitWhilePaused(t *testing.T) {
	a := testApp(t)
	a.cfg.Trading = true
	a.state.Qty = dec("1")
	a.state.Cost = dec("100")
	a.state.Cash = dec("900")
	a.state.Stop = dec("95")
	a.state.Target = dec("120")
	a.market.bid = dec("90")
	a.market.ask = dec("91")
	orders := 0
	a.binance = &Binance{key: "test", secret: "test", client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/ticker/bookTicker":
			return response(`{"bidPrice":"90","askPrice":"91"}`), nil
		case "/api/v3/account":
			return response(`{"canTrade":true,"balances":[{"asset":"BTC","free":"1"}]}`), nil
		case "/api/v3/order":
			orders++
			if r.URL.Query().Get("side") != "SELL" {
				t.Error("protective order not a sell")
			}
			return response(fmt.Sprintf(`{"orderId":1,"clientOrderId":%q,"status":"FILLED","executedQty":"1","cummulativeQuoteQty":"90"}`, r.URL.Query().Get("newClientOrderId"))), nil
		case "/api/v3/myTrades":
			return response(`[{"commission":"0.1","commissionAsset":"USDT"}]`), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", r.URL.Path)
		}
	})}}
	a.poll(context.Background())
	if orders != 1 || !a.state.Qty.IsZero() || !a.state.Cash.Equal(dec("989.9")) || !a.state.Paused {
		t.Fatalf("protective exit failed: %+v", a.state)
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
