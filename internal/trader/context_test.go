package trader

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"automated-trader/internal/strategy"
	"github.com/gorilla/websocket"
)

func fiveMinuteHistory() []strategy.Candle {
	cs := risingCandles(65)
	last := time.Now().UnixMilli() - 1000
	for i := range cs {
		cs[i].CloseTime = last - int64(len(cs)-1-i)*300000
	}
	return cs
}

func TestSignalAndContextRESTWindowsRemainIndependent(t *testing.T) {
	var requested []string
	b := mockBinance(func(r *http.Request) (*http.Response, error) {
		interval := r.URL.Query().Get("interval")
		requested = append(requested, interval)
		cs := risingCandles(65)
		if interval == "5m" {
			cs = fiveMinuteHistory()
		}
		raw := []any{}
		for _, c := range cs {
			raw = append(raw, []any{c.CloseTime - candleDuration(interval).Milliseconds() + 1, fmt.Sprint(c.Open), fmt.Sprint(c.High), fmt.Sprint(c.Low), fmt.Sprint(c.Close), fmt.Sprint(c.Volume), c.CloseTime})
		}
		// REST's unfinished last bar must never enter either window.
		raw = append(raw, []any{0, "100", "101", "99", "100", "1", time.Now().Add(time.Minute).UnixMilli()})
		data, _ := json.Marshal(raw)
		return response(string(data)), nil
	})
	b.interval = "1m"
	for _, interval := range []string{"1m", "5m", "1m"} {
		cs, err := b.Candles(context.Background(), "BTCUSDT", 100, interval)
		if err != nil || len(cs) != 65 {
			t.Fatalf("%s bootstrap: %d bars, %v", interval, len(cs), err)
		}
	}
	if b.interval != "1m" || strings.Join(requested, ",") != "1m,5m,1m" {
		t.Fatal("context request changed signal interval")
	}
}

func TestContextStreamRoutesCompletedCandlesIndependently(t *testing.T) {
	f := NewFeeds([]string{"BTCUSDT"}, "1m", "5m")
	m := f.markets["BTCUSDT"]
	frame := func(interval string, at int64, closed bool) []byte {
		return []byte(fmt.Sprintf(`{"data":{"s":"BTCUSDT","k":{"i":%q,"x":%t,"T":%d,"o":"100","h":"102","l":"99","c":"101","v":"1"}}}`, interval, closed, at))
	}
	for _, item := range []struct {
		interval string
		at       int64
	}{{"1m", 60000}, {"5m", 300000}, {"1m", 120000}, {"5m", 600000}} {
		if err := m.message("BTCUSDT", frame(item.interval, item.at, true)); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.message("BTCUSDT", frame("5m", 900000, false)); err != nil {
		t.Fatal(err)
	}
	if err := m.message("BTCUSDT", frame("5m", 600000, true)); err != nil {
		t.Fatal(err)
	}
	s := m.Snapshot()
	if len(s.Candles) != 2 || len(s.ContextCandles) != 2 || s.Candles[1].CloseTime != 120000 || s.ContextCloseTime != 600000 {
		t.Fatal("windows mixed or unfinished/duplicate candle applied")
	}
	s.ContextCandles[0].Close = 999
	if m.Snapshot().ContextCandles[0].Close == 999 {
		t.Fatal("context snapshot aliases live data")
	}
	if err := m.message("BTCUSDT", frame("5m", 1200000, true)); err == nil {
		t.Fatal("context gap accepted")
	}
	if err := m.message("BTCUSDT", frame("15m", 900000, true)); err == nil {
		t.Fatal("unknown timeframe accepted")
	}
	f.SetPairs([]string{"BTCUSDT", "ETHUSDT"})
	if f.markets["ETHUSDT"].contextInterval != "5m" {
		t.Fatal("new feed lost context window")
	}
	f.SetPairs([]string{"ETHUSDT"})
	if len(m.Snapshot().ContextCandles) != 0 {
		t.Fatal("removed market retained context")
	}
}

func TestContextFeatureValidation(t *testing.T) {
	for _, name := range []string{"missing", "stale", "future", "gap", "invalid"} {
		t.Run(name, func(t *testing.T) {
			cs := fiveMinuteHistory()
			switch name {
			case "missing":
				cs = nil
			case "stale":
				for i := range cs {
					cs[i].CloseTime -= 360000
				}
			case "future":
				cs[len(cs)-1].CloseTime = time.Now().Add(time.Minute).UnixMilli()
			case "gap":
				cs[5].CloseTime++
			case "invalid":
				cs[5].High = math.NaN()
			}
			if _, err := contextFeatures(MarketSnapshot{ContextCandles: cs}, "5m"); err == nil {
				t.Fatal("bad context accepted")
			}
		})
	}
}

func TestOllayaReceivesBothTimeframesWithoutMandatoryContextTrendGate(t *testing.T) {
	a := testApp(t)
	a.cfg.SignalInterval, a.cfg.ContextInterval, a.cfg.EntryPolicy = "1m", "5m", "ollaya"
	m := a.feeds.markets["BTCUSDT"]
	m.connected, m.bid, m.ask, m.quoteAt = true, dec("100"), dec("100.01"), time.Now()
	m.candles, m.contextCandles = risingCandles(65), fiveMinuteHistory()
	// Make the context a downtrend while keeping valid positive OHLCV bars.
	for i := range m.contextCandles {
		price := 200 - float64(i)
		m.contextCandles[i].Open, m.contextCandles[i].Close = price, price
		m.contextCandles[i].Low, m.contextCandles[i].High = price-1, price+1
	}
	a.state.Pairs["BTCUSDT"].Paused = false
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request struct {
			State struct {
				SignalInterval string            `json:"signal_interval"`
				Eligible       bool              `json:"entry_eligible"`
				Features       strategy.Features `json:"features"`
				Context        struct {
					Interval string            `json:"interval"`
					Features strategy.Features `json:"features"`
				} `json:"market_context"`
			} `json:"state"`
			Questions map[string]struct {
				Instructions string `json:"instructions"`
			} `json:"questions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.State.SignalInterval != "1m" || request.State.Context.Interval != "5m" || !request.State.Features.Uptrend || !request.State.Context.Features.Reversal || !request.State.Eligible {
			t.Error("missing/mixed windows or mechanical context trend gate")
		}
		if !strings.Contains(request.Questions["action"].Instructions, "market_context.features") {
			t.Error("model not instructed to use context")
		}
		fmt.Fprint(w, `{"model":"winnow:e4b","done_reason":"decide","answers":{"action":{"type":"choice","choice":"HOLD","probabilities":{"HOLD":1,"ENTER_LONG":0,"EXIT_LONG":0}}}}`)
	}))
	defer s.Close()
	a.client, a.cfg.OllayaURL = s.Client(), s.URL
	a.decide(context.Background(), "BTCUSDT")
	if calls != 1 {
		t.Fatal("model not queried")
	}
	a.refreshMetrics()
	if performanceValue(t, a, "context_ready") != 1 {
		t.Fatal("context freshness not published")
	}
	m.contextCandles = nil
	a.decide(context.Background(), "BTCUSDT")
	if calls != 1 || a.snapshot().Evaluations["BTCUSDT"].Reason != "context_unavailable" {
		t.Fatal("missing context reached model")
	}
}

func TestContextConfigDefaultsAndRejectsUnsupportedInterval(t *testing.T) {
	t.Setenv("CONTROL_TOKEN", strings.Repeat("c", 32))
	t.Setenv("METRICS_TOKEN", strings.Repeat("m", 32))
	t.Setenv("CONTEXT_INTERVAL", "")
	c, err := config()
	if err != nil || c.SignalInterval != "1m" || c.ContextInterval != "5m" {
		t.Fatal("incorrect signal/context defaults", err)
	}
	t.Setenv("CONTEXT_INTERVAL", "15m")
	if _, err := config(); err == nil {
		t.Fatal("unsupported context interval accepted")
	}
}

func TestDualTimeframeFeedBootstrapAndSubscription(t *testing.T) {
	upgrader := websocket.Upgrader{}
	streamPath := make(chan string, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamPath <- r.URL.Query().Get("streams")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"data":{"s":"BTCUSDT","b":"100","a":"100.01"}}`)); err != nil {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	var mu sync.Mutex
	var intervals []string
	b := mockBinance(func(r *http.Request) (*http.Response, error) {
		interval := r.URL.Query().Get("interval")
		mu.Lock()
		intervals = append(intervals, interval)
		mu.Unlock()
		cs := risingCandles(65)
		if interval == "5m" {
			cs = fiveMinuteHistory()
		}
		raw := []any{}
		for _, c := range cs {
			raw = append(raw, []any{0, fmt.Sprint(c.Open), fmt.Sprint(c.High), fmt.Sprint(c.Low), fmt.Sprint(c.Close), fmt.Sprint(c.Volume), c.CloseTime})
		}
		data, _ := json.Marshal(raw)
		return response(string(data)), nil
	})
	b.interval = "1m"
	b.venue.StreamURL = "ws" + strings.TrimPrefix(s.URL, "http")
	f := NewFeeds([]string{"BTCUSDT"}, "1m", "5m")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); f.runPair(ctx, b, "BTCUSDT") }()
	defer func() { cancel(); <-done }()
	select {
	case path := <-streamPath:
		if path != "btcusdt@bookTicker/btcusdt@kline_1m/btcusdt@kline_5m" {
			t.Fatalf("wrong subscription %s", path)
		}
	case <-ctx.Done():
		t.Fatal("no subscription")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := f.Snapshot("BTCUSDT")
		if freshMarket(snapshot, "BTCUSDT") {
			if len(snapshot.Candles) != 65 || len(snapshot.ContextCandles) != 65 || !contextReady(snapshot, "5m") {
				t.Fatal("both windows not bootstrapped")
			}
			mu.Lock()
			got := strings.Join(intervals, ",")
			mu.Unlock()
			if got != "1m,5m" {
				t.Fatalf("REST bootstrap intervals = %s", got)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("feed not ready")
		case <-ticker.C:
		}
	}
}

func TestContextChangingDuringInferenceBlocksNewEntry(t *testing.T) {
	a := executionApp(t)
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("unexpected exchange request: %s", r.URL.Path)
	})
	a.cfg.SignalInterval, a.cfg.ContextInterval, a.cfg.EntryPolicy = "1m", "5m", "ollaya"
	m := a.feeds.markets["BTCUSDT"]
	m.ask = dec("100.01")
	m.candles, m.contextCandles = risingCandles(65), fiveMinuteHistory()
	for i := range m.contextCandles {
		m.contextCandles[i].CloseTime -= 300000
	}
	a.state.Pairs["BTCUSDT"].Paused = false
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		last := m.contextCandles[len(m.contextCandles)-1]
		last.CloseTime += 300000
		m.contextCandles = append(m.contextCandles, last)
		m.mu.Unlock()
		fmt.Fprint(w, `{"model":"winnow:e4b","done_reason":"decide","answers":{"action":{"type":"choice","choice":"ENTER_LONG","probabilities":{"HOLD":0,"ENTER_LONG":1,"EXIT_LONG":0}}}}`)
	}))
	defer s.Close()
	a.client, a.cfg.OllayaURL = s.Client(), s.URL
	a.decide(context.Background(), "BTCUSDT")
	snapshot := a.snapshot()
	if snapshot.Pairs["BTCUSDT"].Pending != nil || snapshot.Evaluations["BTCUSDT"].Reason != "context_changed" {
		t.Fatal("entry used obsolete context")
	}
}
