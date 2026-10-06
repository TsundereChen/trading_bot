package trader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"automated-trader/internal/strategy"
)

func TestFiveMinuteCandlesAndFreshness(t *testing.T) {
	cs := risingCandles(65)
	now := time.Now().UnixMilli()
	for i := range cs {
		cs[i].CloseTime = now - 180000 - int64(len(cs)-1-i)*300000
	}
	if _, err := features(cs, "5m"); err != nil {
		t.Fatal("valid five-minute history considered stale", err)
	}
	if _, err := features(cs, "1m"); err == nil {
		t.Fatal("one-minute freshness bound not enforced")
	}
	b := mockBinance(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("interval") != "5m" {
			t.Fatal("REST did not request five-minute bars")
		}
		raw := []any{}
		for _, c := range cs {
			raw = append(raw, []any{c.CloseTime - 299999, fmt.Sprint(c.Open), fmt.Sprint(c.High), fmt.Sprint(c.Low), fmt.Sprint(c.Close), fmt.Sprint(c.Volume), c.CloseTime})
		}
		data, _ := json.Marshal(raw)
		return response(string(data)), nil
	})
	b.interval = "5m"
	if got, err := b.Candles(context.Background(), "BTCUSDT", 100); err != nil || len(got) != len(cs) {
		t.Fatal("five-minute REST bars rejected", err)
	}
	m := &Market{pair: "BTCUSDT", interval: "5m"}
	candle := func(at int64, interval string, closed bool) []byte {
		return []byte(fmt.Sprintf(`{"data":{"s":"BTCUSDT","k":{"i":%q,"x":%t,"T":%d,"o":"100","h":"102","l":"99","c":"101","v":"1"}}}`, interval, closed, at))
	}
	for _, at := range []int64{300000, 600000} {
		if err := m.message("BTCUSDT", candle(at, "5m", true)); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.message("BTCUSDT", candle(900000, "5m", false)); err != nil || len(m.Snapshot().Candles) != 2 {
		t.Fatal("incomplete candle affected signals", err)
	}
	if err := m.message("BTCUSDT", candle(660000, "1m", true)); err == nil {
		t.Fatal("wrong stream interval accepted")
	}
	feeds := NewFeeds([]string{"BTCUSDT"}, "5m")
	feeds.SetPairs([]string{"BTCUSDT", "ETHUSDT"})
	if feeds.markets["ETHUSDT"].interval != "5m" {
		t.Fatal("added pair lost configured timeframe")
	}
}

func TestReentryRequiresNewPullback(t *testing.T) {
	a := testApp(t)
	p := a.snapshot().Pairs["BTCUSDT"]
	p.Paused, p.LastSetup = false, 123
	m := MarketSnapshot{Bid: dec("100"), Ask: dec("100.01")}
	f := strategy.Features{Uptrend: true, Pullback: true, SetupID: 123}
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != false {
		t.Fatal("consumed pullback allowed re-entry")
	}
	f.SetupID++
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != true {
		t.Fatal("new pullback did not permit re-entry")
	}
	f.Pullback = false
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != false {
		t.Fatal("new candle without pullback allowed entry")
	}
}

func TestModelEveryCheckIncludingSameCandleFailureAndRestart(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			a := testApp(t)
			a.cfg.SignalInterval = "5m"
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if fail {
					w.WriteHeader(503)
					return
				}
				fmt.Fprint(w, `{"model":"winnow:e4b","done_reason":"decide","answers":{"action":{"type":"choice","choice":"HOLD","probabilities":{"HOLD":1,"ENTER_LONG":0,"EXIT_LONG":0}}}}`)
			}))
			defer s.Close()
			a.client, a.cfg.OllayaURL = s.Client(), s.URL
			m := a.feeds.markets["BTCUSDT"]
			m.connected, m.bid, m.ask, m.quoteAt = true, dec("100"), dec("100.01"), time.Now()
			m.candles = risingCandles(65)
			for i := range m.candles {
				m.candles[i].CloseTime = time.Now().UnixMilli() - 1000 - int64(len(m.candles)-1-i)*300000
			}
			a.decide(context.Background(), "BTCUSDT")
			a.decide(context.Background(), "BTCUSDT")
			if calls != 2 {
				t.Fatal("model was not consulted again on the same candle", calls)
			}
			loaded, err := a.repo.Load(context.Background())
			if err != nil || loaded.Pairs["BTCUSDT"].LastDecisionCandle != m.candles[len(m.candles)-1].CloseTime {
				t.Fatal("diagnostic candle timestamp not durable", err)
			}
			a.state = loaded
			a.decide(context.Background(), "BTCUSDT")
			if calls != 3 {
				t.Fatal("reloaded state suppressed model request")
			}
			m.candles[len(m.candles)-1].CloseTime++
			a.decide(context.Background(), "BTCUSDT")
			if calls != 4 {
				t.Fatal("new candle did not trigger model")
			}
		})
	}
}
