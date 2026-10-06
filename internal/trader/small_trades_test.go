package trader

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"automated-trader/internal/strategy"
	dto "github.com/prometheus/client_model/go"
	"github.com/shopspring/decimal"
)

func TestOllayaPolicyKeepsSafetyWithoutTechnicalGate(t *testing.T) {
	a := testApp(t)
	a.cfg.EntryPolicy = "ollaya"
	p := a.snapshot().Pairs["BTCUSDT"]
	p.Paused, p.LastSetup = false, 123
	m := MarketSnapshot{Bid: dec("100"), Ask: dec("100.01")}
	f := strategy.Features{SetupID: 123}
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != true {
		t.Fatal("technical gate still blocks model-led entry")
	}
	p.CooldownUntil = time.Now().Add(time.Second)
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != false {
		t.Fatal("cooldown bypassed")
	}
	p.CooldownUntil = time.Time{}
	p.Pending = &Pending{ID: "uncertain"}
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != false {
		t.Fatal("uncertain order bypassed")
	}
	p.Pending = nil
	p.Paused = true
	if a.decisionSnapshot("BTCUSDT", p, f, m)["entry_eligible"] != false {
		t.Fatal("pause bypassed")
	}
}

func TestDustLedgerAndSmallPositionCap(t *testing.T) {
	a := executionApp(t)
	a.cfg.MaxPosition, a.cfg.RiskPerTrade = dec("20"), dec("0.1")
	symbol := a.symbols["BTCUSDT"]
	symbol.Step = dec("0.01")
	a.symbols["BTCUSDT"] = symbol
	p := a.state.Pairs["BTCUSDT"]
	p.Qty, p.Cost = dec("0.001"), dec("0.1")
	before := p.equity(dec("100"))
	if !retainDust(p, symbol) || !p.Qty.IsZero() || !p.DustQty.Equal(dec("0.001")) || !p.equity(dec("100")).Equal(before) {
		t.Fatal("dust accounting lost assets or equity")
	}
	if removablePairs(a.state, []string{"ETHUSDT"}) == nil {
		t.Fatal("dust ledger silently removable")
	}
	p.Stop = dec("99.9")
	if err := a.checkEntryAllocation(p, dec("0.2"), MarketSnapshot{Bid: dec("100"), Ask: dec("100")}, dec("1000")); err == nil {
		t.Fatal("dust exposure ignored by position cap")
	}
	p.Pending = &Pending{ID: "buy", Side: "BUY", Qty: dec("0.1")}
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) { return fillResponse(1, "0.1", "10", "0", "USDT"), nil })
	if err := a.applyOrder(context.Background(), "BTCUSDT", Order{OrderID: 1, ClientID: "buy", Status: "FILLED", Executed: "0.1", Quote: "10"}, false); err != nil {
		t.Fatal(err)
	}
	p = a.snapshot().Pairs["BTCUSDT"]
	if !p.DustQty.IsZero() || !p.DustCost.IsZero() || !p.Qty.Equal(dec("0.101")) || !p.Cost.Equal(dec("10.1")) {
		t.Fatal("filled buy did not incorporate tracked dust")
	}
}

func TestUSDCFeeValuationAndMixedPortfolio(t *testing.T) {
	a := testApp(t)
	parsed, err := parsePairs("BTCUSDT,BTCUSDC,ETHUSDT,ETHUSDC")
	if err != nil || len(parsed) != 4 {
		t.Fatal("four quote pairs rejected", err)
	}
	a.symbols["BTCUSDC"] = Symbol{Base: "BTC", Quote: "USDC"}
	a.state.Pairs["BTCUSDC"] = &Position{Pair: "BTCUSDC", Cash: dec("1000"), UnvaluedFees: map[string]decimal.Decimal{"BNB": dec("0.01")}}
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("symbol") != "BNBUSDC" {
			t.Fatal("third-asset fee valued in wrong quote currency")
		}
		return response(`{"askPrice":"600","bidPrice":"599"}`), nil
	})
	if err := a.valueExternalFees(context.Background(), "BTCUSDC"); err != nil {
		t.Fatal(err)
	}
	if !a.snapshot().Pairs["BTCUSDC"].ExternalFeeQuote.Equal(dec("6")) {
		t.Fatal("USDC fee accounting incorrect")
	}
	a.refreshMetrics()
	metric := &dto.Metric{}
	if err := a.metrics.PortfolioEquity.Write(metric); err != nil || !math.IsNaN(metric.GetGauge().GetValue()) {
		t.Fatal("mixed quote currencies falsely summed", err)
	}
}

func TestGlobalModelSchedulerRotatesWithoutBurst(t *testing.T) {
	a := testApp(t)
	a.cfg.DecisionSeconds = 1
	a.feeds = NewFeeds(nil) // No network feed supervisors in this test.
	for _, pair := range []string{"BTCUSDT", "ETHUSDC"} {
		a.state.Pairs[pair] = &Position{Pair: pair, Paused: true}
		a.feeds.markets[pair] = &Market{pair: pair, connected: true, bid: dec("100"), ask: dec("100.01"), quoteAt: time.Now(), candles: risingCandles(65)}
	}
	calls := make(chan time.Time, 2)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls <- time.Now()
		fmt.Fprint(w, `{"model":"winnow:e4b","done_reason":"decide","answers":{"action":{"type":"choice","choice":"HOLD","probabilities":{"HOLD":1,"ENTER_LONG":0,"EXIT_LONG":0}}}}`)
	}))
	defer s.Close()
	a.client, a.cfg.OllayaURL = s.Client(), s.URL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	var times []time.Time
	for len(times) < 2 {
		select {
		case at := <-calls:
			times = append(times, at)
		case <-time.After(4 * time.Second):
			t.Fatal("scheduler failed to consult both pairs")
		}
	}
	cancel()
	<-done
	if times[1].Sub(times[0]) < 900*time.Millisecond {
		t.Fatal("model queries burst instead of global spacing")
	}
}

func TestDecisionTimingConfiguration(t *testing.T) {
	t.Setenv("CONTROL_TOKEN", strings.Repeat("c", 32))
	t.Setenv("METRICS_TOKEN", strings.Repeat("m", 32))
	t.Setenv("ENABLE_TRADING", "false")
	t.Setenv("BINANCE_BASE_URL", "")
	t.Setenv("ENTRY_POLICY", "ollaya")
	t.Setenv("DECISION_INTERVAL_SECONDS", "30")
	t.Setenv("OLLAYA_TIMEOUT_SECONDS", "20")
	t.Setenv("REENTRY_COOLDOWN_SECONDS", "10")
	c, err := config()
	if err != nil || c.decisionSeconds() != 30 || c.modelTimeout() != 20*time.Second || c.cooldown() != 10*time.Second {
		t.Fatal("timing configuration incorrect", err)
	}
	t.Setenv("OLLAYA_TIMEOUT_SECONDS", "30")
	if _, err := config(); err == nil {
		t.Fatal("timeout exceeding cadence accepted")
	}
	t.Setenv("OLLAYA_TIMEOUT_SECONDS", "20")
	t.Setenv("METRICS_TOKEN", strings.Repeat("c", 32))
	if _, err := config(); err == nil {
		t.Fatal("exporter credentials could also control trading")
	}
}
