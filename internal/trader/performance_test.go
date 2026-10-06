package trader

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func performanceValue(t *testing.T, a *App, name string) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := a.metrics.Performance[name].WithLabelValues("BTCUSDT", "USDT").Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func TestPerformancePartialFillsFeesPersistenceAndNoDoubleCounting(t *testing.T) {
	a := testApp(t)
	var qty, quote, fee string
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) { return fillResponse(1, qty, quote, fee, "USDT"), nil })
	apply := func(side, status string, q, c, f string) {
		t.Helper()
		qty, quote, fee = q, c, f
		if a.snapshot().Pairs["BTCUSDT"].Pending == nil {
			if err := a.update(context.Background(), "intent", nil, func(s State) error {
				s.Pairs["BTCUSDT"].Pending = &Pending{ID: side, Side: side, Qty: dec("1")}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := a.applyOrder(context.Background(), "BTCUSDT", Order{OrderID: 1, ClientID: side, Status: status, Executed: q, Quote: c}, false); err != nil {
			t.Fatal(err)
		}
	}
	apply("BUY", "PARTIALLY_FILLED", "0.5", "50", "0.05")
	apply("BUY", "PARTIALLY_FILLED", "0.5", "50", "0.05")
	if !a.snapshot().Pairs["BTCUSDT"].Performance.QuoteFees.Equal(dec("0.05")) {
		t.Fatal("duplicate fills counted fees twice")
	}
	apply("BUY", "FILLED", "1", "100", "0.1")
	if err := a.update(context.Background(), "mark", nil, func(s State) error { markPerformance(s.Pairs["BTCUSDT"], dec("80")); return nil }); err != nil {
		t.Fatal(err)
	}
	apply("SELL", "PARTIALLY_FILLED", "0.5", "60", "0.06")
	if !a.snapshot().Pairs["BTCUSDT"].Performance.RealizedExecution.Equal(dec("9.89")) {
		t.Fatal("partial realized cost basis incorrect")
	}
	apply("SELL", "FILLED", "1", "120", "0.12")
	p := a.snapshot().Pairs["BTCUSDT"]
	e := p.Performance
	if e.Completed != 1 || e.Wins != 1 || !e.RealizedExecution.Equal(dec("19.78")) || !e.QuoteFees.Equal(dec("0.22")) || !p.Cash.Equal(dec("1019.78")) {
		t.Fatalf("incorrect net trade ledger: %+v", e)
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Performance.Completed != 1 {
		t.Fatal("performance did not persist", err)
	}
	a.refreshMetrics()
	if math.Abs(performanceValue(t, a, "net_pnl_quote")-19.78) > 1e-9 || math.Abs(performanceValue(t, a, "fees_paid_quote_estimate")-.22) > 1e-9 {
		t.Fatal("fees omitted or subtracted twice from PnL")
	}
	if math.Abs(performanceValue(t, a, "max_drawdown_quote")-20.1) > 1e-9 {
		t.Fatal("drawdown incorrect")
	}
	copy := a.snapshot()
	copy.Pairs["BTCUSDT"].Performance.Fees["USDT"] = dec("999")
	if a.snapshot().Pairs["BTCUSDT"].Performance.Fees["USDT"].Equal(dec("999")) {
		t.Fatal("performance snapshot aliases live fee map")
	}
}

func TestPerformanceDefersTradeResultUntilThirdAssetFeesValued(t *testing.T) {
	a := testApp(t)
	side := "BUY"
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/v3/ticker/bookTicker" {
			return response(`{"bidPrice":"9","askPrice":"10"}`), nil
		}
		if side == "BUY" {
			return fillResponse(1, "1", "100", "0.01", "BNB"), nil
		}
		return fillResponse(1, "1", "110", "0.02", "BNB"), nil
	})
	for _, direction := range []string{"BUY", "SELL"} {
		side = direction
		if err := a.update(context.Background(), "intent", nil, func(s State) error {
			s.Pairs["BTCUSDT"].Pending = &Pending{ID: side, Side: side, Qty: dec("1")}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		quote := "100"
		if side == "SELL" {
			quote = "110"
		}
		if err := a.applyOrder(context.Background(), "BTCUSDT", Order{OrderID: 1, ClientID: side, Status: "FILLED", Executed: "1", Quote: quote}, false); err != nil {
			t.Fatal(err)
		}
		a.refreshMetrics()
		if !math.IsNaN(performanceValue(t, a, "realized_pnl_quote")) {
			t.Fatal("unvalued fees reported as exact net results")
		}
		if a.snapshot().Pairs["BTCUSDT"].Performance.Completed != 0 {
			t.Fatal("trade finalized before commission valuation")
		}
		if err := a.valueExternalFees(context.Background(), "BTCUSDT"); err != nil {
			t.Fatal(err)
		}
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if p.Performance.Completed != 1 || !p.Performance.WinningPnL.Equal(dec("9.7")) {
		t.Fatal("third-asset fees missing from completed trade PnL")
	}
	a.refreshMetrics()
	if math.Abs(performanceValue(t, a, "realized_pnl_quote")-9.7) > 1e-9 {
		t.Fatal("net realized fee accounting incorrect")
	}
}

func TestLegacyTradeNotFabricatedAndMetadataDoesNotInvalidateExecution(t *testing.T) {
	a := testApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Cash, p.Qty, p.Cost = dec("900"), dec("1"), dec("100")
	p.Pending = &Pending{ID: "sell", Side: "SELL", Qty: dec("1")}
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) { return fillResponse(1, "1", "110", "0", "USDT"), nil })
	if err := a.applyOrder(context.Background(), "BTCUSDT", Order{OrderID: 1, ClientID: "sell", Status: "FILLED", Executed: "1", Quote: "110"}, false); err != nil {
		t.Fatal(err)
	}
	p = a.snapshot().Pairs["BTCUSDT"]
	if p.Performance.Completed != 0 || p.Performance.Excluded != 1 {
		t.Fatal("legacy round-trip history invented")
	}
	version := p.Version
	if err := a.update(context.Background(), "mark", nil, func(s State) error { markPerformance(s.Pairs["BTCUSDT"], dec("110")); return nil }); err != nil {
		t.Fatal(err)
	}
	if a.snapshot().Pairs["BTCUSDT"].Version != version {
		t.Fatal("analytics invalidated in-flight execution")
	}
	a.refreshMetrics()
	if !math.IsNaN(performanceValue(t, a, "trade_win_rate")) {
		t.Fatal("win rate fabricated before known completed cycles")
	}
}

func TestInheritedExposureDrawdownStartsAtObservedEquity(t *testing.T) {
	p := &Position{Cash: dec("900"), Qty: dec("1"), Cost: dec("100")}
	ensurePerformance(p)
	markPerformance(p, dec("80"))
	if !p.Performance.PeakEquity.Equal(dec("980")) || !p.Performance.MaxDrawdown.IsZero() {
		t.Fatal("invented a historical mark-to-market peak")
	}
	markPerformance(p, dec("70"))
	if !p.Performance.MaxDrawdown.Equal(dec("10")) {
		t.Fatal("fresh drawdown not recorded")
	}
}

func TestEvaluationDurableLinkAndSkippedModelPreservation(t *testing.T) {
	a := testApp(t)
	version := a.snapshot().Pairs["BTCUSDT"].Version
	modelAt := time.Now().UTC()
	a.recordEvaluation("BTCUSDT", Evaluation{At: modelAt, ModelAt: modelAt, Action: "ENTER_LONG", Latency: 1.2, Outcome: "uncertain", Reason: "submission_uncertain", OrderClientID: "client-1", OrderUncertain: true})
	s, err := a.repo.Load(context.Background())
	if err != nil || s.Evaluations["BTCUSDT"].OrderClientID != "client-1" || !s.Evaluations["BTCUSDT"].OrderUncertain {
		t.Fatal("decision/order linkage not persisted", err)
	}
	a.refreshMetrics()
	if performanceValue(t, a, "last_decision_order_uncertain") != 1 {
		t.Fatal("uncertainty missing from metrics")
	}
	a.recordEvaluation("BTCUSDT", Evaluation{At: time.Now().UTC(), Outcome: "skipped", Reason: "stale_quote"})
	e := a.snapshot().Evaluations["BTCUSDT"]
	if !e.ModelAt.Equal(modelAt) || e.Action != "ENTER_LONG" || e.OrderClientID != "" || e.OrderUncertain {
		t.Fatal("skip mixed current outcome with previous order")
	}
	if a.snapshot().Pairs["BTCUSDT"].Version != version {
		t.Fatal("evaluation invalidated execution state")
	}
	a.refreshMetrics()
	if performanceValue(t, a, "last_decision_order_uncertain") != 0 {
		t.Fatal("old uncertain order still attributed to current evaluation")
	}
	metric := &dto.Metric{}
	if err := a.metrics.NoTrades.WithLabelValues("BTCUSDT", "stale_quote").Write(metric); err != nil || metric.GetCounter().GetValue() != 1 {
		t.Fatal("skip counter missing", err)
	}
}
