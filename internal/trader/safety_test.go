package trader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"automated-trader/internal/strategy"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/shopspring/decimal"
)

func mockBinance(fn roundTrip) *Binance {
	return &Binance{venue: demo(), key: "mock", secret: "mock", accountID: "42", client: &http.Client{Timeout: time.Second, Transport: fn}}
}

func executionApp(t *testing.T) *App {
	a := testApp(t)
	a.cfg.Trading = true
	p := a.state.Pairs["BTCUSDT"]
	p.Paused, p.Stop, p.Target = false, dec("99"), dec("120")
	return a
}

func orderResponse(orderID int64, id, status, qty, quote string) *http.Response {
	return response(fmt.Sprintf(`{"orderId":%d,"clientOrderId":%q,"status":%q,"executedQty":%q,"cummulativeQuoteQty":%q}`, orderID, id, status, qty, quote))
}

func fillResponse(orderID int64, qty, quote, fee, asset string) *http.Response {
	return response(fmt.Sprintf(`[{"id":1,"orderId":%d,"qty":%q,"quoteQty":%q,"commission":%q,"commissionAsset":%q}]`, orderID, qty, quote, fee, asset))
}

func accountResponse() *http.Response {
	return response(`{"uid":42,"canTrade":true,"canWithdraw":true,"balances":[{"asset":"USDT","free":"10000","locked":"0"},{"asset":"BTC","free":"10","locked":"0"},{"asset":"ETH","free":"10","locked":"0"}]}`)
}

func postControl(a *App, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/api/control", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
	w := httptest.NewRecorder()
	a.routes(prometheus.NewRegistry()).ServeHTTP(w, r)
	return w
}

func TestStateBindingRejectsVenueAccountAndUnverifiedLegacy(t *testing.T) {
	s := State{Venue: "demo", AccountID: "42", Pairs: map[string]*Position{"BTCUSDT": {Pair: "BTCUSDT", Qty: dec("1")}}}
	for _, args := range [][3]string{{"live", "42", "live:42"}, {"demo", "43", "demo:43"}} {
		if _, err := bindState(s, args[0], args[1], args[2]); err == nil {
			t.Fatal("known identity mismatch accepted")
		}
	}
	legacy := s.copy()
	legacy.Venue, legacy.AccountID = "", ""
	if _, err := bindState(legacy, "demo", "42", ""); err == nil {
		t.Fatal("legacy exposure adopted without attestation")
	}
	bound, err := bindState(legacy, "demo", "42", "demo:42")
	if err != nil || bound.Venue != "demo" || bound.AccountID != "42" || !bound.Pairs["BTCUSDT"].Qty.Equal(dec("1")) {
		t.Fatal("explicit legacy binding lost state", err)
	}
	observed, err := bindState(s, "demo", "", "")
	if err != nil || observed.AccountID != "42" {
		t.Fatal("observe-only startup erased identity", err)
	}
	newState, err := bindState(State{}, "demo", "42", "")
	if err != nil || newState.AccountID != "42" {
		t.Fatal(err)
	}
}

func TestWrongIdentityNeverSubmitsEvenProtectiveExit(t *testing.T) {
	for _, mismatch := range []string{"venue", "account", "client"} {
		t.Run(mismatch, func(t *testing.T) {
			a := executionApp(t)
			p := a.state.Pairs["BTCUSDT"]
			p.Qty, p.Stop = dec("1"), dec("110")
			a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
				t.Error("unverified identity reached exchange")
				return nil, errors.New("unexpected")
			})
			switch mismatch {
			case "venue":
				a.state.Venue = "live"
			case "account":
				a.state.AccountID = "43"
			case "client":
				a.binance.venue, _ = resolveVenue(liveBaseURL)
			}
			a.poll(context.Background())
			if err := a.submitLocked(context.Background(), "BTCUSDT", "SELL", dec("1"), "test"); err == nil {
				t.Fatal("unverified execution accepted")
			}
		})
	}
}

func TestSQLiteSingletonAliasesAndRestart(t *testing.T) {
	path := t.TempDir() + "/shared.sqlite"
	first, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	s := State{Venue: "demo", AccountID: "42", Pairs: map[string]*Position{"BTCUSDT": {Pair: "BTCUSDT", Pending: &Pending{ID: "durable", Side: "BUY", Qty: dec("1")}}}}
	if err := first.Commit(context.Background(), s, "intent", nil); err != nil {
		t.Fatal(err)
	}
	if second, err := OpenSQLite(path); err == nil {
		second.Close()
		t.Fatal("second SQLite owner accepted")
	}
	alias := t.TempDir() + "/alias.sqlite"
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	if second, err := OpenSQLite(alias); err == nil {
		second.Close()
		t.Fatal("symlink bypassed singleton lock")
	}
	for _, dsn := range []string{"file:" + path, path + "?_pragma=busy_timeout(5000)"} {
		if second, err := OpenSQLite(dsn); err == nil {
			second.Close()
			t.Fatal("driver DSN bypassed singleton lock")
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	loaded, err := restarted.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Pending.ID != "durable" {
		t.Fatal("restart lost intent", err)
	}
}

func TestStartAllFailureIsAtomicAndAuditedOnlyOnSuccess(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["ETHUSDT"] = &Position{Pair: "ETHUSDT", Paused: true, Cash: dec("1000")}
	a.feeds.SetPairs([]string{"BTCUSDT", "ETHUSDT"})
	if err := a.commit(context.Background(), a.state, "initial", nil); err != nil {
		t.Fatal(err)
	}
	w := postControl(a, `{"action":"start"}`)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, p := range a.snapshot().Pairs {
		if !p.Paused {
			t.Fatal("failed start partially enabled a pair")
		}
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || !loaded.Pairs["BTCUSDT"].Paused {
		t.Fatal("failed start changed durable state", err)
	}
	events, _ := a.repo.Events(context.Background())
	if len(events) != 1 {
		t.Fatal("failed validation added an event")
	}
}

func TestSlowExchangeDoesNotBlockStatusPauseOrOtherPair(t *testing.T) {
	a := executionApp(t)
	a.state.Pairs["ETHUSDT"] = &Position{Pair: "ETHUSDT", Paused: true, Cash: dec("900"), Qty: dec("1"), Cost: dec("100"), Stop: dec("110"), Target: dec("120")}
	a.feeds.SetPairs([]string{"BTCUSDT", "ETHUSDT"})
	a.feeds.markets["ETHUSDT"].bid, a.feeds.markets["ETHUSDT"].ask, a.feeds.markets["ETHUSDT"].quoteAt, a.feeds.markets["ETHUSDT"].connected = dec("100"), dec("101"), time.Now(), true
	symbol := a.symbols["BTCUSDT"]
	symbol.Symbol, symbol.Base = "ETHUSDT", "ETH"
	a.symbols["ETHUSDT"] = symbol
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var accounts, ethSells atomic.Int32
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			if accounts.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			return accountResponse(), nil
		case "/api/v3/order":
			if r.URL.Query().Get("symbol") != "ETHUSDT" {
				return nil, errors.New("unexpected BTC order")
			}
			ethSells.Add(1)
			return orderResponse(1, r.URL.Query().Get("newClientOrderId"), "FILLED", "1", "100"), nil
		case "/api/v3/myTrades":
			return fillResponse(1, "1", "100", "0", "USDT"), nil
		}
		return nil, fmt.Errorf("unexpected request %s", r.URL.Path)
	})
	go func() { done <- a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("preflight did not start")
	}
	defer func() { close(release); <-done }()
	statusDone := make(chan int, 1)
	go func() {
		r := httptest.NewRequest("GET", "/api/status", nil)
		r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
		w := httptest.NewRecorder()
		a.routes(prometheus.NewRegistry()).ServeHTTP(w, r)
		statusDone <- w.Code
	}()
	select {
	case code := <-statusDone:
		if code != 200 {
			t.Fatal(code)
		}
	case <-time.After(time.Second):
		t.Fatal("status blocked by exchange I/O")
	}
	if w := postControl(a, `{"action":"pause","pair":"BTCUSDT"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	a.poll(context.Background())
	if ethSells.Load() != 1 || !a.snapshot().Pairs["ETHUSDT"].Qty.IsZero() {
		t.Fatal("other pair's protective exit was blocked")
	}
}

func TestQuoteBecomingStaleDuringBalancesNeverSubmits(t *testing.T) {
	a := executionApp(t)
	orders := 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/v3/account" {
			m := a.feeds.markets["BTCUSDT"]
			m.mu.Lock()
			m.quoteAt = time.Now().Add(-10 * time.Second)
			m.mu.Unlock()
			return accountResponse(), nil
		}
		orders++
		return nil, errors.New("unexpected order")
	})
	if err := a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test"); err == nil {
		t.Fatal("stale quote accepted")
	}
	if orders != 0 || a.snapshot().Pairs["BTCUSDT"].Pending != nil {
		t.Fatal("stale quote produced an intent or submission")
	}
}

func TestRejectionsResolveButUncertainOrdersNeverRetry(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			a := executionApp(t)
			posts := 0
			a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/api/v3/account" {
					return accountResponse(), nil
				}
				if r.Method == "POST" {
					posts++
					if uncertain {
						return nil, errors.New("mock timeout")
					}
					resp := response(`{"code":-1013,"msg":"Filter failure: NOTIONAL"}`)
					resp.StatusCode = 400
					return resp, nil
				}
				resp := response(`{"code":-2013,"msg":"Order does not exist."}`)
				resp.StatusCode = 400
				return resp, nil
			})
			if err := a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test"); err == nil {
				t.Fatal("expected failed submission")
			}
			if (a.snapshot().Pairs["BTCUSDT"].Pending != nil) != uncertain {
				t.Fatal("wrong intent resolution")
			}
			a.poll(context.Background())
			a.poll(context.Background())
			if uncertain {
				_ = a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test")
			}
			if posts != 1 {
				t.Fatal("submission retried")
			}
		})
	}
}

func TestIncompleteFillsCannotFinalizeOrder(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["BTCUSDT"].Pending = &Pending{ID: "buy", Side: "BUY", Qty: dec("1")}
	complete := false
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
		if complete {
			return fillResponse(1, "1", "100", "0.001", "BTC"), nil
		}
		return fillResponse(1, "0.5", "50", "0.0005", "BTC"), nil
	})
	order := Order{OrderID: 1, ClientID: "buy", Status: "FILLED", Executed: "1", Quote: "100"}
	if err := a.applyOrder(context.Background(), "BTCUSDT", order, false); err == nil {
		t.Fatal("incomplete fills accepted")
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if p.Pending == nil || !p.Qty.IsZero() || !p.Cash.Equal(dec("1000")) {
		t.Fatal("incomplete fills changed accounting")
	}
	complete = true
	if err := a.applyOrder(context.Background(), "BTCUSDT", order, false); err != nil {
		t.Fatal(err)
	}
	p = a.snapshot().Pairs["BTCUSDT"]
	if p.Pending != nil || !p.Qty.Equal(dec("0.999")) {
		t.Fatal("complete fills did not finalize correctly")
	}
}

func TestBNBFeeProtectsPrincipalBeforeValuationAndSurvivesRestart(t *testing.T) {
	a := executionApp(t)
	bookDown := true
	stopPosts := 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			return accountResponse(), nil
		case "/api/v3/order":
			if r.URL.Query().Get("type") == "STOP_LOSS" {
				stopPosts++
				return orderResponse(2, r.URL.Query().Get("newClientOrderId"), "NEW", "0", "0"), nil
			}
			return orderResponse(1, r.URL.Query().Get("newClientOrderId"), "FILLED", "0.9", "90"), nil
		case "/api/v3/myTrades":
			return fillResponse(1, "0.9", "90", "0.001", "BNB"), nil
		case "/api/v3/ticker/bookTicker":
			if bookDown {
				return nil, errors.New("valuation temporarily unavailable")
			}
			return response(`{"bidPrice":"599","askPrice":"600"}`), nil
		}
		return nil, fmt.Errorf("unexpected request %s", r.URL.Path)
	})
	if err := a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test"); err == nil {
		t.Fatal("expected valuation failure")
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if p.Pending != nil || p.Protection == nil || !p.Qty.Equal(dec("0.9")) || !p.UnvaluedFees["BNB"].Equal(dec("0.001")) {
		t.Fatal("BNB fee left principal untracked or unprotected")
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Protection.OrderID != 2 {
		t.Fatal("native stop not durable", err)
	}
	a.refreshMetrics()
	metric := new(dto.Metric)
	_ = a.metrics.Equity.WithLabelValues("BTCUSDT").Write(metric)
	if !math.IsNaN(metric.GetGauge().GetValue()) {
		t.Fatal("unvalued fees presented as complete equity")
	}
	bookDown = false
	if err := a.valueExternalFees(context.Background(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if err := a.valueExternalFees(context.Background(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	p = a.snapshot().Pairs["BTCUSDT"]
	if len(p.UnvaluedFees) != 0 || !p.ExternalFeeQuote.Equal(dec("0.6")) || !p.equity(dec("100")).Equal(dec("999.4")) || stopPosts != 1 {
		t.Fatal("external fee valuation duplicated or protection resubmitted")
	}
}

func TestNativeStopCancelFillRaceNeverDoubleSells(t *testing.T) {
	for _, caseName := range []string{"cancel_filled", "cancel_timeout_then_filled", "incomplete_fill_history"} {
		t.Run(caseName, func(t *testing.T) {
			a := executionApp(t)
			p := a.state.Pairs["BTCUSDT"]
			p.Qty, p.Cost, p.Cash, p.Protection = dec("1"), dec("100"), dec("900"), &Pending{ID: "stop", OrderID: 2, Side: "SELL", Qty: dec("1")}
			gets, posts := 0, 0
			a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/api/v3/order":
					if r.Method == "POST" {
						posts++
						return nil, errors.New("double sell")
					}
					if r.Method == "GET" {
						gets++
						if gets == 1 {
							return orderResponse(2, "stop", "NEW", "0", "0"), nil
						}
						return orderResponse(2, "cancel-id", "FILLED", "1", "95"), nil
					}
					if caseName == "cancel_timeout_then_filled" {
						return nil, errors.New("mock cancellation timeout")
					}
					return orderResponse(2, "cancel-id", "FILLED", "1", "95"), nil
				case "/api/v3/myTrades":
					if caseName == "incomplete_fill_history" {
						return fillResponse(2, "0.5", "47.5", "0.0475", "USDT"), nil
					}
					return fillResponse(2, "1", "95", "0.095", "USDT"), nil
				}
				return nil, fmt.Errorf("unexpected request %s", r.URL.Path)
			})
			err := a.closeUnderGate(context.Background(), "BTCUSDT", "test")
			p = a.snapshot().Pairs["BTCUSDT"]
			if posts != 0 {
				t.Fatal("duplicate market sell sent")
			}
			if caseName == "incomplete_fill_history" {
				if err == nil || p.Protection == nil || !p.Qty.Equal(dec("1")) {
					t.Fatal("uncertain fills discarded")
				}
				return
			}
			if err != nil || !p.Qty.IsZero() || p.Protection != nil || !p.Cash.Equal(dec("994.905")) {
				t.Fatal("cancel/fill race accounting failed", err)
			}
		})
	}
}

func TestStopCancellationRestoresProtectionWhenMarketExitCannotSend(t *testing.T) {
	a := executionApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Qty, p.Cost, p.Cash, p.Protection = dec("1"), dec("100"), dec("900"), &Pending{ID: "stop", OrderID: 2, Side: "SELL", Qty: dec("1")}
	stops, markets := 0, 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			m := a.feeds.markets["BTCUSDT"]
			m.mu.Lock()
			m.quoteAt = time.Now().Add(-time.Minute)
			m.mu.Unlock()
			return accountResponse(), nil
		case "/api/v3/order":
			if r.Method == "GET" {
				return orderResponse(2, "stop", "NEW", "0", "0"), nil
			}
			if r.Method == "DELETE" {
				return orderResponse(2, "cancel-id", "CANCELED", "0", "0"), nil
			}
			if r.URL.Query().Get("type") == "STOP_LOSS" {
				stops++
				return orderResponse(3, r.URL.Query().Get("newClientOrderId"), "NEW", "0", "0"), nil
			}
			markets++
			return nil, errors.New("unexpected market order")
		}
		return nil, fmt.Errorf("unexpected request %s", r.URL.Path)
	})
	if err := a.closeUnderGate(context.Background(), "BTCUSDT", "test"); err == nil {
		t.Fatal("expected stale-feed exit failure")
	}
	if stops != 1 || markets != 0 || a.snapshot().Pairs["BTCUSDT"].Protection == nil {
		t.Fatal("canceled stop not restored")
	}
}

func TestFeePaginationAndIdentityValidation(t *testing.T) {
	var firstPage []map[string]any
	for id := 1; id <= 1000; id++ {
		firstPage = append(firstPage, map[string]any{"id": id, "orderId": 7, "qty": "0.001", "quoteQty": "0.1", "commission": "0.000001", "commissionAsset": "BTC"})
	}
	data, _ := json.Marshal(firstPage)
	pages := 0
	b := mockBinance(func(r *http.Request) (*http.Response, error) {
		pages++
		if pages == 1 {
			if r.URL.Query().Get("fromId") != "0" {
				t.Error("first fill page must explicitly start at zero")
			}
			return response(string(data)), nil
		}
		if r.URL.Query().Get("fromId") != "1001" {
			t.Error("pagination cursor missing")
		}
		return response(`[{"id":1001,"orderId":7,"qty":"0.001","quoteQty":"0.1","commission":"0.000001","commissionAsset":"BTC"}]`), nil
	})
	fills, err := b.Fills(context.Background(), "BTCUSDT", 7)
	if err != nil || pages != 2 || !fills.Qty.Equal(dec("1.001")) || !fills.Quote.Equal(dec("100.1")) || !fills.Fees["BTC"].Equal(dec("0.001001")) {
		t.Fatal("pagination lost fills", err)
	}
	b = mockBinance(func(*http.Request) (*http.Response, error) { return fillResponse(8, "1", "100", "0", "USDT"), nil })
	if _, err := b.Fills(context.Background(), "BTCUSDT", 7); err == nil {
		t.Fatal("wrong-order fills accepted")
	}
}

func safePermissions() map[string]bool {
	return map[string]bool{"enableReading": true, "enableSpotAndMarginTrading": true, "ipRestrict": true, "enableWithdrawals": false, "enableInternalTransfer": false, "permitsUniversalTransfer": false, "enableMargin": false, "enableFutures": false, "enableVanillaOptions": false}
}

func TestLiveKeyPermissionsNotAccountWithdrawalCapability(t *testing.T) {
	for _, flag := range []string{"safe", "enableWithdrawals", "enableInternalTransfer", "permitsUniversalTransfer", "enableMargin", "enableFutures", "enableVanillaOptions", "missing"} {
		t.Run(flag, func(t *testing.T) {
			permissions := safePermissions()
			if flag == "missing" {
				delete(permissions, "enableWithdrawals")
			} else if flag != "safe" {
				permissions[flag] = true
			}
			data, _ := json.Marshal(permissions)
			b := mockBinance(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/sapi/v1/account/apiRestrictions" {
					return response(string(data)), nil
				}
				return accountResponse(), nil
			})
			b.venue, _ = resolveVenue(liveBaseURL)
			_, err := b.Balances(context.Background())
			if (err == nil) != (flag == "safe") {
				t.Fatal("incorrect key permission decision", err)
			}
		})
	}
	b := mockBinance(func(*http.Request) (*http.Response, error) {
		return response(`{"uid":43,"canTrade":true,"balances":[]}`), nil
	})
	if _, err := b.Balances(context.Background()); err == nil {
		t.Fatal("account UID changed without rejection")
	}
}

func TestRetryAfterIsSharedAndCancellationDoesNotSend(t *testing.T) {
	requests := 0
	b := mockBinance(func(*http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			resp := response(`{"code":-1003,"msg":"Too many requests"}`)
			resp.StatusCode = 429
			resp.Header.Set("Retry-After", "2")
			return resp, nil
		}
		return response(`{"bidPrice":"100","askPrice":"101"}`), nil
	})
	if _, err := b.Book(context.Background(), "BTCUSDT"); err == nil {
		t.Fatal("rate limit ignored")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Book(ctx, "ETHUSDT"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("backoff did not honor cancellation", err)
	}
	if requests != 1 {
		t.Fatal("request sent during Retry-After")
	}
	b.rateMu.Lock()
	b.retryAt = time.Now().Add(-time.Second)
	b.rateMu.Unlock()
	if _, err := b.Book(context.Background(), "ETHUSDT"); err != nil {
		t.Fatal(err)
	}
}

func TestStaleMetricsDoNotDropHeldPositionsFromPortfolio(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["BTCUSDT"].Qty = dec("1")
	m := a.feeds.markets["BTCUSDT"]
	m.connected = false
	m.quoteAt = time.Now().Add(-time.Minute)
	a.refreshMetrics()
	for _, gauge := range []prometheus.Gauge{a.metrics.Equity.WithLabelValues("BTCUSDT"), a.metrics.Exposure.WithLabelValues("BTCUSDT"), a.metrics.PortfolioEquity, a.metrics.PortfolioExposure} {
		metric := new(dto.Metric)
		_ = gauge.Write(metric)
		if !math.IsNaN(metric.GetGauge().GetValue()) {
			t.Fatal("stale holdings reported as a current or zero valuation")
		}
	}
	metric := new(dto.Metric)
	_ = a.metrics.FeedAge.WithLabelValues("BTCUSDT").Write(metric)
	if metric.GetGauge().GetValue() < 59 {
		t.Fatal("disconnected feed age stayed artificially fresh")
	}
}

func TestRemovedFeedCancelsActiveSessionAndCanBeReadded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upgrader := websocket.Upgrader{}
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	var raw [][]any
	for _, c := range risingCandles(65) {
		raw = append(raw, []any{c.CloseTime - 59999, fmt.Sprint(c.Open), fmt.Sprint(c.High), fmt.Sprint(c.Low), fmt.Sprint(c.Close), fmt.Sprint(c.Volume), c.CloseTime})
	}
	data, _ := json.Marshal(raw)
	b := mockBinance(func(*http.Request) (*http.Response, error) { return response(string(data)), nil })
	f := NewFeeds([]string{"BTCUSDT"})
	old := f.markets["BTCUSDT"]
	done := make(chan error, 1)
	go func() { done <- old.session(ctx, b, "BTCUSDT", "ws"+strings.TrimPrefix(server.URL, "http")) }()
	deadline := time.Now().Add(time.Second)
	for !old.Snapshot().Connected && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !old.Snapshot().Connected {
		t.Fatal("session did not bootstrap")
	}
	f.SetPairs(nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("removed subscription remained active")
	}
	<-handlerDone
	f.SetPairs([]string{"BTCUSDT"})
	if f.markets["BTCUSDT"] == old || f.Snapshot("BTCUSDT").Connected {
		t.Fatal("re-added pair reused the removed session")
	}
	if err := old.message("BTCUSDT", []byte(`{"data":{"s":"BTCUSDT","b":"100","a":"101"}}`)); err == nil {
		t.Fatal("removed feed accepted a late frame")
	}
}

func TestAuditRetentionPreservesStateAndNewestEvents(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["BTCUSDT"].Pending = &Pending{ID: "must-survive", Side: "BUY", Qty: dec("1")}
	for i := 0; i < 10; i++ {
		if err := a.commit(context.Background(), a.state, "audit", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.repo.Prune(context.Background(), time.Now().Add(-time.Hour), 3); err != nil {
		t.Fatal(err)
	}
	events, err := a.repo.Events(context.Background())
	if err != nil || len(events) != 3 || events[0].ID != 10 {
		t.Fatal("retention did not keep newest events", err)
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Pending.ID != "must-survive" {
		t.Fatal("retention pruned execution state", err)
	}
	if err := a.repo.Prune(context.Background(), time.Now().Add(time.Hour), 3); err != nil {
		t.Fatal(err)
	}
	events, _ = a.repo.Events(context.Background())
	if len(events) != 0 {
		t.Fatal("expired events retained")
	}
}

func TestControlRejectsTrailingJSONAndBacktestHugeDecimals(t *testing.T) {
	a := testApp(t)
	if w := postControl(a, `{"action":"start"} {"action":"pause"}`); w.Code != 400 {
		t.Fatal("trailing command accepted")
	}
	input := strategy.BacktestInput{Candles: syntheticCandles(), Budget: dec("1e1000000")}
	if _, err := strategy.Backtest(context.Background(), input); err == nil {
		t.Fatal("unbounded decimal accepted")
	}
}

func TestNativeStopRejectionEmergencyClosesAndUncertaintyNeverRetries(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			a := executionApp(t)
			stops, sells := 0, 0
			a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/api/v3/account":
					return accountResponse(), nil
				case "/api/v3/order":
					if r.URL.Query().Get("type") == "STOP_LOSS" {
						stops++
						if uncertain {
							return nil, errors.New("unknown stop placement outcome")
						}
						resp := response(`{"code":-2010,"msg":"Stop rejected"}`)
						resp.StatusCode = 400
						return resp, nil
					}
					if r.URL.Query().Get("side") == "SELL" {
						sells++
						return orderResponse(3, r.URL.Query().Get("newClientOrderId"), "FILLED", "0.9", "90"), nil
					}
					return orderResponse(1, r.URL.Query().Get("newClientOrderId"), "FILLED", "0.9", "90"), nil
				case "/api/v3/myTrades":
					if r.URL.Query().Get("orderId") == "3" {
						return fillResponse(3, "0.9", "90", "0", "USDT"), nil
					}
					return fillResponse(1, "0.9", "90", "0", "USDT"), nil
				}
				return nil, errors.New("unexpected request")
			})
			if err := a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "test"); err == nil {
				t.Fatal("expected stop placement error")
			}
			p := a.snapshot().Pairs["BTCUSDT"]
			if uncertain {
				if p.Protection == nil || p.Protection.OrderID != 0 || !p.Qty.Equal(dec("0.9")) || sells != 0 {
					t.Fatal("uncertain stop discarded or duplicate exit sent")
				}
				if err := a.ensureProtection(context.Background(), "BTCUSDT"); err != nil {
					t.Fatal(err)
				}
				if stops != 1 {
					t.Fatal("uncertain native stop was resubmitted")
				}
			} else if !p.Qty.IsZero() || p.Protection != nil || p.Pending != nil || sells != 1 || !p.Paused {
				t.Fatal("definite stop rejection did not emergency-close")
			}
		})
	}
}

func TestAcceptedBuyGetsProtectedAfterCallerCancellation(t *testing.T) {
	a := executionApp(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stops := 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			return accountResponse(), nil
		case "/api/v3/order":
			if r.URL.Query().Get("type") == "STOP_LOSS" {
				stops++
				return orderResponse(2, r.URL.Query().Get("newClientOrderId"), "NEW", "0", "0"), nil
			}
			cancel()
			return orderResponse(1, r.URL.Query().Get("newClientOrderId"), "FILLED", "0.9", "90"), nil
		case "/api/v3/myTrades":
			return fillResponse(1, "0.9", "90", "0", "USDT"), nil
		}
		return nil, errors.New("unexpected request")
	})
	if err := a.submitLocked(ctx, "BTCUSDT", "BUY", dec("0.9"), "test"); err != nil {
		t.Fatal(err)
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if p.Protection == nil || p.Pending != nil || stops != 1 || a.fatal {
		t.Fatal("caller cancellation left an accepted buy unprotected")
	}
}

func TestNativeStopPollingDoesNotRewriteUnchangedState(t *testing.T) {
	a := executionApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Qty, p.Protection = dec("1"), &Pending{ID: "stop", OrderID: 2, Side: "SELL", Qty: dec("1"), AppliedFees: map[string]decimal.Decimal{}}
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) { return orderResponse(2, "stop", "NEW", "0", "0"), nil })
	for i := 0; i < 3; i++ {
		if err := a.applyOrder(context.Background(), "BTCUSDT", Order{OrderID: 2, ClientID: "stop", Status: "NEW", Executed: "0", Quote: "0"}, true); err != nil {
			t.Fatal(err)
		}
	}
	events, err := a.repo.Events(context.Background())
	if err != nil || len(events) != 0 {
		t.Fatal("unchanged stop generated repeated state writes", err)
	}
}

func TestConcurrentPairTransactionsDoNotLoseUpdates(t *testing.T) {
	a := testApp(t)
	a.state.Pairs["ETHUSDT"] = &Position{Pair: "ETHUSDT", Cash: dec("1000"), Paused: true}
	var wg sync.WaitGroup
	for _, pair := range []string{"BTCUSDT", "ETHUSDT"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if err := a.update(context.Background(), "test", pair, func(s State) error { s.Pairs[pair].Cash = s.Pairs[pair].Cash.Sub(dec("1")); return nil }); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	loaded, err := a.repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range loaded.Pairs {
		if !p.Cash.Equal(dec("990")) {
			t.Fatal("concurrent pair update was overwritten")
		}
	}
}

type failingCommitRepository struct{ Repository }

func (f failingCommitRepository) Commit(context.Context, State, string, any) error {
	return errors.New("mock storage failure")
}

func TestFailedCommitDoesNotPublishFinancialMutation(t *testing.T) {
	a := testApp(t)
	a.repo = failingCommitRepository{a.repo}
	if err := a.update(context.Background(), "test", nil, func(s State) error { s.Pairs["BTCUSDT"].Qty = dec("1"); return nil }); err == nil {
		t.Fatal("storage error ignored")
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if !a.fatal || !p.Qty.IsZero() || !p.Paused {
		t.Fatal("failed durable change was published")
	}
}

func TestRunJoinsInFlightReconciliationOnShutdown(t *testing.T) {
	a := executionApp(t)
	a.feeds = NewFeeds(nil) // No external WebSocket connections in this test.
	a.state.Pairs["BTCUSDT"].Pending = &Pending{ID: "unknown", Side: "BUY", Qty: dec("0.9")}
	entered, exited := make(chan struct{}), make(chan struct{})
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		close(exited)
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	select {
	case <-entered:
	case <-time.After(7 * time.Second):
		t.Fatal("reconciliation did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not join its workers")
	}
	select {
	case <-exited:
	default:
		t.Fatal("exchange worker outlived Run")
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Pending == nil {
		t.Fatal("shutdown lost uncertain intent", err)
	}
}

func eligibleExecutionApp(t *testing.T) *App {
	a := executionApp(t)
	cs := append([]strategy.Candle(nil), syntheticCandles()[:62]...)
	now := time.Now().UnixMilli()
	for i := range cs {
		cs[i].CloseTime = now - int64(len(cs)-i)*60000
	}
	m := a.feeds.markets["BTCUSDT"]
	m.candles, m.bid, m.ask = cs, dec("120"), dec("120.01")
	a.client = &http.Client{Timeout: time.Second}
	return a
}

func enterAnswer(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(map[string]any{"model": "winnow:e4b", "done_reason": "decide", "state_truncated": false, "answers": map[string]any{"action": map[string]any{"type": "choice", "choice": "ENTER_LONG", "probabilities": map[string]float64{"ENTER_LONG": .8, "EXIT_LONG": .1, "HOLD": .1}}}})
}

func TestModelEntryPersistsBuyAndNativeStopIntents(t *testing.T) {
	a := eligibleExecutionApp(t)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { enterAnswer(w) }))
	defer model.Close()
	a.cfg.OllayaURL = model.URL
	var quantity, quote string
	buys, stops := 0, 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			return accountResponse(), nil
		case "/api/v3/order":
			stored, err := a.repo.Load(context.Background())
			if err != nil {
				return nil, err
			}
			id := r.URL.Query().Get("newClientOrderId")
			if r.URL.Query().Get("type") == "STOP_LOSS" {
				stops++
				if stored.Pairs["BTCUSDT"].Protection == nil || stored.Pairs["BTCUSDT"].Protection.ID != id {
					t.Error("stop sent without durable intent")
				}
				return orderResponse(2, id, "NEW", "0", "0"), nil
			}
			buys++
			if stored.Pairs["BTCUSDT"].Pending == nil || stored.Pairs["BTCUSDT"].Pending.ID != id {
				t.Error("buy sent without durable intent")
			}
			quantity = r.URL.Query().Get("quantity")
			quote = dec(quantity).Mul(dec("120.01")).String()
			return orderResponse(1, id, "FILLED", quantity, quote), nil
		case "/api/v3/myTrades":
			return fillResponse(1, quantity, quote, "0.1", "USDT"), nil
		}
		return nil, errors.New("unexpected request")
	})
	a.decide(context.Background(), "BTCUSDT")
	p := a.snapshot().Pairs["BTCUSDT"]
	if buys != 1 || stops != 1 || p.Pending != nil || p.Protection == nil || p.Protection.OrderID != 2 || !p.Qty.IsPositive() || p.LastSetup == 0 {
		t.Fatal("model-to-protected-position lifecycle failed", p.Error)
	}
}

func TestPauseDuringInferenceInvalidatesModelEntry(t *testing.T) {
	a := eligibleExecutionApp(t)
	entered, release := make(chan struct{}), make(chan struct{})
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; enterAnswer(w) }))
	defer model.Close()
	a.cfg.OllayaURL = model.URL
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
		t.Error("paused decision reached exchange")
		return nil, errors.New("unexpected exchange call")
	})
	done := make(chan struct{})
	go func() { a.decide(context.Background(), "BTCUSDT"); close(done) }()
	<-entered
	if w := postControl(a, `{"action":"pause"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	close(release)
	<-done
	p := a.snapshot().Pairs["BTCUSDT"]
	if !p.Paused || p.Pending != nil || p.Protection != nil || p.LastSetup != 0 {
		t.Fatal("inference used an invalidated entry snapshot")
	}
}

func TestShutdownDrainsControlRequestsBeforeRepositoryCanClose(t *testing.T) {
	a := executionApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Qty, p.Cost, p.Cash = dec("1"), dec("100"), dec("900")
	entered, release := make(chan struct{}), make(chan struct{})
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/account":
			close(entered)
			<-release
			return accountResponse(), nil
		case "/api/v3/order":
			return orderResponse(1, r.URL.Query().Get("newClientOrderId"), "FILLED", "1", "100"), nil
		case "/api/v3/myTrades":
			return fillResponse(1, "1", "100", "0", "USDT"), nil
		}
		return nil, errors.New("unexpected request")
	})
	controlDone := make(chan int, 1)
	go func() { controlDone <- postControl(a, `{"action":"close","pair":"BTCUSDT"}`).Code }()
	<-entered
	a.mu.Lock()
	a.closing = true
	a.mu.Unlock()
	drained := make(chan struct{})
	go func() { a.requests.Wait(); close(drained) }()
	select {
	case <-drained:
		t.Fatal("control worker outlived request drain")
	case <-time.After(20 * time.Millisecond):
	}
	if w := postControl(a, `{"action":"pause"}`); w.Code != 503 {
		t.Fatal("new control request accepted during shutdown")
	}
	close(release)
	if code := <-controlDone; code != 200 {
		t.Fatal("in-flight close failed", code)
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("request did not drain")
	}
	if !a.snapshot().Pairs["BTCUSDT"].Qty.IsZero() {
		t.Fatal("request drain finished before settlement")
	}
}

func TestConcurrentFlatPairRemovalAndPolling(t *testing.T) {
	a := executionApp(t)
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
		t.Error("flat pair unexpectedly reached exchange")
		return nil, errors.New("unexpected exchange request")
	})
	for i := 0; i < 50; i++ {
		if err := a.update(context.Background(), "restore", nil, func(s State) error {
			s.Pairs["BTCUSDT"] = &Position{Pair: "BTCUSDT", Cash: dec("1000"), Paused: true}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); a.poll(context.Background()) }()
		go func() {
			defer wg.Done()
			if err := a.update(context.Background(), "remove", nil, func(s State) error { delete(s.Pairs, "BTCUSDT"); return nil }); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
	}
}

func TestUnfilledAcceptedBuyKeepsPlannedProtectionUntilReconciled(t *testing.T) {
	a := executionApp(t)
	a.state.Pairs["BTCUSDT"].Pending = &Pending{ID: "buy", Side: "BUY", Qty: dec("0.9")}
	stops := 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v3/myTrades":
			return fillResponse(1, "0.9", "90", "0", "USDT"), nil
		case "/api/v3/order":
			if r.Method == "GET" {
				return orderResponse(1, "buy", "FILLED", "0.9", "90"), nil
			}
			stops++
			return orderResponse(2, r.URL.Query().Get("newClientOrderId"), "NEW", "0", "0"), nil
		}
		return nil, errors.New("unexpected request")
	})
	if err := a.applyOrder(context.Background(), "BTCUSDT", Order{OrderID: 1, ClientID: "buy", Status: "NEW", Executed: "0", Quote: "0"}, false); err != nil {
		t.Fatal(err)
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if p.Pending == nil || !p.Stop.Equal(dec("99")) || !p.Target.Equal(dec("120")) {
		t.Fatal("zero-fill acknowledgement discarded planned protection")
	}
	a.poll(context.Background())
	p = a.snapshot().Pairs["BTCUSDT"]
	if p.Pending != nil || p.Protection == nil || stops != 1 || !p.Qty.Equal(dec("0.9")) {
		t.Fatal("reconciled buy was not protected", p.Error)
	}
}

func TestRejectionClassificationPreservesAmbiguousOutcomes(t *testing.T) {
	for _, item := range []struct {
		status, code int
		message      string
		definite     bool
	}{
		{400, -1013, "Filter failure", true}, {401, -2015, "Invalid API-key", true},
		{400, -2010, "Duplicate order sent.", false}, {500, -2010, "Execution status unknown", false},
		{429, -1003, "Too many requests", false}, {400, -2013, "Order does not exist.", false},
	} {
		if definitiveRejection(&APIError{Code: item.code, HTTPStatus: item.status, Message: item.message}) != item.definite {
			t.Fatalf("wrong rejection classification: %+v", item)
		}
	}
}
