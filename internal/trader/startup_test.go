package trader

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func readyStartupApp(t *testing.T) *App {
	t.Helper()
	a := executionApp(t)
	a.cfg.ContextInterval = "5m"
	m := a.feeds.markets["BTCUSDT"]
	m.candles, m.contextCandles = risingCandles(65), fiveMinuteHistory()
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/v3/account" {
			return accountResponse(), nil
		}
		return nil, fmt.Errorf("unexpected exchange request %s", r.URL.Path)
	})
	prepareEntryStartup(a.state.Pairs["BTCUSDT"], true)
	return a
}

func TestStartupAutomaticallyEnablesWithoutControlCommand(t *testing.T) {
	a := readyStartupApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Error = "historical repaired fault"
	a.poll(context.Background())
	p = a.snapshot().Pairs["BTCUSDT"]
	if p.Paused || p.Starting || p.Error != "" {
		t.Fatal("healthy startup did not automatically enable entries", p.Error)
	}
	loaded, err := a.repo.Load(context.Background())
	if err != nil || loaded.Pairs["BTCUSDT"].Paused || loaded.Pairs["BTCUSDT"].Starting {
		t.Fatal("startup readiness not persisted", err)
	}
}

func TestStartupWaitsForBothWindowsAndReconciliation(t *testing.T) {
	for _, name := range []string{"signals", "context", "quote", "pending", "fees", "protection"} {
		t.Run(name, func(t *testing.T) {
			a := readyStartupApp(t)
			p := a.state.Pairs["BTCUSDT"]
			m := a.feeds.markets["BTCUSDT"]
			switch name {
			case "signals":
				m.candles = nil
			case "context":
				m.contextCandles = nil
			case "quote":
				m.quoteAt = time.Now().Add(-time.Minute)
			case "pending":
				p.Pending = &Pending{ID: "pending", Side: "BUY", Qty: dec("1")}
			case "fees":
				p.UnvaluedFees = map[string]decimal.Decimal{"BNB": dec("1")}
			case "protection":
				p.Qty, p.Cost, p.Cash = dec("1"), dec("100"), dec("900")
			}
			calls := 0
			a.binance = mockBinance(func(*http.Request) (*http.Response, error) { calls++; return accountResponse(), nil })
			if err := a.autoEnableEntries(context.Background(), "BTCUSDT"); err != nil {
				t.Fatal(err)
			}
			p = a.snapshot().Pairs["BTCUSDT"]
			if !p.Paused || !p.Starting || calls != 0 {
				t.Fatal("startup bypassed pending safety check")
			}
		})
	}
}

func TestStartupPreservesDailyLossHalt(t *testing.T) {
	a := readyStartupApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Day, p.DayEquity = time.Now().UTC().Format("2006-01-02"), dec("1015")
	a.poll(context.Background())
	p = a.snapshot().Pairs["BTCUSDT"]
	if !p.Paused || p.Starting || !p.DayEquity.Equal(dec("1015")) {
		t.Fatal("restart bypassed or reset daily loss limit")
	}
}

func TestStartupReconcilesCombinedBaseHoldings(t *testing.T) {
	a := readyStartupApp(t)
	a.state.Pairs["BTCUSDT"].DustQty = dec("0.6")
	a.state.Pairs["BTCUSDC"] = &Position{Pair: "BTCUSDC", DustQty: dec("0.6"), Cash: dec("1000")}
	symbol := a.symbols["BTCUSDT"]
	symbol.Symbol, symbol.Quote = "BTCUSDC", "USDC"
	a.symbols["BTCUSDC"] = symbol
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
		return response(`{"uid":42,"balances":[{"asset":"BTC","free":"1","locked":"0"}]}`), nil
	})
	if err := a.autoEnableEntries(context.Background(), "BTCUSDT"); err == nil {
		t.Fatal("two allocations reused the same account holdings")
	}
	if !a.snapshot().Pairs["BTCUSDT"].Paused {
		t.Fatal("holdings mismatch enabled entries")
	}
}

func TestStartupFaultRemainsBlockedUntilProcessRestart(t *testing.T) {
	a := readyStartupApp(t)
	a.fail(context.Background(), "BTCUSDT", "test", fmt.Errorf("unrepaired fault"))
	if err := a.autoEnableEntries(context.Background(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if !p.Paused || p.Starting {
		t.Fatal("fault automatically resumed without restart")
	}
}

func TestObserveOnlyDoesNotAutomaticallyEnableTrading(t *testing.T) {
	a := readyStartupApp(t)
	a.cfg.Trading = false
	prepareEntryStartup(a.state.Pairs["BTCUSDT"], false)
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
		t.Error("observe-only queried execution account")
		return nil, fmt.Errorf("unexpected account request")
	})
	a.poll(context.Background())
	if a.executionAllowed() || a.snapshot().Pairs["BTCUSDT"].Starting {
		t.Fatal("observe-only enabled trading")
	}
}

func TestSuccessfulManualCloseDoesNotPermanentlyStopHealthyPair(t *testing.T) {
	a := readyStartupApp(t)
	p := a.state.Pairs["BTCUSDT"]
	p.Paused, p.Starting = false, false
	cooldown := time.Now().Add(time.Minute)
	p.CooldownUntil = cooldown
	if w := postControl(a, `{"action":"close","pair":"BTCUSDT"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p = a.snapshot().Pairs["BTCUSDT"]
	if !p.Starting || !p.Paused {
		t.Fatal("close did not schedule automatic revalidation")
	}
	a.poll(context.Background())
	p = a.snapshot().Pairs["BTCUSDT"]
	if p.Paused || p.Starting || !p.CooldownUntil.Equal(cooldown) {
		t.Fatal("manual close permanently stopped pair or reset cooldown")
	}
}

func TestAutoEnableRevalidatesAfterSlowHoldingsFetch(t *testing.T) {
	a := readyStartupApp(t)
	a.binance = mockBinance(func(*http.Request) (*http.Response, error) {
		m := a.feeds.markets["BTCUSDT"]
		m.mu.Lock()
		m.quoteAt = time.Now().Add(-time.Minute)
		m.mu.Unlock()
		return accountResponse(), nil
	})
	if err := a.autoEnableEntries(context.Background(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if !p.Paused || !p.Starting {
		t.Fatal("stale quote bypassed post-I/O startup validation")
	}
}
