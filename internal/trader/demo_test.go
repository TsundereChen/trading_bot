package trader

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDemoVenueSelectionAndStateIsolation(t *testing.T) {
	for _, base := range []string{"", demoBaseURL, demoBaseURL + "/", demoBaseURL + "/api", demoBaseURL + "/api/"} {
		v, err := resolveVenue(base)
		if err != nil || v != demo() || v.Live {
			t.Fatalf("demo selection %q: %+v, %v", base, v, err)
		}
	}
	if _, err := resolveVenue("https://testnet.binance.vision"); err == nil {
		t.Fatal("unsupported Testnet endpoint accepted")
	}
	for _, venue := range []string{"paper", "live"} {
		if _, err := bindState(State{Venue: venue, AccountID: "42"}, "demo", "42", "demo:42"); err == nil {
			t.Fatal("different venue state silently adopted by Demo Mode")
		}
	}
}

func TestDemoSignedOrderLifecycle(t *testing.T) {
	a := executionApp(t)
	a.cfg.Venue, a.state.Venue = demo(), "demo"
	var stopID string
	buys, stops, cancels, sells := 0, 0, 0, 0
	a.binance = mockBinance(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "demo-api.binance.com" || !strings.HasPrefix(r.URL.Path, "/api/v3/") || r.Header.Get("X-MBX-APIKEY") != "mock" {
			t.Fatalf("wrong demo host, API path, or authentication: %s", r.URL.Host+r.URL.Path)
		}
		q := r.URL.Query()
		signature := q.Get("signature")
		q.Del("signature")
		mac := hmac.New(sha256.New, []byte("mock"))
		mac.Write([]byte(q.Encode()))
		if signature != hex.EncodeToString(mac.Sum(nil)) || q.Get("timestamp") == "" || q.Get("recvWindow") != "5000" {
			t.Fatal("invalid Spot HMAC signature or timing parameters")
		}
		switch r.URL.Path {
		case "/api/v3/account":
			return accountResponse(), nil
		case "/api/v3/order":
			if r.Method == http.MethodGet {
				return orderResponse(2, stopID, "NEW", "0", "0"), nil
			}
			if r.Method == http.MethodDelete {
				cancels++
				return orderResponse(2, stopID, "CANCELED", "0", "0"), nil
			}
			if r.Method != http.MethodPost {
				t.Fatal("wrong order method")
			}
			id := q.Get("newClientOrderId")
			if q.Get("type") == "STOP_LOSS" {
				stops++
				stopID = id
				if q.Get("stopPrice") != "99" || q.Get("side") != "SELL" {
					t.Fatal("wrong native stop parameters")
				}
				return orderResponse(2, id, "NEW", "0", "0"), nil
			}
			if q.Get("type") != "MARKET" || q.Get("quantity") != "0.9" {
				t.Fatal("wrong market order parameters")
			}
			if q.Get("side") == "BUY" {
				buys++
				return orderResponse(1, id, "FILLED", "0.9", "90"), nil
			}
			sells++
			return orderResponse(3, id, "FILLED", "0.9", "90"), nil
		case "/api/v3/myTrades":
			if q.Get("orderId") == "3" {
				return fillResponse(3, "0.9", "90", "0", "USDT"), nil
			}
			return fillResponse(1, "0.9", "90", "0", "USDT"), nil
		}
		return nil, fmt.Errorf("unexpected demo endpoint %s", r.URL.Path)
	})
	a.binance.venue = demo()
	if err := a.submitLocked(context.Background(), "BTCUSDT", "BUY", dec("0.9"), "demo_test"); err != nil {
		t.Fatal(err)
	}
	if p := a.snapshot().Pairs["BTCUSDT"]; p.Protection == nil || p.Pending != nil || !p.Qty.Equal(dec("0.9")) {
		t.Fatal("demo buy was not reconciled/protected")
	}
	gate := a.pairLock("BTCUSDT")
	gate.Lock()
	err := a.closeUnderGate(context.Background(), "BTCUSDT", "demo_test")
	gate.Unlock()
	if err != nil || buys != 1 || stops != 1 || cancels != 1 || sells != 1 {
		t.Fatal("demo close lifecycle failed", err)
	}
	p := a.snapshot().Pairs["BTCUSDT"]
	if !p.Qty.IsZero() || p.Pending != nil || p.Protection != nil {
		t.Fatal("demo position did not close")
	}
}

// Opt-in public connectivity check: no credentials, signed calls, or orders.
func TestDemoPublicConnectivity(t *testing.T) {
	if os.Getenv("TEST_BINANCE_DEMO_PUBLIC") != "true" {
		t.Skip("set TEST_BINANCE_DEMO_PUBLIC=true for read-only Demo Mode connectivity checks")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b := &Binance{venue: demo(), interval: os.Getenv("SIGNAL_INTERVAL"), client: &http.Client{Timeout: 8 * time.Second}}
	s, err := b.Symbol(ctx, "BTCUSDT")
	if err != nil || !s.StopAllowed {
		t.Fatal("demo exchangeInfo/native-stop support", err)
	}
	cs, err := b.Candles(ctx, "BTCUSDT", 100)
	if err != nil {
		t.Fatal("demo candles", err)
	}
	if _, err := features(cs, b.interval); err != nil {
		t.Fatal("demo candle freshness", err)
	}
	endpoint := demo().StreamURL + "/stream?streams=btcusdt@bookTicker/btcusdt@kline_" + signalInterval(b.interval)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		t.Fatal("demo combined stream", err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetReadDeadline(deadline)
	m := &Market{pair: "BTCUSDT", interval: b.interval, connected: true}
	for !m.Snapshot().Bid.IsPositive() {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatal("demo bookTicker", err)
		}
		if err := m.message("BTCUSDT", payload); err != nil {
			t.Fatal("demo stream decoding", err)
		}
	}
	if !freshMarket(m.Snapshot(), "BTCUSDT") {
		t.Fatal("demo stream quote cannot authorize trading")
	}
}
