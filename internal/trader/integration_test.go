package trader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"automated-trader/internal/strategy"

	"github.com/prometheus/client_golang/prometheus"
)

func repositoryContract(t *testing.T, r Repository) {
	t.Helper()
	ctx := context.Background()
	s := State{Pairs: map[string]*Position{
		"BTCUSDT": {Pair: "BTCUSDT", Cash: dec("1000"), Pending: &Pending{ID: "recovery", Side: "BUY", Qty: dec("0.01")}},
		"ETHUSDT": {Pair: "ETHUSDT", Cash: dec("500")},
	}}
	if err := r.Commit(ctx, s, "contract_test", map[string]string{"test": "yes"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := r.Load(ctx)
	btc := loaded.Pairs["BTCUSDT"]
	if err != nil || btc == nil || btc.Pending == nil || btc.Pending.ID != "recovery" {
		t.Fatalf("load %+v: %v", loaded, err)
	}
	// Multiple pairs must round-trip independently.
	if eth := loaded.Pairs["ETHUSDT"]; eth == nil || !eth.Cash.Equal(dec("500")) {
		t.Fatalf("second pair lost: %+v", loaded)
	}
	before, err := r.Events(ctx)
	if err != nil || len(before) == 0 || before[0].Kind != "contract_test" {
		t.Fatalf("events %v %v", before, err)
	}
	changed := loaded.copy()
	changed.Pairs["BTCUSDT"].Cash = dec("500")
	if r.Commit(ctx, changed, "invalid", make(chan int)) == nil {
		t.Fatal("invalid event committed")
	}
	loaded, err = r.Load(ctx)
	if err != nil || !loaded.Pairs["BTCUSDT"].Cash.Equal(dec("1000")) {
		t.Fatal("failed commit changed state")
	}
	after, _ := r.Events(ctx)
	if len(after) != len(before) {
		t.Fatal("failed commit added event")
	}
}
func TestSQLiteRepositoryContract(t *testing.T) {
	r, err := OpenSQLite(t.TempDir() + "/db.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	repositoryContract(t, r)
}
func TestPostgresRepositoryContract(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a dedicated disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	repositoryContract(t, r)
	// PostgreSQL retention must not disturb singleton ownership or durable state.
	if err := r.Prune(ctx, time.Now().Add(-time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if events, err := r.Events(ctx); err != nil || len(events) != 1 {
		t.Fatal("PostgreSQL retention failed", err)
	}
	postgresConcurrentRollback(t, ctx, r)
	if second, err := OpenPostgres(ctx, dsn); err == nil {
		second.Close()
		t.Fatal("second bot acquired singleton lock")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal("could not reacquire lock", err)
	}
	defer r.Close()
	state, err := r.Load(ctx)
	if err != nil || state.Pairs["BTCUSDT"].Pending == nil || state.Pairs["BTCUSDT"].Pending.ID != "recovery" {
		t.Fatal("restart did not preserve intent", err)
	}
}

func postgresConcurrentRollback(t *testing.T, ctx context.Context, r *Postgres) {
	t.Helper()
	// Force a SQL failure after bot_state has been updated but before commit.
	// Reads/pruning on the leased connection must not join that transaction.
	if _, err := r.lease.ExecContext(ctx, `ALTER TABLE events ADD CONSTRAINT test_reject_audit CHECK (kind <> 'rollback_test')`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := r.lease.ExecContext(context.Background(), `ALTER TABLE events DROP CONSTRAINT test_reject_audit`); err != nil {
			t.Error(err)
		}
	}()
	s, err := r.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	aborted := s.copy()
	aborted.Pairs["BTCUSDT"].Cash = dec("1")
	var wg sync.WaitGroup
	for _, worker := range []string{"commit", "read", "prune"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				switch worker {
				case "commit":
					if r.Commit(ctx, aborted, "rollback_test", nil) == nil {
						t.Error("expected SQL rollback")
					}
				case "read":
					loaded, err := r.Load(ctx)
					if err != nil {
						t.Error("read joined an aborted transaction", err)
						return
					}
					if !loaded.Pairs["BTCUSDT"].Cash.Equal(s.Pairs["BTCUSDT"].Cash) {
						t.Error("uncommitted state was visible")
					}
					if _, err := r.Events(ctx); err != nil {
						t.Error("events joined an aborted transaction", err)
					}
				case "prune":
					if err := r.Prune(ctx, time.Now().Add(-time.Hour), 1000); err != nil {
						t.Error("pruning joined an aborted transaction", err)
					}
				}
			}
		}()
	}
	wg.Wait()
}
func TestMarketMessageValidationAndGap(t *testing.T) {
	m := &Market{pair: "BTCUSDT", connected: true}
	if err := m.message("BTCUSDT", []byte(`{"data":{"s":"BTCUSDT","b":"100","B":"999","a":"101","A":"1"}}`)); err != nil {
		t.Fatal(err)
	}
	if !m.Snapshot().Bid.Equal(dec("100")) {
		t.Fatal("quote missing")
	}
	if m.message("BTCUSDT", []byte(`{"data":{"s":"BTCUSDT","b":"100","a":"99"}}`)) == nil {
		t.Fatal("crossed book accepted")
	}
	candle := func(at int64, closed bool) []byte {
		return []byte(fmt.Sprintf(`{"data":{"s":"BTCUSDT","k":{"x":%t,"t":%d,"T":%d,"L":12345,"o":"100","h":"100","l":"100","c":"100","v":"1","V":"0.1"}}}`, closed, at-59999, at))
	}
	if err := m.message("BTCUSDT", candle(60000, false)); err != nil {
		t.Fatal(err)
	}
	if len(m.Snapshot().Candles) != 0 {
		t.Fatal("unfinished candle included")
	}
	if err := m.message("BTCUSDT", candle(60000, true)); err != nil {
		t.Fatal(err)
	}
	if err := m.message("BTCUSDT", candle(60000, true)); err != nil {
		t.Fatal(err)
	}
	if len(m.Snapshot().Candles) != 1 {
		t.Fatal("duplicate candle included")
	}
	if c := m.Snapshot().Candles[0]; c.CloseTime != 60000 || c.Volume != 1 || c.Low != 100 {
		t.Fatalf("case-sensitive candle fields corrupted: %+v", c)
	}
	if m.message("BTCUSDT", candle(180000, true)) == nil {
		t.Fatal("gap accepted")
	}
	// Frames for another pair must not touch this feed.
	if err := m.message("ETHUSDT", []byte(`{"data":{"s":"ETHUSDT","b":"1","a":"2"}}`)); err == nil {
		t.Fatal("wrong-pair frame accepted")
	}
	if len(m.Snapshot().Candles) != 1 {
		t.Fatal("wrong-pair frame mutated candles")
	}
}

// Feeds must keep several pairs isolated and drop removed ones.
func TestFeedsMultiplePairs(t *testing.T) {
	f := NewFeeds([]string{"BTCUSDT", "ETHUSDT"})
	btc, eth := f.markets["BTCUSDT"], f.markets["ETHUSDT"]
	if err := btc.message("BTCUSDT", []byte(`{"data":{"s":"BTCUSDT","b":"100","a":"101"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := eth.message("ETHUSDT", []byte(`{"data":{"s":"ETHUSDT","b":"50","a":"51"}}`)); err != nil {
		t.Fatal(err)
	}
	if !f.Snapshot("BTCUSDT").Bid.Equal(dec("100")) || !f.Snapshot("ETHUSDT").Bid.Equal(dec("50")) {
		t.Fatal("pair quotes bled into each other")
	}
	if len(f.Pairs()) != 2 {
		t.Fatal("expected two pairs")
	}
	eth.connected = true // a live session marks the feed connected
	f.SetPairs([]string{"ETHUSDT"})
	if _, ok := f.markets["BTCUSDT"]; ok {
		t.Fatal("removed pair retained a feed")
	}
	removed := f.Snapshot("BTCUSDT")
	if removed.Connected || !removed.QuoteAt.IsZero() || len(removed.Candles) > 0 {
		t.Fatal("removed pair retained stale market data")
	}
	if !f.Snapshot("ETHUSDT").Connected || len(f.Pairs()) != 1 {
		t.Fatal("retained pair lost its feed")
	}
}
func syntheticCandles() []strategy.Candle {
	cs := []strategy.Candle{}
	base := int64(1700000000000)
	for i := 0; i < 60; i++ {
		price := 100 + float64(i)*.3
		cs = append(cs, strategy.Candle{CloseTime: base + int64(i)*60000, Open: price, High: price + 1, Low: price - 1, Close: price, Volume: 10})
	}
	// Deep pullback still inside the slower EMA uptrend, then completed recovery.
	cs = append(cs, strategy.Candle{CloseTime: base + 60*60000, Open: 115, High: 116, Low: 112, Close: 113, Volume: 10})
	cs = append(cs, strategy.Candle{CloseTime: base + 61*60000, Open: 113, High: 121, Low: 112, Close: 120, Volume: 10})
	cs = append(cs, strategy.Candle{CloseTime: base + 62*60000, Open: 120, High: 140, Low: 90, Close: 121, Volume: 10})
	cs = append(cs, strategy.Candle{CloseTime: base + 63*60000, Open: 121, High: 122, Low: 120, Close: 121, Volume: 10})
	return cs
}
func TestBacktestAPIAndNoFrontend(t *testing.T) {
	a := testApp(t)
	h := a.routes(prometheus.NewRegistry())
	bytes, _ := json.Marshal(strategy.BacktestInput{Candles: syntheticCandles()})
	r := httptest.NewRequest("POST", "/api/backtest", strings.NewReader(string(bytes)))
	r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	r = httptest.NewRequest("GET", "/", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatal("frontend still served")
	}
}
