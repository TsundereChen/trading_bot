package trader

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"automated-trader/internal/strategy"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const candleIntervalMS = 60000

func freshMarket(m MarketSnapshot, pair string) bool {
	age := time.Since(m.QuoteAt)
	return m.Pair == pair && m.Connected && !m.QuoteAt.IsZero() && age >= 0 && age <= 3*time.Second && m.Bid.IsPositive() && !m.Ask.LessThan(m.Bid)
}

type Market struct {
	mu         sync.RWMutex
	pair       string
	bid, ask   decimal.Decimal
	quoteAt    time.Time
	candles    []strategy.Candle
	connected  bool
	reconnects uint64
	lastError  string
	removed    bool
	cancel     context.CancelFunc
	done       chan struct{}
}

type MarketSnapshot struct {
	Pair       string            `json:"pair"`
	Bid        decimal.Decimal   `json:"bid"`
	Ask        decimal.Decimal   `json:"ask"`
	QuoteAt    time.Time         `json:"quote_received_at"`
	Connected  bool              `json:"connected"`
	Reconnects uint64            `json:"reconnects"`
	Error      string            `json:"error"`
	Candles    []strategy.Candle `json:"-"`
}

// Feeds owns one Market per configured pair and keeps their subscriptions in
// sync with the configured pair list.
type Feeds struct {
	mu      sync.RWMutex
	markets map[string]*Market
	pairs   []string
}

func NewFeeds(pairs []string) *Feeds {
	f := &Feeds{markets: map[string]*Market{}, pairs: append([]string(nil), pairs...)}
	for _, pair := range pairs {
		f.markets[pair] = &Market{pair: pair, done: make(chan struct{})}
	}
	return f
}

func (f *Feeds) Pairs() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, len(f.pairs))
	copy(out, f.pairs)
	sort.Strings(out)
	return out
}

func (f *Feeds) Snapshot(pair string) MarketSnapshot {
	f.mu.RLock()
	market, ok := f.markets[pair]
	f.mu.RUnlock()
	if !ok {
		return MarketSnapshot{Pair: pair}
	}
	return market.Snapshot()
}

// SetPairs adds and removes feeds. Removed pairs are marked disconnected so no
// stale quote can authorize a submission.
func (f *Feeds) SetPairs(pairs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wanted := map[string]bool{}
	for _, pair := range pairs {
		wanted[pair] = true
		if _, ok := f.markets[pair]; !ok {
			f.markets[pair] = &Market{pair: pair, done: make(chan struct{})}
		}
	}
	for pair, market := range f.markets {
		if !wanted[pair] {
			market.mu.Lock()
			market.removed = true
			if market.cancel != nil {
				market.cancel()
			}
			if market.done != nil {
				close(market.done)
			}
			market.connected = false
			market.quoteAt = time.Time{}
			market.candles = nil
			market.mu.Unlock()
			delete(f.markets, pair)
		}
	}
	f.pairs = append([]string(nil), pairs...)
}

func (m *Market) Snapshot() MarketSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return MarketSnapshot{
		Pair: m.pair, Bid: m.bid, Ask: m.ask, QuoteAt: m.quoteAt,
		Connected: m.connected, Reconnects: m.reconnects, Error: m.lastError,
		Candles: append([]strategy.Candle(nil), m.candles...),
	}
}

// message applies one stream frame. Binance field names are case-sensitive on
// the wire but Go decodes case-insensitively, so every colliding key is declared
// explicitly to avoid mixing prices with quantities.
func (m *Market) message(pair string, payload []byte) error {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return err
	}
	var event struct {
		Symbol string `json:"s"`
		Bid    string `json:"b"`
		Ask    string `json:"a"`
		BidQty string `json:"B"`
		AskQty string `json:"A"`
		Kline  *struct {
			Closed            bool   `json:"x"`
			OpenTime          int64  `json:"t"`
			CloseTime         int64  `json:"T"`
			LastTradeID       int64  `json:"L"`
			Open              string `json:"o"`
			High              string `json:"h"`
			Low               string `json:"l"`
			Close             string `json:"c"`
			Volume            string `json:"v"`
			TakerBuyBaseAsset string `json:"V"`
			TakerBuyQuote     string `json:"Q"`
		} `json:"k"`
	}
	if err := json.Unmarshal(envelope.Data, &event); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removed {
		return context.Canceled
	}
	if event.Symbol != m.pair || pair != m.pair {
		return fmt.Errorf("stream pair mismatch: frame %s, session %s, feed %s", event.Symbol, pair, m.pair)
	}
	if event.Kline != nil {
		k := event.Kline
		if !k.Closed {
			return nil
		}
		c := strategy.Candle{CloseTime: k.CloseTime}
		for _, item := range []struct {
			value string
			dst   *float64
		}{{k.Open, &c.Open}, {k.High, &c.High}, {k.Low, &c.Low}, {k.Close, &c.Close}, {k.Volume, &c.Volume}} {
			v, err := strconv.ParseFloat(item.value, 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Errorf("invalid stream candle value")
			}
			*item.dst = v
		}
		if err := strategy.ValidateCandle(c); err != nil {
			return err
		}
		if len(m.candles) > 0 {
			last := m.candles[len(m.candles)-1].CloseTime
			if c.CloseTime <= last {
				return nil // duplicate or out-of-order update
			}
			if c.CloseTime-last != candleIntervalMS {
				return fmt.Errorf("candle gap; reconnect to resynchronize")
			}
		}
		m.candles = append(m.candles, c)
		if len(m.candles) > 100 {
			m.candles = m.candles[len(m.candles)-100:]
		}
		return nil
	}
	bid, e1 := decimal.NewFromString(event.Bid)
	ask, e2 := decimal.NewFromString(event.Ask)
	if e1 != nil || e2 != nil || !bid.IsPositive() || ask.LessThan(bid) {
		return fmt.Errorf("invalid stream quote")
	}
	m.bid, m.ask, m.quoteAt = bid, ask, time.Now()
	return nil
}

func (m *Market) session(ctx context.Context, b *Binance, pair, endpoint string) error {
	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	if m.removed {
		m.mu.Unlock()
		cancel()
		return context.Canceled
	}
	m.cancel = cancel
	m.mu.Unlock()
	defer func() { cancel(); m.mu.Lock(); m.cancel = nil; m.mu.Unlock() }()
	// Connect before the REST bootstrap so buffered stream candles cover the
	// race window between the snapshot and the subscription going live.
	dialer := websocket.Dialer{HandshakeTimeout: 8 * time.Second}
	conn, _, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-closed:
		}
	}()
	cs, err := b.Candles(ctx, pair, 100)
	if err != nil {
		return err
	}
	if _, err := features(cs); err != nil {
		return err
	}
	m.mu.Lock()
	if m.pair != pair || m.removed {
		m.mu.Unlock()
		return nil
	}
	m.candles = cs
	m.connected = true
	m.lastError = ""
	m.quoteAt = time.Time{}
	m.mu.Unlock()

	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		nextPing := time.Now().Add(30 * time.Second)
		for {
			select {
			case <-closed:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.mu.RLock()
				current, removed := m.pair, m.removed
				m.mu.RUnlock()
				if current != pair || removed {
					conn.Close()
					return
				}
				if time.Now().After(nextPing) {
					if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
						conn.Close()
						return
					}
					nextPing = time.Now().Add(30 * time.Second)
				}
			}
		}
	}()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := m.message(pair, payload); err != nil {
			return err
		}
	}
}

// Run supervises configured pairs and keeps supervising pairs added at runtime
// via SetPairs. A feed exists for as long as the pair is configured.
func (f *Feeds) Run(ctx context.Context, b *Binance) {
	supervised := map[string]bool{}
	done := map[string]chan struct{}{}
	var wg sync.WaitGroup
	defer wg.Wait()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		// Start a supervisor for any configured pair not yet supervised.
		for _, pair := range f.Pairs() {
			if supervised[pair] {
				continue
			}
			supervised[pair] = true
			finished := make(chan struct{})
			done[pair] = finished
			wg.Add(1)
			go func(pair string, finished chan struct{}) {
				defer wg.Done()
				defer close(finished)
				f.runPair(ctx, b, pair)
			}(pair, finished)
		}
		// A supervisor exiting means the pair was removed; allow a future
		// re-add to be supervised again.
		for pair, finished := range done {
			select {
			case <-finished:
				supervised[pair] = false
				delete(done, pair)
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *Feeds) runPair(ctx context.Context, b *Binance, pair string) {
	f.mu.RLock()
	market := f.markets[pair]
	f.mu.RUnlock()
	if market == nil {
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		f.mu.RLock()
		current := f.markets[pair]
		f.mu.RUnlock()
		if current != market {
			return // pair removed
		}
		stream := strings.ToLower(pair)
		endpoint := b.venue.StreamURL + "/stream?streams=" + stream + "@bookTicker/" + stream + "@kline_1m"
		started := time.Now()
		err := market.session(ctx, b, pair, endpoint)
		market.mu.Lock()
		market.connected = false
		market.quoteAt = time.Time{}
		market.reconnects++
		if err != nil {
			market.lastError = err.Error()
		}
		market.mu.Unlock()
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-market.done:
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}
