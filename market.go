package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const testnetStreamURL = "wss://stream.testnet.binance.vision/stream?streams="

type Market struct {
	mu         sync.RWMutex
	pair       string
	bid, ask   decimal.Decimal
	quoteAt    time.Time
	candles    []Candle
	connected  bool
	reconnects uint64
	lastError  string
}
type MarketSnapshot struct {
	Pair       string          `json:"pair"`
	Bid        decimal.Decimal `json:"bid"`
	Ask        decimal.Decimal `json:"ask"`
	QuoteAt    time.Time       `json:"quote_received_at"`
	Connected  bool            `json:"connected"`
	Reconnects uint64          `json:"reconnects"`
	Error      string          `json:"error"`
	Candles    []Candle        `json:"-"`
}

func (m *Market) Snapshot() MarketSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return MarketSnapshot{m.pair, m.bid, m.ask, m.quoteAt, m.connected, m.reconnects, m.lastError, append([]Candle(nil), m.candles...)}
}
func (m *Market) SetPair(pair string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pair = pair
	m.connected = false
	m.quoteAt = time.Time{}
	m.candles = nil
}
func (m *Market) validPair(pair string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pair == pair
}

func (m *Market) message(pair string, payload []byte) error {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return err
	}
	var event struct {
		Symbol      string `json:"s"`
		Bid         string `json:"b"`
		Ask         string `json:"a"`
		BidQuantity string `json:"B"`
		AskQuantity string `json:"A"`
		Kline       *struct {
			Closed          bool   `json:"x"`
			CloseTime       int64  `json:"T"`
			OpenTime        int64  `json:"t"`
			Open            string `json:"o"`
			High            string `json:"h"`
			Low             string `json:"l"`
			LastTradeID     int64  `json:"L"`
			Close           string `json:"c"`
			Volume          string `json:"v"`
			TakerBaseVolume string `json:"V"`
		} `json:"k"`
	}
	if err := json.Unmarshal(envelope.Data, &event); err != nil {
		return err
	}
	if event.Symbol != pair {
		return fmt.Errorf("stream pair mismatch")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pair != m.pair {
		return nil
	}
	if event.Kline != nil {
		k := event.Kline
		if !k.Closed {
			return nil
		}
		c := Candle{CloseTime: k.CloseTime}
		for _, item := range []struct {
			value string
			dst   *float64
		}{{k.Open, &c.Open}, {k.High, &c.High}, {k.Low, &c.Low}, {k.Close, &c.Close}, {k.Volume, &c.Volume}} {
			v, err := strconv.ParseFloat(item.value, 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Errorf("invalid stream candle")
			}
			*item.dst = v
		}
		if err := validateCandle(c); err != nil {
			return err
		}
		if len(m.candles) > 0 {
			last := m.candles[len(m.candles)-1].CloseTime
			if c.CloseTime <= last {
				return nil
			}
			if c.CloseTime-last != 60000 {
				return fmt.Errorf("candle gap; reconnect and REST resynchronize")
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
	// Connect before the REST bootstrap so buffered WS candles cover its race window.
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
	cs, err := b.Candles(ctx, pair)
	if err != nil {
		return err
	}
	if _, err = features(cs); err != nil {
		return err
	}
	m.mu.Lock()
	if m.pair != pair {
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
				if !m.validPair(pair) {
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
		if err = m.message(pair, payload); err != nil {
			return err
		}
	}
}
func (m *Market) Run(ctx context.Context, b *Binance) {
	backoff := time.Second
	for ctx.Err() == nil {
		pair := m.Snapshot().Pair
		stream := strings.ToLower(pair)
		started := time.Now()
		err := m.session(ctx, b, pair, testnetStreamURL+stream+"@bookTicker/"+stream+"@kline_1m")
		m.mu.Lock()
		m.connected = false
		m.quoteAt = time.Time{}
		m.reconnects++
		if err != nil {
			m.lastError = err.Error()
		}
		m.mu.Unlock()
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}
