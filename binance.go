package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

// Not configurable: this executable cannot send an order to production Binance.
const testnetURL = "https://testnet.binance.vision"

type Binance struct {
	client      *http.Client
	key, secret string
}

type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"msg"`
}

func (e *APIError) Error() string { return fmt.Sprintf("Binance %d: %s", e.Code, e.Message) }

func (b *Binance) request(ctx context.Context, method, path string, values url.Values, signed bool, target any) error {
	if values == nil {
		values = url.Values{}
	}
	if signed {
		if b.key == "" || b.secret == "" {
			return fmt.Errorf("testnet credentials missing")
		}
		values.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
		values.Set("recvWindow", "5000")
		mac := hmac.New(sha256.New, []byte(b.secret))
		mac.Write([]byte(values.Encode()))
		values.Set("signature", hex.EncodeToString(mac.Sum(nil)))
	}
	req, err := http.NewRequestWithContext(ctx, method, testnetURL+path+"?"+values.Encode(), nil)
	if err != nil {
		return err
	}
	if signed {
		req.Header.Set("X-MBX-APIKEY", b.key)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var api APIError
		if json.Unmarshal(data, &api) == nil && api.Code != 0 {
			return &api
		}
		return fmt.Errorf("Binance HTTP %d", resp.StatusCode)
	}
	return json.Unmarshal(data, target)
}

type Symbol struct {
	Symbol                                         string `json:"symbol"`
	Status                                         string `json:"status"`
	Base                                           string `json:"baseAsset"`
	Quote                                          string `json:"quoteAsset"`
	Step, MinQty, MaxQty, MinNotional, MaxNotional decimal.Decimal
	MarketAllowed                                  bool
}

func (b *Binance) Symbol(ctx context.Context, pair string) (Symbol, error) {
	var response struct {
		Symbols []struct {
			Symbol, Status        string
			BaseAsset, QuoteAsset string
			OrderTypes            []string
			Filters               []struct {
				FilterType, StepSize, MinQty, MaxQty, MinNotional, MaxNotional string
			}
		}
	}
	err := b.request(ctx, "GET", "/api/v3/exchangeInfo", url.Values{"symbol": {pair}}, false, &response)
	if err != nil {
		return Symbol{}, err
	}
	if len(response.Symbols) != 1 {
		return Symbol{}, fmt.Errorf("unknown pair")
	}
	r := response.Symbols[0]
	s := Symbol{Symbol: r.Symbol, Status: r.Status, Base: r.BaseAsset, Quote: r.QuoteAsset}
	for _, t := range r.OrderTypes {
		if t == "MARKET" {
			s.MarketAllowed = true
		}
	}
	parse := func(v string) decimal.Decimal { d, _ := decimal.NewFromString(v); return d }
	for _, f := range r.Filters {
		switch f.FilterType {
		case "LOT_SIZE":
			s.Step = parse(f.StepSize)
			s.MinQty = parse(f.MinQty)
			s.MaxQty = parse(f.MaxQty)
		case "MIN_NOTIONAL", "NOTIONAL":
			s.MinNotional = parse(f.MinNotional)
			s.MaxNotional = parse(f.MaxNotional)
		}
	}
	// Market filters can be stricter than LOT_SIZE. Intersect their limits.
	for _, f := range r.Filters {
		if f.FilterType == "MARKET_LOT_SIZE" {
			if d := parse(f.StepSize); d.IsPositive() && d.GreaterThan(s.Step) {
				s.Step = d
			}
			if d := parse(f.MinQty); d.GreaterThan(s.MinQty) {
				s.MinQty = d
			}
			if d := parse(f.MaxQty); d.IsPositive() && (s.MaxQty.IsZero() || d.LessThan(s.MaxQty)) {
				s.MaxQty = d
			}
		}
	}
	if s.Status != "TRADING" || !s.MarketAllowed || !s.Step.IsPositive() {
		return s, fmt.Errorf("pair is not supported for market trading")
	}
	return s, nil
}

type Book struct {
	Bid string `json:"bidPrice"`
	Ask string `json:"askPrice"`
}

func (b *Binance) Book(ctx context.Context, pair string) (Book, error) {
	var book Book
	err := b.request(ctx, "GET", "/api/v3/ticker/bookTicker", url.Values{"symbol": {pair}}, false, &book)
	return book, err
}

type Candle struct {
	CloseTime                      int64 `json:"close_time"`
	Open, High, Low, Close, Volume float64
}

func (b *Binance) Candles(ctx context.Context, pair string) ([]Candle, error) {
	var raw [][]json.RawMessage
	if err := b.request(ctx, "GET", "/api/v3/klines", url.Values{"symbol": {pair}, "interval": {"1m"}, "limit": {"100"}}, false, &raw); err != nil {
		return nil, err
	}
	result := []Candle{}
	for _, row := range raw {
		if len(row) < 7 {
			return nil, fmt.Errorf("invalid candle")
		}
		var c Candle
		if err := json.Unmarshal(row[6], &c.CloseTime); err != nil {
			return nil, err
		}
		if c.CloseTime >= time.Now().UnixMilli() {
			continue
		}
		for i, p := range []*float64{&c.Open, &c.High, &c.Low, &c.Close, &c.Volume} {
			var s string
			if err := json.Unmarshal(row[i+1], &s); err != nil {
				return nil, err
			}
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, err
			}
			*p = v
		}
		result = append(result, c)
	}
	return result, nil
}

type Order struct {
	OrderID  int64                                          `json:"orderId"`
	ClientID string                                         `json:"clientOrderId"`
	Status   string                                         `json:"status"`
	Executed string                                         `json:"executedQty"`
	Quote    string                                         `json:"cummulativeQuoteQty"`
	Fills    []struct{ Commission, CommissionAsset string } `json:"fills"`
}

func (b *Binance) Place(ctx context.Context, pair, side, qty, id string) (Order, error) {
	var order Order
	err := b.request(ctx, "POST", "/api/v3/order", url.Values{"symbol": {pair}, "side": {side}, "type": {"MARKET"}, "quantity": {qty}, "newClientOrderId": {id}, "newOrderRespType": {"FULL"}}, true, &order)
	return order, err
}
func (b *Binance) Find(ctx context.Context, pair, id string) (Order, error) {
	var order Order
	err := b.request(ctx, "GET", "/api/v3/order", url.Values{"symbol": {pair}, "origClientOrderId": {id}}, true, &order)
	return order, err
}

func (b *Binance) Fees(ctx context.Context, pair string, orderID int64) (map[string]decimal.Decimal, error) {
	var trades []struct{ Commission, CommissionAsset string }
	if err := b.request(ctx, "GET", "/api/v3/myTrades", url.Values{"symbol": {pair}, "orderId": {strconv.FormatInt(orderID, 10)}, "limit": {"1000"}}, true, &trades); err != nil {
		return nil, err
	}
	// Do not silently lose fees if the endpoint's page limit is reached.
	if len(trades) >= 1000 {
		return nil, fmt.Errorf("fee history requires pagination; manual reconciliation required")
	}
	fees := map[string]decimal.Decimal{}
	for _, t := range trades {
		d, err := decimal.NewFromString(t.Commission)
		if err != nil {
			return nil, err
		}
		fees[t.CommissionAsset] = fees[t.CommissionAsset].Add(d)
	}
	return fees, nil
}
func (b *Binance) Balances(ctx context.Context) (map[string]decimal.Decimal, error) {
	var account struct {
		CanTrade bool
		Balances []struct{ Asset, Free string }
	}
	if err := b.request(ctx, "GET", "/api/v3/account", nil, true, &account); err != nil {
		return nil, err
	}
	if !account.CanTrade {
		return nil, fmt.Errorf("account cannot trade")
	}
	balances := map[string]decimal.Decimal{}
	for _, b := range account.Balances {
		d, err := decimal.NewFromString(b.Free)
		if err != nil {
			return nil, err
		}
		balances[b.Asset] = d
	}
	return balances, nil
}

func floorStep(q, step decimal.Decimal) decimal.Decimal {
	quotient, _ := q.QuoRem(step, 0)
	return quotient.Mul(step)
}
