package trader

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"automated-trader/internal/strategy"

	"github.com/shopspring/decimal"
)

type Binance struct {
	client      *http.Client
	key, secret string
	venue       Venue
	accountID   string // Verified at startup; immutable during a run.
	rateMu      sync.Mutex
	retryAt     time.Time
}

type APIError struct {
	Code       int    `json:"code"`
	Message    string `json:"msg"`
	HTTPStatus int    `json:"-"`
}

func (e *APIError) Error() string { return fmt.Sprintf("Binance %d: %s", e.Code, e.Message) }

// Only explicit pre-execution rejections are safe to discard. Timeouts, 5xx,
// duplicate IDs, and unknown errors must retain the durable intent.
func definitiveRejection(err error) bool {
	var api *APIError
	if !errors.As(err, &api) || (api.HTTPStatus != 400 && api.HTTPStatus != 401 && api.HTTPStatus != 403) {
		return false
	}
	if api.Code == -2010 {
		return !strings.Contains(strings.ToLower(api.Message), "duplicate")
	}
	return api.Code == -1013 || api.Code == -1021 || api.Code == -1022 || api.Code == -2015 || (api.Code <= -1100 && api.Code >= -1136)
}

func (b *Binance) waitRateLimit(ctx context.Context) error {
	for {
		b.rateMu.Lock()
		wait := time.Until(b.retryAt)
		b.rateMu.Unlock()
		if wait <= 0 {
			return ctx.Err()
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type preflightError struct{ err error }

func (e *preflightError) Error() string { return e.err.Error() }
func (e *preflightError) Unwrap() error { return e.err }

func (b *Binance) request(ctx context.Context, method, path string, values url.Values, signed bool, target any, before ...func() error) error {
	if err := b.waitRateLimit(ctx); err != nil {
		return &preflightError{err: err}
	}
	if values == nil {
		values = url.Values{}
	}
	if signed {
		if b.key == "" || b.secret == "" {
			return &preflightError{err: fmt.Errorf("BINANCE_API_KEY and BINANCE_API_SECRET must be set to trade")}
		}
		values.Del("signature")
		values.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
		values.Set("recvWindow", "5000")
		mac := hmac.New(sha256.New, []byte(b.secret))
		mac.Write([]byte(values.Encode()))
		values.Set("signature", hex.EncodeToString(mac.Sum(nil)))
	}
	req, err := http.NewRequestWithContext(ctx, method, b.venue.BaseURL+path+"?"+values.Encode(), nil)
	if err != nil {
		return &preflightError{err: err}
	}
	if signed {
		req.Header.Set("X-MBX-APIKEY", b.key)
	}
	for _, check := range before {
		if err := check(); err != nil {
			return &preflightError{err: err}
		}
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 || resp.StatusCode == 418 {
		delay := 60 * time.Second
		if resp.StatusCode == 418 {
			delay = 120 * time.Second
		}
		if seconds, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64); err == nil && seconds > 0 && seconds <= 7*86400 {
			delay = time.Duration(seconds) * time.Second
		} else if at, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && at.After(time.Now()) {
			delay = time.Until(at)
		}
		b.rateMu.Lock()
		if at := time.Now().Add(delay); at.After(b.retryAt) {
			b.retryAt = at
		}
		b.rateMu.Unlock()
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return fmt.Errorf("Binance response exceeds size limit")
	}
	if resp.StatusCode != http.StatusOK {
		var api APIError
		if json.Unmarshal(data, &api) == nil && api.Code != 0 {
			api.HTTPStatus = resp.StatusCode
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
	StopAllowed                                    bool
	Tick, MinPrice, MaxPrice                       decimal.Decimal
}

func (b *Binance) Symbol(ctx context.Context, pair string) (Symbol, error) {
	var response struct {
		Symbols []struct {
			Symbol, Status        string
			BaseAsset, QuoteAsset string
			OrderTypes            []string
			Filters               []struct {
				FilterType, StepSize, MinQty, MaxQty, MinNotional, MaxNotional string
				TickSize, MinPrice, MaxPrice                                   string
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
		if t == "STOP_LOSS" {
			s.StopAllowed = true
		}
	}
	var parseErr error
	parse := func(v string) decimal.Decimal {
		if v == "" {
			return decimal.Zero
		}
		d, err := decimal.NewFromString(v)
		if err != nil || d.IsNegative() {
			parseErr = fmt.Errorf("invalid exchange filter %q", v)
		}
		return d
	}
	for _, f := range r.Filters {
		switch f.FilterType {
		case "PRICE_FILTER":
			s.Tick, s.MinPrice, s.MaxPrice = parse(f.TickSize), parse(f.MinPrice), parse(f.MaxPrice)
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
	if parseErr != nil {
		return s, parseErr
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

func (b *Binance) Candles(ctx context.Context, pair string, limit int) ([]strategy.Candle, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("candle limit must be 1-1000")
	}
	var raw [][]json.RawMessage
	if err := b.request(ctx, "GET", "/api/v3/klines", url.Values{"symbol": {pair}, "interval": {"1m"}, "limit": {strconv.Itoa(limit)}}, false, &raw); err != nil {
		return nil, err
	}
	result := []strategy.Candle{}
	for _, row := range raw {
		if len(row) < 7 {
			return nil, fmt.Errorf("invalid candle")
		}
		var c strategy.Candle
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
		if err := strategy.ValidateCandle(c); err != nil {
			return nil, err
		}
		if len(result) > 0 && c.CloseTime-result[len(result)-1].CloseTime != candleIntervalMS {
			return nil, fmt.Errorf("non-consecutive REST candles")
		}
		result = append(result, c)
	}
	return result, nil
}

type Order struct {
	OrderID          int64  `json:"orderId"`
	ClientID         string `json:"clientOrderId"`
	OriginalClientID string `json:"origClientOrderId"`
	Status           string `json:"status"`
	Executed         string `json:"executedQty"`
	Quote            string `json:"cummulativeQuoteQty"`
}

func (b *Binance) Place(ctx context.Context, pair, side, qty, id string) (Order, error) {
	var order Order
	err := b.request(ctx, "POST", "/api/v3/order", url.Values{"symbol": {pair}, "side": {side}, "type": {"MARKET"}, "quantity": {qty}, "newClientOrderId": {id}, "newOrderRespType": {"FULL"}}, true, &order)
	return order, err
}
func (b *Binance) PlaceChecked(ctx context.Context, pair, side, qty, id string, check func() error) (Order, error) {
	var order Order
	err := b.request(ctx, "POST", "/api/v3/order", url.Values{"symbol": {pair}, "side": {side}, "type": {"MARKET"}, "quantity": {qty}, "newClientOrderId": {id}, "newOrderRespType": {"FULL"}}, true, &order, check)
	return order, err
}
func (b *Binance) Find(ctx context.Context, pair, id string) (Order, error) {
	var order Order
	err := b.request(ctx, "GET", "/api/v3/order", url.Values{"symbol": {pair}, "origClientOrderId": {id}}, true, &order)
	return order, err
}

func (b *Binance) FindPending(ctx context.Context, pair string, pending *Pending) (Order, error) {
	if pending.OrderID == 0 {
		return b.Find(ctx, pair, pending.ID)
	}
	var order Order
	err := b.request(ctx, "GET", "/api/v3/order", url.Values{"symbol": {pair}, "orderId": {strconv.FormatInt(pending.OrderID, 10)}}, true, &order)
	return order, err
}

func (b *Binance) PlaceStop(ctx context.Context, pair, qty, stop, id string) (Order, error) {
	var order Order
	err := b.request(ctx, "POST", "/api/v3/order", url.Values{"symbol": {pair}, "side": {"SELL"}, "type": {"STOP_LOSS"}, "quantity": {qty}, "stopPrice": {stop}, "newClientOrderId": {id}, "newOrderRespType": {"FULL"}}, true, &order)
	return order, err
}

func (b *Binance) Cancel(ctx context.Context, pair string, pending *Pending) (Order, error) {
	values := url.Values{"symbol": {pair}}
	if pending.OrderID > 0 {
		values.Set("orderId", strconv.FormatInt(pending.OrderID, 10))
	} else {
		values.Set("origClientOrderId", pending.ID)
	}
	var order Order
	err := b.request(ctx, "DELETE", "/api/v3/order", values, true, &order)
	if order.OriginalClientID == pending.ID {
		order.ClientID = pending.ID
	}
	return order, err
}

type FillTotals struct {
	Qty, Quote decimal.Decimal
	Fees       map[string]decimal.Decimal
}

func (b *Binance) Fills(ctx context.Context, pair string, orderID int64) (FillTotals, error) {
	total := FillTotals{Fees: map[string]decimal.Decimal{}}
	fromID := int64(0)
	for page := 0; page < 100; page++ {
		var trades []struct {
			ID, OrderID                                int64
			Qty, QuoteQty, Commission, CommissionAsset string
		}
		values := url.Values{"symbol": {pair}, "orderId": {strconv.FormatInt(orderID, 10)}, "limit": {"1000"}}
		// Without fromId Binance returns the newest page, which would silently
		// miss the beginning of an order with more than 1,000 fills.
		values.Set("fromId", strconv.FormatInt(fromID, 10))
		if err := b.request(ctx, "GET", "/api/v3/myTrades", values, true, &trades); err != nil {
			return total, err
		}
		lastID := fromID - 1
		for _, trade := range trades {
			if trade.ID < fromID || trade.ID <= lastID || trade.OrderID != orderID || trade.CommissionAsset == "" {
				return total, fmt.Errorf("invalid or non-monotonic fill identity")
			}
			qty, e1 := decimal.NewFromString(trade.Qty)
			quote, e2 := decimal.NewFromString(trade.QuoteQty)
			fee, e3 := decimal.NewFromString(trade.Commission)
			if e1 != nil || e2 != nil || e3 != nil || !qty.IsPositive() || !quote.IsPositive() || fee.IsNegative() {
				return total, fmt.Errorf("invalid fill amounts")
			}
			total.Qty, total.Quote = total.Qty.Add(qty), total.Quote.Add(quote)
			total.Fees[trade.CommissionAsset] = total.Fees[trade.CommissionAsset].Add(fee)
			lastID = trade.ID
		}
		if len(trades) < 1000 {
			return total, nil
		}
		if lastID == math.MaxInt64 {
			return total, fmt.Errorf("fill ID overflow")
		}
		fromID = lastID + 1
	}
	return total, fmt.Errorf("fill pagination limit exceeded; keep order pending")
}

type Account struct {
	UID      int64 `json:"uid"`
	CanTrade bool  `json:"canTrade"`
	Balances []struct{ Asset, Free, Locked string }
}

func (b *Binance) Account(ctx context.Context) (Account, error) {
	var account Account
	if err := b.request(ctx, "GET", "/api/v3/account", nil, true, &account); err != nil {
		return account, err
	}
	if b.accountID != "" && strconv.FormatInt(account.UID, 10) != b.accountID {
		return account, fmt.Errorf("exchange account identity changed")
	}
	if !account.CanTrade {
		return account, fmt.Errorf("account cannot trade")
	}
	return account, nil
}

func (b *Binance) CheckPermissions(ctx context.Context) error {
	if !b.venue.Live {
		return nil
	} // Spot Testnet has no SAPI endpoints.
	var permissions struct {
		EnableReading, EnableSpotAndMarginTrading, IpRestrict               *bool
		EnableWithdrawals, EnableInternalTransfer, PermitsUniversalTransfer *bool
		EnableMargin, EnableFutures, EnableVanillaOptions                   *bool
	}
	if err := b.request(ctx, "GET", "/sapi/v1/account/apiRestrictions", nil, true, &permissions); err != nil {
		return err
	}
	for _, required := range []*bool{permissions.EnableReading, permissions.EnableSpotAndMarginTrading, permissions.IpRestrict} {
		if required == nil || !*required {
			return fmt.Errorf("live API key must allow reading/spot trading and restrict access by IP")
		}
	}
	for _, unsafe := range []*bool{permissions.EnableWithdrawals, permissions.EnableInternalTransfer, permissions.PermitsUniversalTransfer, permissions.EnableMargin, permissions.EnableFutures, permissions.EnableVanillaOptions} {
		if unsafe == nil || *unsafe {
			return fmt.Errorf("live key permissions are incomplete or allow withdrawals, transfers, margin, futures, or options")
		}
	}
	return nil
}

func (b *Binance) Balances(ctx context.Context) (map[string]decimal.Decimal, error) {
	return b.balances(ctx, false)
}
func (b *Binance) Holdings(ctx context.Context) (map[string]decimal.Decimal, error) {
	return b.balances(ctx, true)
}
func (b *Binance) balances(ctx context.Context, includeLocked bool) (map[string]decimal.Decimal, error) {
	if err := b.CheckPermissions(ctx); err != nil {
		return nil, err
	}
	account, err := b.Account(ctx)
	if err != nil {
		return nil, err
	}
	balances := map[string]decimal.Decimal{}
	for _, b := range account.Balances {
		d, err := decimal.NewFromString(b.Free)
		if err != nil || d.IsNegative() {
			return nil, fmt.Errorf("invalid account balance")
		}
		balances[b.Asset] = d
		if includeLocked && b.Locked != "" {
			locked, err := decimal.NewFromString(b.Locked)
			if err != nil || locked.IsNegative() {
				return nil, fmt.Errorf("invalid locked balance")
			}
			balances[b.Asset] = d.Add(locked)
		}
	}
	return balances, nil
}
