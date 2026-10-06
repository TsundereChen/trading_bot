package strategy

import (
	"context"
	"testing"
)

func syntheticCandles() []Candle {
	cs := []Candle{}
	base := int64(1700000000000)
	for i := 0; i < 60; i++ {
		price := 100 + float64(i)*.3
		cs = append(cs, Candle{CloseTime: base + int64(i)*60000, Open: price, High: price + 1, Low: price - 1, Close: price, Volume: 10})
	}
	// Deep pullback still inside the slower EMA uptrend, then completed recovery.
	cs = append(cs, Candle{CloseTime: base + 60*60000, Open: 115, High: 116, Low: 112, Close: 113, Volume: 10})
	cs = append(cs, Candle{CloseTime: base + 61*60000, Open: 113, High: 121, Low: 112, Close: 120, Volume: 10})
	cs = append(cs, Candle{CloseTime: base + 62*60000, Open: 120, High: 140, Low: 90, Close: 121, Volume: 10})
	cs = append(cs, Candle{CloseTime: base + 63*60000, Open: 121, High: 122, Low: 120, Close: 121, Volume: 10})
	return cs
}

func TestBacktestNoLookaheadFeesAndConservativeStops(t *testing.T) {
	cs := syntheticCandles()
	f, err := CalculateFeatures(cs[:62])
	if err != nil || !f.Uptrend || !f.Pullback {
		t.Fatalf("test setup not eligible: %+v %v", f, err)
	}
	in := BacktestInput{Candles: cs}
	result, err := Backtest(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Trades) != 1 {
		t.Fatalf("expected one trade, got %+v", result.Trades)
	}
	trade := result.Trades[0]
	if trade.EntryTime != cs[62].CloseTime-59999 || trade.Reason != "stop" || !trade.Fees.IsPositive() {
		t.Fatalf("lookahead/stop/fees failure: %+v", trade)
	}
	if !result.NetPnL.Equal(trade.PnL) || !result.EndingEquity.Equal(result.StartingEquity.Add(trade.PnL)) {
		t.Fatal("accounting conservation failed")
	}
	changed := append([]Candle(nil), cs...)
	changed[62].Open = 125
	changed[62].Close = 126
	other, err := Backtest(context.Background(), BacktestInput{Candles: changed})
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Trades) != 1 || other.Trades[0].Entry.LessThanOrEqual(trade.Entry) {
		t.Fatal("entry did not use next open")
	}
	for _, bad := range []BacktestInput{{Candles: cs[:60]}, {Candles: []Candle{}}, {Candles: append(cs[:62:62], cs[61])}} {
		if _, err = Backtest(context.Background(), bad); err == nil {
			t.Fatal("invalid history accepted")
		}
	}
}
