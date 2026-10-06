package strategy

import (
	"context"
	"testing"
)

func TestFiveMinuteBacktest(t *testing.T) {
	cs := syntheticCandles()
	base := cs[0].CloseTime
	for i := range cs {
		cs[i].CloseTime = base + int64(i)*300000
	}
	r, err := Backtest(context.Background(), BacktestInput{Candles: cs, SignalInterval: "5m"})
	if err != nil || len(r.Trades) != 1 || r.Trades[0].EntryTime != cs[62].CloseTime-299999 || r.Trades[0].Reason != "stop" {
		t.Fatal("five-minute execution timing or conservative stop incorrect", err)
	}
	if _, err := Backtest(context.Background(), BacktestInput{Candles: cs, SignalInterval: "1m"}); err == nil {
		t.Fatal("wrong timeframe accepted")
	}
}
