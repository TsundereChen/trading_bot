package strategy

import (
	"math"
	"testing"
)

func TestIndicatorsUseSuppliedHistory(t *testing.T) {
	cs := syntheticCandles()[:62]
	want, err := CalculateFeatures(cs)
	if err != nil {
		t.Fatal(err)
	}
	shifted := append([]Candle(nil), cs...)
	for i := range shifted {
		shifted[i].CloseTime += 24 * 60 * 60000
	}
	got, err := CalculateFeatures(shifted)
	if err != nil {
		t.Fatal(err)
	}
	if got.EMA20 != want.EMA20 || got.EMA50 != want.EMA50 || got.ATR != want.ATR || got.Uptrend != want.Uptrend || got.Pullback != want.Pullback || got.SetupID != shifted[len(shifted)-1].CloseTime {
		t.Fatal("pure indicators changed with wall-clock-independent timestamps")
	}
	if _, err := CalculateFeatures(cs[:59]); err == nil {
		t.Fatal("insufficient history accepted")
	}
}

func TestValidateCandle(t *testing.T) {
	valid := Candle{CloseTime: 60000, Open: 100, High: 101, Low: 99, Close: 100, Volume: 1}
	if err := ValidateCandle(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []Candle{
		{CloseTime: 0, Open: 100, High: 101, Low: 99, Close: 100},
		{CloseTime: 60000, Open: 100, High: 99, Low: 101, Close: 100},
		{CloseTime: 60000, Open: 100, High: 101, Low: 99, Close: math.NaN()},
		{CloseTime: 60000, Open: 100, High: 101, Low: 99, Close: 100, Volume: -1},
	} {
		if err := ValidateCandle(invalid); err == nil {
			t.Fatalf("invalid candle accepted: %+v", invalid)
		}
	}
}
