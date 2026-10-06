package strategy

import (
	"fmt"
	"math"
)

// Candle is one completed OHLCV bar, with its close time in Unix milliseconds.
type Candle struct {
	CloseTime                      int64 `json:"close_time"`
	Open, High, Low, Close, Volume float64
}

// Features describes a completed trend-pullback setup.
type Features struct {
	EMA20          float64 `json:"ema20"`
	EMA50          float64 `json:"ema50"`
	ATR            float64 `json:"atr14"`
	RSI            float64 `json:"rsi14"`
	RelativeVolume float64 `json:"relative_volume"`
	Uptrend        bool    `json:"uptrend"`
	Pullback       bool    `json:"pullback_recovered"`
	Reversal       bool    `json:"trend_reversal"`
	SetupID        int64   `json:"setup_id"`
}

// ValidateCandle checks OHLCV values and the candle timestamp.
func ValidateCandle(c Candle) error {
	for _, v := range []float64{c.Open, c.High, c.Low, c.Close, c.Volume} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("candle values must be finite")
		}
	}
	if c.CloseTime <= 0 || c.Low <= 0 || c.High < c.Low || c.Open < c.Low || c.Open > c.High || c.Close < c.Low || c.Close > c.High || c.Volume < 0 {
		return fmt.Errorf("invalid candle OHLCV or timestamp")
	}
	return nil
}

func ema(cs []Candle, period int) float64 {
	v := cs[0].Close
	alpha := 2.0 / float64(period+1)
	for _, c := range cs[1:] {
		v += alpha * (c.Close - v)
	}
	return v
}

// CalculateFeatures uses only supplied completed candles, without a wall clock.
// Live callers are responsible for enforcing candle freshness.
func CalculateFeatures(cs []Candle) (Features, error) {
	if len(cs) < 60 {
		return Features{}, fmt.Errorf("not enough completed candles")
	}
	n := len(cs)
	last, prev := cs[n-1], cs[n-2]
	f := Features{EMA20: ema(cs, 20), EMA50: ema(cs, 50), SetupID: last.CloseTime}
	prevEMA := ema(cs[:n-1], 20)
	f.Uptrend, f.Reversal = f.EMA20 > f.EMA50, f.EMA20 < f.EMA50
	f.Pullback = prev.Low <= prevEMA && prev.Close <= prevEMA && last.Close > f.EMA20
	gain, loss := 0.0, 0.0
	for i := n - 14; i < n; i++ {
		c, p := cs[i], cs[i-1]
		f.ATR += math.Max(c.High-c.Low, math.Max(math.Abs(c.High-p.Close), math.Abs(c.Low-p.Close))) / 14
		if delta := c.Close - p.Close; delta > 0 {
			gain += delta
		} else {
			loss -= delta
		}
	}
	if loss == 0 {
		f.RSI = 100
		if gain == 0 {
			f.RSI = 50
		}
	} else {
		f.RSI = 100 - 100/(1+gain/loss)
	}
	vol := 0.0
	for _, c := range cs[n-21 : n-1] {
		vol += c.Volume / 20
	}
	if vol > 0 {
		f.RelativeVolume = last.Volume / vol
	}
	if f.ATR <= 0 {
		return f, fmt.Errorf("invalid volatility")
	}
	return f, nil
}
