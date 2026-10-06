package trader

import (
	"fmt"
	"time"

	"automated-trader/internal/strategy"
)

// Both timeframes use independently completed candles. Context is evidence for
// the model, not a mandatory higher-timeframe trend filter.
func contextFeatures(m MarketSnapshot, interval string) (strategy.Features, error) {
	if len(m.ContextCandles) == 0 {
		return strategy.Features{}, fmt.Errorf("context candles unavailable")
	}
	for i, c := range m.ContextCandles {
		if err := strategy.ValidateCandle(c); err != nil {
			return strategy.Features{}, err
		}
		if c.CloseTime >= time.Now().UnixMilli() {
			return strategy.Features{}, fmt.Errorf("context candle not completed")
		}
		if i > 0 && c.CloseTime-m.ContextCandles[i-1].CloseTime != candleDuration(interval).Milliseconds() {
			return strategy.Features{}, fmt.Errorf("context candle gap")
		}
	}
	return features(m.ContextCandles, interval)
}

func contextReady(m MarketSnapshot, interval string) bool {
	_, err := contextFeatures(m, interval)
	return err == nil
}
