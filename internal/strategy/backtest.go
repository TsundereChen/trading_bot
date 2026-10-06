package strategy

import (
	"context"
	"fmt"
	"math"

	"github.com/shopspring/decimal"
)

type BacktestInput struct {
	Candles      []Candle        `json:"candles"`
	Budget       decimal.Decimal `json:"budget"`
	MaxPosition  decimal.Decimal `json:"max_position"`
	RiskPerTrade decimal.Decimal `json:"risk_per_trade"`
	DailyLoss    decimal.Decimal `json:"daily_loss_limit"`
	FeeBps       *float64        `json:"fee_bps"`
	SlippageBps  *float64        `json:"slippage_bps"`
	QuantityStep decimal.Decimal `json:"quantity_step"`
	MinNotional  decimal.Decimal `json:"min_notional"`
}
type BacktestTrade struct {
	EntryTime int64           `json:"entry_time_ms"`
	ExitTime  int64           `json:"exit_time_ms"`
	Entry     decimal.Decimal `json:"entry_price"`
	Exit      decimal.Decimal `json:"exit_price"`
	Quantity  decimal.Decimal `json:"quantity"`
	Fees      decimal.Decimal `json:"fees"`
	PnL       decimal.Decimal `json:"pnl"`
	Reason    string          `json:"reason"`
}
type EquityPoint struct {
	Time   int64           `json:"time_ms"`
	Equity decimal.Decimal `json:"equity"`
}
type BacktestResult struct {
	Strategy       string          `json:"strategy"`
	StartingEquity decimal.Decimal `json:"starting_equity"`
	EndingEquity   decimal.Decimal `json:"ending_equity"`
	NetPnL         decimal.Decimal `json:"net_pnl"`
	ReturnPct      decimal.Decimal `json:"return_pct"`
	MaxDrawdownPct decimal.Decimal `json:"max_close_to_close_drawdown_pct"`
	TotalFees      decimal.Decimal `json:"total_fees"`
	WinRate        float64         `json:"win_rate"`
	Trades         []BacktestTrade `json:"trades"`
	Equity         []EquityPoint   `json:"equity_curve"`
	Assumptions    []string        `json:"assumptions"`
}

func backtestDefaults(input BacktestInput) (BacktestInput, error) {
	for _, v := range []struct {
		dst *decimal.Decimal
		def string
	}{{&input.Budget, "1000"}, {&input.MaxPosition, "100"}, {&input.RiskPerTrade, "2.5"}, {&input.DailyLoss, "10"}, {&input.QuantityStep, "0.00000001"}, {&input.MinNotional, "5"}} {
		if !BoundedDecimal(*v.dst) {
			return input, fmt.Errorf("backtest decimals exceed numeric limits")
		}
		if v.dst.IsZero() {
			*v.dst = decimal.RequireFromString(v.def)
		}
		if !v.dst.IsPositive() {
			return input, fmt.Errorf("backtest budgets and exchange limits must be positive")
		}
	}
	fee, slip := 10.0, 1.0
	if input.FeeBps == nil {
		input.FeeBps = &fee
	}
	if input.SlippageBps == nil {
		input.SlippageBps = &slip
	}
	for _, v := range []float64{*input.FeeBps, *input.SlippageBps} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1000 {
			return input, fmt.Errorf("fee/slippage bps must be between 0 and 1000")
		}
	}
	if input.MaxPosition.GreaterThan(input.Budget) || input.RiskPerTrade.GreaterThan(input.DailyLoss) {
		return input, fmt.Errorf("invalid backtest risk allocation")
	}
	return input, nil
}

// Backtest never calls exchanges or Ollaya. Decisions use previous completed
// candles only; fills are at the next open with adverse slippage and quote fees.
func Backtest(ctx context.Context, input BacktestInput) (BacktestResult, error) {
	in, err := backtestDefaults(input)
	if err != nil {
		return BacktestResult{}, err
	}
	cs := in.Candles
	if len(cs) < 61 || len(cs) > 50000 {
		return BacktestResult{}, fmt.Errorf("provide 61–50000 consecutive completed 1-minute candles")
	}
	for i, c := range cs {
		if err := ValidateCandle(c); err != nil {
			return BacktestResult{}, fmt.Errorf("candle %d: %w", i, err)
		}
		if i > 0 && c.CloseTime-cs[i-1].CloseTime != 60000 {
			return BacktestResult{}, fmt.Errorf("candles must be ordered, unique, and consecutive at 1-minute intervals")
		}
	}
	feeRate := decimal.NewFromFloat(*in.FeeBps).Div(decimal.NewFromInt(10000))
	slip := decimal.NewFromFloat(*in.SlippageBps).Div(decimal.NewFromInt(10000))
	result := BacktestResult{Strategy: "trend-pullback-v1-rules-only", StartingEquity: in.Budget, Trades: []BacktestTrade{}, Equity: []EquityPoint{}, Assumptions: []string{"No Ollaya or future data; 100-candle rolling feature window", "Signals at previous candle close, entries/reversal exits at next open", "Adverse slippage and quote-currency fees on both sides; no order-book/liquidity simulation", "Stops take priority if both stop and target touched; stop gaps execute at worse open", "Daily limit evaluated at bar open/close; cooldown one minute; end position liquidated", "OHLC bars cannot reproduce five-second decisions or live spread filters; exchange limits are supplied inputs"}}
	cash, qty, cost, stop, target := in.Budget, decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero
	peak, drawdown, dayEquity := in.Budget, decimal.Zero, in.Budget
	day := int64(-1)
	blocked := false
	cooldown := int64(0)
	var trade BacktestTrade
	wins := 0
	closePosition := func(price decimal.Decimal, at int64, reason string) {
		price = price.Mul(decimal.NewFromInt(1).Sub(slip))
		gross := qty.Mul(price)
		exitFee := gross.Mul(feeRate)
		cash = cash.Add(gross).Sub(exitFee)
		trade.Exit = price
		trade.ExitTime = at
		trade.PnL = gross.Sub(exitFee).Sub(cost)
		trade.Fees = trade.Fees.Add(exitFee)
		trade.Reason = reason
		result.TotalFees = result.TotalFees.Add(exitFee)
		result.Trades = append(result.Trades, trade)
		if trade.PnL.IsPositive() {
			wins++
		}
		qty = decimal.Zero
		cost = decimal.Zero
		cooldown = at + 60000
	}
	for i := 60; i < len(cs); i++ {
		if err := ctx.Err(); err != nil {
			return BacktestResult{}, err
		}
		bar := cs[i]
		open := decimal.NewFromFloat(bar.Open)
		openAt := bar.CloseTime - 59999
		equity := cash.Add(qty.Mul(open))
		today := openAt / 86400000
		if today != day {
			day = today
			dayEquity = equity
			blocked = false
		}
		if dayEquity.Sub(equity).GreaterThanOrEqual(in.DailyLoss) {
			blocked = true
		}
		f, err := CalculateFeatures(cs[max(0, i-100):i])
		if err != nil {
			return BacktestResult{}, err
		}
		if qty.IsPositive() {
			if open.LessThanOrEqual(stop) {
				closePosition(open, openAt, "stop_gap")
			} else if open.GreaterThanOrEqual(target) {
				closePosition(target, openAt, "target_gap")
			} else if blocked {
				closePosition(open, openAt, "daily_loss")
			} else if f.Reversal {
				closePosition(open, openAt, "trend_reversal")
			}
		} else if !blocked && openAt >= cooldown && f.Uptrend && f.Pullback {
			entry := open.Mul(decimal.NewFromInt(1).Add(slip))
			distance := decimal.NewFromFloat(f.ATR * 1.5)
			q := decimal.Min(in.MaxPosition.Div(entry), in.RiskPerTrade.Div(distance))
			q = decimal.Min(q, cash.Div(entry.Mul(decimal.NewFromInt(1).Add(decimal.Max(feeRate, decimal.NewFromFloat(.01))))))
			q = FloorStep(q, in.QuantityStep)
			if q.IsPositive() && q.Mul(entry).GreaterThanOrEqual(in.MinNotional) && entry.Sub(distance).IsPositive() {
				gross := q.Mul(entry)
				entryFee := gross.Mul(feeRate)
				cost = gross.Add(entryFee)
				cash = cash.Sub(cost)
				qty = q
				stop = entry.Sub(distance)
				target = entry.Add(distance.Mul(decimal.NewFromInt(2)))
				trade = BacktestTrade{EntryTime: openAt, Entry: entry, Quantity: q, Fees: entryFee}
				result.TotalFees = result.TotalFees.Add(entryFee)
			}
		}
		if qty.IsPositive() {
			if decimal.NewFromFloat(bar.Low).LessThanOrEqual(stop) {
				closePosition(stop, bar.CloseTime, "stop")
			} else if decimal.NewFromFloat(bar.High).GreaterThanOrEqual(target) {
				closePosition(target, bar.CloseTime, "target")
			}
		}
		equity = cash.Add(qty.Mul(decimal.NewFromFloat(bar.Close)))
		if dayEquity.Sub(equity).GreaterThanOrEqual(in.DailyLoss) {
			blocked = true
			if qty.IsPositive() {
				closePosition(decimal.NewFromFloat(bar.Close), bar.CloseTime, "daily_loss")
				equity = cash
			}
		}
		if i == len(cs)-1 && qty.IsPositive() {
			closePosition(decimal.NewFromFloat(bar.Close), bar.CloseTime, "end_of_data")
			equity = cash
		}
		peak = decimal.Max(peak, equity)
		drawdown = decimal.Max(drawdown, peak.Sub(equity).Div(peak).Mul(decimal.NewFromInt(100)))
		result.Equity = append(result.Equity, EquityPoint{bar.CloseTime, equity})
	}
	result.EndingEquity = cash
	result.NetPnL = cash.Sub(in.Budget)
	result.ReturnPct = result.NetPnL.Div(in.Budget).Mul(decimal.NewFromInt(100))
	result.MaxDrawdownPct = drawdown
	if len(result.Trades) > 0 {
		result.WinRate = float64(wins) / float64(len(result.Trades))
	}
	return result, nil
}
