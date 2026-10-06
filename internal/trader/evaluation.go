package trader

import (
	"context"
	"strings"
	"time"

	"automated-trader/internal/strategy"
	"github.com/shopspring/decimal"
)

type Evaluation struct {
	At             time.Time `json:"at"`
	ModelAt        time.Time `json:"model_at"`
	Action         string    `json:"action"`
	Latency        float64   `json:"model_latency_seconds"`
	Outcome        string    `json:"outcome"`
	Reason         string    `json:"reason"`
	OrderClientID  string    `json:"order_client_id,omitempty"`
	OrderAccepted  bool      `json:"order_accepted"`
	OrderUncertain bool      `json:"order_uncertain"`
}

type submissionObservation struct {
	ClientID            string
	Accepted, Uncertain bool
}

func (a *App) recordEvaluation(pair string, e Evaluation) {
	recorded := false
	err := a.update(context.Background(), "evaluation_outcome", map[string]any{"pair": pair, "evaluation": e}, func(s State) error {
		if s.Pairs[pair] == nil {
			return errNoChange
		}
		if e.ModelAt.IsZero() {
			previous := s.Evaluations[pair]
			e.ModelAt, e.Action, e.Latency = previous.ModelAt, previous.Action, previous.Latency
		}
		s.Evaluations[pair] = e
		recorded = true
		return nil
	})
	if err == nil && recorded && !e.OrderAccepted && !e.OrderUncertain {
		a.metrics.NoTrades.WithLabelValues(pair, e.Reason).Inc()
	}
}

func executionReason(err error) string {
	if definitiveRejection(err) {
		return "exchange_rejected"
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "balance"):
		return "insufficient_balance"
	case strings.Contains(text, "risk allocation"):
		return "risk_limit"
	case strings.Contains(text, "allocation"):
		return "insufficient_allocation"
	case strings.Contains(text, "outside exchange limits"):
		return "exchange_limits"
	case strings.Contains(text, "stale"):
		return "stale_quote"
	default:
		return "execution_error"
	}
}

func entryBlockReason(p *Position, c Config, m MarketSnapshot, f strategy.Features, signalsValid bool) string {
	switch {
	case !c.Trading:
		return "execution_disabled"
	case c.DailyLoss.IsPositive() && p.Day == time.Now().UTC().Format("2006-01-02") && freshMarket(m, p.Pair) && len(p.UnvaluedFees) == 0 && p.DayEquity.Sub(p.equity(m.Bid)).GreaterThanOrEqual(c.DailyLoss):
		return "daily_loss_limit"
	case p.Paused:
		return "paused"
	case p.Pending != nil:
		return "unresolved_order"
	case p.Qty.IsPositive():
		return "position_open"
	case p.Protection != nil:
		return "unresolved_protection"
	case len(p.UnvaluedFees) > 0:
		return "fees_unvalued"
	case !freshMarket(m, p.Pair):
		return "stale_quote"
	case !signalsValid:
		return "invalid_candles"
	case time.Now().Before(p.CooldownUntil):
		return "cooldown"
	case m.Ask.Sub(m.Bid).Div(m.Bid).Mul(decimal.NewFromInt(10000)).GreaterThan(decimal.NewFromInt(10)):
		return "wide_spread"
	case !p.Cash.Sub(p.ExternalFeeQuote).IsPositive():
		return "insufficient_allocation"
	case c.EntryPolicy != "ollaya" && !f.Uptrend:
		return "no_uptrend"
	case c.EntryPolicy != "ollaya" && !f.Pullback:
		return "no_pullback"
	case c.EntryPolicy != "ollaya" && f.SetupID == p.LastSetup:
		return "signal_consumed"
	default:
		return "none"
	}
}
