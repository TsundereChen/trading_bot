package trader

import (
	"fmt"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

type Pending struct {
	ID              string                     `json:"id"`
	OrderID         int64                      `json:"order_id,omitempty"`
	Side            string                     `json:"side"`
	Qty             decimal.Decimal            `json:"quantity"`
	AppliedQty      decimal.Decimal            `json:"applied_quantity"`
	AppliedQuote    decimal.Decimal            `json:"applied_quote"`
	AppliedBaseFee  decimal.Decimal            `json:"applied_base_fee"`
	AppliedQuoteFee decimal.Decimal            `json:"applied_quote_fee"`
	AppliedFees     map[string]decimal.Decimal `json:"applied_fees,omitempty"`
}

type Position struct {
	Performance        *Performance               `json:"performance,omitempty"`
	DustQty            decimal.Decimal            `json:"dust_quantity"`
	DustCost           decimal.Decimal            `json:"dust_cost"`
	LastDecisionCandle int64                      `json:"last_decision_candle"`
	Pair               string                     `json:"pair"`
	Version            uint64                     `json:"version"`
	Paused             bool                       `json:"paused"`
	Starting           bool                       `json:"starting"`
	Cash               decimal.Decimal            `json:"cash"`
	Qty                decimal.Decimal            `json:"quantity"`
	Cost               decimal.Decimal            `json:"cost"`
	Stop               decimal.Decimal            `json:"stop"`
	Target             decimal.Decimal            `json:"target"`
	Pending            *Pending                   `json:"pending"`
	Protection         *Pending                   `json:"native_stop"`
	ExternalFees       map[string]decimal.Decimal `json:"external_fees,omitempty"`
	UnvaluedFees       map[string]decimal.Decimal `json:"unvalued_fees,omitempty"`
	ExternalFeeQuote   decimal.Decimal            `json:"external_fee_quote_estimate"`
	LastSetup          int64                      `json:"last_setup"`
	Day                string                     `json:"day"`
	DayEquity          decimal.Decimal            `json:"day_start_equity"`
	CooldownUntil      time.Time                  `json:"cooldown_until"`
	Error              string                     `json:"error"`
}

func (p Position) equity(bid decimal.Decimal) decimal.Decimal {
	return p.Cash.Add(p.Qty.Add(p.DustQty).Mul(bid)).Sub(p.ExternalFeeQuote)
}

type State struct {
	Evaluations map[string]Evaluation `json:"evaluations,omitempty"`
	Venue       string                `json:"venue"`
	AccountID   string                `json:"account_id,omitempty"`
	Version     uint64                `json:"version"`
	Pairs       map[string]*Position  `json:"pairs"`
}

func copyDecimals(in map[string]decimal.Decimal) map[string]decimal.Decimal {
	if in == nil {
		return nil
	}
	out := make(map[string]decimal.Decimal, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyPending(p *Pending) *Pending {
	if p == nil {
		return nil
	}
	out := *p
	out.AppliedFees = copyDecimals(p.AppliedFees)
	return &out
}

func (s State) copy() State {
	out := s
	out.Pairs = make(map[string]*Position, len(s.Pairs))
	out.Evaluations = make(map[string]Evaluation, len(s.Evaluations))
	for pair, evaluation := range s.Evaluations {
		out.Evaluations[pair] = evaluation
	}
	for pair, p := range s.Pairs {
		if p == nil {
			continue
		}
		clone := *p
		if p.Performance != nil {
			e := *p.Performance
			e.Fees = copyDecimals(e.Fees)
			if e.Cycle != nil {
				c := *e.Cycle
				e.Cycle = &c
			}
			clone.Performance = &e
		}
		clone.Pending, clone.Protection = copyPending(p.Pending), copyPending(p.Protection)
		clone.ExternalFees, clone.UnvaluedFees = copyDecimals(p.ExternalFees), copyDecimals(p.UnvaluedFees)
		out.Pairs[pair] = &clone
	}
	return out
}

func (s State) symbols() []string {
	out := make([]string, 0, len(s.Pairs))
	for pair := range s.Pairs {
		out = append(out, pair)
	}
	sort.Strings(out)
	return out
}

// Identity is bound before feeds or execution start. Adopting an old database
// requires an explicit attestation; known venue/account mismatches never do.
func bindState(s State, venue, accountID, confirmation string) (State, error) {
	if s.Venue != "" && s.Venue != venue {
		return s, fmt.Errorf("state belongs to venue %s, not %s; use a separate database", s.Venue, venue)
	}
	if s.AccountID != "" && accountID != "" && s.AccountID != accountID {
		return s, fmt.Errorf("state belongs to a different exchange account; use a separate database")
	}
	if s.Venue == "" && len(s.Pairs) > 0 && confirmation != venue+":"+accountID {
		return s, fmt.Errorf("unbound legacy state: verify its original venue/account, then set STATE_BINDING_CONFIRM=%s:%s", venue, accountID)
	}
	if accountID != "" && s.AccountID == "" {
		for _, p := range s.Pairs {
			if p != nil && (p.Qty.IsPositive() || p.DustQty.IsPositive() || p.Pending != nil || p.Protection != nil) && confirmation != venue+":"+accountID {
				return s, fmt.Errorf("existing exposure has no account binding; operator verification is required")
			}
		}
		s.AccountID = accountID
	}
	s.Venue = venue
	return s, nil
}

// reconcilePairs retains persisted exposure until it is explicitly retired.
func reconcilePairs(state State, configured []string, cfg Config) ([]string, State) {
	state = state.copy()
	retired := []string{}
	for pair, p := range state.Pairs {
		if p.Pair == "" {
			p.Pair = pair
		}
		if !containsString(configured, pair) {
			retired = append(retired, pair)
		}
	}
	sort.Strings(retired)
	for _, pair := range configured {
		if state.Pairs[pair] == nil {
			state.Pairs[pair] = &Position{Pair: pair, Paused: true, Cash: cfg.PerPairBudget}
		}
	}
	return retired, state
}
