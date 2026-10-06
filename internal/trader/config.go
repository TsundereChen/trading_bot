package trader

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"automated-trader/internal/strategy"

	"github.com/shopspring/decimal"
)

type Config struct {
	Database, Listen, OllayaURL, OllayaKey, ControlToken string
	DatabaseURL, MetricsToken, BindingConfirmation       string
	Trading                                              bool
	PerPairBudget, MaxPosition, RiskPerTrade, DailyLoss  decimal.Decimal
	MaxPairs                                             int
	AuditDays, AuditMaxEvents                            int
	Venue                                                Venue
	Pairs                                                []string
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func config() (Config, error) {
	c := Config{
		Database:            env("DATABASE_PATH", "data/trader.sqlite"),
		Listen:              env("HTTP_ADDR", "127.0.0.1:8080"),
		OllayaURL:           env("OLLAYA_URL", "http://127.0.0.1:11435"),
		OllayaKey:           os.Getenv("OLLAYA_API_KEY"),
		ControlToken:        os.Getenv("CONTROL_TOKEN"),
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		Trading:             strings.EqualFold(os.Getenv("ENABLE_TRADING"), "true"),
		MaxPairs:            8,
		BindingConfirmation: os.Getenv("STATE_BINDING_CONFIRM"),
	}
	c.MetricsToken = env("METRICS_TOKEN", c.ControlToken)
	pairs, err := parsePairs(env("TRADING_PAIRS", "BTCUSDT"))
	if err != nil {
		return c, err
	}
	if len(pairs) > c.MaxPairs {
		return c, fmt.Errorf("TRADING_PAIRS lists %d pairs; the limit is %d", len(pairs), c.MaxPairs)
	}
	c.Pairs = pairs
	c.Venue, err = resolveVenue(os.Getenv("BINANCE_BASE_URL"))
	if err != nil {
		return c, err
	}
	for _, token := range []struct{ name, value string }{{"CONTROL_TOKEN", c.ControlToken}, {"METRICS_TOKEN", c.MetricsToken}} {
		if len(token.value) < 24 || strings.HasPrefix(token.value, "replace-with-") {
			return c, fmt.Errorf("%s must be a generated secret with at least 24 characters", token.name)
		}
	}
	for _, item := range []struct {
		key, fallback string
		dest          *decimal.Decimal
	}{
		{"PER_PAIR_BUDGET", "1000", &c.PerPairBudget},
		{"MAX_POSITION_QUOTE", "100", &c.MaxPosition},
		{"RISK_PER_TRADE_QUOTE", "2.5", &c.RiskPerTrade},
		{"DAILY_LOSS_LIMIT_QUOTE", "10", &c.DailyLoss},
	} {
		d, err := decimal.NewFromString(env(item.key, item.fallback))
		if err != nil || !d.IsPositive() || !strategy.BoundedDecimal(d) {
			return c, fmt.Errorf("%s must be a bounded positive decimal", item.key)
		}
		*item.dest = d
	}
	if c.MaxPosition.GreaterThan(c.PerPairBudget) || c.RiskPerTrade.GreaterThan(c.DailyLoss) {
		return c, fmt.Errorf("position exceeds per-pair budget or trade risk exceeds daily limit")
	}
	for _, item := range []struct {
		key, fallback string
		dest          *int
		max           int
	}{
		{"AUDIT_RETENTION_DAYS", "30", &c.AuditDays, 3650},
		{"AUDIT_MAX_EVENTS", "100000", &c.AuditMaxEvents, 10000000},
	} {
		n, err := strconv.Atoi(env(item.key, item.fallback))
		if err != nil || n < 1 || n > item.max {
			return c, fmt.Errorf("%s must be between 1 and %d", item.key, item.max)
		}
		*item.dest = n
	}
	if c.Trading && (os.Getenv("BINANCE_API_KEY") == "" || os.Getenv("BINANCE_API_SECRET") == "") {
		return c, fmt.Errorf("ENABLE_TRADING requires both BINANCE_API_KEY and BINANCE_API_SECRET")
	}
	return c, nil
}

func parsePairs(raw string) ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	for _, field := range strings.Split(raw, ",") {
		pair := strings.ToUpper(strings.TrimSpace(field))
		if pair == "" {
			continue
		}
		valid := strings.HasSuffix(pair, "USDT") && len(pair) >= 7 && len(pair) <= 32
		for _, c := range pair {
			if !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
				valid = false
			}
		}
		if !valid {
			return nil, fmt.Errorf("TRADING_PAIRS entries must be alphanumeric USDT spot pairs, got %q", pair)
		}
		if !seen[pair] {
			seen[pair] = true
			out = append(out, pair)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("TRADING_PAIRS must list at least one USDT spot pair")
	}
	return out, nil
}

func normalizePairList(pairs []string, single string) ([]string, error) {
	if len(pairs) == 0 {
		if strings.TrimSpace(single) == "" {
			return nil, fmt.Errorf("provide pairs")
		}
		pairs = []string{single}
	}
	return parsePairs(strings.Join(pairs, ","))
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
