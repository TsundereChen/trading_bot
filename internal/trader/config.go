package trader

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"automated-trader/internal/strategy"

	"github.com/shopspring/decimal"
)

type Config struct {
	DecisionSeconds, ModelTimeoutSeconds, CooldownSeconds int
	EntryPolicy                                           string
	SignalInterval                                        string
	ContextInterval                                       string
	OllayaModel                                           string
	Database, Listen, OllayaURL, OllayaKey, ControlToken  string
	DatabaseURL, MetricsToken, BindingConfirmation        string
	Trading                                               bool
	PerPairBudget, MaxPosition, RiskPerTrade, DailyLoss   decimal.Decimal
	MaxPairs                                              int
	AuditDays, AuditMaxEvents                             int
	Venue                                                 Venue
	Pairs                                                 []string
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
		OllayaModel:         strings.TrimSpace(env("OLLAYA_MODEL", "winnow:e4b")),
		SignalInterval:      strings.TrimSpace(env("SIGNAL_INTERVAL", "1m")),
		ContextInterval:     strings.TrimSpace(env("CONTEXT_INTERVAL", "5m")),
		EntryPolicy:         env("ENTRY_POLICY", "ollaya"),
		ControlToken:        os.Getenv("CONTROL_TOKEN"),
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		Trading:             strings.EqualFold(os.Getenv("ENABLE_TRADING"), "true"),
		MaxPairs:            8,
		BindingConfirmation: os.Getenv("STATE_BINDING_CONFIRM"),
	}
	c.MetricsToken = os.Getenv("METRICS_TOKEN")
	if c.MetricsToken != "" && c.MetricsToken == c.ControlToken {
		return c, fmt.Errorf("METRICS_TOKEN must differ from CONTROL_TOKEN so the exporter cannot control trading")
	}
	if c.EntryPolicy != "pullback" && c.EntryPolicy != "ollaya" {
		return c, fmt.Errorf("ENTRY_POLICY must be pullback or ollaya")
	}
	for _, item := range []struct {
		key, fallback string
		dest          *int
		max           int
	}{
		{"DECISION_INTERVAL_SECONDS", "30", &c.DecisionSeconds, 300},
		{"OLLAYA_TIMEOUT_SECONDS", "20", &c.ModelTimeoutSeconds, 299},
		{"REENTRY_COOLDOWN_SECONDS", "10", &c.CooldownSeconds, 3600},
	} {
		n, err := strconv.Atoi(env(item.key, item.fallback))
		if err != nil || n < 1 || n > item.max {
			return c, fmt.Errorf("%s must be between 1 and %d", item.key, item.max)
		}
		*item.dest = n
	}
	if c.ModelTimeoutSeconds >= c.DecisionSeconds {
		return c, fmt.Errorf("OLLAYA_TIMEOUT_SECONDS must be less than DECISION_INTERVAL_SECONDS")
	}
	if c.SignalInterval != "1m" && c.SignalInterval != "5m" {
		return c, fmt.Errorf("SIGNAL_INTERVAL must be 1m or 5m")
	}
	if c.ContextInterval != "5m" {
		return c, fmt.Errorf("CONTEXT_INTERVAL must be 5m")
	}
	if c.OllayaModel == "" {
		return c, fmt.Errorf("OLLAYA_MODEL must not be blank")
	}
	pairs, err := parsePairs(env("TRADING_PAIRS", "BTCUSDT,BTCUSDC,ETHUSDT,ETHUSDC"))
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
		{"PER_PAIR_BUDGET", "4000", &c.PerPairBudget},
		{"MAX_POSITION_QUOTE", "20", &c.MaxPosition},
		{"RISK_PER_TRADE_QUOTE", "0.1", &c.RiskPerTrade},
		{"DAILY_LOSS_LIMIT_QUOTE", "400", &c.DailyLoss},
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
		valid := (strings.HasSuffix(pair, "USDT") || strings.HasSuffix(pair, "USDC")) && len(pair) >= 7 && len(pair) <= 32
		for _, c := range pair {
			if !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
				valid = false
			}
		}
		if !valid {
			return nil, fmt.Errorf("TRADING_PAIRS entries must be alphanumeric USDT/USDC spot pairs, got %q", pair)
		}
		if !seen[pair] {
			seen[pair] = true
			out = append(out, pair)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("TRADING_PAIRS must list at least one USDT/USDC spot pair")
	}
	return out, nil
}

func (c Config) decisionSeconds() int {
	if c.DecisionSeconds > 0 {
		return c.DecisionSeconds
	}
	return 30
}

func (c Config) modelTimeout() time.Duration {
	if c.ModelTimeoutSeconds > 0 {
		return time.Duration(c.ModelTimeoutSeconds) * time.Second
	}
	return 20 * time.Second
}

func (c Config) cooldown() time.Duration {
	if c.CooldownSeconds > 0 {
		return time.Duration(c.CooldownSeconds) * time.Second
	}
	return time.Minute
}

func supportedQuote(quote string) bool { return quote == "USDT" || quote == "USDC" }

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
