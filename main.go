package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shopspring/decimal"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func config() (Config, error) {
	c := Config{
		Database:     env("DATABASE_PATH", "data/trader.sqlite"),
		Listen:       env("HTTP_ADDR", "127.0.0.1:8080"),
		OllayaURL:    env("OLLAYA_URL", "http://127.0.0.1:11435"),
		OllayaKey:    os.Getenv("OLLAYA_API_KEY"),
		ControlToken: os.Getenv("CONTROL_TOKEN"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		Trading:      strings.EqualFold(os.Getenv("ENABLE_TRADING"), "true"),
		PaperMode:    strings.EqualFold(os.Getenv("PAPER_MODE"), "true"),
		MaxPairs:     8,
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

	venue, err := resolveVenue(c.PaperMode, os.Getenv("BINANCE_BASE_URL"))
	if err != nil {
		return c, err
	}
	c.Venue = venue

	if len(c.ControlToken) < 24 {
		return c, fmt.Errorf("CONTROL_TOKEN must be at least 24 characters")
	}
	if len(c.MetricsToken) < 24 {
		return c, fmt.Errorf("METRICS_TOKEN must be at least 24 characters")
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
		if err != nil || !d.IsPositive() {
			return c, fmt.Errorf("%s must be a positive decimal", item.key)
		}
		*item.dest = d
	}
	if c.MaxPosition.GreaterThan(c.PerPairBudget) || c.RiskPerTrade.GreaterThan(c.DailyLoss) {
		return c, fmt.Errorf("position exceeds per-pair budget or trade risk exceeds daily limit")
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
		if !strings.HasSuffix(pair, "USDT") || len(pair) < 7 {
			return nil, fmt.Errorf("TRADING_PAIRS entries must be USDT spot pairs, got %q", pair)
		}
		if seen[pair] {
			continue
		}
		seen[pair] = true
		out = append(out, pair)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("TRADING_PAIRS must list at least one USDT spot pair")
	}
	return out, nil
}

func (a *App) routes(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(v)
	}
	protected := func(handler http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(a.cfg.ControlToken)) != 1 {
				http.Error(w, "unauthorized", 401)
				return
			}
			handler(w, r)
		}
	}
	mux.HandleFunc("GET /internal/metrics", func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(a.cfg.MetricsToken)) != 1 || a.cfg.MetricsToken == "" {
			http.Error(w, "unauthorized", 401)
			return
		}
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, map[string]string{"status": "alive"}) })

	mux.HandleFunc("GET /api/status", protected(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		markets := map[string]MarketSnapshot{}
		for _, pair := range a.state.symbols() {
			markets[pair] = a.feeds.Snapshot(pair)
		}
		write(w, map[string]any{
			"venue":                  a.cfg.Venue.label(),
			"live_trading":           a.cfg.Venue.Live,
			"trading_enabled":        a.cfg.Trading,
			"state":                  a.state,
			"markets":                markets,
			"model":                  "winnow:e4b",
			"decision_cycle_seconds": decisionCycleSeconds(len(a.state.Pairs)),
			"last_decisions":         a.lastDecisions,
			"fatal":                  a.fatal,
		})
	}))
	mux.HandleFunc("GET /api/events", protected(func(w http.ResponseWriter, r *http.Request) {
		events, err := a.repo.Events(r.Context())
		if err != nil {
			http.Error(w, "storage error", 500)
			return
		}
		write(w, events)
	}))
	mux.HandleFunc("POST /api/control", protected(a.control))
	mux.HandleFunc("POST /api/backtest", protected(func(w http.ResponseWriter, r *http.Request) {
		var input BacktestInput
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid backtest JSON", 400)
			return
		}
		result, err := Backtest(r.Context(), input)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		write(w, result)
	}))
	return mux
}

// decisionCycleSeconds is the per-pair interval: one inference at a time, so N
// pairs each get a decision every N * decisionInterval seconds.
func decisionCycleSeconds(pairs int) int {
	if pairs < 1 {
		pairs = 1
	}
	return decisionIntervalSeconds * pairs
}

func (a *App) control(w http.ResponseWriter, r *http.Request) {
	writeJSON := func(v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(v)
	}
	var cmd struct {
		Action string   `json:"action"`
		Pair   string   `json:"pair,omitempty"`
		Pairs  []string `json:"pairs,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cmd) != nil {
		http.Error(w, "invalid command", 400)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fatal {
		http.Error(w, "storage fault; restart after repair", 409)
		return
	}
	s := a.state
	pair := strings.ToUpper(strings.TrimSpace(cmd.Pair))

	// Pause and close act on one pair, or on every pair when Pair is omitted.
	targets := []*Position{}
	switch cmd.Action {
	case "pause", "start", "close":
		if pair == "" {
			targets = a.positions(s)
		} else if p := s.Pairs[pair]; p != nil {
			targets = []*Position{p}
		} else {
			http.Error(w, "unknown pair", 404)
			return
		}
	}

	switch cmd.Action {
	case "pause":
		for _, p := range targets {
			p.Paused = true
		}
	case "start":
		for _, p := range targets {
			market := a.feeds.Snapshot(p.Pair)
			if p.Pending != nil || !market.Connected || time.Since(market.QuoteAt) > 3*time.Second {
				http.Error(w, "unresolved order or stale quote on "+p.Pair, 409)
				return
			}
			if p.DayEquity.Sub(p.equity(market.Bid)).GreaterThanOrEqual(a.cfg.DailyLoss) {
				http.Error(w, "daily loss limit reached on "+p.Pair, 409)
				return
			}
			if a.cfg.Trading {
				symbol, ok := a.symbolFor(p.Pair)
				if !ok {
					http.Error(w, "unknown pair rules for "+p.Pair, 409)
					return
				}
				balances, err := a.binance.Balances(r.Context())
				if err != nil || balances[symbol.Base].LessThan(p.Qty) {
					http.Error(w, "account reconciliation failed on "+p.Pair, 409)
					return
				}
			}
			p.Paused = false
			p.Error = ""
		}
	case "close":
		for _, p := range targets {
			if !a.cfg.Trading || p.Pending != nil || !p.Qty.IsPositive() {
				http.Error(w, "no closable position or execution disabled on "+p.Pair, 409)
				return
			}
			p.Paused = true
			if a.commit(r.Context(), s, "control", cmd) != nil {
				http.Error(w, "storage failure", 500)
				return
			}
			if err := a.submitLocked(r.Context(), p.Pair, "SELL", p.Qty, "manual_close"); err != nil {
				a.fail(r.Context(), p.Pair, "execution", err)
				http.Error(w, err.Error(), 409)
				return
			}
		}
	case "pairs":
		requested, err := normalizePairList(cmd.Pairs, cmd.Pair)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if len(requested) > a.cfg.MaxPairs {
			http.Error(w, fmt.Sprintf("at most %d pairs", a.cfg.MaxPairs), 400)
			return
		}
		for _, p := range a.positions(s) {
			if !containsString(requested, p.Pair) && (p.Qty.IsPositive() || p.Pending != nil) {
				http.Error(w, "close "+p.Pair+" before removing it", 409)
				return
			}
		}
		next := s.copy()
		symbols := map[string]Symbol{}
		for _, p := range requested {
			symbol, err := a.binance.Symbol(r.Context(), p)
			if err != nil || symbol.Quote != "USDT" {
				http.Error(w, "unsupported USDT spot pair "+p, 400)
				return
			}
			symbols[p] = symbol
			if next.Pairs[p] == nil {
				next.Pairs[p] = &Position{Pair: p, Paused: true, Cash: a.cfg.PerPairBudget}
			}
		}
		for _, p := range a.positions(next) {
			if !containsString(requested, p.Pair) {
				delete(next.Pairs, p.Pair)
			}
		}
		if a.commit(r.Context(), next, "control", map[string]any{"action": "pairs", "pairs": requested}) != nil {
			http.Error(w, "storage failure", 500)
			return
		}
		a.symbols = symbols
		a.feeds.SetPairs(requested)
		a.state = next
		writeJSON(map[string]any{"pairs": next.symbols()})
		return
	default:
		http.Error(w, "unknown action", 400)
		return
	}
	if a.commit(r.Context(), s, "control", cmd) != nil {
		http.Error(w, "storage failure", 500)
		return
	}
	writeJSON(s)
}

func normalizePairList(pairs []string, single string) ([]string, error) {
	if len(pairs) == 0 {
		if strings.TrimSpace(single) == "" {
			return nil, fmt.Errorf("provide pairs")
		}
		pairs = []string{single}
	}
	out, err := parsePairs(strings.Join(pairs, ","))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func run() error {
	cfg, err := config()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var repo Repository
	if cfg.DatabaseURL != "" {
		dbCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		repo, err = OpenPostgres(dbCtx, cfg.DatabaseURL)
		cancel()
	} else {
		if err = os.MkdirAll(filepath.Dir(cfg.Database), 0700); err != nil {
			return err
		}
		repo, err = OpenSQLite(cfg.Database)
	}
	if err != nil {
		return err
	}
	defer repo.Close()

	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	b := &Binance{client: client, key: os.Getenv("BINANCE_API_KEY"), secret: os.Getenv("BINANCE_API_SECRET"), venue: cfg.Venue}

	state, err := repo.Load(ctx)
	if err != nil {
		return err
	}
	retired, state := reconcilePairs(state, cfg.Pairs, cfg)
	if len(retired) > 0 {
		slog.Warn("persisted pairs are no longer configured; keeping them so positions are never silently dropped", "pairs", retired)
	}
	symbols := map[string]Symbol{}
	for _, pair := range state.symbols() {
		symbol, err := b.Symbol(ctx, pair)
		if err != nil {
			return fmt.Errorf("%s: %w", pair, err)
		}
		if symbol.Quote != "USDT" {
			return fmt.Errorf("%s is not a USDT spot pair", pair)
		}
		symbols[pair] = symbol
		state.Pairs[pair].Paused = true // Startup never resumes trading automatically.
	}
	if err = repo.Commit(ctx, state, "startup", map[string]any{"trading_enabled": cfg.Trading, "venue": cfg.Venue.Name, "model": "winnow:e4b"}); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	a := &App{
		cfg: cfg, repo: repo, binance: b, feeds: NewFeeds(state.symbols()),
		client: client, state: state, symbols: symbols, lastDecisions: map[string]any{},
		metrics: newMetrics(reg),
	}
	server := &http.Server{Addr: cfg.Listen, Handler: a.routes(reg), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()

	slog.Info("trader started", "listen", cfg.Listen, "venue", cfg.Venue.Name, "trading_enabled", cfg.Trading, "pairs", state.symbols(), "per_pair_cycle_seconds", decisionCycleSeconds(len(state.Pairs)))
	if cfg.Venue.Live {
		slog.Warn("LIVE VENUE: orders are sent to production with real funds at the operator's risk", "base_url", cfg.Venue.BaseURL)
	}
	select {
	case <-ctx.Done():
	case err = <-errCh:
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	<-done
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// reconcilePairs keeps persisted pairs so a restart never silently drops an open
// position, adds newly configured pairs with a fresh per-pair budget, and
// reports pairs that are no longer configured so the operator can retire them
// deliberately.
func reconcilePairs(state State, configured []string, cfg Config) ([]string, State) {
	if state.Pairs == nil {
		state.Pairs = map[string]*Position{}
	}
	retired := []string{}
	for pair, p := range state.Pairs {
		if p == nil {
			delete(state.Pairs, pair)
			continue
		}
		if p.Pair == "" {
			p.Pair = pair
		}
		if !containsString(configured, pair) {
			retired = append(retired, pair)
		}
	}
	sortStrings(retired)
	for _, pair := range configured {
		if state.Pairs[pair] == nil {
			// A brand-new pair starts paused with a fresh per-pair allocation.
			state.Pairs[pair] = &Position{Pair: pair, Paused: true, Cash: cfg.PerPairBudget}
		}
	}
	return retired, state
}

func main() {
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "exporter":
		err = runExporter()
	case len(os.Args) > 1 && os.Args[1] == "backtest":
		err = runBacktestCLI(os.Args[2:])
	case len(os.Args) > 1 && os.Args[1] == "healthcheck":
		err = healthcheck()
	case len(os.Args) == 1:
		err = run()
	default:
		err = fmt.Errorf("usage: trader [exporter|backtest --input file.json|healthcheck]")
	}
	if err != nil {
		slog.Error("trader stopped", "error", err)
		os.Exit(1)
	}
}
