package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	c := Config{Database: env("DATABASE_PATH", "data/trader.sqlite"), Listen: env("HTTP_ADDR", "127.0.0.1:8080"), OllayaURL: env("OLLAYA_URL", "http://127.0.0.1:11435"), OllayaKey: os.Getenv("OLLAYA_API_KEY"), ControlToken: os.Getenv("CONTROL_TOKEN"), DatabaseURL: os.Getenv("DATABASE_URL"), Trading: strings.EqualFold(os.Getenv("ENABLE_TRADING"), "true"), MaxPairs: 8, BindingConfirmation: os.Getenv("STATE_BINDING_CONFIRM")}
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
		{"PER_PAIR_BUDGET", "1000", &c.PerPairBudget}, {"MAX_POSITION_QUOTE", "100", &c.MaxPosition}, {"RISK_PER_TRADE_QUOTE", "2.5", &c.RiskPerTrade}, {"DAILY_LOSS_LIMIT_QUOTE", "10", &c.DailyLoss},
	} {
		d, err := decimal.NewFromString(env(item.key, item.fallback))
		if err != nil || !d.IsPositive() || !boundedDecimal(d) {
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
		{"AUDIT_RETENTION_DAYS", "30", &c.AuditDays, 3650}, {"AUDIT_MAX_EVENTS", "100000", &c.AuditMaxEvents, 10000000},
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

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any, limit int64) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("body must contain exactly one JSON value")
	}
	return nil
}

func authenticated(r *http.Request, token string) bool {
	value := r.Header.Get("Authorization")
	return token != "" && strings.HasPrefix(value, "Bearer ") && subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(value, "Bearer ")), []byte(token)) == 1
}

func (a *App) routes(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	protected := func(handler http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !authenticated(r, a.cfg.ControlToken) {
				http.Error(w, "unauthorized", 401)
				return
			}
			a.mu.Lock()
			if a.closing {
				a.mu.Unlock()
				http.Error(w, "shutting down", 503)
				return
			}
			a.requests.Add(1)
			a.mu.Unlock()
			defer a.requests.Done()
			handler(w, r)
		}
	}
	metrics := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	mux.HandleFunc("GET /internal/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !authenticated(r, a.cfg.MetricsToken) {
			http.Error(w, "unauthorized", 401)
			return
		}
		a.refreshMetrics()
		metrics.ServeHTTP(w, r)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /api/status", protected(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		state, fatal := a.state.copy(), a.fatal
		decisions := map[string]any{}
		for pair, result := range a.lastDecisions {
			decisions[pair] = result
		}
		a.mu.Unlock()
		markets := map[string]MarketSnapshot{}
		for _, pair := range state.symbols() {
			markets[pair] = a.feeds.Snapshot(pair)
		}
		writeJSON(w, map[string]any{"venue": a.cfg.Venue.label(), "live_trading": a.cfg.Venue.Live, "trading_enabled": a.cfg.Trading, "state": state, "markets": markets, "model": "winnow:e4b", "decision_cycle_seconds": decisionCycleSeconds(len(state.Pairs)), "last_decisions": decisions, "fatal": fatal})
	}))
	mux.HandleFunc("GET /api/events", protected(func(w http.ResponseWriter, r *http.Request) {
		events, err := a.repo.Events(r.Context())
		if err != nil {
			http.Error(w, "storage error", 500)
			return
		}
		writeJSON(w, events)
	}))
	mux.HandleFunc("POST /api/control", protected(a.control))
	mux.HandleFunc("POST /api/backtest", protected(func(w http.ResponseWriter, r *http.Request) {
		if !a.backtestMu.TryLock() {
			http.Error(w, "a backtest is already running", 503)
			return
		}
		defer a.backtestMu.Unlock()
		var input BacktestInput
		if err := decodeJSON(w, r, &input, 8<<20); err != nil {
			http.Error(w, "invalid backtest JSON", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		result, err := Backtest(ctx, input)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, result)
	}))
	return mux
}

func decisionCycleSeconds(pairs int) int {
	if pairs < 1 {
		pairs = 1
	}
	return decisionIntervalSeconds * pairs
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
	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	b := &Binance{client: client, key: os.Getenv("BINANCE_API_KEY"), secret: os.Getenv("BINANCE_API_SECRET"), venue: cfg.Venue}
	state, err := repo.Load(ctx)
	if err != nil {
		return err
	}
	// Reject a known venue mismatch before even authenticating to a new venue.
	if state.Venue != "" && state.Venue != cfg.Venue.Name {
		return fmt.Errorf("state belongs to another venue; use a separate database")
	}
	accountID := ""
	if cfg.Trading {
		if err := b.CheckPermissions(ctx); err != nil {
			return err
		}
		account, err := b.Account(ctx)
		if err != nil {
			return err
		}
		if account.UID <= 0 {
			return fmt.Errorf("exchange did not return a verifiable account UID")
		}
		accountID = strconv.FormatInt(account.UID, 10)
		b.accountID = accountID
	}
	state, err = bindState(state, cfg.Venue.Name, accountID, cfg.BindingConfirmation)
	if err != nil {
		return err
	}
	retired, state := reconcilePairs(state, cfg.Pairs, cfg)
	if len(retired) > 0 {
		slog.Warn("keeping persisted pairs until positions are deliberately retired", "pairs", retired)
	}
	if len(state.Pairs) > cfg.MaxPairs {
		slog.Warn("retained positions exceed pair limit; close and retire old pairs before enabling new ones", "pairs", len(state.Pairs))
	}
	symbols := map[string]Symbol{}
	for _, pair := range state.symbols() {
		symbol, err := b.Symbol(ctx, pair)
		if err != nil {
			return fmt.Errorf("%s: %w", pair, err)
		}
		if symbol.Quote != "USDT" || (cfg.Trading && !symbol.StopAllowed) {
			return fmt.Errorf("%s must support USDT spot trading and native STOP_LOSS", pair)
		}
		symbols[pair] = symbol
		state.Pairs[pair].Paused = true
	}
	if err := repo.Commit(ctx, state, "startup", map[string]any{"trading_enabled": cfg.Trading, "venue": cfg.Venue.Name, "account_id": state.AccountID, "model": "winnow:e4b"}); err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	a := &App{cfg: cfg, repo: repo, binance: b, feeds: NewFeeds(state.symbols()), client: client, state: state, symbols: symbols, lastDecisions: map[string]any{}, accountID: accountID, metrics: newMetrics(reg)}
	server := &http.Server{Addr: cfg.Listen, Handler: a.routes(reg), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	slog.Info("trader started", "listen", cfg.Listen, "venue", cfg.Venue.Name, "trading_enabled", cfg.Trading, "pairs", state.symbols())
	if cfg.Venue.Live {
		slog.Warn("LIVE VENUE: real funds are at risk", "base_url", cfg.Venue.BaseURL)
	}
	select {
	case <-ctx.Done():
	case err = <-errCh:
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.mu.Lock()
	a.closing = true
	a.mu.Unlock()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
	}
	<-done
	// Accepted orders finish detached settlement/protection even after their
	// caller is disconnected. Keep the repository/lease alive until they finish.
	a.requests.Wait()
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

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
	sortStrings(retired)
	for _, pair := range configured {
		if state.Pairs[pair] == nil {
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
