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
	c := Config{Pair: strings.ToUpper(env("TRADING_PAIR", "BTCUSDT")), Database: env("DATABASE_PATH", "data/trader.sqlite"), Listen: env("HTTP_ADDR", "127.0.0.1:8080"), OllayaURL: env("OLLAYA_URL", "http://127.0.0.1:11435"), OllayaKey: os.Getenv("OLLAYA_API_KEY"), ControlToken: os.Getenv("CONTROL_TOKEN"), Trading: os.Getenv("ENABLE_TESTNET_TRADING") == "true"}
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	c.MetricsToken = env("METRICS_TOKEN", c.ControlToken)
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
		{"PAPER_BUDGET", "1000", &c.Budget}, {"MAX_POSITION_QUOTE", "100", &c.MaxPosition}, {"RISK_PER_TRADE_QUOTE", "2.5", &c.RiskPerTrade}, {"DAILY_LOSS_LIMIT_QUOTE", "10", &c.DailyLoss},
	} {
		d, err := decimal.NewFromString(env(item.key, item.fallback))
		if err != nil || !d.IsPositive() {
			return c, fmt.Errorf("%s must be a positive decimal", item.key)
		}
		*item.dest = d
	}
	if c.MaxPosition.GreaterThan(c.Budget) || c.RiskPerTrade.GreaterThan(c.DailyLoss) {
		return c, fmt.Errorf("position exceeds budget or trade risk exceeds daily limit")
	}
	if c.Trading && (os.Getenv("BINANCE_TESTNET_API_KEY") == "" || os.Getenv("BINANCE_TESTNET_API_SECRET") == "") {
		return c, fmt.Errorf("testnet trading requires both Binance testnet credentials")
	}
	return c, nil
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
		write(w, map[string]any{"state": a.state, "bid": a.bid, "ask": a.ask, "market": a.market.Snapshot(), "testnet_trading_enabled": a.cfg.Trading, "model": "winnow:e4b", "last_decision": a.lastDecision, "fatal": a.fatal})
	}))
	mux.HandleFunc("GET /api/events", protected(func(w http.ResponseWriter, r *http.Request) {
		events, err := a.repo.Events(r.Context())
		if err != nil {
			http.Error(w, "storage error", 500)
			return
		}
		write(w, events)
	}))
	mux.HandleFunc("POST /api/control", protected(func(w http.ResponseWriter, r *http.Request) {
		var cmd struct {
			Action string `json:"action"`
			Pair   string `json:"pair"`
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
		switch cmd.Action {
		case "pause":
			s.Paused = true
		case "start":
			if s.Pending != nil || time.Since(a.bookAt) > 3*time.Second {
				http.Error(w, "unresolved order or stale quote", 409)
				return
			}
			if s.DayEquity.Sub(s.Cash.Add(s.Qty.Mul(a.bid))).GreaterThanOrEqual(a.cfg.DailyLoss) {
				http.Error(w, "daily loss limit reached", 409)
				return
			}
			if a.cfg.Trading {
				balances, err := a.binance.Balances(r.Context())
				if err != nil || balances[a.symbol.Base].LessThan(s.Qty) {
					http.Error(w, "account reconciliation failed", 409)
					return
				}
			}
			s.Paused = false
			s.Error = ""
		case "pair":
			if !s.Paused || s.Qty.IsPositive() || s.Pending != nil {
				http.Error(w, "pause and close position before changing pair", 409)
				return
			}
			pair := strings.ToUpper(strings.TrimSpace(cmd.Pair))
			symbol, err := a.binance.Symbol(r.Context(), pair)
			if err != nil || symbol.Quote != "USDT" {
				http.Error(w, "v1 supports tradable USDT spot pairs only", 400)
				return
			}
			s.Pair = pair
			s.LastSetup = 0
			if a.commit(r.Context(), s, "control", cmd) != nil {
				http.Error(w, "storage failure", 500)
				return
			}
			a.symbol = symbol
			a.bookAt = time.Time{}
			a.market.SetPair(pair)
			write(w, s)
			return
		case "close":
			if !a.cfg.Trading || s.Pending != nil || !s.Qty.IsPositive() {
				http.Error(w, "no closable position or execution disabled", 409)
				return
			}
			s.Paused = true
			if a.commit(r.Context(), s, "control", cmd) != nil {
				http.Error(w, "storage failure", 500)
				return
			}
			if err := a.submit(r.Context(), "SELL", s.Qty, "manual_close"); err != nil {
				a.fail(r.Context(), "execution", err)
				http.Error(w, err.Error(), 409)
				return
			}
			write(w, a.state)
			return
		default:
			http.Error(w, "unknown action", 400)
			return
		}
		if a.commit(r.Context(), s, "control", cmd) != nil {
			http.Error(w, "storage failure", 500)
			return
		}
		write(w, s)
	}))
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
	b := &Binance{client: client, key: os.Getenv("BINANCE_TESTNET_API_KEY"), secret: os.Getenv("BINANCE_TESTNET_API_SECRET")}
	state, err := repo.Load(ctx)
	if err != nil {
		return err
	}
	if state.Pair == "" {
		state = State{Pair: cfg.Pair, Cash: cfg.Budget}
	}
	state.Paused = true // Startup never resumes trading automatically.
	symbol, err := b.Symbol(ctx, state.Pair)
	if err != nil {
		return err
	}
	if symbol.Quote != "USDT" {
		return fmt.Errorf("v1 requires USDT quote pairs")
	}
	if err = repo.Commit(ctx, state, "startup", map[string]any{"trading_enabled": cfg.Trading, "model": "winnow:e4b"}); err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	a := &App{cfg: cfg, repo: repo, binance: b, market: &Market{pair: state.Pair}, client: client, state: state, symbol: symbol, metrics: newMetrics(reg)}
	server := &http.Server{Addr: cfg.Listen, Handler: a.routes(reg), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	slog.Info("trader started", "listen", cfg.Listen, "testnet_trading", cfg.Trading, "pair", state.Pair, "paused", true)
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
