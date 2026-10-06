package trader

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Run loads configuration and durable state, then serves the bot until shutdown.
func Run() error {
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
	b := &Binance{client: client, key: os.Getenv("BINANCE_API_KEY"), secret: os.Getenv("BINANCE_API_SECRET"), venue: cfg.Venue, interval: cfg.SignalInterval}
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
		if !supportedQuote(symbol.Quote) || (cfg.Trading && !symbol.StopAllowed) {
			return fmt.Errorf("%s must support USDT/USDC spot trading and native STOP_LOSS", pair)
		}
		symbols[pair] = symbol
		ensurePerformance(state.Pairs[pair])
		if retainDust(state.Pairs[pair], symbol) {
			slog.Info("sub-step residual retained in dust ledger", "pair", pair, "quantity", state.Pairs[pair].DustQty.String())
		}
		state.Pairs[pair].Paused = true
	}
	if err := repo.Commit(ctx, state, "startup", map[string]any{"trading_enabled": cfg.Trading, "venue": cfg.Venue.Name, "account_id": state.AccountID, "model": cfg.OllayaModel}); err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	modelClient := &http.Client{Timeout: cfg.modelTimeout() + time.Second, CheckRedirect: client.CheckRedirect}
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	a := &App{cfg: cfg, repo: repo, binance: b, feeds: NewFeeds(state.symbols(), cfg.SignalInterval), client: modelClient, state: state, symbols: symbols, lastDecisions: map[string]any{}, accountID: accountID, metrics: newMetrics(reg)}
	server := &http.Server{Addr: cfg.Listen, Handler: a.routes(reg), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	slog.Info("trader started", "listen", cfg.Listen, "venue", cfg.Venue.Name, "trading_enabled", cfg.Trading, "entries", "paused until start command", "model", cfg.OllayaModel, "pairs", state.symbols())
	slog.Info("strategy configured", "signal_interval", cfg.SignalInterval, "entry_policy", cfg.EntryPolicy, "decision_interval_seconds", cfg.decisionSeconds(), "model_timeout_seconds", cfg.modelTimeout().Seconds(), "reentry_cooldown_seconds", cfg.cooldown().Seconds(), "monitor_interval_seconds", 5, "per_pair_budget", cfg.PerPairBudget.String(), "max_position", cfg.MaxPosition.String(), "risk_per_trade", cfg.RiskPerTrade.String(), "daily_loss_limit", cfg.DailyLoss.String())
	if cfg.Venue.Live {
		slog.Warn("LIVE VENUE: real funds are at risk", "base_url", cfg.Venue.BaseURL)
	}
	select {
	case <-ctx.Done():
	case err = <-errCh:
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	slog.Info("trader shutting down; draining workers and requests")
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
