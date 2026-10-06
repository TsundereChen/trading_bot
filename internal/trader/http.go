package trader

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"automated-trader/internal/strategy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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
		var input strategy.BacktestInput
		if err := decodeJSON(w, r, &input, 8<<20); err != nil {
			http.Error(w, "invalid backtest JSON", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		result, err := strategy.Backtest(ctx, input)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, result)
	}))
	return mux
}
