package exporter

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Exporter fetches a protected bot endpoint, then exposes bounded, cached
// Prometheus text on a separate listener without giving it control credentials.
type Exporter struct {
	mu         sync.RWMutex
	client     *http.Client
	url, token string
	body       []byte
	at         time.Time
	up         bool
	errors     uint64
}

func (e *Exporter) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", e.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	resp, err := e.client.Do(req)
	var body []byte
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			err = fmt.Errorf("bot metrics HTTP %d", resp.StatusCode)
		} else {
			body, err = io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
			if len(body) == 0 || len(body) > 4<<20 {
				err = fmt.Errorf("metrics body empty or too large")
			}
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err != nil {
		e.up = false
		e.errors++
		return err
	}
	e.up = true
	e.at = time.Now()
	e.body = body
	return nil
}
func (e *Exporter) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		e.mu.RLock()
		defer e.mu.RUnlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		up := 0
		age := -1.0
		if !e.at.IsZero() {
			age = time.Since(e.at).Seconds()
		}
		if e.up && age >= 0 && age <= 15 {
			up = 1
		}
		fmt.Fprintf(w, "# HELP trader_exporter_up Whether recent bot metrics were fetched successfully\n# TYPE trader_exporter_up gauge\ntrader_exporter_up %d\n", up)
		fmt.Fprintf(w, "# HELP trader_exporter_snapshot_age_seconds Age of the last successful fetch, -1 before first fetch\n# TYPE trader_exporter_snapshot_age_seconds gauge\ntrader_exporter_snapshot_age_seconds %g\n", age)
		fmt.Fprintf(w, "# HELP trader_exporter_fetch_errors_total Failed metrics fetches\n# TYPE trader_exporter_fetch_errors_total counter\ntrader_exporter_fetch_errors_total %d\n", e.errors)
		if up == 1 {
			w.Write(e.body)
		} // Never present stale equity as current data.
	})
	return mux
}

// Run serves the metrics proxy until interrupted or its HTTP server fails.
func Run() error {
	token := os.Getenv("METRICS_TOKEN")
	if len(token) < 24 {
		return fmt.Errorf("exporter requires METRICS_TOKEN with at least 24 characters")
	}
	e := &Exporter{client: &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, url: strings.TrimRight(env("BOT_URL", "http://127.0.0.1:8080"), "/") + "/internal/metrics", token: token}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			if err := e.fetch(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("exporter fetch failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	server := &http.Server{Addr: env("EXPORTER_ADDR", "127.0.0.1:9091"), Handler: e.routes(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	slog.Info("exporter started", "listen", server.Addr)
	var err error
	select {
	case <-ctx.Done():
	case err = <-errCh:
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	<-done
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Healthcheck probes the endpoint configured by HEALTHCHECK_URL.
func Healthcheck() error {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(env("HEALTHCHECK_URL", "http://127.0.0.1:8080/healthz"))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("healthcheck HTTP %d", resp.StatusCode)
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
