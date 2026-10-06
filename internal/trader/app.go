package trader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type App struct {
	mu              sync.Mutex // Only in-memory snapshots/publication; never exchange or storage I/O.
	writeMu         sync.Mutex // Serializes durable state transactions, not exchange execution.
	pairLocks       sync.Map
	backtestMu      sync.Mutex
	requests        sync.WaitGroup
	closing         bool
	cfg             Config
	repo            Repository
	binance         *Binance
	feeds           *Feeds
	client          *http.Client
	state           State
	symbols         map[string]Symbol
	lastDecisions   map[string]any
	reconnectCounts map[string]uint64
	accountID       string
	metrics         Metrics
	fatal           bool
}

var errNoChange = errors.New("no state change")

func (a *App) snapshot() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state.copy()
}

func (a *App) symbolFor(pair string) (Symbol, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	symbol, ok := a.symbols[pair]
	return symbol, ok
}

func (a *App) pairLock(pair string) *sync.Mutex {
	value, _ := a.pairLocks.LoadOrStore(pair, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func (a *App) executionAllowed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.Trading && !a.fatal && a.binance != nil && a.binance.venue == a.cfg.Venue && a.accountID != "" && a.state.AccountID == a.accountID && a.state.Venue == a.cfg.Venue.Name
}

func (a *App) persist(ctx context.Context, next State, kind string, data any, publish func()) error {
	storageCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := a.repo.Commit(storageCtx, next, kind, data); err != nil {
		a.mu.Lock()
		a.fatal = true
		for _, p := range a.state.Pairs {
			p.Paused = true
			p.Error = "Persistence failure: " + err.Error()
		}
		a.mu.Unlock()
		a.metrics.Failures.WithLabelValues("storage").Inc()
		slog.Error("state persistence failed; entries paused")
		return err
	}
	a.mu.Lock()
	a.state = next
	if publish != nil {
		publish()
	}
	a.mu.Unlock()
	switch kind {
	case "startup", "control", "order_intent", "order_rejected", "order_update", "native_stop_intent", "daily_loss_limit":
		slog.Info("state committed", "event", kind, "version", next.Version)
	}
	return nil
}

// commit is for initialization/tests. Runtime callers use update so they cannot
// overwrite another pair's state using an old whole-portfolio snapshot.
func (a *App) commit(ctx context.Context, s State, kind string, data any) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.persist(ctx, s.copy(), kind, data, nil)
}

func (a *App) update(ctx context.Context, kind string, data any, change func(State) error) error {
	return a.updateWith(ctx, kind, data, change, nil)
}

func (a *App) updateWith(ctx context.Context, kind string, data any, change func(State) error, publish func()) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	if a.fatal {
		a.mu.Unlock()
		return fmt.Errorf("storage fault; restart after repair")
	}
	old := a.state.copy()
	a.mu.Unlock()
	next := old.copy()
	if err := change(next); err != nil {
		if errors.Is(err, errNoChange) {
			return nil
		}
		return err
	}
	next.Version = old.Version + 1
	for pair, p := range next.Pairs {
		if previous := old.Pairs[pair]; previous == nil || !executionPositionEqual(previous, p) {
			p.Version = next.Version
		}
	}
	return a.persist(ctx, next, kind, data, publish)
}

func (a *App) fail(ctx context.Context, pair, component string, err error) {
	slog.Error("pair fault; entries paused", "pair", pair, "component", component)
	a.metrics.Failures.WithLabelValues(component).Inc()
	_ = a.update(ctx, "fault", map[string]string{"pair": pair, "component": component, "error": err.Error()}, func(s State) error {
		changed := false
		for name, p := range s.Pairs {
			if pair != "" && pair != name {
				continue
			}
			message := component + ": " + err.Error()
			if !p.Paused || p.Error != message {
				changed = true
			}
			p.Paused, p.Error = true, message
		}
		if !changed {
			return errNoChange
		}
		return nil
	})
}
