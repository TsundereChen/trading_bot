package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type controlCommand struct {
	Action string   `json:"action"`
	Pair   string   `json:"pair,omitempty"`
	Pairs  []string `json:"pairs,omitempty"`
}

func controlTargets(s State, pair string) ([]string, error) {
	if pair == "" {
		return s.symbols(), nil
	}
	if s.Pairs[pair] == nil {
		return nil, fmt.Errorf("unknown pair")
	}
	return []string{pair}, nil
}

func removablePairs(s State, requested []string) error {
	for pair, p := range s.Pairs {
		if !containsString(requested, pair) && (p.Qty.IsPositive() || p.Pending != nil || p.Protection != nil || len(p.UnvaluedFees) > 0) {
			return fmt.Errorf("close and reconcile %s before removing it", pair)
		}
	}
	return nil
}

func (a *App) control(w http.ResponseWriter, r *http.Request) {
	var cmd controlCommand
	if err := decodeJSON(w, r, &cmd, 4096); err != nil {
		http.Error(w, "invalid command", 400)
		return
	}
	pair := strings.ToUpper(strings.TrimSpace(cmd.Pair))
	snapshot := a.snapshot()
	a.mu.Lock()
	fatal := a.fatal
	a.mu.Unlock()
	if fatal {
		http.Error(w, "storage fault; restart after repair", 409)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if cmd.Action == "pairs" {
		requested, err := normalizePairList(cmd.Pairs, cmd.Pair)
		if err != nil || len(requested) > a.cfg.MaxPairs {
			http.Error(w, fmt.Sprintf("provide at most %d valid pairs", a.cfg.MaxPairs), 400)
			return
		}
		if err := removablePairs(snapshot, requested); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		symbols := map[string]Symbol{}
		for _, name := range requested {
			symbol, err := a.binance.Symbol(ctx, name)
			if err != nil || symbol.Quote != "USDT" || (a.cfg.Trading && !symbol.StopAllowed) {
				http.Error(w, "unsupported USDT spot/native-stop pair "+name, 400)
				return
			}
			symbols[name] = symbol
		}
		err = a.updateWith(ctx, "control", cmd, func(s State) error {
			if err := removablePairs(s, requested); err != nil {
				return err
			}
			for name := range s.Pairs {
				if !containsString(requested, name) {
					delete(s.Pairs, name)
				}
			}
			for _, name := range requested {
				if s.Pairs[name] == nil {
					s.Pairs[name] = &Position{Pair: name, Paused: true, Cash: a.cfg.PerPairBudget}
				}
			}
			return nil
		}, func() {
			for name := range a.symbols {
				if !containsString(requested, name) {
					a.removeMetrics(name)
				}
			}
			a.symbols = symbols
			a.feeds.SetPairs(requested)
		})
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		writeJSON(w, map[string]any{"pairs": a.snapshot().symbols()})
		return
	}
	if cmd.Action != "pause" && cmd.Action != "start" && cmd.Action != "close" {
		http.Error(w, "unknown action", 400)
		return
	}
	targets, err := controlTargets(snapshot, pair)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	balances := map[string]decimal.Decimal{}
	if cmd.Action == "start" && a.cfg.Trading {
		if !a.executionAllowed() {
			http.Error(w, "execution identity unverified", 409)
			return
		}
		balances, err = a.binance.Holdings(ctx)
		if err != nil {
			http.Error(w, "account reconciliation failed", 409)
			return
		}
	}
	if cmd.Action == "close" && !a.cfg.Trading {
		http.Error(w, "execution disabled", 409)
		return
	}
	err = a.update(ctx, "control", cmd, func(s State) error {
		// Validate every target against the latest state before changing any one.
		for _, name := range targets {
			p := s.Pairs[name]
			if p == nil {
				return fmt.Errorf("pair was removed")
			}
			if cmd.Action != "start" {
				continue
			}
			if p.Version != snapshot.Pairs[name].Version {
				return fmt.Errorf("state changed during start validation; retry")
			}
			m := a.feeds.Snapshot(name)
			if p.Pending != nil || !freshMarket(m, name) || len(p.UnvaluedFees) > 0 {
				return fmt.Errorf("unresolved order, fee valuation, or stale quote on %s", name)
			}
			day := time.Now().UTC().Format("2006-01-02")
			if p.Day == day && p.DayEquity.Sub(p.equity(m.Bid)).GreaterThanOrEqual(a.cfg.DailyLoss) {
				return fmt.Errorf("daily loss limit reached on %s", name)
			}
			if a.cfg.Trading {
				symbol, ok := a.symbolFor(name)
				if !ok || !symbol.StopAllowed || balances[symbol.Base].LessThan(p.Qty) {
					return fmt.Errorf("account or exchange rule reconciliation failed on %s", name)
				}
				if p.Qty.IsPositive() && (p.Protection == nil || p.Protection.OrderID == 0) {
					return fmt.Errorf("native protection not yet verified on %s", name)
				}
			}
		}
		for _, name := range targets {
			p := s.Pairs[name]
			p.Paused = cmd.Action != "start"
			if cmd.Action == "start" {
				p.Error = ""
				day := time.Now().UTC().Format("2006-01-02")
				if p.Day != day {
					p.Day, p.DayEquity = day, p.equity(a.feeds.Snapshot(name).Bid)
				}
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	if cmd.Action != "close" {
		writeJSON(w, a.snapshot())
		return
	}
	// Exchange closes cannot be transactional as a batch. Pause atomically, then
	// close independently and report every result instead of stopping halfway.
	results := map[string]string{}
	var resultMu sync.Mutex
	var wg sync.WaitGroup
	for _, name := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := "closed"
			gate := a.pairLock(name)
			if !gate.TryLock() {
				result = "execution busy; pair remains paused"
			} else {
				err := a.closeUnderGate(ctx, name, "manual_close")
				gate.Unlock()
				if err != nil {
					a.fail(context.WithoutCancel(ctx), name, "execution", err)
					result = err.Error()
				}
			}
			resultMu.Lock()
			results[name] = result
			resultMu.Unlock()
		}()
	}
	wg.Wait()
	status := http.StatusOK
	for _, result := range results {
		if result != "closed" {
			status = http.StatusConflict
			break
		}
	}
	writeJSONStatus(w, status, map[string]any{"results": results, "state": a.snapshot()})
}
