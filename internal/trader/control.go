package trader

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
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
		if !containsString(requested, pair) && (p.Qty.IsPositive() || p.DustQty.IsPositive() || p.Pending != nil || p.Protection != nil || len(p.UnvaluedFees) > 0) {
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
			if err != nil || !supportedQuote(symbol.Quote) || (a.cfg.Trading && !symbol.StopAllowed) {
				http.Error(w, "unsupported USDT/USDC spot/native-stop pair "+name, 400)
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
					delete(s.Evaluations, name)
				}
			}
			for _, name := range requested {
				if s.Pairs[name] == nil {
					s.Pairs[name] = &Position{Pair: name, Paused: a.cfg.Trading, Starting: a.cfg.Trading, Cash: a.cfg.PerPairBudget}
					ensurePerformance(s.Pairs[name])
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
	if cmd.Action != "close" {
		http.Error(w, "unknown action", 400)
		return
	}
	targets, err := controlTargets(snapshot, pair)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	if !a.executionAllowed() {
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
		}
		for _, name := range targets {
			p := s.Pairs[name]
			p.Paused, p.Starting = true, false
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), 409)
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
				if err != nil {
					a.fail(context.WithoutCancel(ctx), name, "execution", err)
					result = err.Error()
				} else if err := a.update(context.WithoutCancel(ctx), "manual_close_complete", map[string]string{"pair": name}, func(s State) error {
					p := s.Pairs[name]
					if p == nil {
						return errNoChange
					}
					if p.Error != "" {
						return errNoChange
					}
					p.Starting = a.cfg.Trading
					return nil
				}); err != nil {
					result = err.Error()
				}
				gate.Unlock()
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
