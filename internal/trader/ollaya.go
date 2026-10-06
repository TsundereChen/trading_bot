package trader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"
)

func (a *App) askOllaya(ctx context.Context, pair string, snapshot map[string]any) (*ollayaAnswer, error) {
	questions := map[string]any{
		"action":        map[string]any{"type": "choice", "instructions": "Choose only from allowed_actions. ENTER_LONG requires entry_eligible=true. EXIT_LONG requires an existing position and trend_reversal=true. Otherwise HOLD. Model probabilities are not probabilities of trading profit.", "criteria": map[string]string{"ENTER_LONG": "Eligible long-only entry", "EXIT_LONG": "Close tracked long on trend reversal", "HOLD": "No order"}},
		"regime":        map[string]any{"type": "choice", "instructions": "Classify market regime from supplied features.", "criteria": map[string]string{"UPTREND": "EMA20 above EMA50", "DOWNTREND": "EMA20 below EMA50", "RANGE": "No clear trend"}},
		"setup_quality": map[string]any{"type": "score", "instructions": "Rate the match to the stated trend-pullback entry rules, not future profitability.", "criteria": []string{"Not eligible", "Weak", "Moderate", "Strong"}},
	}
	if a.cfg.EntryPolicy == "ollaya" {
		questions["action"] = map[string]any{"type": "choice", "instructions": "Choose only from allowed_actions. ENTER_LONG requires entry_eligible=true, but no EMA trend or pullback is mandatory. Judge whether supplied quotes and completed-candle features support a small long trade after spread and estimated fees. EXIT_LONG requires an existing tracked position; a trend reversal is not mandatory. HOLD when evidence is weak or costs outweigh the opportunity; do not trade simply to increase frequency. Model probabilities are not probabilities of profit.", "criteria": map[string]string{"ENTER_LONG": "Open a small eligible long", "EXIT_LONG": "Close the tracked long", "HOLD": "Wait; no order"}}
		questions["setup_quality"] = map[string]any{"type": "score", "instructions": "Rate the evidence for a small long after estimated transaction costs, not the match to a required pullback and not guaranteed profitability.", "criteria": []string{"Poor", "Weak", "Moderate", "Strong"}}
	}
	if a.cfg.ContextInterval != "" {
		action := questions["action"].(map[string]any)
		action["instructions"] = action["instructions"].(string) + " Use features on signal_interval for entry/exit timing and market_context.features for the higher-timeframe trend and volatility backdrop. Weigh agreement or conflict between timeframes; higher-timeframe uptrend is not a mandatory entry gate. Both windows contain only completed candles."
		questions["regime"] = map[string]any{"type": "choice", "instructions": "Classify the higher-timeframe market regime using market_context.features; distinguish it from short-term signal features.", "criteria": map[string]string{"UPTREND": "Higher-timeframe EMA20 above EMA50", "DOWNTREND": "Higher-timeframe EMA20 below EMA50", "RANGE": "No clear higher-timeframe trend"}}
	}
	body, err := json.Marshal(map[string]any{"model": a.cfg.OllayaModel, "state": snapshot, "questions": questions, "keep_alive": "10m"})
	if err != nil {
		return nil, err
	}
	inferCtx, cancel := context.WithTimeout(ctx, a.cfg.modelTimeout())
	defer cancel()
	req, err := http.NewRequestWithContext(inferCtx, "POST", strings.TrimRight(a.cfg.OllayaURL, "/")+"/api/decide", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.OllayaKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.OllayaKey)
	}
	start, outcome := time.Now(), "error"
	slog.Info("model request started", "pair", pair, "model", a.cfg.OllayaModel)
	defer func() { a.metrics.Latency.WithLabelValues(outcome).Observe(time.Since(start).Seconds()) }()
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Ollaya HTTP %d", resp.StatusCode)
	}
	var result ollayaAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return nil, err
	}
	outcome = "success"
	return &result, nil
}

type ollayaAnswer struct {
	Model     string                     `json:"model"`
	Truncated bool                       `json:"state_truncated"`
	Reason    string                     `json:"done_reason"`
	Answers   map[string]json.RawMessage `json:"answers"`
}

func validatedAction(result *ollayaAnswer, expectedModel string) (string, bool) {
	if expectedModel == "" || result == nil || result.Truncated || result.Reason != "decide" || result.Model != expectedModel {
		return "", false
	}
	var action struct {
		Type          string             `json:"type"`
		Choice        string             `json:"choice"`
		Probabilities map[string]float64 `json:"probabilities"`
	}
	if json.Unmarshal(result.Answers["action"], &action) != nil || action.Type != "choice" {
		return "", false
	}
	if action.Choice != "ENTER_LONG" && action.Choice != "EXIT_LONG" && action.Choice != "HOLD" {
		return "", false
	}
	if !validProbabilities(action.Choice, action.Probabilities) {
		return "", false
	}
	return action.Choice, true
}

func validProbabilities(choice string, probabilities map[string]float64) bool {
	if len(probabilities) != 3 {
		return false
	}
	sum := 0.0
	for _, label := range []string{"ENTER_LONG", "EXIT_LONG", "HOLD"} {
		p, ok := probabilities[label]
		if !ok || math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 || p > probabilities[choice] {
			return false
		}
		sum += p
	}
	return math.Abs(sum-1) <= .001
}
