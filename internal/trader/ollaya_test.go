package trader

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestConfiguredOllayaModel(t *testing.T) {
	a := testApp(t)
	a.cfg.OllayaModel = "custom-decision:v2"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Model string }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Model != a.cfg.OllayaModel {
			t.Error("request did not use configured model", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"model": request.Model, "done_reason": "decide", "answers": map[string]any{"action": map[string]any{"type": "choice", "choice": "HOLD", "probabilities": map[string]float64{"HOLD": .8, "ENTER_LONG": .1, "EXIT_LONG": .1}}}})
	}))
	defer s.Close()
	a.client = s.Client()
	a.cfg.OllayaURL = s.URL
	answer, err := a.askOllaya(context.Background(), "BTCUSDT", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if action, ok := validatedAction(answer, a.cfg.OllayaModel); !ok || action != "HOLD" {
		t.Fatal("configured model response rejected")
	}
	if _, ok := validatedAction(answer, "winnow:e4b"); ok {
		t.Fatal("wrong model response accepted")
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/status", nil)
	r.Header.Set("Authorization", "Bearer "+a.cfg.ControlToken)
	a.routes(prometheus.NewRegistry()).ServeHTTP(w, r)
	var status struct{ Model string }
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil || status.Model != a.cfg.OllayaModel {
		t.Fatal("status did not report configured model", err)
	}
}

func TestOllayaModelConfiguration(t *testing.T) {
	t.Setenv("CONTROL_TOKEN", strings.Repeat("c", 32))
	t.Setenv("METRICS_TOKEN", strings.Repeat("m", 32))
	t.Setenv("ENABLE_TRADING", "false")
	t.Setenv("BINANCE_BASE_URL", "")
	for _, tc := range []struct{ value, want string }{{"", "winnow:e4b"}, {" custom:v2 ", "custom:v2"}} {
		t.Setenv("OLLAYA_MODEL", tc.value)
		cfg, err := config()
		if err != nil || cfg.OllayaModel != tc.want {
			t.Fatal("incorrect model configuration", cfg.OllayaModel, err)
		}
	}
	t.Setenv("OLLAYA_MODEL", "   ")
	if _, err := config(); err == nil {
		t.Fatal("blank model accepted")
	}
}
