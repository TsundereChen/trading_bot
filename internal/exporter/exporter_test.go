package exporter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExporterFreshnessAndCredentials(t *testing.T) {
	broken := false
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer metrics-secret" {
			t.Error("metrics auth missing")
		}
		if broken {
			http.Error(w, "down", 503)
			return
		}
		fmt.Fprintln(w, "# TYPE trader_equity_quote gauge\ntrader_equity_quote 1000")
	}))
	defer s.Close()
	e := &Exporter{client: s.Client(), url: s.URL, token: "metrics-secret"}
	if err := e.fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := func() string {
		r := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()
		e.routes().ServeHTTP(w, r)
		return w.Body.String()
	}
	if body := request(); !strings.Contains(body, "trader_equity_quote 1000") || !strings.Contains(body, "trader_exporter_up 1") {
		t.Fatal(body)
	}
	broken = true
	if e.fetch(context.Background()) == nil {
		t.Fatal("upstream error ignored")
	}
	if body := request(); strings.Contains(body, "trader_equity_quote 1000") || !strings.Contains(body, "trader_exporter_up 0") {
		t.Fatal("stale metrics served", body)
	}
}
