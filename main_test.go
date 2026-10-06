package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestUnknownCommand(t *testing.T) {
	if err := run([]string{"unknown"}); err == nil || !strings.Contains(err.Error(), "usage: trader") {
		t.Fatal("unknown command did not return usage", err)
	}
}

func TestBacktestCommandDispatch(t *testing.T) {
	path := t.TempDir() + "/missing.json"
	if err := run([]string{"backtest", "--input", path}); !os.IsNotExist(err) {
		t.Fatal("backtest command did not forward its arguments", err)
	}
}

func TestHealthcheckCommandDispatch(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()
	t.Setenv("HEALTHCHECK_URL", s.URL)
	if err := run([]string{"healthcheck"}); err != nil {
		t.Fatal("healthcheck command did not use the configured endpoint", err)
	}
}
