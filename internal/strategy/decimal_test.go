package strategy

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestFloorStep(t *testing.T) {
	for _, c := range []struct{ q, step, want string }{{"1.239", "0.01", "1.23"}, {"1.239", "0.05", "1.2"}, {"0.009", "0.01", "0"}} {
		if got := FloorStep(decimal.RequireFromString(c.q), decimal.RequireFromString(c.step)); !got.Equal(decimal.RequireFromString(c.want)) {
			t.Fatalf("%s / %s: %s", c.q, c.step, got)
		}
	}
}
