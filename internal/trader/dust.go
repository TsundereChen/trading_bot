package trader

import (
	"automated-trader/internal/strategy"
	"github.com/shopspring/decimal"
)

// retainDust separates a proven sub-step residual, without writing off assets
// or pretending it has exchange protection. The next filled buy absorbs it.
func retainDust(p *Position, symbol Symbol) bool {
	if p.Pending != nil || p.Protection != nil || !p.Qty.IsPositive() || !symbol.Step.IsPositive() || !strategy.FloorStep(p.Qty, symbol.Step).IsZero() {
		return false
	}
	p.DustQty, p.DustCost = p.DustQty.Add(p.Qty), p.DustCost.Add(p.Cost)
	p.Qty, p.Cost, p.Stop, p.Target = decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero
	return true
}
