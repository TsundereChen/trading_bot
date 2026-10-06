package strategy

import "github.com/shopspring/decimal"

// BoundedDecimal rejects oversized coefficients/exponents before comparisons
// can expand a tiny JSON decimal into an enormous allocation.
func BoundedDecimal(d decimal.Decimal) bool {
	return d.Exponent() >= -18 && d.Exponent() <= 18 && d.Coefficient().BitLen() <= 128
}

// FloorStep rounds a nonnegative quantity down to a positive exchange step.
func FloorStep(q, step decimal.Decimal) decimal.Decimal {
	quotient, _ := q.QuoRem(step, 0)
	return quotient.Mul(step)
}
