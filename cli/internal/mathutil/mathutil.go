// Package mathutil contains tiny numeric helpers for internal use.
package mathutil

// MinInt returns the smaller of a and b.
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
