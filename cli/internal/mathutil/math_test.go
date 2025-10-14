package mathutil

import "testing"

func TestMinInt(t *testing.T) {
	tests := []struct{ a, b, want int }{
		{1, 2, 1},
		{2, 1, 1},
		{0, 0, 0},
		{-5, 3, -5},
		{3, -5, -5},
		{42, 42, 42},
	}
	for _, tt := range tests {
		got := MinInt(tt.a, tt.b)
		if got != tt.want {
			t.Fatalf("MinInt(%d,%d) = %d; want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
