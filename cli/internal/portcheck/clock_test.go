package portcheck

import (
	"testing"
	"time"
)

func TestFakeClock_NowAndSleepAndAfter(t *testing.T) {
	fc := NewFakeClock()
	t0 := fc.Now()
	// Sleep should be a no-op and not change now
	fc.Sleep(10 * time.Millisecond)
	if got := fc.Now(); !got.Equal(t0) {
		t.Fatalf("expected Now to remain fixed, got %v != %v", got, t0)
	}
	// After should deliver immediately
	select {
	case <-fc.After(5 * time.Second):
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("fake After did not fire promptly")
	}
}
