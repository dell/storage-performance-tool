/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestLaunchHooksSubmissionStateIsSharedAndExactlyOnce(t *testing.T) {
	var calls atomic.Int32
	hooks := NewLaunchHooks(func() { calls.Add(1) })
	copyOfHooks := hooks

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copyOfHooks.NotifySubmitted()
		}()
	}
	wg.Wait()

	if !hooks.Submitted() || !copyOfHooks.Submitted() {
		t.Fatal("submission state was not shared across copied hooks")
	}
	if calls.Load() != 1 {
		t.Fatalf("submission callback calls = %d, want exactly 1", calls.Load())
	}
}

func TestZeroValueLaunchHooksRemainCompatible(t *testing.T) {
	var called bool
	hooks := LaunchHooks{OnSubmitted: func() { called = true }}
	hooks.NotifySubmitted()
	if !called {
		t.Fatal("zero-value-compatible launch callback was not invoked")
	}
	if hooks.Submitted() {
		t.Fatal("legacy hooks unexpectedly claimed inspectable submission state")
	}
}
