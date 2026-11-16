package tui

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// runningCPUMonitors counts the goroutines running a CPU monitor loop.
func runningCPUMonitors() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "(*CPUMonitor).monitorLoop")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// A multi-host launch that returns early because no host is ready never
// reaches the program, so only its deferred Stop ends the live view's CPU
// monitor. Its renderer is not reachable from the test, so this counts the
// monitor goroutines instead.
func TestMultiHostTUIEarlyErrorStopsLiveViewCPUMonitor(t *testing.T) {
	before := runningCPUMonitors()
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}, 1)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusPending)
	}

	err := StartTUIWithMultiHostOrchestrator(orchestrator, "test-image", "/test/scenario.js",
		scenario.ScenarioParams{WorkloadType: "mock", Duration: "5s", Threads: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "no ready hosts available for TUI startup") {
		t.Fatalf("StartTUIWithMultiHostOrchestrator() error = %v, want the no-ready-hosts error", err)
	}

	// Stop waits for the loop to close its done channel; allow the goroutine a
	// moment to return after that.
	deadline := time.Now().Add(2 * time.Second)
	for runningCPUMonitors() > before {
		if time.Now().After(deadline) {
			t.Fatalf("CPU monitors running: %d after the early error, %d before", runningCPUMonitors(), before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
