/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

func TestLocalFinalizerPreservesCleanupFailureAndRetries(t *testing.T) {
	manager := NewMockDockerManager()
	manager.SetContainerID("container-1")
	manager.SetFailOnCleanup(true)
	orchestrator := NewTestOrchestrator(manager, "9999", t.TempDir())

	first := orchestrator.FinalizeDiagnosticsAndCleanupOutcome(context.Background())
	if first.Removal.Err == nil || first.Resources != runcontrol.ResourceDispositionRetained {
		t.Fatalf("first finalization = %+v", first)
	}
	manager.SetFailOnCleanup(false)
	second := orchestrator.FinalizeDiagnosticsAndCleanupOutcome(context.Background())
	if second.Error() != nil || second.Resources != runcontrol.ResourceDispositionRemoved {
		t.Fatalf("second finalization = %+v", second)
	}
	if got := manager.GetCleanupCallCount(); got != 2 {
		t.Fatalf("cleanup calls = %d, want 2", got)
	}
}

func TestLocalDiagnosticsTimeoutCannotSkipFreshRemoval(t *testing.T) {
	manager := &phaseBudgetDockerManager{
		MockDockerManager:   NewMockDockerManager(),
		diagnosticsReturned: make(chan struct{}),
		cleanupStarted:      make(chan struct{}),
	}
	manager.SetContainerID("container-1")
	orchestrator := NewTestOrchestrator(manager, "9999", t.TempDir())
	orchestrator.diagnosticsTimeout = 10 * time.Millisecond
	orchestrator.cleanupTimeout = time.Second

	outcome := orchestrator.FinalizeDiagnosticsAndCleanupOutcome(context.Background())
	if !errors.Is(outcome.Diagnostics.Err, context.DeadlineExceeded) {
		t.Fatalf("diagnostics error = %v, want deadline", outcome.Diagnostics.Err)
	}
	if outcome.Removal.Err != nil || outcome.Resources != runcontrol.ResourceDispositionRemoved {
		t.Fatalf("removal outcome = %+v", outcome)
	}
	if manager.resourceOnlyCalls.Load() != 1 || manager.normalCleanupCalls.Load() != 0 {
		t.Fatalf("resource/normal cleanup calls = %d/%d",
			manager.resourceOnlyCalls.Load(), manager.normalCleanupCalls.Load())
	}
}
