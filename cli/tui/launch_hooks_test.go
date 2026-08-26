/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

func TestLaunchHooksRunPreSubmissionCheck(t *testing.T) {
	type contextKey struct{}
	wantErr := errors.New("identity mismatch")
	hooks := NewLaunchHooks(nil).WithPreSubmissionCheck(func(ctx context.Context) ([]string, error) {
		if ctx.Value(contextKey{}) != "active" {
			t.Fatal("pre-submission check did not receive launch context")
		}
		return []string{"Engine identity: mismatch"}, wantErr
	})

	lines, err := hooks.RunPreSubmissionCheck(context.WithValue(context.Background(), contextKey{}, "active"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunPreSubmissionCheck() error = %v, want sentinel", err)
	}
	if len(lines) != 1 || lines[0] != "Engine identity: mismatch" {
		t.Fatalf("RunPreSubmissionCheck() lines = %v", lines)
	}
}

func TestZeroValueLaunchHooksPreSubmissionCheckIsCompatible(t *testing.T) {
	lines, err := (LaunchHooks{}).RunPreSubmissionCheck(context.Background())
	if err != nil || len(lines) != 0 {
		t.Fatalf("zero hook result = %v, %v", lines, err)
	}
}

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

	if !hooks.Submitted() || !copyOfHooks.Submitted() || !hooks.NormalEvidencePermitted() {
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

func TestLaunchHooksSubmissionUnknownCanAdvanceButNotRegress(t *testing.T) {
	hooks := NewLaunchHooks(nil)
	hooks.NotifySubmissionUnknown()
	if hooks.NormalEvidencePermitted() {
		t.Fatal("unknown submission permitted ordinary evidence")
	}
	if got := hooks.SubmissionState(); got != SubmissionUnknown {
		t.Fatalf("submission state = %s, want %s", got, SubmissionUnknown)
	}

	hooks.NotifySubmitted()
	hooks.NotifySubmissionUnknown()
	if got := hooks.SubmissionState(); got != SubmissionSubmitted {
		t.Fatalf("submission state = %s, want %s", got, SubmissionSubmitted)
	}
	if !hooks.NormalEvidencePermitted() {
		t.Fatal("confirmed submission did not permit ordinary evidence")
	}
}

func TestAcceptedForCleanupDoesNotArmNormalEvidence(t *testing.T) {
	var armCalls atomic.Int32
	session := runcontrol.NewSession()
	hooks := NewSessionLaunchHooks(session, func() { armCalls.Add(1) })

	hooks.NotifyAcceptedForCleanup()
	if got := session.SubmissionState(); got != runcontrol.SubmissionSubmitted {
		t.Fatalf("session submission = %s, want submitted", got)
	}
	if hooks.NormalEvidencePermitted() || armCalls.Load() != 0 {
		t.Fatalf("accepted-for-cleanup trusted=%t arm-calls=%d",
			hooks.NormalEvidencePermitted(), armCalls.Load())
	}

	// A later authoritative confirmation may upgrade trust without losing the
	// exactly-once callback guarantee.
	hooks.NotifySubmitted()
	hooks.NotifySubmitted()
	if !hooks.NormalEvidencePermitted() || armCalls.Load() != 1 {
		t.Fatalf("validated upgrade trusted=%t arm-calls=%d",
			hooks.NormalEvidencePermitted(), armCalls.Load())
	}
}

func TestAcceptedForCleanupConcurrentUpgradeIsMonotonic(t *testing.T) {
	var armCalls atomic.Int32
	session := runcontrol.NewSession()
	hooks := NewSessionLaunchHooks(session, func() { armCalls.Add(1) })
	copiedHooks := hooks

	const callers = 48
	start := make(chan struct{})
	var group sync.WaitGroup
	for i := range callers {
		group.Add(1)
		go func(call int) {
			defer group.Done()
			<-start
			switch call % 3 {
			case 0:
				copiedHooks.NotifyAcceptedForCleanup()
			case 1:
				copiedHooks.NotifySubmissionUnknown()
			default:
				copiedHooks.NotifySubmitted()
			}
		}(i)
	}
	close(start)
	group.Wait()

	if got := session.SubmissionState(); got != runcontrol.SubmissionSubmitted {
		t.Fatalf("raced submission state = %s, want submitted", got)
	}
	if !hooks.NormalEvidencePermitted() {
		t.Fatal("authoritative submitted notification did not establish evidence trust")
	}
	if calls := armCalls.Load(); calls != 1 {
		t.Fatalf("raced submission callback calls = %d, want exactly 1", calls)
	}

	// Weaker facts arriving after the authoritative upgrade cannot regress
	// cleanup ownership, evidence permission, or callback cardinality.
	copiedHooks.NotifySubmissionUnknown()
	copiedHooks.NotifyAcceptedForCleanup()
	if got := session.SubmissionState(); got != runcontrol.SubmissionSubmitted ||
		!hooks.NormalEvidencePermitted() || armCalls.Load() != 1 {
		t.Fatalf("weaker notification regressed state=%s trusted=%t calls=%d",
			got, hooks.NormalEvidencePermitted(), armCalls.Load())
	}
}

func TestSessionLaunchHooksShareSubmissionAndFinalizerAuthority(t *testing.T) {
	var armCalls atomic.Int32
	session := runcontrol.NewSession()
	hooks := NewSessionLaunchHooks(session, func() { armCalls.Add(1) })
	if !hooks.SessionManaged() {
		t.Fatal("hooks are not session managed")
	}
	hooks.NotifySubmissionUnknown()
	if got := session.SubmissionState(); got != runcontrol.SubmissionUnknown {
		t.Fatalf("session submission = %s", got)
	}
	if hooks.ProvenNotSubmitted() || !hooks.PotentiallySubmitted() || armCalls.Load() != 1 {
		t.Fatalf("unknown submission facts proven-not=%t potential=%t arm-calls=%d",
			hooks.ProvenNotSubmitted(), hooks.PotentiallySubmitted(), armCalls.Load())
	}
	hooks.NotifySubmitted()
	if got := session.SubmissionState(); got != runcontrol.SubmissionSubmitted {
		t.Fatalf("session submission = %s", got)
	}
	if armCalls.Load() != 1 {
		t.Fatalf("confirmed-after-unknown rearmed monitor: calls=%d", armCalls.Load())
	}
	if err := hooks.RegisterResourceFinalizer(func(context.Context) runcontrol.FinalizationOutcome {
		return runcontrol.FinalizationOutcome{
			Removal: runcontrol.CompletedPhase(nil), Resources: runcontrol.ResourceDispositionRemoved,
		}
	}); err != nil {
		t.Fatal(err)
	}
	outcome := session.FinalizeResources(context.Background())
	if outcome.Error() != nil || outcome.Resources != runcontrol.ResourceDispositionRemoved {
		t.Fatalf("finalization = %+v", outcome)
	}
}

func TestSessionLaunchHooksExposeAuthoritativeWorkloadTerminal(t *testing.T) {
	session := runcontrol.NewSession()
	hooks := NewSessionLaunchHooks(session, nil)
	if terminal := NewLaunchHooks(nil).WorkloadTerminal(); terminal != nil {
		t.Fatal("compatibility hooks exposed a session terminal channel")
	}
	select {
	case <-hooks.WorkloadTerminal():
		t.Fatal("terminal signal closed before tracker completion")
	default:
	}
	session.MarkWorkloadTerminal()
	select {
	case <-hooks.WorkloadTerminal():
	case <-time.After(time.Second):
		t.Fatal("launch hooks did not expose session terminal signal")
	}
}
