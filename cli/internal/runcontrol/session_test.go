/*
Copyright © 2026 Dell Technologies
*/

package runcontrol

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionSubmissionStateNeverRegresses(t *testing.T) {
	session := NewSession()
	if !session.MarkSubmissionUnknown() {
		t.Fatal("unknown transition was not recorded")
	}
	if !session.MarkSubmitted() {
		t.Fatal("submitted transition was not recorded")
	}
	if session.MarkSubmissionUnknown() || session.MarkSubmitted() {
		t.Fatal("confirmed submission changed or repeated")
	}
	if got := session.SubmissionState(); got != SubmissionSubmitted {
		t.Fatalf("submission = %s", got)
	}
}

func TestSessionWorkloadTerminalBroadcastIsIdempotent(t *testing.T) {
	session := NewSession()
	select {
	case <-session.WorkloadTerminal():
		t.Fatal("terminal signal closed before authoritative completion")
	default:
	}

	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			session.MarkWorkloadTerminal()
		}()
	}
	group.Wait()

	select {
	case <-session.WorkloadTerminal():
	case <-time.After(time.Second):
		t.Fatal("terminal signal was not broadcast")
	}
}

func TestSessionCanceledWaiterJoinsOneContinuingFinalizer(t *testing.T) {
	session := NewSession()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	if err := session.RegisterResourceFinalizer(func(ctx context.Context) FinalizationOutcome {
		calls.Add(1)
		close(started)
		<-release
		return FinalizationOutcome{Removal: CompletedPhase(nil), Resources: ResourceDispositionRemoved}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan FinalizationOutcome, 1)
	go func() { done <- session.FinalizeResources(ctx) }()
	<-started
	cancel()
	if outcome := <-done; !errors.Is(outcome.WaitErr, context.Canceled) {
		t.Fatalf("wait error = %v", outcome.WaitErr)
	}
	joined := make(chan FinalizationOutcome, 1)
	go func() { joined <- session.FinalizeResources(context.Background()) }()
	select {
	case <-joined:
		t.Fatal("joined caller returned before canonical finalizer")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if outcome := <-joined; outcome.Resources != ResourceDispositionRemoved {
		t.Fatalf("joined outcome = %+v", outcome)
	}
	if calls.Load() != 1 {
		t.Fatalf("finalizer calls = %d, want 1", calls.Load())
	}
}

func TestSessionRetainedFinalizationCanRetryWithoutOverlap(t *testing.T) {
	session := NewSession()
	removeErr := errors.New("remove failed")
	var calls atomic.Int32
	if err := session.RegisterResourceFinalizer(func(context.Context) FinalizationOutcome {
		if calls.Add(1) == 1 {
			return FinalizationOutcome{
				Removal: CompletedPhase(removeErr), Resources: ResourceDispositionRetained,
			}
		}
		return FinalizationOutcome{Removal: CompletedPhase(nil), Resources: ResourceDispositionRemoved}
	}); err != nil {
		t.Fatal(err)
	}
	first := session.FinalizeResources(context.Background())
	if !errors.Is(first.Removal.Err, removeErr) || first.Resources != ResourceDispositionRetained {
		t.Fatalf("first outcome = %+v", first)
	}
	second := session.FinalizeResources(context.Background())
	if second.Error() != nil || second.Resources != ResourceDispositionRemoved {
		t.Fatalf("second outcome = %+v", second)
	}
	if calls.Load() != 2 {
		t.Fatalf("finalizer calls = %d, want 2", calls.Load())
	}
}
