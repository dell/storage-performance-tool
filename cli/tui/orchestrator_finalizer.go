/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

// FinalizeDiagnosticsAndCleanupOutcome is the canonical local post-submission
// resource finalizer. Diagnostics and mandatory removal receive independent
// budgets, and concurrent callers join one active attempt.
func (o *TestOrchestrator) FinalizeDiagnosticsAndCleanupOutcome(
	ctx context.Context,
) runcontrol.FinalizationOutcome {
	if o == nil {
		return runcontrol.FinalizationOutcome{Resources: runcontrol.ResourceDispositionNotOwned}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	o.finalizeMu.Lock()
	if o.finalizeAttempt != nil {
		select {
		case <-o.finalizeAttempt.done:
			if o.finalizeAttempt.outcome.Resources == runcontrol.ResourceDispositionRetained {
				o.finalizeAttempt = nil
			} else {
				attempt := o.finalizeAttempt
				o.finalizeMu.Unlock()
				return attempt.outcome
			}
		default:
			attempt := o.finalizeAttempt
			o.finalizeMu.Unlock()
			return waitForLocalFinalization(ctx, attempt)
		}
	}
	attempt := &cleanupAttempt{done: make(chan struct{})}
	o.finalizeAttempt = attempt
	o.finalizeMu.Unlock()

	go func() {
		base := context.WithoutCancel(ctx)
		diagnosticsTimeout := o.diagnosticsTimeout
		if diagnosticsTimeout <= 0 {
			diagnosticsTimeout = constants.DiagnosticsCollectionTimeout
		}
		cleanupTimeout := o.cleanupTimeout
		if cleanupTimeout <= 0 {
			cleanupTimeout = constants.ContainerCleanupTimeout
		}
		o.stopOnce.Do(func() { close(o.stopCh) })
		hadResources := o.dockerManager != nil && hasManagedDockerResources(o.dockerManager)

		diagnosticsCtx, cancelDiagnostics := context.WithTimeout(
			base, diagnosticsTimeout)
		diagnosticsErr := o.collectLocalDiagnostics(diagnosticsCtx)
		cancelDiagnostics()
		attempt.outcome.Diagnostics = runcontrol.CompletedPhase(diagnosticsErr)

		var removalErr error
		if o.dockerManager != nil {
			removalCtx, cancelRemoval := context.WithTimeout(base, cleanupTimeout)
			removalErr = cleanupDockerResourcesWithinContext(removalCtx, o.dockerManager)
			cancelRemoval()
		}
		attempt.outcome.Removal = runcontrol.CompletedPhase(removalErr)
		switch {
		case o.dockerManager == nil || !hadResources:
			attempt.outcome.Resources = runcontrol.ResourceDispositionNotOwned
		case hasManagedDockerResources(o.dockerManager):
			attempt.outcome.Resources = runcontrol.ResourceDispositionRetained
		default:
			attempt.outcome.Resources = runcontrol.ResourceDispositionRemoved
		}
		o.stoppedOnce.Do(func() { close(o.stoppedCh) })
		close(attempt.done)
	}()
	return waitForLocalFinalization(ctx, attempt)
}

func waitForLocalFinalization(
	ctx context.Context, attempt *cleanupAttempt,
) runcontrol.FinalizationOutcome {
	select {
	case <-attempt.done:
		return attempt.outcome
	case <-ctx.Done():
		return runcontrol.FinalizationOutcome{
			Resources: runcontrol.ResourceDispositionRetained,
			WaitErr:   ctx.Err(),
		}
	}
}

func (o *TestOrchestrator) collectLocalDiagnostics(ctx context.Context) error {
	if o.dockerManager == nil {
		return nil
	}
	collector, ok := o.dockerManager.(diagnosticsCollector)
	if !ok {
		return nil
	}
	var errs []error
	if err := collector.gracefulStopForDiagnostics(ctx); err != nil {
		errs = append(errs, fmt.Errorf("stop for diagnostics: %w", err))
	}
	record, err := collector.collectDiagnostics(ctx)
	if err != nil {
		errs = append(errs, err)
	}
	if record != nil {
		if manifestErr := writeDiagnosticsAggregateManifest(
			o.resultsRoot, []diagnosticsRecord{*record}); manifestErr != nil {
			errs = append(errs, manifestErr)
		}
	}
	return errors.Join(errs...)
}
