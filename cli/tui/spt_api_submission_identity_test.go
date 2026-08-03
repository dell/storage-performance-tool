/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestAcceptedResponseIdentityMatchesConfiguredRun(t *testing.T) {
	tests := []struct {
		name string
		etag string
		body string
	}{
		{name: "numeric ETag", etag: "77"},
		{name: "quoted ETag", etag: `"77"`},
		{name: "legacy body string", body: `{"runId":"77"}`},
		{name: "canonical body number", body: `{"run_id":77}`},
		{name: "canonical body string", body: `{"run_id":"77"}`},
		{name: "all matching", etag: "77", body: `{"run_id":77,"runId":"77"}`},
		{name: "nested identity ignored", body: `{"metadata":{"run_id":78},"run_id":77}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := acceptedIdentityServer(t, test.etag, test.body, "")
			client := NewSptAPIClient(server.URL)
			outcome, err := client.StartTestContext(
				context.Background(), []byte("scenario"), []byte("defaults"), 77)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Submission != SubmissionSubmitted || outcome.RunID != "77" {
				t.Fatalf("outcome = %+v", outcome)
			}
			if got := client.getRunID(); got != "77" {
				t.Fatalf("owned run ID = %q, want 77", got)
			}
		})
	}
}

func TestAcceptedResponseIdentityRejectsInvalidConfiguredRun(t *testing.T) {
	tests := []struct {
		name string
		etag string
		body string
	}{
		{name: "mismatched ETag", etag: "78"},
		{name: "mismatched body", body: `{"run_id":78}`},
		{name: "conflicting header and body", etag: "77", body: `{"run_id":78}`},
		{name: "conflicting body aliases", body: `{"run_id":77,"runId":"78"}`},
		{name: "nonnumeric ETag", etag: "not-a-run"},
		{name: "malformed quoted ETag", etag: `"77`},
		{name: "nonnumeric canonical body", body: `{"run_id":"not-a-run"}`},
		{name: "null legacy body", body: `{"runId":null}`},
		{name: "duplicate canonical conflict", body: `{"run_id":78,"run_id":77}`},
		{name: "duplicate canonical identical", body: `{"run_id":77,"run_id":77}`},
		{name: "duplicate legacy conflict", body: `{"runId":78,"runId":77}`},
		{name: "duplicate legacy identical", body: `{"runId":77,"runId":77}`},
		{name: "escaped duplicate canonical", body: `{"run_id":77,"\u0072un_id":77}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := acceptedIdentityServer(t, test.etag, test.body, "")
			client := NewSptAPIClient(server.URL)
			outcome, err := client.StartTestContext(
				context.Background(), []byte("scenario"), []byte("defaults"), 77)
			var identityErr *SubmissionIdentityError
			if !errors.As(err, &identityErr) {
				t.Fatalf("error = %v, want SubmissionIdentityError", err)
			}
			if outcome.Submission != SubmissionSubmitted || outcome.RunID != "" {
				t.Fatalf("outcome = %+v, want submitted without trusted identity", outcome)
			}
			if got := client.getRunID(); got != "" {
				t.Fatalf("invalid response established owned run ID %q", got)
			}
			if stopErr := client.StopTest(); !errors.Is(stopErr, ErrRunOwnershipUnknown) {
				t.Fatalf("StopTest() error = %v, want %v", stopErr, ErrRunOwnershipUnknown)
			}
		})
	}
}

func TestAcceptedResponseMissingIdentityUsesBoundedStatusReconciliation(t *testing.T) {
	t.Run("matching status confirms identity", func(t *testing.T) {
		server := acceptedIdentityServer(
			t, "", "", `{"state":"RUNNING","run_id":77}`)
		client := NewSptAPIClient(server.URL)
		client.submissionReconcileTimeout = time.Second
		client.submissionReconcilePollInterval = time.Millisecond
		outcome, err := client.StartTestContext(
			context.Background(), []byte("scenario"), []byte("defaults"), 77)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Submission != SubmissionSubmitted || outcome.RunID != "77" ||
			client.getRunID() != "77" {
			t.Fatalf("reconciled outcome = %+v, owned = %q", outcome, client.getRunID())
		}
	})

	t.Run("deadline preserves submitted identity error", func(t *testing.T) {
		server := acceptedIdentityServer(
			t, "", "", `{"state":"RUNNING","run_id":78}`)
		client := NewSptAPIClient(server.URL)
		client.submissionReconcileTimeout = 25 * time.Millisecond
		client.submissionReconcilePollInterval = time.Millisecond
		outcome, err := client.StartTestContext(
			context.Background(), []byte("scenario"), []byte("defaults"), 77)
		var identityErr *SubmissionIdentityError
		if !errors.As(err, &identityErr) {
			t.Fatalf("error = %v, want SubmissionIdentityError", err)
		}
		if outcome.Submission != SubmissionSubmitted || outcome.RunID != "" {
			t.Fatalf("outcome = %+v, want submitted without trusted identity", outcome)
		}
		if got := client.getRunID(); got != "" {
			t.Fatalf("unreconciled response established owned run ID %q", got)
		}
	})
}

func TestAcceptedResponseIdentityPreservesUnconfiguredCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		etag      string
		body      string
		wantRunID string
	}{
		{name: "legacy ETag", etag: "legacy-etag", wantRunID: "legacy-etag"},
		{name: "legacy body", body: `{"runId":"legacy-body"}`, wantRunID: "legacy-body"},
		{name: "legacy duplicate remains compatibility scoped", body: `{"runId":"first","runId":"legacy-body"}`, wantRunID: "legacy-body"},
		{name: "missing identity uses compatibility fallback"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := acceptedIdentityServer(t, test.etag, test.body, "")
			client := NewSptAPIClient(server.URL)
			outcome, err := client.StartTestContext(
				context.Background(), []byte("scenario"), []byte("defaults"))
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Submission != SubmissionSubmitted || outcome.RunID == "" {
				t.Fatalf("compatibility outcome = %+v", outcome)
			}
			if test.wantRunID != "" && outcome.RunID != test.wantRunID {
				t.Fatalf("run ID = %q, want %q", outcome.RunID, test.wantRunID)
			}
			if test.wantRunID == "" && !strings.HasPrefix(outcome.RunID, "run-") {
				t.Fatalf("fallback run ID = %q", outcome.RunID)
			}
		})
	}
}

func TestAcceptedIdentityErrorRetainsSubmittedCleanupOwnership(t *testing.T) {
	var statusCalls atomic.Int32
	var metricsCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		switch {
		case request.URL.Path == "/ready":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"ready":true,"status":"ready"}`))
		case request.URL.Path == "/health":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"status":"ok"}`))
		case request.URL.Path == "/run" && request.Method == http.MethodHead:
			writer.WriteHeader(http.StatusNoContent)
		case request.URL.Path == "/run" && request.Method == http.MethodPost:
			writer.Header().Set("ETag", "78")
			writer.WriteHeader(http.StatusAccepted)
		case request.URL.Path == "/status":
			statusCalls.Add(1)
			writer.WriteHeader(http.StatusOK)
		case request.URL.Path == "/metrics" || request.URL.Path == "/metrics/json":
			metricsCalls.Add(1)
			writer.WriteHeader(http.StatusOK)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	manager := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(manager, "", "")
	orchestrator.apiClient = NewSptAPIClient(server.URL)
	session := runcontrol.NewSession()
	var normalEvidenceArms atomic.Int32
	hooks := NewSessionLaunchHooks(session, func() { normalEvidenceArms.Add(1) })
	if err := hooks.RegisterResourceFinalizer(func(ctx context.Context) runcontrol.FinalizationOutcome {
		cleanupErr := manager.CleanupContext(ctx)
		disposition := runcontrol.ResourceDispositionRemoved
		if manager.HasManagedResources() {
			disposition = runcontrol.ResourceDispositionRetained
		}
		return runcontrol.FinalizationOutcome{
			Removal: runcontrol.CompletedPhase(cleanupErr), Resources: disposition,
		}
	}); err != nil {
		t.Fatal(err)
	}

	err := orchestrator.StartTestWithContentAndLaunchHooks(
		context.Background(),
		"test-image",
		scenario.Params{WorkloadType: "write", RunID: 77},
		[]byte("Load.run({});"),
		[]byte("run:\n  id: 77\n"),
		hooks,
	)
	var identityErr *SubmissionIdentityError
	if !errors.As(err, &identityErr) {
		t.Fatalf("launch error = %v, want SubmissionIdentityError", err)
	}
	if state := session.SubmissionState(); state != runcontrol.SubmissionSubmitted {
		t.Fatalf("session submission = %s, want submitted", state)
	}
	if hooks.NormalEvidencePermitted() || normalEvidenceArms.Load() != 0 {
		t.Fatalf("identity-invalid launch trusted=%t normal evidence arms=%d",
			hooks.NormalEvidencePermitted(), normalEvidenceArms.Load())
	}
	if !manager.HasManagedResources() || manager.GetCleanupCallCount() != 0 {
		t.Fatalf("accepted launch was prematurely cleaned: managed=%t calls=%d",
			manager.HasManagedResources(), manager.GetCleanupCallCount())
	}
	time.Sleep(20 * time.Millisecond)
	if statusCalls.Load() != 0 || metricsCalls.Load() != 0 {
		t.Fatalf("identity-invalid launch started monitoring: status=%d metrics=%d",
			statusCalls.Load(), metricsCalls.Load())
	}

	first := session.FinalizeResources(context.Background())
	second := session.FinalizeResources(context.Background())
	if first.Error() != nil || second.Error() != nil ||
		first.Resources != runcontrol.ResourceDispositionRemoved ||
		second.Resources != runcontrol.ResourceDispositionRemoved ||
		manager.GetCleanupCallCount() != 1 {
		t.Fatalf("cleanup outcomes = %+v / %+v, calls=%d",
			first, second, manager.GetCleanupCallCount())
	}
}

func acceptedIdentityServer(t *testing.T, etag, body, statusBody string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		switch request.URL.Path {
		case "/run":
			if request.Method != http.MethodPost {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			if etag != "" {
				writer.Header().Set("ETag", etag)
			}
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(body))
		case "/status":
			if statusBody == "" {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(statusBody))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}
