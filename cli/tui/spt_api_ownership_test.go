package tui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReconciliationDoesNotAdoptUnrelatedRunForStop(t *testing.T) {
	var stoppedRunID string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/status":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"state":"RUNNING","run_id":78}`))
		case request.URL.Path == "/run" && request.Method == http.MethodDelete:
			stoppedRunID = request.URL.Query().Get("runId")
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.SetRunID("77")
	client.submissionReconcileTimeout = 20 * time.Millisecond
	client.submissionReconcilePollInterval = time.Millisecond
	outcome, err := client.reconcileAmbiguousSubmission(
		context.Background(), 77, errors.New("ambiguous POST"))
	if outcome.Submission != SubmissionUnknown {
		if err == nil {
			t.Fatal("reconciliation unexpectedly succeeded")
		}
		t.Fatalf("submission = %s, want %s", outcome.Submission, SubmissionUnknown)
	}
	if got := client.getRunID(); got != "77" {
		t.Fatalf("unrelated status changed owned run ID to %q", got)
	}
	if err := client.StopTest(); err != nil {
		t.Fatal(err)
	}
	if stoppedRunID != "77" {
		t.Fatalf("stop targeted run ID %q, want 77", stoppedRunID)
	}
}

func TestPublicStatusObservationNeverAdoptsUnattributedRunID(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		payload       string
		observedRunID string
		wantError     bool
	}{
		{name: "different numeric", payload: `{"state":"RUNNING","run_id":78}`, observedRunID: "78"},
		{name: "missing", payload: `{"state":"RUNNING"}`},
		{name: "malformed numeric", payload: `{"state":"RUNNING","run_id":"bad"}`, wantError: true},
		{name: "malformed json", payload: `{"state":`, wantError: true},
		{name: "legacy nonnumeric", payload: `{"state":"RUNNING","runId":"legacy"}`, observedRunID: "legacy"},
		{name: "different terminal", payload: `{"state":"COMPLETED","run_id":78}`, observedRunID: "78"},
		{name: "different unattributable state", payload: `{"state":"UNKNOWN","run_id":78}`, observedRunID: "78"},
		{name: "not found", statusCode: http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				if test.statusCode != 0 {
					writer.WriteHeader(test.statusCode)
					return
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(test.payload))
			}))
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			client.SetRunID("77")
			status, err := client.GetStatusContext(context.Background())
			if test.wantError {
				if err == nil {
					t.Fatal("GetStatusContext() succeeded, want error")
				}
			} else {
				if err != nil {
					t.Fatalf("GetStatusContext() error = %v", err)
				}
				if status.RunID != test.observedRunID {
					t.Fatalf("observed run ID = %q, want %q", status.RunID, test.observedRunID)
				}
			}
			if got := client.getRunID(); got != "77" {
				t.Fatalf("observation changed owned run ID to %q", got)
			}
		})
	}
}

func TestOrdinaryStatusPollingDoesNotRetargetStop(t *testing.T) {
	var stoppedRunID string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/status":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"state":"RUNNING","run_id":78}`))
		case request.URL.Path == "/run" && request.Method == http.MethodDelete:
			stoppedRunID = request.URL.Query().Get("runId")
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.SetRunID("77")
	status, err := client.GetStatusContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.RunID != "78" {
		t.Fatalf("observed run ID = %q, want 78", status.RunID)
	}
	if err = client.StopTest(); err != nil {
		t.Fatal(err)
	}
	if stoppedRunID != "77" {
		t.Fatalf("stop targeted run ID %q, want owned run 77", stoppedRunID)
	}
}

func TestStopWithoutOwnedRunIDRefusesObservedTarget(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"RUNNING","run_id":78}`))
	}))
	defer server.Close()

	err := NewSptAPIClient(server.URL).StopTest()
	if !errors.Is(err, ErrRunOwnershipUnknown) {
		t.Fatalf("StopTest() error = %v, want %v", err, ErrRunOwnershipUnknown)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("StopTest() sent %d requests without an owned run ID", got)
	}
}

func TestConcurrentPublicStatusObservationDoesNotChangeOwnership(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"RUNNING","run_id":78}`))
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.SetRunID("77")
	var wait sync.WaitGroup
	for index := 0; index < 16; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for attempt := 0; attempt < 8; attempt++ {
				_, _ = client.GetStatusContext(context.Background())
				_ = client.getRunID()
			}
		}()
	}
	wait.Wait()
	if got := client.getRunID(); got != "77" {
		t.Fatalf("concurrent observation changed owned run ID to %q", got)
	}
}

func TestConcurrentReconciliationAdoptsOnlyExpectedRun(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		runID := 78
		if calls.Add(1)%2 == 0 {
			runID = 77
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"RUNNING","run_id":` +
			strconv.Itoa(runID) + `}`))
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.SetRunID("77")
	client.submissionReconcileTimeout = time.Second
	client.submissionReconcilePollInterval = time.Millisecond
	var wait sync.WaitGroup
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for attempt := 0; attempt < 8; attempt++ {
				_, _ = client.GetStatusContext(context.Background())
			}
		}()
	}
	outcome, err := client.reconcileAmbiguousSubmission(
		context.Background(), 77, errors.New("ambiguous POST"))
	wait.Wait()
	if outcome.Submission != SubmissionSubmitted || outcome.RunID != "77" {
		t.Fatalf("reconciliation outcome = %+v, error = %v", outcome, err)
	}
	if got := client.getRunID(); got != "77" {
		t.Fatalf("reconciled ownership = %q, want 77", got)
	}
}
