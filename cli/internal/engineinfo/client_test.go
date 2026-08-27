package engineinfo_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
)

func TestClientFetchesCompleteSchemaOneBuildInformation(t *testing.T) {
	dirty := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/version" {
			t.Errorf("request = %s %s, want GET /version", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"schema_version": 1,
			"product": "spt-engine",
			"version": "5.14.2",
			"revision": "0123456789abcdef0123456789abcdef01234567",
			"build_time": "2026-08-26T12:34:56Z",
			"development": false,
			"source_dirty": false,
			"additive_field": "ignored"
		}`)
	}))
	defer server.Close()

	result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if result.Status != engineinfo.StatusCollected || !result.Complete {
		t.Fatalf("Fetch() status/complete = %q/%t, want collected/true", result.Status, result.Complete)
	}
	want := engineinfo.BuildInformation{
		SchemaVersion: 1,
		Product:       "spt-engine",
		Version:       "5.14.2",
		Revision:      "0123456789abcdef0123456789abcdef01234567",
		BuildTime:     "2026-08-26T12:34:56Z",
		Development:   false,
		SourceDirty:   &dirty,
	}
	if result.Build == nil || !reflect.DeepEqual(*result.Build, want) {
		t.Fatalf("Fetch() build = %+v, want %+v", result.Build, want)
	}
	if result.ReportedSchemaVersion != 1 || result.Reason != "" {
		t.Fatalf("Fetch() metadata = schema %d reason %q", result.ReportedSchemaVersion, result.Reason)
	}
}

func TestClientClassifiesSupportedBuildInformationCompleteness(t *testing.T) {
	tests := []struct {
		name         string
		version      string
		revision     string
		buildTime    string
		sourceDirty  string
		wantStatus   engineinfo.CollectionStatus
		wantComplete bool
	}{
		{
			name:         "unknown build time remains complete",
			version:      "5.14.2",
			revision:     "0123456789abcdef0123456789abcdef01234567",
			buildTime:    "unknown",
			sourceDirty:  "false",
			wantStatus:   engineinfo.StatusCollected,
			wantComplete: true,
		},
		{
			name:         "unknown revision is incomplete",
			version:      "5.14.2",
			revision:     "unknown",
			buildTime:    "2026-08-26T12:34:56Z",
			sourceDirty:  "false",
			wantStatus:   engineinfo.StatusIncompleteBuildInfo,
			wantComplete: false,
		},
		{
			name:         "unknown source state is incomplete",
			version:      "5.14.2",
			revision:     "0123456789abcdef0123456789abcdef01234567",
			buildTime:    "2026-08-26T12:34:56Z",
			sourceDirty:  "null",
			wantStatus:   engineinfo.StatusIncompleteBuildInfo,
			wantComplete: false,
		},
		{
			name:         "unknown version is incomplete",
			version:      "unknown",
			revision:     "0123456789abcdef0123456789abcdef01234567",
			buildTime:    "2026-08-26T12:34:56Z",
			sourceDirty:  "false",
			wantStatus:   engineinfo.StatusIncompleteBuildInfo,
			wantComplete: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{
					"schema_version": 1,
					"product": "spt-engine",
					"version": %q,
					"revision": %q,
					"build_time": %q,
					"development": true,
					"source_dirty": %s
				}`, test.version, test.revision, test.buildTime, test.sourceDirty)
			}))
			defer server.Close()

			result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
			if err != nil {
				t.Fatalf("Fetch() error = %v", err)
			}
			if result.Status != test.wantStatus || result.Complete != test.wantComplete {
				t.Fatalf("Fetch() status/complete = %q/%t, want %q/%t", result.Status, result.Complete, test.wantStatus, test.wantComplete)
			}
			if result.Build == nil {
				t.Fatal("Fetch() discarded supported build information")
			}
		})
	}
}

func TestClientVersionMatchesTheSharedSemanticVersionContract(t *testing.T) {
	for _, fixture := range semanticVersionFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			body := strings.Replace(validDevelopmentBuildJSON, `"version": "5.14.2"`,
				fmt.Sprintf(`"version": %q`, fixture.version), 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, body)
			}))
			defer server.Close()

			result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
			if fixture.valid {
				if err != nil || result.Status != engineinfo.StatusCollected || result.Build == nil {
					t.Fatalf("Fetch() = (%+v, %v), want collected", result, err)
				}
				if result.Build.Version != fixture.version {
					t.Fatalf("version = %q, want exact %q", result.Build.Version, fixture.version)
				}
				return
			}
			if err == nil || result.Status != engineinfo.StatusCollectionFailed || result.Build != nil {
				t.Fatalf("Fetch() = (%+v, %v), want non-forceable contract failure", result, err)
			}
		})
	}
}

func TestClientClassifiesLegacyVersionEndpoints(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.WriteHeader(status)
				_, _ = fmt.Fprint(w, `credential=LEGACY_SECRET path=/private/location`)
			}))
			defer server.Close()

			result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
			if err != nil {
				t.Fatalf("Fetch() error = %v", err)
			}
			if result.Status != engineinfo.StatusLegacyEndpointUnavailable || result.Build != nil || result.ReportedSchemaVersion != 0 {
				t.Fatalf("Fetch() result = %+v, want safe legacy classification", result)
			}
			if attempts.Load() != 1 || result.Attempts != 1 {
				t.Fatalf("requests/reported attempts = %d/%d, want immediate 1/1", attempts.Load(), result.Attempts)
			}
			assertExcludes(t, result.Reason, "LEGACY_SECRET", "/private/location")
		})
	}
}

func TestClientClassifiesFutureSchemaWithoutRetainingUnknownResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"schema_version": 27,
			"future_secret": "FUTURE_SECRET_71e9",
			"host_path": "/private/future/path",
			"topology": {"container_id": "unsafe-container"}
		}`)
	}))
	defer server.Close()

	result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if result.Status != engineinfo.StatusUnsupportedSchema || result.ReportedSchemaVersion != 27 || result.Build != nil || result.Complete {
		t.Fatalf("Fetch() result = %+v, want safe unsupported-schema classification", result)
	}
	if result.Reason == "" {
		t.Fatal("Fetch() omitted unsupported-schema reason")
	}
	assertExcludes(t, result.Reason, "FUTURE_SECRET_71e9", "/private/future/path", "unsafe-container", server.URL)
}

func TestClientRejectsMalformedAndContractInvalidSchemaOneResponses(t *testing.T) {
	valid := `{
		"schema_version": 1,
		"product": "spt-engine",
		"version": "5.14.2",
		"revision": "0123456789abcdef0123456789abcdef01234567",
		"build_time": "2026-08-26T12:34:56Z",
		"development": true,
		"source_dirty": false
	}`
	tests := []struct {
		name        string
		contentType string
		body        string
	}{
		{name: "malformed JSON", contentType: "application/json", body: `{"schema_version": 1, "secret": "RAW_SECRET_91ba"`},
		{name: "non-object JSON", contentType: "application/json", body: `[]`},
		{name: "missing schema", contentType: "application/json", body: strings.Replace(valid, `"schema_version": 1,`, "", 1)},
		{name: "invalid schema type", contentType: "application/json", body: strings.Replace(valid, `"schema_version": 1`, `"schema_version": "1"`, 1)},
		{name: "invalid schema number", contentType: "application/json", body: strings.Replace(valid, `"schema_version": 1`, `"schema_version": 0`, 1)},
		{name: "missing product", contentType: "application/json", body: strings.Replace(valid, `"product": "spt-engine",`, "", 1)},
		{name: "wrong product", contentType: "application/json", body: strings.Replace(valid, `"product": "spt-engine"`, `"product": "other"`, 1)},
		{name: "duplicate field", contentType: "application/json", body: strings.Replace(valid, `"product": "spt-engine"`, `"product": "spt-engine", "product": "spt-engine"`, 1)},
		{name: "missing version", contentType: "application/json", body: strings.Replace(valid, `"version": "5.14.2",`, "", 1)},
		{name: "invalid version", contentType: "application/json", body: strings.Replace(valid, `"version": "5.14.2"`, `"version": "not semantic"`, 1)},
		{name: "invalid numeric prerelease", contentType: "application/json", body: strings.Replace(valid, `"version": "5.14.2"`, `"version": "5.14.2-01"`, 1)},
		{name: "missing revision", contentType: "application/json", body: strings.Replace(valid, `"revision": "0123456789abcdef0123456789abcdef01234567",`, "", 1)},
		{name: "invalid revision", contentType: "application/json", body: strings.Replace(valid, `"revision": "0123456789abcdef0123456789abcdef01234567"`, `"revision": "/private/revision"`, 1)},
		{name: "missing build time", contentType: "application/json", body: strings.Replace(valid, `"build_time": "2026-08-26T12:34:56Z",`, "", 1)},
		{name: "invalid build time", contentType: "application/json", body: strings.Replace(valid, `"build_time": "2026-08-26T12:34:56Z"`, `"build_time": "/private/build/time"`, 1)},
		{name: "missing development", contentType: "application/json", body: strings.Replace(valid, `"development": true,`, "", 1)},
		{name: "invalid development type", contentType: "application/json", body: strings.Replace(valid, `"development": true`, `"development": "true"`, 1)},
		{name: "missing source dirty", contentType: "application/json", body: strings.Replace(valid, `,
		"source_dirty": false`, "", 1)},
		{name: "invalid source dirty type", contentType: "application/json", body: strings.Replace(valid, `"source_dirty": false`, `"source_dirty": "false"`, 1)},
		{name: "incomplete release", contentType: "application/json", body: strings.NewReplacer(`"development": true`, `"development": false`, `"revision": "0123456789abcdef0123456789abcdef01234567"`, `"revision": "unknown"`).Replace(valid)},
		{name: "unknown release version", contentType: "application/json", body: strings.NewReplacer(`"development": true`, `"development": false`, `"version": "5.14.2"`, `"version": "unknown"`).Replace(valid)},
		{name: "unknown release build time", contentType: "application/json", body: strings.NewReplacer(`"development": true`, `"development": false`, `"build_time": "2026-08-26T12:34:56Z"`, `"build_time": "unknown"`).Replace(valid)},
		{name: "unknown release source state", contentType: "application/json", body: strings.NewReplacer(`"development": true`, `"development": false`, `"source_dirty": false`, `"source_dirty": null`).Replace(valid)},
		{name: "dirty release", contentType: "application/json", body: strings.NewReplacer(`"development": true`, `"development": false`, `"source_dirty": false`, `"source_dirty": true`).Replace(valid)},
		{name: "wrong content type", contentType: "text/plain", body: valid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.Header().Set("Content-Type", test.contentType)
				_, _ = fmt.Fprint(w, test.body)
			}))
			defer server.Close()

			result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
			if err == nil {
				t.Fatalf("Fetch() result = %+v, want hard collection failure", result)
			}
			if result.Status != engineinfo.StatusCollectionFailed || result.Build != nil || result.Reason == "" {
				t.Fatalf("Fetch() result = %+v, want safe failed classification", result)
			}
			if attempts.Load() != 1 || result.Attempts != 1 {
				t.Fatalf("requests/reported attempts = %d/%d, want terminal 1/1", attempts.Load(), result.Attempts)
			}
			assertExcludes(t, result.Reason, "RAW_SECRET_91ba", "/private/revision", "/private/build/time", test.body, server.URL)
			assertExcludes(t, err.Error(), "RAW_SECRET_91ba", "/private/revision", "/private/build/time", test.body, server.URL)
		})
	}
}

func TestClientRetriesEveryTransientHTTPStatusWithinConfiguredBounds(t *testing.T) {
	statuses := []int{http.StatusTooManyRequests}
	for status := http.StatusInternalServerError; status <= 599; status++ {
		statuses = append(statuses, status)
	}

	for _, status := range statuses {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if attempts.Add(1) == 1 {
					w.WriteHeader(status)
					_, _ = fmt.Fprint(w, `token=TRANSIENT_SECRET path=/private/transient`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, validDevelopmentBuildJSON)
			}))
			defer server.Close()

			result, err := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
				RequestAttempts: 2,
				RequestTimeout:  time.Second,
				RetryDelay:      time.Nanosecond,
			}).Fetch(context.Background(), server.URL)
			if err != nil || result.Status != engineinfo.StatusCollected {
				t.Fatalf("Fetch() = (%+v, %v), want collected after HTTP %d retry", result, err, status)
			}
			if attempts.Load() != 2 || result.Attempts != 2 {
				t.Fatalf("requests/reported attempts = %d/%d, want 2/2", attempts.Load(), result.Attempts)
			}
		})
	}
}

func TestClientTransientHTTPStatusExhaustionIsSafeAndBounded(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.WriteHeader(status)
				_, _ = fmt.Fprint(w, `token=EXHAUSTION_SECRET path=/private/exhaustion`)
			}))
			defer server.Close()

			result, err := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
				RequestAttempts: 3,
				RequestTimeout:  time.Second,
				RetryDelay:      time.Nanosecond,
			}).Fetch(context.Background(), server.URL)
			if err == nil || result.Status != engineinfo.StatusCollectionFailed {
				t.Fatalf("Fetch() = (%+v, %v), want fatal HTTP %d exhaustion", result, err, status)
			}
			if attempts.Load() != 3 || result.Attempts != 3 {
				t.Fatalf("requests/reported attempts = %d/%d, want bounded 3/3", attempts.Load(), result.Attempts)
			}
			assertExcludes(t, result.Reason, "EXHAUSTION_SECRET", "/private/exhaustion", server.URL)
			assertExcludes(t, err.Error(), "EXHAUSTION_SECRET", "/private/exhaustion", server.URL)
		})
	}
}

func TestClientCancellationStopsTransientHTTPBackoffWithoutAnotherRequest(t *testing.T) {
	var attempts atomic.Int32
	firstResponse := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			close(firstResponse)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
		RequestAttempts: 3,
		RequestTimeout:  time.Second,
		RetryDelay:      time.Minute,
	})
	type fetchOutcome struct {
		result engineinfo.CollectionResult
		err    error
	}
	done := make(chan fetchOutcome, 1)
	go func() {
		result, err := client.Fetch(ctx, server.URL)
		done <- fetchOutcome{result: result, err: err}
	}()
	<-firstResponse
	cancel()

	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("Fetch() error = %v, want context cancellation", outcome.err)
		}
		if attempts.Load() != 1 || outcome.result.Attempts != 1 {
			t.Fatalf("requests/reported attempts = %d/%d, want canceled 1/1", attempts.Load(), outcome.result.Attempts)
		}
	case <-time.After(time.Second):
		t.Fatal("Fetch() did not stop promptly during transient HTTP backoff")
	}
}

func TestClientDoesNotRetryPermanentHTTPStatuses(t *testing.T) {
	for status := http.StatusBadRequest; status <= 499; status++ {
		if status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusTooManyRequests {
			continue
		}
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.WriteHeader(status)
				_, _ = fmt.Fprint(w, `token=PERMANENT_SECRET path=/private/permanent`)
			}))
			defer server.Close()

			result, err := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
				RequestAttempts: 3,
				RequestTimeout:  time.Second,
				RetryDelay:      time.Nanosecond,
			}).Fetch(context.Background(), server.URL)
			if err == nil || result.Status != engineinfo.StatusCollectionFailed {
				t.Fatalf("Fetch() = (%+v, %v), want terminal HTTP %d failure", result, err, status)
			}
			if attempts.Load() != 1 || result.Attempts != 1 {
				t.Fatalf("requests/reported attempts = %d/%d, want terminal 1/1", attempts.Load(), result.Attempts)
			}
			assertExcludes(t, result.Reason, "PERMANENT_SECRET", "/private/permanent", server.URL)
			assertExcludes(t, err.Error(), "PERMANENT_SECRET", "/private/permanent", server.URL)
		})
	}
}

func TestClientRetriesTransportFailuresWithinConfiguredBounds(t *testing.T) {
	t.Run("retry succeeds", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) < 3 {
				dropConnection(t, w)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, validDevelopmentBuildJSON)
		}))
		defer server.Close()

		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			RequestAttempts: 3,
			RequestTimeout:  time.Second,
			RetryDelay:      time.Millisecond,
		})
		result, err := client.Fetch(context.Background(), server.URL)
		if err != nil || result.Status != engineinfo.StatusCollected {
			t.Fatalf("Fetch() = (%+v, %v), want collected after retries", result, err)
		}
		if got := attempts.Load(); got != 3 {
			t.Fatalf("requests = %d, want 3", got)
		}
		if result.Attempts != 3 {
			t.Fatalf("reported attempts = %d, want 3 for verbose fleet detail", result.Attempts)
		}
	})

	t.Run("retry exhaustion is fatal", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			dropConnection(t, w)
		}))
		defer server.Close()

		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			RequestAttempts: 3,
			RequestTimeout:  time.Second,
			RetryDelay:      time.Millisecond,
		})
		result, err := client.Fetch(context.Background(), server.URL)
		if err == nil || result.Status != engineinfo.StatusCollectionFailed {
			t.Fatalf("Fetch() = (%+v, %v), want hard exhaustion failure", result, err)
		}
		if got := attempts.Load(); got != 3 {
			t.Fatalf("requests = %d, want bounded 3", got)
		}
		if result.Attempts != 3 {
			t.Fatalf("reported attempts = %d, want 3", result.Attempts)
		}
		assertExcludes(t, result.Reason, server.URL)
		assertExcludes(t, err.Error(), server.URL)
	})

	t.Run("per-attempt timeout is bounded", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			<-r.Context().Done()
		}))
		defer server.Close()

		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			RequestAttempts: 2,
			RequestTimeout:  20 * time.Millisecond,
			RetryDelay:      time.Millisecond,
		})
		started := time.Now()
		result, err := client.Fetch(context.Background(), server.URL)
		if err == nil || result.Status != engineinfo.StatusCollectionFailed {
			t.Fatalf("Fetch() = (%+v, %v), want bounded timeout failure", result, err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("Fetch() took %s, want bounded completion", elapsed)
		}
		if got := attempts.Load(); got != 2 {
			t.Fatalf("requests = %d, want 2", got)
		}
	})
}

func TestClientCancellationStopsIOAndBackoffPromptly(t *testing.T) {
	t.Run("in-flight request", func(t *testing.T) {
		started := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			RequestAttempts: 3,
			RequestTimeout:  time.Minute,
			RetryDelay:      time.Minute,
		})
		done := make(chan error, 1)
		go func() {
			_, err := client.Fetch(ctx, server.URL)
			done <- err
		}()
		<-started
		cancel()
		assertPromptCancellation(t, done)
	})

	t.Run("retry backoff", func(t *testing.T) {
		attempted := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(attempted)
			dropConnection(t, w)
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			RequestAttempts: 3,
			RequestTimeout:  time.Minute,
			RetryDelay:      time.Minute,
		})
		done := make(chan error, 1)
		go func() {
			_, err := client.Fetch(ctx, server.URL)
			done <- err
		}()
		<-attempted
		time.Sleep(10 * time.Millisecond)
		cancel()
		assertPromptCancellation(t, done)
	})
}

func TestClientBoundsResponsesAndDoesNotRetryContractFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"future_secret":"`+strings.Repeat("x", 256)+`"}`)
	}))
	defer server.Close()

	client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
		RequestAttempts:  3,
		RequestTimeout:   time.Second,
		RetryDelay:       time.Millisecond,
		ResponseMaxBytes: 32,
	})
	result, err := client.Fetch(context.Background(), server.URL)
	if err == nil || result.Status != engineinfo.StatusCollectionFailed {
		t.Fatalf("Fetch() = (%+v, %v), want bounded hard failure", result, err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("requests = %d, want no retry for contract failure", got)
	}
	assertExcludes(t, result.Reason, strings.Repeat("x", 32), server.URL)
}

func TestClientRejectsContractInvalidNonOKSuccessStatuses(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusAccepted, http.StatusPartialContent} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = fmt.Fprint(w, validDevelopmentBuildJSON)
			}))
			defer server.Close()

			result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
			if err == nil || result.Status != engineinfo.StatusCollectionFailed {
				t.Fatalf("Fetch() = (%+v, %v), want contract failure for HTTP %d", result, err, status)
			}
			if got := attempts.Load(); got != 1 {
				t.Fatalf("requests = %d, want no retry", got)
			}
		})
	}
}

func TestClientProductionDefaultsAreBounded(t *testing.T) {
	t.Run("attempts and retry delay", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			dropConnection(t, w)
		}))
		defer server.Close()

		started := time.Now()
		_, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("Fetch() error = nil, want transport exhaustion")
		}
		if got := attempts.Load(); got != constants.EngineVersionRequestAttempts {
			t.Fatalf("requests = %d, want shipped bound %d", got, constants.EngineVersionRequestAttempts)
		}
		minimumDelay := time.Duration(constants.EngineVersionRequestAttempts-1) * constants.EngineVersionRetryDelay
		if elapsed < minimumDelay {
			t.Fatalf("Fetch() took %s, want at least shipped retry delay %s", elapsed, minimumDelay)
		}
	})

	t.Run("per-attempt timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer server.Close()

		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{RequestAttempts: 1})
		started := time.Now()
		_, err := client.Fetch(context.Background(), server.URL)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("Fetch() error = nil, want request timeout")
		}
		if elapsed < constants.EngineVersionRequestTimeout-500*time.Millisecond || elapsed > constants.EngineVersionRequestTimeout+2*time.Second {
			t.Fatalf("Fetch() took %s, want shipped timeout near %s", elapsed, constants.EngineVersionRequestTimeout)
		}
	})

	t.Run("response size", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, strings.Repeat("x", int(constants.EngineVersionResponseMaxBytes)+1))
		}))
		defer server.Close()

		result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
		if err == nil || result.Status != engineinfo.StatusCollectionFailed || !strings.Contains(result.Reason, "size limit") {
			t.Fatalf("Fetch() = (%+v, %v), want shipped response-size failure", result, err)
		}
	})
}

func TestClientRetriesTruncatedResponseTransportFailure(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeTruncatedJSONResponse(t, w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, validDevelopmentBuildJSON)
	}))
	defer server.Close()

	client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
		RequestAttempts: 2,
		RequestTimeout:  time.Second,
		RetryDelay:      time.Millisecond,
	})
	result, err := client.Fetch(context.Background(), server.URL)
	if err != nil || result.Status != engineinfo.StatusCollected {
		t.Fatalf("Fetch() = (%+v, %v), want retry success after truncated body", result, err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
}

func TestClientReturnsSafeDiagnosticsForRequestAndHTTPFailures(t *testing.T) {
	t.Run("invalid request URL", func(t *testing.T) {
		const unsafeURL = "http://user:URL_SECRET_53af@example.invalid/\n/private/request/path"
		result, err := engineinfo.NewClient().Fetch(context.Background(), unsafeURL)
		if err == nil || result.Status != engineinfo.StatusCollectionFailed {
			t.Fatalf("Fetch() = (%+v, %v), want request failure", result, err)
		}
		assertExcludes(t, result.Reason, "URL_SECRET_53af", "/private/request/path", unsafeURL)
		assertExcludes(t, err.Error(), "URL_SECRET_53af", "/private/request/path", unsafeURL)
	})

	t.Run("redirect is not followed", func(t *testing.T) {
		var originRequests atomic.Int32
		var redirectedRequests atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			redirectedRequests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, validDevelopmentBuildJSON)
		}))
		defer target.Close()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			originRequests.Add(1)
			w.Header().Set("Location", target.URL+"/private/redirect?token=REDIRECT_SECRET_f6a1")
			w.WriteHeader(http.StatusFound)
		}))
		defer server.Close()

		result, err := engineinfo.NewClient().Fetch(context.Background(), server.URL)
		if err == nil || result.Status != engineinfo.StatusCollectionFailed {
			t.Fatalf("Fetch() = (%+v, %v), want redirect contract failure", result, err)
		}
		if originRequests.Load() != 1 || result.Attempts != 1 || redirectedRequests.Load() != 0 {
			t.Fatalf("origin/reported/redirect target requests = %d/%d/%d, want 1/1/0",
				originRequests.Load(), result.Attempts, redirectedRequests.Load())
		}
		assertExcludes(t, result.Reason, "REDIRECT_SECRET_f6a1", "/private/redirect", target.URL)
		assertExcludes(t, err.Error(), "REDIRECT_SECRET_f6a1", "/private/redirect", target.URL)
	})

	t.Run("permanent HTTP status", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusTeapot)
			_, _ = fmt.Fprint(w, `token=HTTP_SECRET_d2f8 path=/private/http/path`)
		}))
		defer server.Close()

		result, err := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			RequestAttempts: 3,
			RetryDelay:      time.Millisecond,
		}).Fetch(context.Background(), server.URL)
		if err == nil || result.Status != engineinfo.StatusCollectionFailed {
			t.Fatalf("Fetch() = (%+v, %v), want HTTP status failure", result, err)
		}
		if got := attempts.Load(); got != 1 {
			t.Fatalf("requests = %d, want no retry", got)
		}
		assertExcludes(t, result.Reason, "HTTP_SECRET_d2f8", "/private/http/path", server.URL)
		assertExcludes(t, err.Error(), "HTTP_SECRET_d2f8", "/private/http/path", server.URL)
	})
}

const validDevelopmentBuildJSON = `{
	"schema_version": 1,
	"product": "spt-engine",
	"version": "5.14.2",
	"revision": "0123456789abcdef0123456789abcdef01234567",
	"build_time": "2026-08-26T12:34:56Z",
	"development": true,
	"source_dirty": false
}`

type semanticVersionFixture struct {
	valid   bool
	name    string
	version string
}

func semanticVersionFixtures(t *testing.T) []semanticVersionFixture {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate semantic-version fixture from test source")
	}
	path := filepath.Join(filepath.Dir(testFile), "..", "..", "..", "test-fixtures", "engine-build-info", "semantic-versions.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read semantic-version fixture: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	fixtures := make([]semanticVersionFixture, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "|", 3)
		if len(parts) != 3 || (parts[0] != "valid" && parts[0] != "invalid") {
			t.Fatalf("invalid semantic-version fixture line %q", line)
		}
		fixtures = append(fixtures, semanticVersionFixture{
			valid: parts[0] == "valid", name: parts[1], version: parts[2],
		})
	}
	return fixtures
}

func dropConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		t.Fatal("test server does not support connection hijacking")
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	_ = connection.Close()
}

func writeTruncatedJSONResponse(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		t.Fatal("test server does not support connection hijacking")
	}
	connection, buffer, err := hijacker.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	_, _ = buffer.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"schema_version\":1}")
	_ = buffer.Flush()
	_ = connection.Close()
}

func assertPromptCancellation(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Fetch() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Fetch() did not stop promptly after cancellation")
	}
}

func assertExcludes(t *testing.T, value string, forbidden ...string) {
	t.Helper()
	for _, text := range forbidden {
		if strings.Contains(value, text) {
			t.Errorf("%q contains forbidden diagnostic text %q", value, text)
		}
	}
}
