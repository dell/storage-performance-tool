package results

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for p, h := range handlers {
		mux.HandleFunc(p, h)
	}
	return httptest.NewServer(mux)
}

func TestFetcher_HappyPath_AllArtifacts(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": 3},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
					{"logger": "metrics.File", "href": "/logs/" + step + "/metrics.File", "size": 5},
					{"logger": "Scenario", "href": "/logs/" + step + "/Scenario", "size": 3},
					{"logger": "metrics.threshold.FileTotal", "href": "/logs/" + step + "/metrics.threshold.FileTotal", "size": 2},
					{"logger": "OpTraces", "href": "/logs/" + step + "/OpTraces", "size": 5},
					{"logger": "PartsUpload", "href": "/logs/" + step + "/PartsUpload", "size": 10},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal":           func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Config":                      func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cfg")) },
		"/logs/" + step + "/Cli":                         func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cli")) },
		"/logs/" + step + "/Messages":                    func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msgs")) },
		"/logs/" + step + "/Errors":                      func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("errs")) },
		"/logs/" + step + "/metrics.File":                func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("mavg")) },
		"/logs/" + step + "/Scenario":                    func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("scn")) },
		"/logs/" + step + "/metrics.threshold.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("th")) },
		"/logs/" + step + "/OpTraces":                    func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("trace")) },
		// multipart via one of the aliases
		"/logs/" + step + "/PartsUpload": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("part,ts\n1,2\n")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {} // no-op

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}

	// Verify files and manifest
	required := []string{
		step + ".metrics.total.csv",
		step + ".config.yaml",
		step + ".cli.args.log",
		step + ".messages.log",
		step + ".errors.log",
	}
	for _, name := range required {
		p := filepath.Join(out, name)
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("missing required file: %s: %v", name, err)
		}
	}

	// Verify multipart saved
	if _, err := os.Stat(filepath.Join(out, step+".multipart.csv")); err != nil {
		t.Fatalf("missing multipart file: %v", err)
	}

	// Manifest exists and contains our step
	data, err := os.ReadFile(filepath.Join(out, "index.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(data), step) {
		t.Fatalf("manifest does not reference step %s", step)
	}
	if man.BaseURL == "" || man.OutputDir == "" || len(man.Steps) != 1 {
		t.Fatalf("manifest fields not populated correctly: %#v", man)
	}
}

func TestFetcher_DownloadsLargeArtifactsWithRanges(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	const pageSize = 1024 * 1024
	largeContent := bytes.Repeat([]byte("0123456789abcdef"), (2*pageSize+123)/16+1)
	largeContent = largeContent[:2*pageSize+123]
	var rangeCalls int32
	var plainItemsCalls int32

	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "items.csv", "href": "/logs/" + step + "/items.csv", "size": len(largeContent)},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/items.csv": func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				atomic.AddInt32(&plainItemsCalls, 1)
				_, _ = w.Write(largeContent[:pageSize])
				return
			}
			atomic.AddInt32(&rangeCalls, 1)
			var start, end int
			if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
				http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if start < 0 || end < start || end >= len(largeContent) {
				http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			_, _ = w.Write(largeContent[start : end+1])
		},
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}
	f.Artifacts = []ArtifactSpec{
		{Loggers: []string{"metrics.FileTotal"}, Suffix: "metrics.total.csv", Required: true},
		{Loggers: []string{"items.csv"}, Suffix: "items.csv", Required: false},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := f.FetchArtifactsForSteps(ctx, []string{step}); err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(out, step+".items.csv"))
	if err != nil {
		t.Fatalf("read items artifact: %v", err)
	}
	if !bytes.Equal(got, largeContent) {
		t.Fatalf("downloaded items length/content mismatch: got %d bytes, want %d", len(got), len(largeContent))
	}
	if atomic.LoadInt32(&rangeCalls) == 0 {
		t.Fatal("expected range requests for large artifact")
	}
	if atomic.LoadInt32(&plainItemsCalls) != 0 {
		t.Fatal("large artifact should not be fetched with a plain GET")
	}
}

func TestFetcher_LargeArtifactRangeFailureMarksError(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	const pageSize = 1024 * 1024
	largeSize := int64(pageSize + 10)
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "items.csv", "href": "/logs/" + step + "/items.csv", "size": largeSize},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/items.csv": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Range") == "bytes=0-1048575" {
				_, _ = w.Write(bytes.Repeat([]byte("a"), pageSize))
				return
			}
			http.Error(w, "chunk failed", http.StatusInternalServerError)
		},
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}
	f.Retries = 1
	f.Artifacts = []ArtifactSpec{
		{Loggers: []string{"metrics.FileTotal"}, Suffix: "metrics.total.csv", Required: true},
		{Loggers: []string{"items.csv"}, Suffix: "items.csv", Required: false},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error = %v", err)
	}
	var itemStatus *FileStatus
	for i := range man.Steps[0].Files {
		if man.Steps[0].Files[i].Name == step+".items.csv" {
			itemStatus = &man.Steps[0].Files[i]
			break
		}
	}
	if itemStatus == nil {
		t.Fatal("items.csv status not recorded")
	}
	if itemStatus.Status != "error" || !strings.Contains(itemStatus.Error, "chunk failed") {
		t.Fatalf("items.csv status = %#v, want error containing chunk failure", itemStatus)
	}
	if _, err := os.Stat(filepath.Join(out, step+".items.csv")); !os.IsNotExist(err) {
		t.Fatalf("partial items artifact should not exist, stat err = %v", err)
	}
}

func TestFetcher_MissingOptionalFiles(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	// Provide only required files; optional will 404
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": 3},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Config":            func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cfg")) },
		"/logs/" + step + "/Cli":               func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cli")) },
		"/logs/" + step + "/Messages":          func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msgs")) },
		"/logs/" + step + "/Errors":            func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("errs")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}

	// Required files present
	for _, name := range []string{
		".metrics.total.csv", ".config.yaml", ".cli.args.log", ".messages.log", ".errors.log",
	} {
		if _, statErr := os.Stat(filepath.Join(out, step+name)); statErr != nil {
			t.Fatalf("required file missing: %s: %v", step+name, statErr)
		}
	}

	// Manifest should mark missing optional with status missing
	b, _ := os.ReadFile(filepath.Join(out, "index.json"))
	if strings.Count(string(b), "\"status\": \"missing\"") == 0 {
		t.Fatalf("expected some missing statuses for optional files in manifest: %s", string(b))
	}
	_ = man
}

func TestFetcher_SanitizesConfigArtifact(t *testing.T) {
	step := "replay-001-20260606.000000.000-create"
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": 120},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Config": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`storage:
  auth:
    uid: LOCALACCESSKEY
    secret: LOCALSECRETKEY
  driver:
    type: s3
aws:
  accessKeyId: AWSACCESSKEY
  secretAccessKey: AWSSECRETKEY
`))
		},
		"/logs/" + step + "/Cli":      func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cli")) },
		"/logs/" + step + "/Messages": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msgs")) },
		"/logs/" + step + "/Errors":   func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("errs")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := f.FetchArtifactsForSteps(ctx, []string{step}); err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(out, step+".config.yaml"))
	if err != nil {
		t.Fatalf("read sanitized config: %v", err)
	}
	got := string(data)
	for _, leaked := range []string{"LOCALACCESSKEY", "LOCALSECRETKEY", "AWSACCESSKEY", "AWSSECRETKEY"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("sanitized config leaked %q:\n%s", leaked, got)
		}
	}
	if strings.Count(got, "***") < 4 {
		t.Fatalf("sanitized config did not mask expected fields:\n%s", got)
	}
}

func TestFetcher_RetryOnServerError(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	var count int32
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": 3},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) {
			c := atomic.AddInt32(&count, 1)
			if c < 3 {
				http.Error(w, "temporary", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte("total"))
		},
		// other required files OK
		"/logs/" + step + "/Config":   func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cfg")) },
		"/logs/" + step + "/Cli":      func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cli")) },
		"/logs/" + step + "/Messages": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msgs")) },
		"/logs/" + step + "/Errors":   func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("errs")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {} // avoid waiting between retries
	f.Retries = 5

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}

	// Verify the retried file exists
	if _, err := os.Stat(filepath.Join(out, step+".metrics.total.csv")); err != nil {
		t.Fatalf("metrics.total.csv not saved after retries: %v", err)
	}

	// Manifest JSON decodes
	data, err := os.ReadFile(filepath.Join(out, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var parsed Manifest
	if json.Unmarshal(data, &parsed) != nil {
		t.Fatalf("manifest not valid JSON: %s", string(data))
	}
	_ = man
}

func TestFetcher_IndexRetrySucceeds(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	var indexCalls int32
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&indexCalls, 1)
			if n < 3 {
				// Return empty items array for first two attempts
				_ = json.NewEncoder(w).Encode(map[string]any{"step_id": step, "items": []map[string]any{}})
				return
			}
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": 3},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Config":            func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cfg")) },
		"/logs/" + step + "/Cli":               func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cli")) },
		"/logs/" + step + "/Messages":          func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msgs")) },
		"/logs/" + step + "/Errors":            func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("errs")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}
	f.Retries = 5

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, step+".metrics.total.csv")); err != nil {
		t.Fatalf("metrics.total.csv not found after index retry: %v", err)
	}
	if atomic.LoadInt32(&indexCalls) < 3 {
		t.Fatalf("expected at least 3 index calls for retry, got %d", atomic.LoadInt32(&indexCalls))
	}
	_ = man
}

func TestFetcher_IndexRetryExhausted(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			// Always return empty items
			_ = json.NewEncoder(w).Encode(map[string]any{"step_id": step, "items": []map[string]any{}})
		},
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}
	f.Retries = 3

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err == nil {
		t.Fatal("expected error when all retries exhausted with empty index, got nil")
	}
	// Manifest should still be returned with missing files
	if man == nil {
		t.Fatal("expected non-nil manifest even on error")
	}
}

func TestFetcher_UsesIndexJsonForDiscovery(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	// Provide index.json listing required artifacts; serve those endpoints; omit optionals
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5, "modified": time.Now().UTC().Format(time.RFC1123), "content_type": "text/csv"},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": 3, "modified": time.Now().UTC().Format(time.RFC1123), "content_type": "text/yaml"},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Config":            func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cfg")) },
		"/logs/" + step + "/Cli":               func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("cli")) },
		"/logs/" + step + "/Messages":          func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msg")) },
		"/logs/" + step + "/Errors":            func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("err")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}
	// All required files present
	for _, name := range []string{
		".metrics.total.csv", ".config.yaml", ".cli.args.log", ".messages.log", ".errors.log",
	} {
		if _, statErr := os.Stat(filepath.Join(out, step+name)); statErr != nil {
			t.Fatalf("required file missing via index.json path: %s: %v", step+name, statErr)
		}
	}
	_ = man
}

func TestNormalizeResultXML_BrokenHeaderFooterFirst(t *testing.T) {
	// Reproduces the real bug: log4j2 writes <result>\n</result>\n before the entries.
	input := "<result>\n</result>\n" +
		`<result id="step-1" operation="READ" tps="100" />` + "\n" +
		`<result id="step-1" operation="CREATE" tps="50" />` + "\n"
	want := "<result>\n" +
		`<result id="step-1" operation="READ" tps="100" />` + "\n" +
		`<result id="step-1" operation="CREATE" tps="50" />` + "\n" +
		"</result>\n"

	f := filepath.Join(t.TempDir(), "result.xml")
	if err := os.WriteFile(f, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	size := normalizeResultXML(f, "result")
	got, _ := os.ReadFile(f)
	if string(got) != want {
		t.Fatalf("normalizeResultXML mismatch:\ngot:  %q\nwant: %q", string(got), want)
	}
	if size != int64(len(want)) {
		t.Fatalf("size mismatch: got %d, want %d", size, len(want))
	}
}

func TestNormalizeResultXML_AlreadyCorrect(t *testing.T) {
	// Content that's already properly wrapped (e.g. from a future fixed engine).
	input := "<result>\n" +
		`<result id="step-1" operation="READ" tps="100" />` + "\n" +
		"</result>\n"
	want := input

	f := filepath.Join(t.TempDir(), "result.xml")
	if err := os.WriteFile(f, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	normalizeResultXML(f, "result")
	got, _ := os.ReadFile(f)
	if string(got) != want {
		t.Fatalf("normalizeResultXML changed correct input:\ngot:  %q\nwant: %q", string(got), want)
	}
}

func TestNormalizeResultXML_RawEntriesNoWrapper(t *testing.T) {
	// New engine: no header/footer at all, just bare entries.
	input := `<result id="step-1" operation="READ" tps="100" />` + "\n" +
		`<result id="step-1" operation="CREATE" tps="50" />` + "\n"
	want := "<result>\n" +
		`<result id="step-1" operation="READ" tps="100" />` + "\n" +
		`<result id="step-1" operation="CREATE" tps="50" />` + "\n" +
		"</result>\n"

	f := filepath.Join(t.TempDir(), "result.xml")
	if err := os.WriteFile(f, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	normalizeResultXML(f, "result")
	got, _ := os.ReadFile(f)
	if string(got) != want {
		t.Fatalf("normalizeResultXML mismatch:\ngot:  %q\nwant: %q", string(got), want)
	}
}

func TestNormalizeResultXML_ThresholdRootTag(t *testing.T) {
	input := "<result-with-threshold>\n</result-with-threshold>\n" +
		`<result id="step-1" operation="READ" />` + "\n"
	want := "<result-with-threshold>\n" +
		`<result id="step-1" operation="READ" />` + "\n" +
		"</result-with-threshold>\n"

	f := filepath.Join(t.TempDir(), "result-threshold.xml")
	if err := os.WriteFile(f, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	normalizeResultXML(f, "result-with-threshold")
	got, _ := os.ReadFile(f)
	if string(got) != want {
		t.Fatalf("normalizeResultXML mismatch:\ngot:  %q\nwant: %q", string(got), want)
	}
}

func TestNormalizeResultXML_SingleEntry(t *testing.T) {
	// Single-op step: one entry, no wrapper from engine.
	input := `<result id="step-1" operation="READ" tps="100" />` + "\n"
	want := "<result>\n" +
		`<result id="step-1" operation="READ" tps="100" />` + "\n" +
		"</result>\n"

	f := filepath.Join(t.TempDir(), "result.xml")
	if err := os.WriteFile(f, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	normalizeResultXML(f, "result")
	got, _ := os.ReadFile(f)
	if string(got) != want {
		t.Fatalf("normalizeResultXML mismatch:\ngot:  %q\nwant: %q", string(got), want)
	}
}

func TestNormalizeResultXML_EmptyFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "result.xml")
	if err := os.WriteFile(f, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	normalizeResultXML(f, "result")
	got, _ := os.ReadFile(f)
	want := "<result>\n</result>\n"
	if string(got) != want {
		t.Fatalf("normalizeResultXML empty mismatch:\ngot:  %q\nwant: %q", string(got), want)
	}
}
