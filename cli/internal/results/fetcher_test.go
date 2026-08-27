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

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func newTestServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for p, h := range handlers {
		mux.HandleFunc(p, h)
	}
	return httptest.NewServer(mux)
}

func enginePagedArtifactHandler(content []byte, pageSize int, plainCalls, rangeCalls *int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(content)))
			return
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			if plainCalls != nil {
				atomic.AddInt32(plainCalls, 1)
			}
			_, _ = w.Write(content[:min(len(content), pageSize-1)])
			return
		}
		if rangeCalls != nil {
			atomic.AddInt32(rangeCalls, 1)
		}
		var start, end int64
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start < 0 || end < start || start >= int64(len(content)) {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		n := min(end+1, int64(pageSize), int64(len(content))-start)
		_, _ = w.Write(content[int(start):int(start+n)])
	}
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
					{"logger": "metrics.File", "href": "/logs/" + step + "/metrics.File", "size": 4},
					{"logger": "Scenario", "href": "/logs/" + step + "/Scenario", "size": 3},
					{"logger": "metrics.threshold.FileTotal", "href": "/logs/" + step + "/metrics.threshold.FileTotal", "size": 2},
					{"logger": "OpTraces", "href": "/logs/" + step + "/OpTraces", "size": 5},
					{"logger": "PartsUpload", "href": "/logs/" + step + "/PartsUpload", "size": 12},
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

func TestDefaultArtifactsRegisterCompleteDeleteEvidence(t *testing.T) {
	want := map[string]string{
		"delete.metrics.total.csv":   "DeleteMetricsTotal",
		"delete.requests.csv":        "DeleteRequests",
		"delete.objects.csv":         "DeleteObjects",
		"delete.verification.csv":    "DeleteVerification",
		"items.csv":                  "DeleteResidual",
		"verify-input.csv":           "DeleteSelection",
		"verify-input.complete.json": "DeleteSelectionCompletion",
		"delete.complete.json":       "DeleteCompletion",
	}
	for suffix, logger := range want {
		found := false
		for _, spec := range DefaultArtifacts {
			if spec.Suffix != suffix {
				continue
			}
			for _, candidate := range spec.Loggers {
				found = found || candidate == logger
			}
		}
		if !found {
			t.Errorf("DELETE artifact %q is not registered with logger %q", suffix, logger)
		}
	}
}

func TestSuccessfulDeleteTotalsDoesNotSubstituteForRequiredGenericTotals(t *testing.T) {
	sm := StepManifest{StepID: "mt-001-delete", Files: []FileStatus{
		{Name: "mt-001-delete.delete.metrics.total.csv", Status: fileStatusOK},
		{Name: "mt-001-delete.metrics.total.csv", Status: fileStatusMissing},
	}}
	if hasSuccessfulGenericTotals(sm) {
		t.Fatal("DELETE totals v1 must not satisfy the required generic totals gate")
	}
	sm.Files[1].Status = fileStatusOK
	if !hasSuccessfulGenericTotals(sm) {
		t.Fatal("exact step-scoped generic totals should satisfy the gate")
	}
}

func TestDeleteNodeSourcesAreDiscoveredAsRecoveryEvidence(t *testing.T) {
	for _, name := range []string{
		"delete.metrics.total.node-000.csv",
		"delete.requests.node-001.csv",
		"delete.objects.node-002.csv",
		"items.node-003.csv",
	} {
		if !integrityNodeSourcePattern.MatchString(name) {
			t.Fatalf("DELETE node source %q was not dynamically discoverable", name)
		}
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
		"/logs/" + step + "/items.csv":         enginePagedArtifactHandler(largeContent, pageSize, &plainItemsCalls, &rangeCalls),
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

func TestFetcher_DownloadsPageSizedArtifactWithRanges(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	const pageSize = 1024 * 1024
	content := bytes.Repeat([]byte("x"), pageSize)
	var rangeCalls int32
	var plainCalls int32
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "items.csv", "href": "/logs/" + step + "/items.csv", "size": len(content)},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/items.csv":         enginePagedArtifactHandler(content, pageSize, &plainCalls, &rangeCalls),
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
	if !bytes.Equal(got, content) {
		t.Fatalf("downloaded items length/content mismatch: got %d bytes, want %d", len(got), len(content))
	}
	if atomic.LoadInt32(&rangeCalls) == 0 || atomic.LoadInt32(&plainCalls) != 0 {
		t.Fatalf("rangeCalls=%d plainCalls=%d, want ranged only", rangeCalls, plainCalls)
	}
}

func TestFetcher_UsesHeadSizeWhenIndexOmitsSize(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	const pageSize = 1024 * 1024
	content := bytes.Repeat([]byte("abcdef"), (pageSize+100)/6+1)
	content = content[:pageSize+100]
	var rangeCalls int32
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "items.csv", "href": "/logs/" + step + "/items.csv"},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/items.csv":         enginePagedArtifactHandler(content, pageSize, nil, &rangeCalls),
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
	if !bytes.Equal(got, content) {
		t.Fatalf("downloaded items length/content mismatch: got %d bytes, want %d", len(got), len(content))
	}
	if atomic.LoadInt32(&rangeCalls) == 0 {
		t.Fatal("expected range requests after resolving size with HEAD")
	}
}

func TestFetcher_SmallArtifactCanGrowAfterIndex(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Messages":          func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("msgs\n")) },
	})
	defer srv.Close()

	out := t.TempDir()
	f := NewFetcher(srv.URL, out)
	f.Sleeper = func(time.Duration) {}
	f.Artifacts = []ArtifactSpec{
		{Loggers: []string{"metrics.FileTotal"}, Suffix: "metrics.total.csv", Required: true},
		{Loggers: []string{"Messages"}, Suffix: "messages.log", Required: true},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
	}
	var messagesStatus *FileStatus
	for i := range man.Steps[0].Files {
		if man.Steps[0].Files[i].Name == step+".messages.log" {
			messagesStatus = &man.Steps[0].Files[i]
			break
		}
	}
	if messagesStatus == nil {
		t.Fatal("messages status not recorded")
	}
	if messagesStatus.Status != "ok" || messagesStatus.Size != 5 {
		t.Fatalf("messages status = %#v, want ok with fetched size 5", messagesStatus)
	}
}

func TestFetcher_SmallArtifactSizeMismatchMarksError(t *testing.T) {
	step := "mt-001-20250101.000000.000-create"
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "items.csv", "href": "/logs/" + step + "/items.csv", "size": 7},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/items.csv":         func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("short!")) },
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
	man, err := f.FetchArtifactsForSteps(ctx, []string{step})
	if err != nil {
		t.Fatalf("FetchArtifactsForSteps error: %v", err)
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
	if itemStatus.Status != "error" || !strings.Contains(itemStatus.Error, "downloaded size 6 is smaller than index size 7") {
		t.Fatalf("items.csv status = %#v, want short-read error", itemStatus)
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
	configArtifact := []byte(`storage:
  auth:
    uid: LOCALACCESSKEY
    secret: LOCALSECRETKEY
  driver:
    type: s3
aws:
  accessKeyId: AWSACCESSKEY
  secretAccessKey: AWSSECRETKEY
`)
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
			idx := map[string]any{
				"step_id": step,
				"items": []map[string]any{
					{"logger": "metrics.FileTotal", "href": "/logs/" + step + "/metrics.FileTotal", "size": 5},
					{"logger": "Config", "href": "/logs/" + step + "/Config", "size": len(configArtifact)},
					{"logger": "Cli", "href": "/logs/" + step + "/Cli", "size": 3},
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 4},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 4},
				},
			}
			_ = json.NewEncoder(w).Encode(idx)
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/Config": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(configArtifact)
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
					{"logger": "Messages", "href": "/logs/" + step + "/Messages", "size": 3},
					{"logger": "Errors", "href": "/logs/" + step + "/Errors", "size": 3},
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

func TestFetcherPreservesDiscoveredIntegrityNodeSources(t *testing.T) {
	step := "mt-002-test-verify"
	sourceName := "integrity.failures.node-001.csv"
	content := []byte("header\r\nrow\r\n")
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
				{"logger": "metrics.FileTotal", "size": 5},
				{"logger": sourceName, "size": len(content), "content_type": "text/csv"},
			}})
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("total")) },
		"/logs/" + step + "/" + sourceName:     func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(content) },
	})
	defer srv.Close()

	out := t.TempDir()
	fetcher := NewFetcher(srv.URL, out)
	fetcher.Artifacts = []ArtifactSpec{{Loggers: []string{"metrics.FileTotal"}, Suffix: "metrics.total.csv", Required: true}}
	manifest, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{step})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, step+"."+sourceName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("node source content = %q, want %q", got, content)
	}
	found := false
	for _, status := range manifest.Steps[0].Files {
		found = found || status.Name == step+"."+sourceName && status.Status == "ok"
	}
	if !found {
		t.Fatal("node source status was not recorded")
	}
}

func TestFetcherReplacesStepEvidenceWithoutErasingIndependentIndexFields(t *testing.T) {
	step := "mt-002-test-read"
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"/logs/" + step + "/index.json": func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
				{"logger": "metrics.FileTotal", "size": 5},
			}})
		},
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("total"))
		},
	})
	defer srv.Close()

	out := t.TempDir()
	prior := Manifest{
		Steps: []StepManifest{{StepID: "obsolete-step"}},
		RunFiles: []FileStatus{
			{Name: "engine.info.json", Size: 701, Status: fileStatusOK, ContentType: "application/json"},
			{Name: "trace.log", Size: 19, Status: fileStatusOK, ContentType: "text/plain"},
		},
		Integrity: &IntegritySummary{Complete: true, SelectionSourceCount: 41, VerifiedCount: 41},
	}
	priorData, err := json.Marshal(prior)
	if err != nil {
		t.Fatal(err)
	}
	var priorMembers map[string]json.RawMessage
	if err := json.Unmarshal(priorData, &priorMembers); err != nil {
		t.Fatal(err)
	}
	priorMembers["futureEvidence"] = json.RawMessage(`{"owner":"independent","generation":2}`)
	priorData, err = json.Marshal(priorMembers)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, constants.ResultsManifestFileName), priorData, 0o644); err != nil {
		t.Fatal(err)
	}

	fetcher := NewFetcher(srv.URL, out)
	fetcher.Artifacts = []ArtifactSpec{{
		Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true,
	}}
	manifest, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{step})
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.Steps) != 1 || manifest.Steps[0].StepID != step {
		t.Fatalf("step evidence = %+v, want only current step %q", manifest.Steps, step)
	}
	if len(manifest.RunFiles) != 2 || manifest.RunFiles[0] != prior.RunFiles[0] || manifest.RunFiles[1] != prior.RunFiles[1] {
		t.Fatalf("run files = %+v, want preserved %+v", manifest.RunFiles, prior.RunFiles)
	}
	if manifest.Integrity == nil || !manifest.Integrity.Complete ||
		manifest.Integrity.SelectionSourceCount != 41 || manifest.Integrity.VerifiedCount != 41 {
		t.Fatalf("integrity = %+v, want preserved %+v", manifest.Integrity, prior.Integrity)
	}

	persistedData, err := os.ReadFile(filepath.Join(out, constants.ResultsManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Manifest
	if err := json.Unmarshal(persistedData, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.RunFiles) != 2 || persisted.Integrity == nil || persisted.Integrity.VerifiedCount != 41 {
		t.Fatalf("persisted independent fields = runFiles %+v, integrity %+v", persisted.RunFiles, persisted.Integrity)
	}
	var persistedMembers map[string]json.RawMessage
	if err := json.Unmarshal(persistedData, &persistedMembers); err != nil {
		t.Fatal(err)
	}
	var futureEvidence struct {
		Owner      string `json:"owner"`
		Generation int    `json:"generation"`
	}
	if err := json.Unmarshal(persistedMembers["futureEvidence"], &futureEvidence); err != nil {
		t.Fatalf("additive independently owned field was not preserved: %v", err)
	}
	if futureEvidence.Owner != "independent" || futureEvidence.Generation != 2 {
		t.Fatalf("additive independently owned field changed: %+v", futureEvidence)
	}
}
