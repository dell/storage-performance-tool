package replay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFetchArtifactsClassifiesMissingRunScript(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="max.s3.sanity.json">scenario</a>`)
	}))
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want missing run script")
	}
	if got := ErrorClass(err); got != failureMissingRunScript {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureMissingRunScript, err)
	}
}

func TestFetchArtifactsClassifiesAmbiguousRunScript(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run-a.sh">run-a</a><a href="run-b.sh">run-b</a><a href="max.s3.sanity.json">scenario</a>`)
	}))
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want ambiguous run script")
	}
	if got := ErrorClass(err); got != failureAmbiguousRunScript {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureAmbiguousRunScript, err)
	}
}

func TestFetchArtifactsClassifiesScenarioNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="different.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want scenario_not_found")
	}
	if got := ErrorClass(err); got != failureScenarioNotFound {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureScenarioNotFound, err)
	}
}

func TestFetchArtifactsClassifiesScenarioMissing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want scenario_missing")
	}
	if got := ErrorClass(err); got != failureScenarioMissing {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureScenarioMissing, err)
	}
}

func TestFetchArtifactsDiscoversTimestampedScenario(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.result.2025-10-21.16:29:50.sh">run</a><a href="max.s3.sanity.result.2025-10-21.16:29:50.json">scenario</a>`)
	})
	mux.HandleFunc("/run.result.2025-10-21.16:29:50.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.result.2025-10-21.16:29:50.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"type":"sequential","steps":[]}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	got, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err != nil {
		t.Fatalf("FetchArtifacts() error = %v", err)
	}
	if got.RunScriptName != "run.result.2025-10-21.16:29:50.sh" {
		t.Fatalf("RunScriptName = %q", got.RunScriptName)
	}
	if got.ScenarioName != "max.s3.sanity.result.2025-10-21.16:29:50.json" {
		t.Fatalf("ScenarioName = %q", got.ScenarioName)
	}
	if got.ScenarioFormat != "json" {
		t.Fatalf("ScenarioFormat = %q", got.ScenarioFormat)
	}
}

func TestParseLinksDeduplicatesHrefs(t *testing.T) {
	links := parseLinks(`<a href="run.sh">run</a><a href="run.sh">again</a><a href="max.s3.json">scenario</a>`)
	if len(links) != 2 {
		t.Fatalf("len(links) = %d, want 2: %#v", len(links), links)
	}
	if _, err := selectRunScript(links); err != nil {
		t.Fatalf("selectRunScript() error = %v", err)
	}
}

func TestSelectScenarioRejectsLooseStemMatch(t *testing.T) {
	_, err := selectScenario([]string{"max.s3.sanity.json"}, "/tmp/perf/max.s3.json")
	if err == nil {
		t.Fatal("selectScenario() error = nil, want mismatch error")
	}
	if !strings.Contains(err.Error(), "scenario max.s3.json was not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestSelectScenarioAllowsTimestampedResultMatch(t *testing.T) {
	got, err := selectScenario([]string{"max.s3.sanity.result.2025-10-21.16:29:50.json"}, "/tmp/perf/max.s3.sanity.json")
	if err != nil {
		t.Fatalf("selectScenario() error = %v", err)
	}
	if got != "max.s3.sanity.result.2025-10-21.16:29:50.json" {
		t.Fatalf("scenario = %q", got)
	}
}

func TestFetchArtifactsFollowsArtifactRedirects(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/run.real.sh", http.StatusFound)
	})
	mux.HandleFunc("/run.real.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/max.s3.sanity.real.json", http.StatusFound)
	})
	mux.HandleFunc("/max.s3.sanity.real.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"type":"sequential","steps":[]}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	got, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err != nil {
		t.Fatalf("FetchArtifacts() error = %v", err)
	}
	if got.RunScriptName != "run.sh" {
		t.Fatalf("RunScriptName = %q", got.RunScriptName)
	}
	if got.ScenarioName != "max.s3.sanity.json" {
		t.Fatalf("ScenarioName = %q", got.ScenarioName)
	}
}

func TestFetchArtifactsReportsHTTPStatusForFolderListing(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
	}{
		{name: "not_found", statusCode: http.StatusNotFound},
		{name: "internal_error", statusCode: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, http.StatusText(tc.statusCode), tc.statusCode)
			}))
			defer server.Close()

			_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
			if err == nil {
				t.Fatal("FetchArtifacts() error = nil, want HTTP status error")
			}
			want := fmt.Sprintf("HTTP %d", tc.statusCode)
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %v, want contains %q", err, want)
			}
		})
	}
}

func TestFetchArtifactsPropagatesTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a>`)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := FetchArtifacts(ctx, server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want timeout")
	}
	if !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
}

func TestFetchArtifactsRejectsOversizedScenarioArtifact(t *testing.T) {
	oversized := strings.Repeat("x", int(maxArtifactBytes+1))
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, oversized)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want size-limit error")
	}
	if !strings.Contains(err.Error(), "artifact exceeds max size") {
		t.Fatalf("error = %v, want size-limit error", err)
	}
	if got := ErrorClass(err); got != failureArtifactTooLarge {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureArtifactTooLarge, err)
	}
}

func TestFetchArtifactsRejectsEscapingRunScriptHref(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/folder/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="../run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL+"/folder/", server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want escaping-link error")
	}
	if !strings.Contains(err.Error(), "escapes replay folder") {
		t.Fatalf("error = %v, want escape error", err)
	}
	if got := ErrorClass(err); got != failureEscapedArtifactLink {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureEscapedArtifactLink, err)
	}
}

func TestFetchArtifactsClassifiesHTTPFetchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want HTTP fetch error")
	}
	if got := ErrorClass(err); got != failureHTTPFetchError {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureHTTPFetchError, err)
	}
}

func TestFetchArtifactsFailsOnMalformedFolderListing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href=>broken<a data="x">still broken</a></body></html>`)
	}))
	defer server.Close()

	_, err := FetchArtifacts(context.Background(), server.URL, server.Client())
	if err == nil {
		t.Fatal("FetchArtifacts() error = nil, want missing run script error")
	}
	if !strings.Contains(err.Error(), "no .sh run script found in replay folder") {
		t.Fatalf("error = %v, want missing-run-script error", err)
	}
}
