package replay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
