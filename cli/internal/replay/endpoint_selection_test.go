package replay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func endpointSelectionArchive(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export RUN_TIME=900
export RUN_TIME_FOR_SMALL_OBJ=1800
export WAIT_TIME=60
export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, maxS3SanityJSON)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestGenerateAppliesEndpointSelectionToReplayDefaults(t *testing.T) {
	server := endpointSelectionArchive(t)

	got, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"http://10.0.0.1:9020", "http://10.0.0.2:9020"},
		Bucket:        "local-bucket",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
		EndpointSelection: scenario.EndpointSelection{
			Mode: scenario.EndpointSelectionRoundRobin, Hostname: "s3.example.test", ConnectTimeoutMillis: 2000,
		},
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defaults := string(got.DefaultsYAML)
	for _, want := range []string{"selection: round-robin", "hostname: s3.example.test", "timeoutMilliSec: 2000"} {
		if !strings.Contains(defaults, want) {
			t.Fatalf("replay defaults missing %q:\n%s", want, defaults)
		}
	}
}

func TestGenerateRejectsEndpointSelectionInvalidForLocalEndpoints(t *testing.T) {
	server := endpointSelectionArchive(t)

	_, err := Generate(context.Background(), Options{
		SourceURL:         server.URL,
		Endpoints:         []string{"http://s3.example.test:9020"},
		Bucket:            "local-bucket",
		BaseTimestamp:     "20260605.121400.000",
		HTTPClient:        server.Client(),
		EndpointSelection: scenario.EndpointSelection{Mode: scenario.EndpointSelectionRoundRobin},
	})
	if got := ErrorClass(err); got != failureInvalidEndpointSelection {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureInvalidEndpointSelection, err)
	}
}

func TestConvertJSONCarriesArchivedEndpointSelectionModeAndTimeouts(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {
    "storage": {
      "driver": {"type": "s3"},
      "net": {
        "endpoint": {
          "selection": "per-request-dns",
          "hostname": "archive.example.test",
          "dns": {"server": "10.9.9.53", "timeoutMilliSec": 1500},
          "connect": {"timeoutMilliSec": 2500}
        }
      }
    }
  },
  "steps": [{
    "type": "load",
    "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}}
  }]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"https://s3.example.test:9021"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{`"selection": "per-request-dns"`, `"timeoutMilliSec": 1500`, `"timeoutMilliSec": 2500`} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	for _, environmentSpecific := range []string{"archive.example.test", "10.9.9.53"} {
		if strings.Contains(js, environmentSpecific) {
			t.Fatalf("scenario carried environment-specific %q\n%s", environmentSpecific, js)
		}
	}
	for _, path := range []string{"storage.net.endpoint.hostname", "storage.net.endpoint.dns.server"} {
		if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "ignores unmodeled JSON config path "+path) {
			t.Fatalf("Diagnostics = %+v, want warning for %s", got.Diagnostics, path)
		}
	}
}
