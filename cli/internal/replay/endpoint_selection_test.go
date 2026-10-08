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

const archivedDNSScenario = `{
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
}`

func archiveServing(t *testing.T, scenarioJSON string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="archived.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/archived.json`)
	})
	mux.HandleFunc("/archived.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, scenarioJSON)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func generateFromArchive(t *testing.T, scenarioJSON string, endpoints []string, flags scenario.EndpointSelection) (*Generated, error) {
	t.Helper()
	server := archiveServing(t, scenarioJSON)
	return Generate(context.Background(), Options{
		SourceURL:         server.URL,
		Endpoints:         endpoints,
		Bucket:            "local-bucket",
		BaseTimestamp:     "20260605.121400.000",
		HTTPClient:        server.Client(),
		EndpointSelection: flags,
	})
}

func TestConvertJSONRecordsArchivedEndpointSelectionWithoutWritingSteps(t *testing.T) {
	got, err := ConvertJSON([]byte(archivedDNSScenario), RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"https://s3.example.test:9021"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	want := scenario.EndpointSelection{Mode: scenario.EndpointSelectionPerRequestDNS, DNSTimeoutMillis: 1500, ConnectTimeoutMillis: 2500}
	if got.ArchivedEndpointSelection == nil || *got.ArchivedEndpointSelection != want {
		t.Fatalf("ArchivedEndpointSelection = %+v, want %+v", got.ArchivedEndpointSelection, want)
	}
	js := string(got.ScenarioJS)
	for _, unwanted := range []string{"per-request-dns", "timeoutMilliSec", "archive.example.test", "10.9.9.53"} {
		if strings.Contains(js, unwanted) {
			t.Fatalf("step config must not carry endpoint selection (%q); the defaults do\n%s", unwanted, js)
		}
	}
	for _, path := range []string{"storage.net.endpoint.hostname", "storage.net.endpoint.dns.server"} {
		if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "ignores unmodeled JSON config path "+path) {
			t.Fatalf("Diagnostics = %+v, want warning for %s", got.Diagnostics, path)
		}
	}
}

func TestConvertJSRecordsArchivedEndpointSelection(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `"net" : {
      "node" : {
        "port" : 9020
      }
    },`, `"net" : {
      "node" : {
        "port" : 9020
      },
      "endpoint" : {
        "selection" : "per-request-dns",
        "hostname" : "archive.example.test",
        "dns" : {
          "server" : "10.9.9.53",
          "timeoutMilliSec" : 1500
        },
        "connect" : {
          "timeoutMilliSec" : 2500
        }
      }
    },`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"https://s3.example.test:9021"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	want := scenario.EndpointSelection{Mode: scenario.EndpointSelectionPerRequestDNS, DNSTimeoutMillis: 1500, ConnectTimeoutMillis: 2500}
	if got.ArchivedEndpointSelection == nil || *got.ArchivedEndpointSelection != want {
		t.Fatalf("ArchivedEndpointSelection = %+v, want %+v", got.ArchivedEndpointSelection, want)
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "environment-specific endpoint selection setting(s) storage.net.endpoint.hostname, storage.net.endpoint.dns.server") {
		t.Fatalf("Diagnostics = %+v, want environment-specific warning", got.Diagnostics)
	}
	if strings.Contains(string(got.ScenarioJS), "per-request-dns") {
		t.Fatalf("parent config must not carry endpoint selection; the defaults do\n%s", got.ScenarioJS)
	}
}

func TestConflictingArchivedSelectionsNeedAnExplicitMode(t *testing.T) {
	conflicting := strings.Replace(archivedDNSScenario, `"steps": [{
    "type": "load",
    "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}}
  }]`, `"steps": [{
    "type": "load",
    "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}}
  }, {
    "type": "load",
    "config": {
      "storage": {"net": {"endpoint": {"selection": "round-robin"}}},
      "test": {"step": {"id": "MAX-R10KB", "limit": {"count": 1}}}
    }
  }]`, 1)
	if conflicting == archivedDNSScenario {
		t.Fatal("fixture replacement failed")
	}
	runScript := RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}

	_, err := ConvertJSON([]byte(conflicting), runScript, Options{
		Endpoints: []string{"https://s3.example.test:9021"}, BaseTimestamp: "20260605.121400.000",
	})
	if err == nil || !strings.Contains(err.Error(), "different endpoint selection settings") {
		t.Fatalf("want conflict error without --endpoint-selection, got %v", err)
	}

	got, err := ConvertJSON([]byte(conflicting), runScript, Options{
		Endpoints: []string{"https://s3.example.test:9021"}, BaseTimestamp: "20260605.121400.000",
		EndpointSelection: scenario.EndpointSelection{Mode: scenario.EndpointSelectionDefault},
	})
	if err != nil || !hasDiagnosticContaining(got.Diagnostics, severityWarning, "replay uses the --endpoint-selection flags") {
		t.Fatalf("explicit mode must turn the conflict into a warning: err=%v diagnostics=%+v", err, got)
	}
}

func TestGenerateValidatesArchivedSelectionAgainstLocalEndpoints(t *testing.T) {
	// The archived per-request DNS mode applies (no flag given) but needs one local hostname endpoint.
	_, err := generateFromArchive(t, archivedDNSScenario, []string{"http://10.0.0.1:9020"}, scenario.EndpointSelection{})
	if got := ErrorClass(err); got != failureInvalidEndpointSelection {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureInvalidEndpointSelection, err)
	}
}

func TestGenerateUsesArchivedSelectionWhenNoFlagsAreGiven(t *testing.T) {
	got, err := generateFromArchive(t, archivedDNSScenario, []string{"https://s3.example.test:9021"}, scenario.EndpointSelection{})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defaults := string(got.DefaultsYAML)
	for _, want := range []string{"selection: per-request-dns", "timeoutMilliSec: 1500", "timeoutMilliSec: 2500"} {
		if !strings.Contains(defaults, want) {
			t.Fatalf("replay defaults missing %q:\n%s", want, defaults)
		}
	}
	if strings.Contains(defaults, "10.9.9.53") || strings.Contains(defaults, "archive.example.test") {
		t.Fatalf("environment-specific archived values reached the defaults:\n%s", defaults)
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "replay uses the archived endpoint selection per-request-dns") {
		t.Fatalf("Diagnostics = %+v, want archived-selection notice", got.Diagnostics)
	}
}

func TestGenerateExplicitFlagsOverrideArchivedSelection(t *testing.T) {
	got, err := generateFromArchive(t, archivedDNSScenario,
		[]string{"http://10.0.0.1:9020", "http://10.0.0.2:9020"},
		scenario.EndpointSelection{Mode: scenario.EndpointSelectionRoundRobin, ConnectTimeoutMillis: 100})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defaults := string(got.DefaultsYAML)
	if !strings.Contains(defaults, "selection: round-robin") || !strings.Contains(defaults, "timeoutMilliSec: 100") {
		t.Fatalf("explicit flags must win:\n%s", defaults)
	}
	for _, archived := range []string{"per-request-dns", "timeoutMilliSec: 2500", "timeoutMilliSec: 1500"} {
		if strings.Contains(defaults, archived) {
			t.Fatalf("archived %q leaked into the executed defaults:\n%s", archived, defaults)
		}
	}
	if strings.Contains(string(got.ScenarioJS), "per-request-dns") {
		t.Fatalf("archived selection leaked into the executed steps:\n%s", got.ScenarioJS)
	}
}
