package replay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
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

var archivedDNSDeclaration = scenario.EndpointSelection{
	Mode: scenario.EndpointSelectionPerRequestDNS, DNSTimeoutMillis: 1500, ConnectTimeoutMillis: 2500,
}

// scenarioWithEndpoints builds a legacy JSON scenario with an optional top-level endpoint object
// and one load step per stepEndpoints entry ("" means the step declares nothing).
func scenarioWithEndpoints(parentEndpoint string, stepEndpoints ...string) string {
	var steps []string
	for i, endpoint := range stepEndpoints {
		storage := ""
		if endpoint != "" {
			storage = fmt.Sprintf(`"storage": {"net": {"endpoint": %s}}, `, endpoint)
		}
		steps = append(steps, fmt.Sprintf(`{"type": "load", "config": {%s"test": {"step": {"id": "STEP-%d", "limit": {"count": 1}}}}}`, storage, i))
	}
	parent := ""
	if parentEndpoint != "" {
		parent = fmt.Sprintf(`, "net": {"endpoint": %s}`, parentEndpoint)
	}
	return fmt.Sprintf(`{"type": "sequential", "config": {"storage": {"driver": {"type": "s3"}%s}}, "steps": [%s]}`,
		parent, strings.Join(steps, ", "))
}

func TestConvertJSONRecordsArchivedEndpointSelectionWithoutWritingSteps(t *testing.T) {
	got, err := ConvertJSON([]byte(archivedDNSScenario), RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"https://s3.example.test:9021"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	if want := []scenario.EndpointSelection{archivedDNSDeclaration}; !reflect.DeepEqual(got.ArchivedEndpointSelection.Declarations, want) {
		t.Fatalf("Declarations = %+v, want %+v", got.ArchivedEndpointSelection.Declarations, want)
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

const jsParentEndpoint = `"net" : {
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
    },`

func convertJSWith(t *testing.T, raw string) *Generated {
	t.Helper()
	got, err := ConvertJS([]byte(raw), RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"https://s3.example.test:9021"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	return got
}

func TestConvertJSRecordsAndStripsParentEndpointSelection(t *testing.T) {
	got := convertJSWith(t, strings.Replace(maxS3SanityJS, `"net" : {
      "node" : {
        "port" : 9020
      }
    },`, jsParentEndpoint, 1))

	if want := []scenario.EndpointSelection{archivedDNSDeclaration}; !reflect.DeepEqual(got.ArchivedEndpointSelection.Declarations, want) {
		t.Fatalf("Declarations = %+v, want %+v", got.ArchivedEndpointSelection.Declarations, want)
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "environment-specific endpoint selection setting(s) storage.net.endpoint.hostname, storage.net.endpoint.dns.server") {
		t.Fatalf("Diagnostics = %+v, want environment-specific warning", got.Diagnostics)
	}
	if js := string(got.ScenarioJS); strings.Contains(js, "per-request-dns") || strings.Contains(js, `"endpoint"`) {
		t.Fatalf("parent config must not carry endpoint selection; the defaults do\n%s", js)
	}
}

const jsInlineDNSStorage = `"storage" : {
        "net" : {
          "endpoint" : {
            "selection" : "per-request-dns"
          }
        },
        "driver" : {
          "limit" : {
            "concurrency" : 70`

func inlineDNSArchiveJS(t *testing.T) string {
	t.Helper()
	raw := strings.Replace(maxS3SanityJS, `"storage" : {
        "driver" : {
          "limit" : {
            "concurrency" : 70`, jsInlineDNSStorage, 1)
	if raw == maxS3SanityJS {
		t.Fatal("inline fixture replacement failed")
	}
	return raw
}

func TestConvertJSRecordsAndStripsInlineEndpointSelection(t *testing.T) {
	got := convertJSWith(t, inlineDNSArchiveJS(t))

	want := []scenario.EndpointSelection{{Mode: scenario.EndpointSelectionPerRequestDNS}}
	if !reflect.DeepEqual(got.ArchivedEndpointSelection.Declarations, want) {
		t.Fatalf("Declarations = %+v, want %+v", got.ArchivedEndpointSelection.Declarations, want)
	}
	js := string(got.ScenarioJS)
	if strings.Contains(js, "per-request-dns") || strings.Contains(js, `"endpoint"`) {
		t.Fatalf("inline step config must not carry endpoint selection; the defaults do\n%s", js)
	}
	if !strings.Contains(js, `"concurrency" : 70`) {
		t.Fatalf("stripping the endpoint must keep the rest of the inline config\n%s", js)
	}
}

func TestGenerateExplicitFlagsOverrideInlineJSSelection(t *testing.T) {
	got, err := generateFromJSArchive(t, inlineDNSArchiveJS(t), []string{"http://10.0.0.1:9020", "http://10.0.0.2:9020"},
		scenario.EndpointSelection{Mode: scenario.EndpointSelectionRoundRobin})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if !strings.Contains(string(got.DefaultsYAML), "selection: round-robin") {
		t.Fatalf("defaults must carry the validated round-robin selection:\n%s", got.DefaultsYAML)
	}
	if strings.Contains(string(got.ScenarioJS), "per-request-dns") {
		t.Fatalf("an inline step must not override the validated selection:\n%s", got.ScenarioJS)
	}
}

func generateFromJSArchive(t *testing.T, raw string, endpoints []string, flags scenario.EndpointSelection) (*Generated, error) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="archived.js">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export RUN_TIME=900
export RUN_TIME_FOR_SMALL_OBJ=1800
export WAIT_TIME=60
export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/archived.js`)
	})
	mux.HandleFunc("/archived.js", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, raw)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return Generate(context.Background(), Options{
		SourceURL: server.URL, Endpoints: endpoints, Bucket: "local-bucket",
		BaseTimestamp: "20260605.121400.000", HTTPClient: server.Client(), EndpointSelection: flags,
	})
}

func convertJSResult(raw string, exports map[string]string) (*Generated, error) {
	vars := map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"}
	for k, v := range exports {
		vars[k] = v
	}
	return ConvertJS([]byte(raw), RunScript{Exports: vars, ItemOutputPath: "bucket"}, Options{
		Endpoints: []string{"https://s3.example.test:9021"}, BaseTimestamp: "20260605.121400.000",
	})
}

// withInlineNet inserts a "net" entry into the first step's inline storage config.
func withInlineNet(t *testing.T, net string) string {
	t.Helper()
	raw := strings.Replace(maxS3SanityJS, `"storage" : {
        "driver" : {
          "limit" : {
            "concurrency" : 70`, `"storage" : {
        "net" : `+net+`,
        "driver" : {
          "limit" : {
            "concurrency" : 70`, 1)
	if raw == maxS3SanityJS {
		t.Fatal("inline fixture replacement failed")
	}
	return raw
}

func TestConvertJSRejectsEndpointDeclarationsItCannotEvaluate(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want string
	}{
		"variable reference": {
			raw:  "var archivedEndpoint = {\"selection\" : \"per-request-dns\"};\n" + withInlineNet(t, `{ "endpoint" : archivedEndpoint }`),
			want: "cannot evaluate",
		},
		"key outside a net object": {
			raw:  strings.Replace(withInlineNet(t, `{}`), `"net" : {}`, `"endpoint" : { "selection" : "per-request-dns" }`, 1),
			want: "cannot evaluate",
		},
		"expression value": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : mode + "-dns" } }`),
			want: `expression in the value of "selection"`,
		},
		"object expression": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "round-robin" } && { "selection" : "per-request-dns" } }`),
			want: "expression rather than a single object literal",
		},
		"dns object expression": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "per-request-dns", "dns" : { "timeoutMilliSec" : 8000 } || fallback } }`),
			want: `expression in the value of "dns"`,
		},
		"connect object expression": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "round-robin", "connect" : Object.assign({}, base) } }`),
			want: `expression in the value of "connect"`,
		},
		"connect variable": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "round-robin", "connect" : CONNECT } }`),
			want: "wrong shape",
		},
		"spread": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "per-request-dns", ...extras } }`),
			want: "spread",
		},
		"nested spread": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "per-request-dns", "dns" : { ...dnsExtras } } }`),
			want: "spread",
		},
		"repeated key": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : "per-request-dns", "selection" : "round-robin" } }`),
			want: "more than once",
		},
		"unexported variable value": {
			raw:  withInlineNet(t, `{ "endpoint" : { "selection" : chosenMode } }`),
			want: "variable chosenMode",
		},
		"partial override inheriting the mode": {
			raw: strings.Replace(withInlineNet(t, `{ "endpoint" : { "dns" : { "timeoutMilliSec" : 8000 } } }`), `"net" : {
      "node" : {
        "port" : 9020
      }
    },`, jsParentEndpoint, 1),
			want: "without a selection mode",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := convertJSResult(tc.raw, map[string]string{"CONNECT": "4000"})
			if ErrorClass(err) != failureInvalidEndpointSelection || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an endpoint selection error containing %q, got %v", tc.want, err)
			}
			if got != nil && len(got.ScenarioJS) != 0 {
				t.Fatalf("a rejected archive must not produce a scenario:\n%s", got.ScenarioJS)
			}
		})
	}
}

func TestConvertJSAcceptsUnquotedLiteralKeys(t *testing.T) {
	got, err := convertJSResult(withInlineNet(t, `{ endpoint : { selection : 'per-request-dns', dns : { timeoutMilliSec : 8000 } } }`), nil)
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	want := []scenario.EndpointSelection{{Mode: scenario.EndpointSelectionPerRequestDNS, DNSTimeoutMillis: 8000}}
	if !reflect.DeepEqual(got.ArchivedEndpointSelection.Declarations, want) {
		t.Fatalf("Declarations = %+v, want %+v", got.ArchivedEndpointSelection.Declarations, want)
	}
	if js := string(got.ScenarioJS); strings.Contains(js, "per-request-dns") || strings.Contains(js, "endpoint") {
		t.Fatalf("the inline step config must not carry endpoint selection; the defaults do\n%s", js)
	}
}

func TestEndpointConfigInsideStringsDoesNotAffectReplay(t *testing.T) {
	raw := `print('Previous config: {"storage":{"net":{"endpoint":{"selection":"per-request-dns"}}}}');` + "\n" +
		`var note = "\"net\": {\"endpoint\": {\"selection\": \"round-robin\"}}";` + "\n" +
		"var template = `\"net\": {\"endpoint\": {\"selection\": \"round-robin\"}}`;\n" + maxS3SanityJS

	got, err := generateFromJSArchive(t, raw, []string{"http://10.0.0.1:9020"}, scenario.EndpointSelection{})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(got.ArchivedEndpointSelection.Declarations) != 0 || strings.Contains(string(got.DefaultsYAML), "endpoint:") {
		t.Fatalf("string contents must not declare endpoint selection: %+v\n%s", got.ArchivedEndpointSelection, got.DefaultsYAML)
	}
	if !strings.Contains(string(got.ScenarioJS), `print('Previous config: {"storage":{"net":{"endpoint":{"selection":"per-request-dns"}}}}');`) {
		t.Fatalf("string contents must be left unchanged:\n%s", got.ScenarioJS)
	}
}

func TestConvertJSResolvesExportedVariableValues(t *testing.T) {
	got, err := convertJSResult(withInlineNet(t, `{ "endpoint" : { "selection" : DNS_MODE, "connect" : { "timeoutMilliSec" : CONNECT_MS } } }`),
		map[string]string{"DNS_MODE": "per-request-dns", "CONNECT_MS": "4000"})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	want := []scenario.EndpointSelection{{Mode: scenario.EndpointSelectionPerRequestDNS, ConnectTimeoutMillis: 4000}}
	if !reflect.DeepEqual(got.ArchivedEndpointSelection.Declarations, want) {
		t.Fatalf("Declarations = %+v, want %+v", got.ArchivedEndpointSelection.Declarations, want)
	}
}

func TestCommentedEndpointConfigsDoNotAffectReplay(t *testing.T) {
	raw := "// Previous config: {\"storage\": {\"net\": {\"endpoint\": {\"selection\": \"per-request-dns\"}}}}\n" +
		"/* \"net\" : { \"endpoint\" : { \"selection\" : \"round-robin\" } } */\n" + maxS3SanityJS

	got, err := generateFromJSArchive(t, raw, []string{"http://10.0.0.1:9020"}, scenario.EndpointSelection{})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(got.ArchivedEndpointSelection.Declarations) != 0 || strings.Contains(string(got.DefaultsYAML), "endpoint:") {
		t.Fatalf("comments must not declare endpoint selection: %+v\n%s", got.ArchivedEndpointSelection, got.DefaultsYAML)
	}
}

func TestGenerateRejectsConflictingArchivedModesWithoutAFlag(t *testing.T) {
	cases := map[string]string{
		"dns and round robin":      scenarioWithEndpoints(`{"selection": "per-request-dns"}`, "", `{"selection": "round-robin"}`),
		"dns and explicit default": scenarioWithEndpoints(`{"selection": "per-request-dns"}`, "", `{"selection": "default"}`),
	}
	for name, archive := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := generateFromArchive(t, archive, []string{"https://s3.example.test:9021"}, scenario.EndpointSelection{})
			if got := ErrorClass(err); got != failureInvalidEndpointSelection || !strings.Contains(err.Error(), "choose one with --endpoint-selection") {
				t.Fatalf("ErrorClass() = %q, err = %v; want a mode conflict", got, err)
			}
			got, err := generateFromArchive(t, archive, []string{"https://s3.example.test:9021"},
				scenario.EndpointSelection{Mode: scenario.EndpointSelectionDefault})
			if err != nil || !hasDiagnosticContaining(got.Diagnostics, severityWarning, "overrides the archived endpoint selection") {
				t.Fatalf("an explicit mode must resolve the conflict: err=%v", err)
			}
			if strings.Contains(string(got.DefaultsYAML), "endpoint:") {
				t.Fatalf("explicit default must emit no endpoint settings:\n%s", got.DefaultsYAML)
			}
		})
	}
}

func TestGenerateRequiresAFlagForConflictingArchivedTimeouts(t *testing.T) {
	archive := scenarioWithEndpoints("",
		`{"selection": "per-request-dns", "dns": {"timeoutMilliSec": 1500}}`,
		`{"selection": "per-request-dns", "dns": {"timeoutMilliSec": 8000}}`)
	modeOnly := scenario.EndpointSelection{Mode: scenario.EndpointSelectionPerRequestDNS}

	_, err := generateFromArchive(t, archive, []string{"https://s3.example.test:9021"}, modeOnly)
	if ErrorClass(err) != failureInvalidEndpointSelection || !strings.Contains(err.Error(), "set --dns-timeout") {
		t.Fatalf("want a timeout conflict naming --dns-timeout, got %v", err)
	}

	withTimeout := modeOnly
	withTimeout.DNSTimeoutMillis = 3000
	got, err := generateFromArchive(t, archive, []string{"https://s3.example.test:9021"}, withTimeout)
	if err != nil || !strings.Contains(string(got.DefaultsYAML), "timeoutMilliSec: 3000") {
		t.Fatalf("an explicit timeout must resolve the conflict: err=%v", err)
	}
}

func TestGenerateAcceptsRoundRobinArchiveWithShippedDNSTimeout(t *testing.T) {
	archive := scenarioWithEndpoints(`{"selection": "round-robin", "dns": {"timeoutMilliSec": 5000}, "connect": {"timeoutMilliSec": 30000}}`, "")

	got, err := generateFromArchive(t, archive, []string{"http://10.0.0.1:9020", "http://10.0.0.2:9020"}, scenario.EndpointSelection{})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defaults := string(got.DefaultsYAML)
	if !strings.Contains(defaults, "selection: round-robin") || !strings.Contains(defaults, "timeoutMilliSec: 30000") {
		t.Fatalf("round robin and its connect timeout must carry over:\n%s", defaults)
	}
	if strings.Contains(defaults, "dns:") {
		t.Fatalf("a round-robin archive must not import a DNS timeout:\n%s", defaults)
	}
}

func TestGenerateRejectsOutOfRangeArchivedTimeouts(t *testing.T) {
	for _, timeout := range []string{"-1", "2147483648"} {
		archive := scenarioWithEndpoints(fmt.Sprintf(`{"selection": "per-request-dns", "dns": {"timeoutMilliSec": %s}}`, timeout), "")
		_, err := generateFromArchive(t, archive, []string{"https://s3.example.test:9021"}, scenario.EndpointSelection{})
		if ErrorClass(err) != failureInvalidEndpointSelection || !strings.Contains(err.Error(), "must be between 1 and") {
			t.Fatalf("timeout %s: want a range error, got %v", timeout, err)
		}
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
