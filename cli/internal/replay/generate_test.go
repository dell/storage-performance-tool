package replay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestGenerateAndWriteGeneratedUsePrivateArtifacts(t *testing.T) {
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
	defer server.Close()

	got, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "local-bucket",
		TestHosts:     "127.0.0.1",
		Label:         "replay",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if !strings.Contains(got.Preflight, "Auth: local environment") {
		t.Fatalf("preflight did not summarize local auth\n%s", got.Preflight)
	}
	if strings.Contains(got.Preflight, "local_secret_key") {
		t.Fatalf("preflight leaked placeholder secret\n%s", got.Preflight)
	}
	if !strings.Contains(got.Preflight, "\nCommand operations\n  - converted: sleep ${WAIT_TIME} (sleep converted to pauseSeconds)\n") {
		t.Fatalf("preflight missing command-operation section\n%s", got.Preflight)
	}
	if !strings.Contains(got.Preflight, "\nWarnings\n  - converted legacy JSON scenario to JS\n  - source uses unauthenticated HTTP\n") {
		t.Fatalf("preflight missing separate HTTP source warning\n%s", got.Preflight)
	}
	if strings.Contains(got.Preflight, "source uses unauthenticated HTTP; command operations") {
		t.Fatalf("preflight should not combine HTTP and command warnings\n%s", got.Preflight)
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "converted" || got.CommandOps[0].Command != "sleep ${WAIT_TIME}" {
		t.Fatalf("CommandOps = %+v, want converted sleep command", got.CommandOps)
	}
	if !strings.Contains(string(got.DefaultsYAML), "local_access_key") {
		t.Fatalf("defaults should contain placeholder auth for generate-only without local credentials")
	}
	if !got.Params.ObjectDataDedupable {
		t.Fatalf("replay params should keep dedupe-friendly data by default")
	}
	if got.Params.ObjectSize != "10KB" {
		t.Fatalf("replay params object size = %q, want 10KB", got.Params.ObjectSize)
	}
	if strings.Contains(string(got.DefaultsYAML), "dedupable: false") {
		t.Fatalf("defaults should not enable anti-dedupe stamping for replay:\n%s", string(got.DefaultsYAML))
	}

	outDir := filepath.Join(t.TempDir(), "generated")
	paths, err := WriteGenerated(got, outDir)
	if err != nil {
		t.Fatalf("WriteGenerated() error = %v", err)
	}
	for _, path := range []string{paths.Scenario, paths.Defaults, paths.Metadata} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatalf("stat %s: %v", path, statErr)
		}
		if gotMode := info.Mode().Perm(); gotMode != 0o600 {
			t.Fatalf("%s mode = %o, want 600", path, gotMode)
		}
	}
	info, err := os.Stat(paths.Dir)
	if err != nil {
		t.Fatalf("stat output dir: %v", err)
	}
	if gotMode := info.Mode().Perm(); gotMode != 0o700 {
		t.Fatalf("output dir mode = %o, want 700", gotMode)
	}
}

func TestGenerateBuildsPreflightForRejectedCommands(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}},
    {"type": "command", "value": "rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB", "blocking": true}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	got, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "local-bucket",
		TestHosts:     "127.0.0.1",
		Label:         "replay",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
	})
	if err == nil {
		t.Fatal("Generate() error = nil, want rejected command error")
	}
	if !strings.Contains(err.Error(), "unsupported command step") {
		t.Fatalf("Generate() error = %v", err)
	}
	if got == nil {
		t.Fatal("Generate() result = nil, want partial preflight")
	}
	for _, want := range []string{
		"\nCommand operations\n",
		"  - rejected: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB (not recognized by replay command whitelist)\n",
		"\nErrors\n  - unsupported command step: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB\n",
		"\nWarnings\n  - converted legacy JSON scenario to JS\n  - source uses unauthenticated HTTP\n",
	} {
		if !strings.Contains(got.Preflight, want) {
			t.Fatalf("preflight missing %q\n%s", want, got.Preflight)
		}
	}
	metadata := string(got.MetadataJSON)
	for _, want := range []string{
		`"commandOperations"`,
		`"action": "rejected"`,
		`"unsupported command step: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB"`,
	} {
		if !strings.Contains(metadata, want) {
			t.Fatalf("metadata missing %q\n%s", want, metadata)
		}
	}
}

func TestGenerateSupportsJavaScriptScenarioArtifacts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.2026-06-05.sh">run</a><a href="max.s3.sanity.js">scenario</a>`)
	})
	mux.HandleFunc("/run.2026-06-05.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export RUN_TIME=900
export RUN_TIME_FOR_SMALL_OBJ=1800
export WAIT_TIME=60
export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --run-scenario=/tmp/perf/max.s3.sanity.js`)
	})
	mux.HandleFunc("/max.s3.sanity.js", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, maxS3SanityJS)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	got, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "local-bucket",
		TestHosts:     "127.0.0.1",
		Label:         "replay",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if !strings.Contains(got.Preflight, "  Format: JS\n") {
		t.Fatalf("preflight missing JS format\n%s", got.Preflight)
	}
	if !strings.Contains(got.Preflight, "\nWarnings\n  - adapted legacy JS scenario for replay\n  - source uses unauthenticated HTTP\n") {
		t.Fatalf("preflight missing JS conversion warning\n%s", got.Preflight)
	}
	if !strings.Contains(string(got.ScenarioJS), "replay-001-20260605.121400.000-create") {
		t.Fatalf("generated scenario missing canonical step ID\n%s", string(got.ScenarioJS))
	}
	if !strings.Contains(string(got.MetadataJSON), `"scenarioFormat": "js"`) {
		t.Fatalf("metadata missing JS scenario format\n%s", string(got.MetadataJSON))
	}
}

func TestGenerateClassifiesUnsupportedScenarioFormat(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.10kb100kbjson">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.10kb100kbjson`)
	})
	mux.HandleFunc("/max.s3.sanity.10kb100kbjson", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `placeholder`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "local-bucket",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
	})
	if err == nil {
		t.Fatal("Generate() error = nil, want unsupported scenario format")
	}
	if got := ErrorClass(err); got != failureScenarioFormatUnsupported {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureScenarioFormatUnsupported, err)
	}
}

func TestGenerateClassifiesReplayDefaultsGenerationError(t *testing.T) {
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
	defer server.Close()

	_, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"10.0.0.1:9020"}, // invalid URL scheme for defaults generation
		Bucket:        "local-bucket",
		TestHosts:     "127.0.0.1",
		Label:         "replay",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
	})
	if err == nil {
		t.Fatal("Generate() error = nil, want defaults generation error")
	}
	if got := ErrorClass(err); got != failureReplayDefaultsGeneration {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureReplayDefaultsGeneration, err)
	}
}

func TestGenerateOmitsCommandNoiseWhenNoArchivedCommands(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	got, err := Generate(context.Background(), Options{
		SourceURL:     server.URL,
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "local-bucket",
		TestHosts:     "127.0.0.1",
		Label:         "replay",
		BaseTimestamp: "20260605.121400.000",
		HTTPClient:    server.Client(),
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if strings.Contains(got.Preflight, "Command operations") {
		t.Fatalf("preflight should omit command-operation section when no commands exist\n%s", got.Preflight)
	}
	if strings.Contains(got.Preflight, "no archived command operations found") {
		t.Fatalf("preflight should not report no-op command details\n%s", got.Preflight)
	}
	if !strings.Contains(got.Preflight, "\nWarnings\n  - converted legacy JSON scenario to JS\n  - source uses unauthenticated HTTP\n") {
		t.Fatalf("preflight missing plain HTTP warning\n%s", got.Preflight)
	}
	metadata := string(got.MetadataJSON)
	if strings.Contains(metadata, "commandOperations") || strings.Contains(metadata, "no archived command operations found") {
		t.Fatalf("metadata should omit empty command-operation details\n%s", metadata)
	}
}

func TestApplyReplayShapeToParamsCapturesUniformShape(t *testing.T) {
	params := scenario.Params{}
	applyReplayShapeToParams(&params, []StepSummary{
		{Size: "10KB", Duration: "30s", Concurrency: 50},
	})

	if params.ObjectSize != "10KB" {
		t.Fatalf("ObjectSize = %q, want 10KB", params.ObjectSize)
	}
	if params.Duration != "30s" {
		t.Fatalf("Duration = %q, want 30s", params.Duration)
	}
	if params.ObjectCount != 0 {
		t.Fatalf("ObjectCount = %d, want 0 for duration-limited replay", params.ObjectCount)
	}
}

func TestReplayDefaultConcurrencyRequiresUniformExplicitSteps(t *testing.T) {
	if got := replayDefaultConcurrency([]StepSummary{
		{StepID: "step-1", Concurrency: 50, concurrencyExplicit: true},
		{StepID: "step-2"},
	}); got != 1 {
		t.Fatalf("replayDefaultConcurrency() = %d, want 1 when a step omits concurrency", got)
	}
	if got := replayDefaultConcurrency([]StepSummary{
		{StepID: "step-1", Concurrency: 50, concurrencyExplicit: true},
		{StepID: "step-2", Concurrency: 50, concurrencyExplicit: true},
	}); got != 50 {
		t.Fatalf("replayDefaultConcurrency() = %d, want 50 for uniform explicit concurrency", got)
	}
	if got := replayDefaultConcurrency([]StepSummary{
		{StepID: "step-1", Concurrency: 50, concurrencyExplicit: true},
		{StepID: "step-2", Concurrency: 10, concurrencyExplicit: true},
	}); got != 1 {
		t.Fatalf("replayDefaultConcurrency() = %d, want 1 for mixed explicit concurrency", got)
	}
}

func TestApplyReplayShapeToParamsLeavesMixedShapeBlank(t *testing.T) {
	params := scenario.Params{}
	applyReplayShapeToParams(&params, []StepSummary{
		{Size: "10KB", Duration: "30s"},
		{Size: "1MB", Duration: "30s"},
	})

	if params.ObjectSize != "" {
		t.Fatalf("ObjectSize = %q, want blank for mixed-size replay", params.ObjectSize)
	}
	if params.Duration != "30s" {
		t.Fatalf("Duration = %q, want common duration preserved", params.Duration)
	}
}

func TestApplyReplayShapeToParamsCapturesUniformCount(t *testing.T) {
	params := scenario.Params{}
	applyReplayShapeToParams(&params, []StepSummary{
		{Size: "4KB", Count: 100},
		{Size: "4KB", Count: 100},
	})

	if params.ObjectSize != "4KB" {
		t.Fatalf("ObjectSize = %q, want 4KB", params.ObjectSize)
	}
	if params.ObjectCount != 100 {
		t.Fatalf("ObjectCount = %d, want 100", params.ObjectCount)
	}
	if params.Duration != "" {
		t.Fatalf("Duration = %q, want blank for count-limited replay", params.Duration)
	}
}

func TestWriteGeneratedWithOptionsCanOmitDefaults(t *testing.T) {
	generated := &Generated{
		ScenarioJS:   []byte("// scenario\n"),
		DefaultsYAML: []byte("storage:\n  auth:\n    secret: local-secret\n"),
		MetadataJSON: []byte("{}\n"),
	}
	outDir := filepath.Join(t.TempDir(), "runtime")

	paths, err := WriteGeneratedWithOptions(generated, WriteGeneratedOptions{
		OutputDir:       outDir,
		IncludeDefaults: false,
	})
	if err != nil {
		t.Fatalf("WriteGeneratedWithOptions() error = %v", err)
	}
	if paths.Defaults != "" {
		t.Fatalf("Defaults path = %q, want empty when defaults are not persisted", paths.Defaults)
	}
	if _, err := os.Stat(filepath.Join(outDir, "defaults.yaml")); !os.IsNotExist(err) {
		t.Fatalf("defaults.yaml stat error = %v, want not exist", err)
	}
	for _, path := range []string{paths.Scenario, paths.Metadata} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to be written: %v", path, err)
		}
	}
}
