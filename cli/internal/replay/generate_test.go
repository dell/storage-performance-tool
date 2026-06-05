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
	if !strings.Contains(string(got.DefaultsYAML), "local_access_key") {
		t.Fatalf("defaults should contain placeholder auth for generate-only without local credentials")
	}
	if !got.Params.ObjectDataDedupable {
		t.Fatalf("replay params should keep dedupe-friendly data by default")
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
