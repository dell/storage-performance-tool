package results

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOperationLifecycleFetchAndAbsentCompatibility(t *testing.T) {
	for _, present := range []bool{true, false} {
		t.Run(map[bool]string{true: "present", false: "absent"}[present], func(t *testing.T) {
			step := "evidence-read"
			body := "schema_version,step_id,worker_id\n1,evidence-read,worker-a\n"
			handlers := map[string]http.HandlerFunc{
				"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("total")) },
				"/logs/" + step + "/index.json": func(w http.ResponseWriter, r *http.Request) {
					items := []map[string]any{{"logger": "metrics.FileTotal", "size": 5}}
					if present {
						for _, name := range []string{"OperationLifecycle", "operation.lifecycle.node-000.csv"} {
							items = append(items, map[string]any{"logger": name, "size": len(body)})
						}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"step_id": step, "items": items})
				},
			}
			if present {
				for _, name := range []string{"OperationLifecycle", "operation.lifecycle.node-000.csv"} {
					handlers["/logs/"+step+"/"+name] = func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }
				}
			}
			srv := newTestServer(t, handlers)
			defer srv.Close()
			f := NewFetcher(srv.URL, t.TempDir())
			f.Sleeper = func(time.Duration) {}
			f.Artifacts = []ArtifactSpec{{Loggers: []string{"metrics.FileTotal"}, Suffix: "metrics.total.csv", Required: true}}
			for _, spec := range DefaultArtifacts {
				if spec.Suffix == "operation.lifecycle.csv" {
					f.Artifacts = append(f.Artifacts, spec)
				}
			}
			if len(f.Artifacts) != 2 {
				t.Fatal("lifecycle artifact not registered")
			}
			manifest, err := f.FetchArtifactsForSteps(context.Background(), []string{step})
			if err != nil {
				t.Fatal(err)
			}
			for _, suffix := range []string{"operation.lifecycle.csv", "operation.lifecycle.node-000.csv"} {
				data, err := os.ReadFile(filepath.Join(f.OutputDir, step+"."+suffix))
				if present && (err != nil || string(data) != body) {
					t.Fatalf("%s: %q %v", suffix, data, err)
				}
				if !present && !os.IsNotExist(err) {
					t.Fatalf("unexpected artifact %s", suffix)
				}
			}
			found := false
			for _, file := range manifest.Steps[0].Files {
				if file.Name == step+".operation.lifecycle.csv" {
					found = true
					want := fileStatusMissing
					if present {
						want = fileStatusOK
					}
					if file.Status != want {
						t.Fatalf("status %s", file.Status)
					}
				}
			}
			if !found {
				t.Fatal("missing lifecycle manifest status")
			}
		})
	}
}
