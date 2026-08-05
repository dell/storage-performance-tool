package results

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestDiscoverStepIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":"mt-001-20250101.000000.000-create"},
          {"step_id":"mt-002-20250101.000000.000-delete"}
        ]`))
	}))
	defer srv.Close()

	ids, err := DiscoverStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverStepIDs error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
}

func TestDiscoverFleetStepIDs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":"fleet-001-20250101.000000.000-create"},
          {"step_id":"fleet-002-20250101.000000.000-delete"}
        ]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ids, err := DiscoverFleetStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverFleetStepIDs error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
	if ids[0] != "fleet-001-20250101.000000.000-create" {
		t.Fatalf("expected first id fleet-001-20250101.000000.000-create, got %s", ids[0])
	}
	if ids[1] != "fleet-002-20250101.000000.000-delete" {
		t.Fatalf("expected second id fleet-002-20250101.000000.000-delete, got %s", ids[1])
	}
}

func TestDiscoverFleetStepIDs_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ids, err := DiscoverFleetStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("expected nil error on 404, got: %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil ids on 404, got: %v", ids)
	}
}

func TestDiscoverStepIDs_EmptyStepIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":""},
          {"step_id":""},
          {"step_id":""}
        ]`))
	}))
	defer srv.Close()

	ids, err := DiscoverStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverStepIDs error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %d: %v", len(ids), ids)
	}
}

func TestDiscoverStepIDs_DeduplicatesAndSorts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":"zebra-step"},
          {"step_id":"alpha-step"},
          {"step_id":"zebra-step"},
          {"step_id":"middle-step"},
          {"step_id":"alpha-step"}
        ]`))
	}))
	defer srv.Close()

	ids, err := DiscoverStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverStepIDs error: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 unique ids, got %d: %v", len(ids), ids)
	}
	expected := []string{"alpha-step", "middle-step", "zebra-step"}
	for i, want := range expected {
		if ids[i] != want {
			t.Fatalf("ids[%d] = %q, want %q", i, ids[i], want)
		}
	}
}

func TestDiscoverFleetStepIDs_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ids, err := DiscoverFleetStepIDs(srv.URL)
	if err == nil {
		t.Fatalf("expected error on 500, got nil with ids: %v", ids)
	}
	if ids != nil {
		t.Fatalf("expected nil ids on error, got: %v", ids)
	}
	if !strings.Contains(err.Error(), "/metrics/fleet/json status: 500") {
		t.Fatalf("server error diagnostic = %q", err)
	}
}

func TestDiscoverStepIDsForRunFiltersNodeAndFleetRetainedMetrics(t *testing.T) {
	mux := http.NewServeMux()
	response := `[
      {"run_id":"16","step_id":"prior-verify"},
      {"run_id":"17","step_id":"current-verify"},
      {"run_id":17,"step_id":"current-create"}
    ]`
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(response))
	})
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(response))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	for _, discover := range []struct {
		name string
		fn   func(context.Context, string, int64) ([]string, error)
	}{
		{name: "node", fn: DiscoverStepIDsForRunContext},
		{name: "fleet", fn: DiscoverFleetStepIDsForRunContext},
	} {
		t.Run(discover.name, func(t *testing.T) {
			ids, err := discover.fn(context.Background(), server.URL, 17)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"current-create", "current-verify"}
			if !reflect.DeepEqual(ids, want) {
				t.Fatalf("filtered IDs = %v, want %v", ids, want)
			}
		})
	}
}
