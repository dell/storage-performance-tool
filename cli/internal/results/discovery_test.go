package results

import (
	"net/http"
	"net/http/httptest"
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
