package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleRunRejectsCustomLimitViolations(t *testing.T) {
	for _, tt := range []struct {
		name, mode, message string
		change              func(*options)
	}{
		{"guided count", "guided", "Object count", func(o *options) { o.ObjectCount = 10 }},
		{"guided size", "guided", "Object size", func(o *options) { o.ObjectSize = "64KiB" }},
		{"guided threads", "guided", "Thread count", func(o *options) { o.Threads = 1 }},
		{"guided runtime", "guided", "Maximum runtime", func(o *options) { o.MaxRuntimeSeconds = 60 }},
		{"expert count", "expert", "Object count", func(o *options) { o.ObjectCount = 13 }},
		{"expert size", "expert", "Object size", func(o *options) { o.ObjectSize = "4097KiB" }},
		{"expert threads", "expert", "Thread count", func(o *options) { o.Threads = 5 }},
		{"expert runtime", "expert", "Maximum runtime", func(o *options) { o.MaxRuntimeSeconds = 31 }},
		{"expert total", "expert", "Total test data", func(o *options) { o.ObjectCount, o.ObjectSize = 9, "1MiB" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker := newRunHandlerTestApp(t)
			a.settings = customWorkloadSettings(t)
			opts := options{Mode: tt.mode, ObjectCount: 3, ObjectSize: "2KiB", Threads: 3, MaxRuntimeSeconds: 20}
			// Prove the starting request is valid before changing one setting.
			if err := a.validateOptions(opts); err != nil {
				t.Fatal(err)
			}
			tt.change(&opts)
			body, err := json.Marshal(opts)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader(string(body)))
			request.Host = "localhost:8000"
			request.Header.Set("Origin", "http://localhost:8000")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			a.handleRun(response, request)
			assertRejectedRun(t, a, marker, response, http.StatusBadRequest)
			if !strings.Contains(response.Body.String(), tt.message) {
				t.Fatalf("wrong rejection reason: %s; want %q", response.Body.String(), tt.message)
			}
			// Rejection must leave the cross-process lock available as well.
			lock, err := a.lockRun()
			if err != nil {
				t.Fatalf("rejected request left lock held: %v", err)
			}
			unlockClose(lock)
		})
	}
}
