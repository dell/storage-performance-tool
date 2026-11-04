package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

func TestLaunchRequestMediaTypes(t *testing.T) {
	for _, path := range []string{"/api/runs", "/api/storage/verify"} {
		t.Run(path, func(t *testing.T) {
			for _, tt := range []struct {
				contentType string
				status      int
			}{
				{"application/jsonp", http.StatusUnsupportedMediaType},
				{"application/json-patch+json", http.StatusUnsupportedMediaType},
				{"application/json; charset", http.StatusUnsupportedMediaType},
				{"application/json; charset=utf-8; charset=latin1", http.StatusUnsupportedMediaType},
				{`application/json; charset="unterminated`, http.StatusUnsupportedMediaType},
				{"application/json, text/plain", http.StatusUnsupportedMediaType},
				{`Application/JSON; charset="utf-8"`, http.StatusAccepted},
			} {
				t.Run(tt.contentType, func(t *testing.T) {
					a, marker, argsPath, _ := newLaunchContractApp(t)
					beforeCfg, beforeVerify := a.storageSnapshot(), a.verification
					var calls atomic.Int32
					a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
						calls.Add(1)
						return &verifyapi.Report{}
					}
					transport := &storageBoundaryTransport{}
					a.endpointClient = &http.Client{Transport: transport}
					body := validRunRequest
					if path == "/api/storage/verify" {
						body = `{"hosts":["127.0.0.1"],"endpoint":"http://storage.example.invalid:8333"}`
					}
					r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
					r.Host = "localhost:8000"
					r.Header.Set("Origin", "http://localhost:8000")
					r.Header.Set("Content-Type", tt.contentType)
					w := httptest.NewRecorder()
					a.httpServer().Handler.ServeHTTP(w, r)
					waitForRecoveryRun(t, a)
					if tt.status == http.StatusAccepted {
						if w.Code != tt.status {
							t.Fatalf("valid JSON media type = %d %s; want 202", w.Code, w.Body.String())
						}
						if path == "/api/runs" {
							_ = readLaunchFields(t, argsPath)
						} else if calls.Load() != 1 || transport.calls.Load() != 1 {
							t.Error("valid JSON media type did not reach verification")
						}
						return
					}
					assertLaunchRejection(t, w, tt.status)
					assertRejectedRun(t, a, marker, w, tt.status)
					if calls.Load() != 0 || transport.calls.Load() != 0 {
						t.Error("unsupported media type started verification")
					}
					if !reflect.DeepEqual(a.storageSnapshot(), beforeCfg) || !reflect.DeepEqual(a.verification, beforeVerify) {
						t.Error("unsupported media type changed configuration or approval")
					}
					for _, file := range []string{argsPath, a.storageFile, a.verifyFile} {
						if _, err := os.Stat(file); !os.IsNotExist(err) {
							t.Errorf("unsupported media type created %s: %v", file, err)
						}
					}
				})
			}
		})
	}
}
