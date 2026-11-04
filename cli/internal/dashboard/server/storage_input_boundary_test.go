package server

import (
	"encoding/json"
	"errors"
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

func TestStorageEndpointAddressBoundaries(t *testing.T) {
	for _, endpoint := range []string{
		"http://localhost", "https://storage.example.invalid:443/",
		"http://127.0.0.1:1", "https://storage.example.invalid:65535",
		"http://[::1]:8333", "https://[2001:db8::1]/s3/proxy",
		"http://[fe80::1%25eth0]:8333",
		"http://storage.example.invalid./", "HTTP://Storage.Example.Invalid:8333",
		"https://storage.example.invalid/s3/a%20b",
		"http://storage.example.invalid/$(id)",
	} {
		t.Run(endpoint, func(t *testing.T) {
			got, label, ok := validateEndpoint(endpoint)
			if !ok || got != endpoint || label == "" {
				t.Fatalf("validateEndpoint(%q) = %q, %q, %v", endpoint, got, label, ok)
			}
		})
	}
	for _, endpoint := range invalidStorageEndpoints() {
		t.Run(endpoint, func(t *testing.T) {
			if got, label, ok := validateEndpoint(endpoint); ok || got != "" || label != "" {
				t.Fatalf("unsafe endpoint accepted: %q => %q, %q, %v", endpoint, got, label, ok)
			}
		})
	}
}

func invalidStorageEndpoints() []string {
	return []string{
		"ftp://storage.invalid", "file:///tmp/storage", "//storage.invalid", "http:///bucket",
		"http://user:secret@storage.invalid", "http://storage.invalid?list=1", "http://storage.invalid#fragment",
		"http://storage.invalid:0", "https://storage.invalid:65536", "http://storage.invalid:",
		"http://storage.invalid:not-a-port", "http://[::::]:8333", "http://[not-an-ip]:8333",
		"http://::1", "http://2001:db8::1:8333", "http://-storage.invalid", "http://storage..invalid",
		"http://storage.invalid:9999999999999999999999999999",
		"http://storage.invalid/\n--help", "http://storage.invalid/\tpath",
	}
}

func TestStorageHostAddressBoundaries(t *testing.T) {
	longHost := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	input := storageConfig{Hosts: []string{"  root@node-a  ", "", "node-b", "192.0.2.1", "2001:db8::1", "user@localhost", "node-c.", longHost, "node-d"}, Endpoint: "  http://storage.invalid:8333  "}
	before := storageConfig{Hosts: append([]string(nil), input.Hosts...), Endpoint: input.Endpoint}
	got, err := validateStorage(input)
	if err != nil {
		t.Fatal(err)
	}
	want := storageConfig{Hosts: []string{"root@node-a", "node-b", "192.0.2.1", "2001:db8::1", "user@localhost", "node-c.", longHost, "node-d"}, Endpoint: "http://storage.invalid:8333"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized maximum-host configuration = %+v; want %+v", got, want)
	}
	if !reflect.DeepEqual(input, before) {
		t.Fatal("validation mutated the caller's configuration")
	}
	if _, _, ok := validateEndpoint("http://" + longHost + ":8333"); !ok {
		t.Fatal("rejected a valid 253-character DNS hostname")
	}
}

// The HTTP verification boundary must reject invalid settings before changing
// saved configuration, probing an endpoint, or invoking the node verifier.
func TestStorageInputRejectedBeforeVerification(t *testing.T) {
	type invalidConfig struct {
		name string
		cfg  storageConfig
	}
	var tests []invalidConfig
	for _, host := range []string{
		"-oProxyCommand=id", "root@-node", "node;id", "node$(id)", "node`id`", "node|id",
		"node\n--help", "node\x00name", "root@@node", "root@", "node:22", "[::1]",
		"a..invalid", strings.Repeat("a", 64) + ".invalid", strings.Repeat("a.", 127) + "a",
	} {
		tests = append(tests, invalidConfig{"host " + host, storageConfig{Hosts: []string{host}}})
	}
	for _, endpoint := range invalidStorageEndpoints() {
		tests = append(tests, invalidConfig{"endpoint " + endpoint, storageConfig{Hosts: []string{"node-a"}, Endpoint: endpoint}})
	}
	tests = append(tests, invalidConfig{"too many hosts", storageConfig{Hosts: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}}})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, marker := newVerificationTestApp(t)
			beforeCfg := a.storageSnapshot()
			beforeVerify := verification{Status: verifyPassed, Summary: "previous verification"}
			a.verification = beforeVerify
			for path, value := range map[string]any{a.storageFile: beforeCfg, a.verifyFile: beforeVerify} {
				if err := writeFileAtomic(path, value); err != nil {
					t.Fatal(err)
				}
			}
			beforeFiles := map[string]string{}
			for _, path := range []string{a.storageFile, a.verifyFile} {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				beforeFiles[path] = string(data)
			}
			var verifierCalls atomic.Int32
			a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
				verifierCalls.Add(1)
				return &verifyapi.Report{}
			}
			transport := &storageBoundaryTransport{}
			a.endpointClient = &http.Client{Transport: transport}
			body, err := json.Marshal(tt.cfg)
			if err != nil {
				t.Fatal(err)
			}
			w := verifyRequest(a, string(body))
			waitVerification(t, a)
			assertLaunchRejection(t, w, http.StatusBadRequest)
			if verifierCalls.Load() != 0 || transport.calls.Load() != 0 {
				t.Error("invalid input reached node verification or an endpoint probe")
			}
			if !reflect.DeepEqual(a.storageSnapshot(), beforeCfg) || !reflect.DeepEqual(a.verification, beforeVerify) || a.verifying {
				t.Error("invalid input changed storage or verification state")
			}
			for path, before := range beforeFiles {
				after, err := os.ReadFile(path)
				if err != nil || string(after) != before {
					t.Errorf("invalid input changed saved file %s: %v", path, err)
				}
			}
			if a.lockBusy() || a.activeJob != nil || a.activeCmd != nil || a.activeLock != nil {
				t.Error("invalid input created a job or retained the run lock")
			}
			assertNotInvoked(t, marker)
		})
	}
}

type storageBoundaryTransport struct{ calls atomic.Int32 }

func (t *storageBoundaryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return nil, errors.New("test forbids endpoint traffic")
}

func assertLaunchRejection(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Error == "" || w.Code != status {
		t.Fatalf("rejection = %d %s, %v; want %d with a JSON error", w.Code, w.Body.String(), err, status)
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Error("rejection response is not JSON")
	}
}
