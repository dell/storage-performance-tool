/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// integritySchemaJSON mirrors the shape the engine serves at /config/schema: a nested document whose
// leaves are confuse type descriptors, not runtime values.
const integritySchemaJSON = `{
  "storage": {
    "auth": {"uid": "string"},
    "driver": {"type": "string"},
    "integrity": {
      "algorithm": "string",
      "input": {
        "expectedProducerId": "string",
        "provenance": "string"
      },
      "mode": "string",
      "selection": {"maxCount": "long"}
    }
  },
  "run": {"id": "long"}
}`

func schemaServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != constants.SptConfigSchemaEndpoint {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if accept := r.Header.Get("Accept"); accept != "application/json" {
			t.Errorf("expected Accept: application/json, got %q", accept)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestVerifyIntegrityCapability_ValidSchema(t *testing.T) {
	server := schemaServer(t, http.StatusOK, integritySchemaJSON)
	defer server.Close()

	if err := NewSptAPIClient(server.URL).VerifyIntegrityCapability("ghcr.io/dell/spt:test"); err != nil {
		t.Fatalf("expected a valid schema to pass, got: %v", err)
	}
}

// The probe must accept whatever the engine puts at a leaf. Confuse rewrites Java type names before
// serialising, so leaf contents are descriptors the CLI must not interpret.
func TestVerifyIntegrityCapability_ArbitraryLeafDescriptorsAccepted(t *testing.T) {
	body := `{"storage":{"integrity":{
		"algorithm": null,
		"input": {"expectedProducerId": 17, "provenance": ["anything"]},
		"mode": {"nested": "still-present"},
		"selection": {"maxCount": "anything"}
	}}}`
	server := schemaServer(t, http.StatusOK, body)
	defer server.Close()

	if err := NewSptAPIClient(server.URL).VerifyIntegrityCapability(""); err != nil {
		t.Fatalf("leaf descriptors must not be interpreted, got: %v", err)
	}
}

func TestVerifyIntegrityCapability_MissingPaths(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMissing []string
	}{
		{
			name:        "no integrity subtree at all",
			body:        `{"storage":{"driver":{"type":"string"}}}`,
			wantMissing: constants.RequiredIntegritySchemaPaths,
		},
		{
			name: "integrity present but input subtree absent",
			body: `{"storage":{"integrity":{"mode":"string","algorithm":"string",
				"selection":{"maxCount":"long"}}}}`,
			wantMissing: []string{
				constants.IntegrityInputProvenancePath,
				constants.IntegrityInputProducerIDPath,
			},
		},
		{
			name: "single leaf absent",
			body: `{"storage":{"integrity":{"mode":"string","algorithm":"string",
				"input":{"provenance":"string"},"selection":{"maxCount":"long"}}}}`,
			wantMissing: []string{constants.IntegrityInputProducerIDPath},
		},
		{
			name:        "intermediate segment is a leaf, not a subtree",
			body:        `{"storage":{"integrity":"string"}}`,
			wantMissing: constants.RequiredIntegritySchemaPaths,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := schemaServer(t, http.StatusOK, tt.body)
			defer server.Close()

			err := NewSptAPIClient(server.URL).VerifyIntegrityCapability("")
			if err == nil {
				t.Fatal("expected an incompatible-engine error")
			}
			if !errors.Is(err, ErrEngineIncompatible) {
				t.Errorf("error should match ErrEngineIncompatible, got %v", err)
			}

			var incompatible *IncompatibleEngineError
			if !errors.As(err, &incompatible) {
				t.Fatalf("expected *IncompatibleEngineError, got %T", err)
			}
			if len(incompatible.MissingPaths) != len(tt.wantMissing) {
				t.Fatalf("missing paths = %v, want %v", incompatible.MissingPaths, tt.wantMissing)
			}
			for i, want := range tt.wantMissing {
				if incompatible.MissingPaths[i] != want {
					t.Errorf("missing[%d] = %q, want %q", i, incompatible.MissingPaths[i], want)
				}
			}
		})
	}
}

func TestVerifyIntegrityCapability_TransportAndPayloadFailures(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		reasonSubstr string
	}{
		{
			name:         "endpoint not found on an older engine",
			status:       http.StatusNotFound,
			body:         "",
			reasonSubstr: "HTTP 404",
		},
		{
			name:         "server error",
			status:       http.StatusInternalServerError,
			body:         "boom",
			reasonSubstr: "HTTP 500",
		},
		{
			name:         "malformed json",
			status:       http.StatusOK,
			body:         `{"storage": {`,
			reasonSubstr: "not valid JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := schemaServer(t, tt.status, tt.body)
			defer server.Close()

			err := NewSptAPIClient(server.URL).VerifyIntegrityCapability("")
			if err == nil {
				t.Fatal("expected an incompatible-engine error")
			}
			if !errors.Is(err, ErrEngineIncompatible) {
				t.Errorf("error should match ErrEngineIncompatible, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.reasonSubstr) {
				t.Errorf("error %q should mention %q", err.Error(), tt.reasonSubstr)
			}
		})
	}
}

func TestVerifyIntegrityCapability_ConnectionFailure(t *testing.T) {
	server := schemaServer(t, http.StatusOK, integritySchemaJSON)
	baseURL := server.URL
	server.Close() // nothing is listening now

	err := NewSptAPIClient(baseURL).VerifyIntegrityCapability("")
	if err == nil {
		t.Fatal("expected an incompatible-engine error")
	}
	if !errors.Is(err, ErrEngineIncompatible) {
		t.Errorf("error should match ErrEngineIncompatible, got %v", err)
	}
	if !strings.Contains(err.Error(), "unreachable") {
		t.Errorf("error %q should report the endpoint as unreachable", err.Error())
	}
}

// The message must let an operator act: which node, which image, and what was missing.
func TestIncompatibleEngineError_MessageNamesNodeImageAndMissingPaths(t *testing.T) {
	err := &IncompatibleEngineError{
		EntryNode:    "http://worker-1:9999",
		EngineImage:  "ghcr.io/dell/storage-performance-tool:v5.13.0",
		MissingPaths: []string{constants.IntegrityModePath},
		Reason:       "the configuration schema does not declare the required integrity paths",
	}

	msg := err.Error()
	for _, want := range []string{
		"http://worker-1:9999",
		"ghcr.io/dell/storage-performance-tool:v5.13.0",
		constants.IntegrityModePath,
		"storage.integrity",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q should contain %q", msg, want)
		}
	}
}
