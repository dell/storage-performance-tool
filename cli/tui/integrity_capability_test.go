/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
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
      "selection": {"maxCount": "long", "requireNonEmpty": "boolean"}
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
	server := schemaServer(t, http.StatusOK, `{
		"storage":{"integrity":{
			"algorithm":"string",
			"input":{"expectedProducerId":"string","provenance":"string"},
			"mode":"string",
			"selection":{"maxCount":"long"}
		}}
	}`)
	defer server.Close()

	if err := NewSptAPIClient(server.URL).VerifyIntegrityCapability("ghcr.io/dell/spt:test"); err != nil {
		t.Fatalf("legacy verification schema should remain compatible, got: %v", err)
	}
}

func TestScenarioIntegrityCapabilityPathsAreScopedToTheSelectedWorkload(t *testing.T) {
	containsPath := func(paths []string, want string) bool {
		for _, path := range paths {
			if path == want {
				return true
			}
		}
		return false
	}
	if containsPath(constants.RequiredIntegritySchemaPaths, constants.IntegritySelectionNonEmptyPath) {
		t.Fatalf("legacy verification paths unexpectedly require %q", constants.IntegritySelectionNonEmptyPath)
	}
	verification := scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify}
	if !scenario.RequiresIntegrityCapability(verification) {
		t.Fatal("write verification should require the integrity capability probe")
	}
	if containsPath(scenario.IntegritySchemaPathsFor(verification), constants.IntegritySelectionNonEmptyPath) {
		t.Fatal("write verification inherited the delete-existing-only nonempty guard")
	}
	existingDelete := scenario.Params{WorkloadType: scenario.WorkloadTypeDelete, DeleteExisting: true}
	if !scenario.RequiresIntegrityCapability(existingDelete) {
		t.Fatal("existing-prefix DELETE should require the integrity capability probe")
	}
	if !scenario.RequiresIntegrityRuntimeIdentity(existingDelete) {
		t.Fatal("existing-prefix DELETE should require immutable runtime identity")
	}
	if !containsPath(scenario.IntegritySchemaPathsFor(existingDelete), constants.IntegritySelectionNonEmptyPath) {
		t.Fatalf("existing-prefix DELETE paths omitted %q", constants.IntegritySelectionNonEmptyPath)
	}
	seededDelete := scenario.Params{WorkloadType: scenario.WorkloadTypeDelete}
	if scenario.RequiresIntegrityCapability(seededDelete) ||
		scenario.RequiresIntegrityRuntimeIdentity(seededDelete) ||
		len(scenario.IntegritySchemaPathsFor(seededDelete)) != 0 {
		t.Fatal("seeded DELETE unexpectedly activated the existing-prefix capability probe")
	}
}

func TestVerifyScenarioIntegrityCapability_ExistingDeleteRequiresNonemptyGuard(t *testing.T) {
	body := `{"storage":{"integrity":{"mode":"string","algorithm":"string",
		"input":{"provenance":"string","expectedProducerId":"string"},
		"selection":{"maxCount":"long"}}}}`
	server := schemaServer(t, http.StatusOK, body)
	defer server.Close()

	err := NewSptAPIClient(server.URL).VerifyScenarioIntegrityCapabilityContext(
		context.Background(), "", scenario.Params{
			WorkloadType:   scenario.WorkloadTypeDelete,
			DeleteExisting: true,
		})
	if !errors.Is(err, ErrEngineIncompatible) {
		t.Fatalf("existing-prefix capability error = %v, want ErrEngineIncompatible", err)
	}
	var incompatible *IncompatibleEngineError
	if !errors.As(err, &incompatible) {
		t.Fatalf("existing-prefix capability error type = %T", err)
	}
	if len(incompatible.MissingPaths) != 1 ||
		incompatible.MissingPaths[0] != constants.IntegritySelectionNonEmptyPath {
		t.Fatalf("existing-prefix missing paths = %v", incompatible.MissingPaths)
	}
}

// The probe must accept whatever the engine puts at a leaf. Confuse rewrites Java type names before
// serialising, so leaf contents are descriptors the CLI must not interpret.
func TestVerifyIntegrityCapability_ArbitraryLeafDescriptorsAccepted(t *testing.T) {
	body := `{"storage":{"integrity":{
		"algorithm": null,
		"input": {"expectedProducerId": 17, "provenance": ["anything"]},
		"mode": {"nested": "still-present"},
		"selection": {"maxCount": "anything", "requireNonEmpty": {"descriptor": true}}
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
				"selection":{"maxCount":"long","requireNonEmpty":"boolean"}}}}`,
			wantMissing: []string{
				constants.IntegrityInputProvenancePath,
				constants.IntegrityInputProducerIDPath,
			},
		},
		{
			name: "single leaf absent",
			body: `{"storage":{"integrity":{"mode":"string","algorithm":"string",
				"input":{"provenance":"string"},"selection":{"maxCount":"long","requireNonEmpty":"boolean"}}}}`,
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
