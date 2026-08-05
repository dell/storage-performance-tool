/*
Copyright © 2026 Dell Technologies
*/

package scenario

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestGenerateDefaultsSanitizesSensitiveOverrideErrors(t *testing.T) {
	tests := []struct {
		name     string
		parent   string
		override string
	}{
		{name: "dotted secret", parent: "storage.auth=scalar", override: "storage.auth.secret="},
		{name: "dash secret", parent: "storage-auth=scalar", override: "storage-auth-secret="},
		{name: "password", parent: "storage.auth=scalar", override: "storage.auth.password="},
		{name: "token", parent: "storage.auth=scalar", override: "storage.auth.token="},
		{name: "credential", parent: "storage.credentials=scalar", override: "storage.credentials.value="},
		{name: "access key", parent: "storage.auth=scalar", override: "storage.auth.access-key="},
		{name: "auth uid", parent: "storage.auth=scalar", override: "storage.auth.uid="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const secret = "OVERRIDE_ERROR_CREDENTIAL_7f34"
			params := engineOverrideTestParams()
			params.EngineOverrides = []string{tt.parent, tt.override + secret}

			_, err := GenerateDefaults(params)
			if err == nil {
				t.Fatal("GenerateDefaults() succeeded, want scalar conflict")
			}
			diagnostic := err.Error()
			if strings.Contains(diagnostic, secret) {
				t.Fatalf("error exposed credential value: %v", err)
			}
			if !strings.Contains(diagnostic, tt.override+"***") {
				t.Fatalf("error lost safe override path: %v", err)
			}
			if !strings.Contains(diagnostic, "already contains a scalar value") {
				t.Fatalf("error lost failure reason: %v", err)
			}
		})
	}
}

func TestGenerateDefaultsSanitizesSensitiveInvalidYAML(t *testing.T) {
	const secret = "INVALID_YAML_CREDENTIAL_4c19"
	params := engineOverrideTestParams()
	params.EngineOverrides = []string{"storage.auth.secret=[" + secret}

	_, err := GenerateDefaults(params)
	if err == nil {
		t.Fatal("GenerateDefaults() succeeded, want invalid YAML error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("invalid-YAML error exposed credential value: %v", err)
	}
	if !strings.Contains(err.Error(), "storage.auth.secret=***") ||
		!strings.Contains(err.Error(), "value is not valid YAML") {
		t.Fatalf("invalid-YAML error lost safe context: %v", err)
	}
}

func TestGenerateDefaultsDoesNotEchoMalformedOverride(t *testing.T) {
	for _, override := range []string{
		"MALFORMED_OVERRIDE_CREDENTIAL_263b",
		"=EMPTY_PATH_CREDENTIAL_871d",
	} {
		t.Run(override[:1], func(t *testing.T) {
			params := engineOverrideTestParams()
			params.EngineOverrides = []string{override}

			_, err := GenerateDefaults(params)
			if err == nil {
				t.Fatal("GenerateDefaults() succeeded, want malformed override error")
			}
			if strings.Contains(err.Error(), override) {
				t.Fatalf("malformed override was echoed: %v", err)
			}
			if !strings.Contains(err.Error(), "<invalid-engine-override>") {
				t.Fatalf("malformed override error lost safe placeholder: %v", err)
			}
		})
	}
}

func TestGenerateDefaultsPreservesUsefulNonSensitiveOverrideError(t *testing.T) {
	const override = "storage.driver.threads=diagnostic-value"
	params := engineOverrideTestParams()
	params.EngineOverrides = []string{"storage.driver=scalar", override}

	_, err := GenerateDefaults(params)
	if err == nil {
		t.Fatal("GenerateDefaults() succeeded, want scalar conflict")
	}
	if !strings.Contains(err.Error(), override) ||
		!strings.Contains(err.Error(), "driver already contains a scalar value") {
		t.Fatalf("non-sensitive diagnostic lost useful detail: %v", err)
	}
}

func TestGenerateDefaultsKeepsValidSensitiveOverrideUnredacted(t *testing.T) {
	const secret = "VALID_OVERRIDE_CREDENTIAL_518e"
	params := engineOverrideTestParams()
	params.EngineOverrides = []string{"storage.auth.secret=" + secret}

	defaults, err := GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(defaults), "secret: "+secret) {
		t.Fatalf("generated defaults did not retain exact sensitive value:\n%s", defaults)
	}
	if params.EngineOverrides[0] != "storage.auth.secret="+secret {
		t.Fatal("GenerateDefaults() mutated the original override")
	}
}

func engineOverrideTestParams() Params {
	return Params{
		WorkloadType:  workload.Write,
		Endpoint:      "http://s3.example:9000",
		AccessKey:     "access",
		SecretKey:     "secret",
		Bucket:        "bucket",
		Threads:       1,
		ObjectSize:    "1KB",
		ObjectCount:   1,
		BaseTimestamp: "20260803.120000.000",
	}
}
