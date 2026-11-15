package secretmask

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestEnvironmentValuesMasksCredentialAndEndpointValues(t *testing.T) {
	t.Setenv(constants.EnvS3AccessKey, "ACCESSKEY123")
	t.Setenv(constants.EnvS3SecretKey, "abc") // shorter than the minimum: left alone
	t.Setenv(constants.EnvS3Endpoint, "")
	t.Setenv(constants.EnvS3Endpoints, "http://10.0.0.1:9020/, 10.0.0.2:9020")
	in := "key ACCESSKEY123 to http://10.0.0.1:9020/ and 10.0.0.1:9020 and 10.0.0.2:9020; abc"
	want := "key *** to *** and *** and ***; abc"
	if got := EnvironmentValues(in); got != want {
		t.Fatalf("EnvironmentValues() = %q, want %q", got, want)
	}
}

func TestEnvironmentValuesWithoutSettingsLeavesText(t *testing.T) {
	for _, name := range environmentValueVars {
		t.Setenv(name, "")
	}
	const in = "nothing to mask: http://10.0.0.1:9020"
	if got := EnvironmentValues(in); got != in {
		t.Fatalf("EnvironmentValues() = %q, want the text unchanged", got)
	}
}
