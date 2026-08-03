package cmd

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func TestMetadataSanitizesCredentialEngineOverrides(t *testing.T) {
	const (
		secretOverride = "ROUND12_OVERRIDE_SECRET_91bc"
		uidOverride    = "ROUND12_OVERRIDE_UID_7f3a"
	)
	oldArgs := os.Args
	os.Args = []string{
		"spt", "run", "write-verify",
		"--engine-override", "storage.auth.secret=" + secretOverride,
		"--engine-override=storage-auth-uid=" + uidOverride,
		"--engine-override", "storage.driver.threads=8",
	}
	t.Cleanup(func() { os.Args = oldArgs })

	command := &cobra.Command{Use: "test"}
	command.Flags().StringArray(flagEngineOverride, nil, "")
	if err := command.Flags().Set(flagEngineOverride, "storage.auth.secret="+secretOverride); err != nil {
		t.Fatal(err)
	}
	if err := command.Flags().Set(flagEngineOverride, "storage.driver.threads=8"); err != nil {
		t.Fatal(err)
	}

	meta := buildRunMetadata(runMetadataInput{
		WorkloadType: scenario.WorkloadTypeWriteVerify,
		Params: scenario.Params{
			EngineOverrides: []string{
				"storage.auth.secret=" + secretOverride,
				"storage-auth-uid=" + uidOverride,
				"storage.driver.threads=8",
			},
		},
		ScenarioPath: "scenario.js",
		Command:      command,
	})
	body, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, credential := range []string{secretOverride, uidOverride} {
		if strings.Contains(text, credential) {
			t.Fatalf("metadata retained credential %q: %s", credential, text)
		}
	}
	if !strings.Contains(text, "storage.driver.threads=8") {
		t.Fatalf("metadata lost non-sensitive override: %s", text)
	}
	if !strings.Contains(text, "storage.auth.secret=***") ||
		!strings.Contains(text, "storage-auth-uid=***") {
		t.Fatalf("metadata does not retain useful redacted override paths: %s", text)
	}
}

func TestMetadataSanitizesEnvironmentEngineOverrides(t *testing.T) {
	const (
		secretOverride = "ROUND12_ENV_SECRET_91bc"
		uidOverride    = "ROUND12_ENV_UID_7f3a"
	)
	command := &cobra.Command{Use: "test"}
	command.Flags().StringArray(flagEngineOverride, nil, "")
	markEnvApplied(command, flagEngineOverride,
		"storage.auth.secret="+secretOverride+"; storage-auth-uid="+uidOverride+
			"; storage.driver.threads=6")

	got := captureEnvAppliedFlags(command)[flagEngineOverride]
	if strings.Contains(got, secretOverride) || strings.Contains(got, uidOverride) {
		t.Fatalf("environment metadata retained credentials: %q", got)
	}
	if !strings.Contains(got, "storage.auth.secret=***") ||
		!strings.Contains(got, "storage-auth-uid=***") ||
		!strings.Contains(got, "storage.driver.threads=6") {
		t.Fatalf("environment override metadata = %q", got)
	}
}
