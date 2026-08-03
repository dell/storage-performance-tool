/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func TestPrepareRunBundleSanitizesCredentialOverrideError(t *testing.T) {
	const secret = "PREPARED_RUN_OVERRIDE_CREDENTIAL_53c9"
	params := overrideErrorParams([]string{
		"storage.auth=scalar",
		"storage.auth.secret=" + secret,
	})

	_, err := prepareRunBundle(params, filepath.Join(t.TempDir(), "scenario.js"), false)
	assertCredentialOverrideErrorSanitized(t, err, secret)
}

func TestEnvironmentCredentialOverrideIsSanitizedFromCobraStderr(t *testing.T) {
	const secret = "ENV_OVERRIDE_CREDENTIAL_8d72"
	clearEnvDefaultsTestEnv(t)
	t.Setenv(constants.EnvEngineOverrides,
		"storage.auth=scalar; storage.auth.secret="+secret)

	command := newRunLikeCmd()
	command.PreRunE = func(cmd *cobra.Command, _ []string) error {
		return applyEnvDefaultsToRunFlags(cmd)
	}
	var appliedOverrides []string
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		appliedOverrides, _ = cmd.Flags().GetStringArray(flagEngineOverride)
		params := overrideErrorParams(appliedOverrides)
		_, err := prepareRunBundle(
			params, filepath.Join(t.TempDir(), "scenario.js"), false)
		return err
	}

	root := &cobra.Command{Use: "spt", SilenceUsage: true}
	root.AddCommand(command)
	root.SetArgs([]string{"run"})
	var stderr bytes.Buffer
	root.SetErr(&stderr)

	_, err := root.ExecuteC()
	assertCredentialOverrideErrorSanitized(t, err, secret)
	if strings.Contains(stderr.String(), secret) {
		t.Fatalf("Cobra stderr exposed environment credential: %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "storage.auth.secret=***") {
		t.Fatalf("Cobra stderr lost safe override context: %s", stderr.String())
	}
	if len(appliedOverrides) != 2 || appliedOverrides[1] != "storage.auth.secret="+secret {
		t.Fatalf("environment override was altered before execution: %#v", appliedOverrides)
	}
}

func overrideErrorParams(overrides []string) scenario.Params {
	return scenario.Params{
		WorkloadType:  WorkloadTypeWrite,
		Endpoint:      "http://s3.example:9000",
		AccessKey:     "access",
		SecretKey:     "secret",
		Bucket:        "bucket",
		Threads:       1,
		ObjectSize:    "1KB",
		ObjectCount:   1,
		BaseTimestamp: "20260803.120000.000",
		EngineOverrides: append(
			[]string(nil), overrides...),
	}
}

func assertCredentialOverrideErrorSanitized(t *testing.T, err error, secret string) {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want conflicting override error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("returned error exposed credential override: %v", err)
	}
	if !strings.Contains(err.Error(), "storage.auth.secret=***") ||
		!strings.Contains(err.Error(), "already contains a scalar value") {
		t.Fatalf("returned error lost safe diagnostic context: %v", err)
	}
}
