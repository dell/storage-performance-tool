/*
Copyright © 2026 Dell Technologies
*/

package headless

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestDryRunCredentialOverrideErrorIsSanitizedFromAllOutputs(t *testing.T) {
	const secret = "DRY_RUN_OVERRIDE_CREDENTIAL_41b7"
	params := scenario.ScenarioParams{
		WorkloadType:  workload.Write,
		Endpoint:      "http://s3.example:9000",
		AccessKey:     "access",
		SecretKey:     "secret",
		Bucket:        "bucket",
		Threads:       1,
		ObjectSize:    "1KB",
		ObjectCount:   1,
		BaseTimestamp: "20260803.120000.000",
		EngineOverrides: []string{
			"storage.auth=scalar",
			"storage.auth.secret=" + secret,
		},
	}
	tracePath := filepath.Join(t.TempDir(), "error.trace")
	runner, err := NewHeadlessRunner(nil, HeadlessOptions{
		TraceFile: tracePath,
		DryRun:    true,
		APIPort:   "9999",
	})
	if err != nil {
		t.Fatal(err)
	}

	var runErr error
	stdout := captureHeadlessStdout(t, func() {
		runErr = runner.runDryModeWithParams("image", "scenario.js", params)
	})
	if err = runner.Close(); err != nil {
		t.Fatal(err)
	}
	trace := readHeadlessTrace(t, tracePath)

	if runErr == nil {
		t.Fatal("dry run succeeded, want conflicting override error")
	}
	for name, output := range map[string]string{
		"returned error": runErr.Error(),
		"stdout":         stdout,
		"trace":          trace,
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("%s exposed credential-bearing override: %s", name, output)
		}
		if !strings.Contains(output, "storage.auth.secret=***") {
			t.Fatalf("%s lost safe override context: %s", name, output)
		}
	}
}
