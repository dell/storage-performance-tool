package cmd

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestPrepareExternalItemFilesForRunRejectsWindowsVerificationBeforeStaging(t *testing.T) {
	originalGOOS := integrityRuntimeGOOS
	integrityRuntimeGOOS = "windows"
	t.Cleanup(func() {
		integrityRuntimeGOOS = originalGOOS
	})

	missingItemsFile := filepath.Join(t.TempDir(), "must-not-be-opened.csv")
	params := scenario.Params{
		WorkloadType: scenario.WorkloadTypeReadVerify,
		RunID:        1,
		ItemsFile:    missingItemsFile,
	}
	_, err := prepareExternalItemFilesForRun(params)
	if err == nil || !strings.Contains(err.Error(), "unsupported on windows") {
		t.Fatalf("prepareExternalItemFilesForRun() error = %v, want platform rejection", err)
	}
	if strings.Contains(err.Error(), missingItemsFile) {
		t.Fatalf("item staging ran before platform rejection: %v", err)
	}
}

func TestPrepareExternalItemFilesForRunRetainsSupportedPaths(t *testing.T) {
	originalGOOS := integrityRuntimeGOOS
	t.Cleanup(func() {
		integrityRuntimeGOOS = originalGOOS
	})

	t.Run("ordinary workload on Windows", func(t *testing.T) {
		integrityRuntimeGOOS = "windows"
		params := scenario.Params{WorkloadType: scenario.WorkloadTypeRead}
		if _, err := prepareExternalItemFilesForRun(params); err != nil {
			t.Fatalf("ordinary workload rejected: %v", err)
		}
	})
	t.Run("verification workload on Linux", func(t *testing.T) {
		integrityRuntimeGOOS = "linux"
		params := scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify}
		if _, err := prepareExternalItemFilesForRun(params); err != nil {
			t.Fatalf("Linux verification workload rejected: %v", err)
		}
	})
}
