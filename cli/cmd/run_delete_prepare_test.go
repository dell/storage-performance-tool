package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

func TestPrepareDeleteManifestBundleStagesBeforeScenarioAndExecution(t *testing.T) {
	source := filepath.Join(t.TempDir(), "delete.csv")
	if err := os.WriteFile(source, []byte(
		"bucket,key,size,version_id\n"+
			"b,key-z,7,version-z\n"+
			"b,key-a,9,\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	scenarioPath := filepath.Join(t.TempDir(), "delete.js")
	prepared, err := prepareRunBundle(scenario.Params{
		WorkloadType:    WorkloadTypeDelete,
		Endpoint:        "http://127.0.0.1:9000",
		AccessKey:       "access",
		SecretKey:       "secret",
		ItemsFile:       source,
		ObjectCount:     1,
		Threads:         1,
		DeleteBatchSize: 1,
		RunID:           901,
		BaseTimestamp:   "20260822.120000.000",
	}, scenarioPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := prepared.Cleanup(context.Background()); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	params := prepared.Params()
	if params.SelectionSourceCount != 2 || params.SelectionUniqueCount != 2 ||
		params.SelectionSelectedCount != 1 || params.SelectionOrder != scenario.SelectionOrderCanonical {
		t.Fatalf("prepared selection metadata = %+v", params)
	}
	if len(params.ItemFileMounts) != 2 {
		t.Fatalf("item mounts = %+v", params.ItemFileMounts)
	}
	if !strings.Contains(string(prepared.ScenarioJS()), "DeleteLoad.config") ||
		!strings.Contains(string(prepared.ScenarioJS()), `"standalone": true`) {
		t.Fatalf("prepared scenario does not contain timed standalone DELETE:\n%s", prepared.ScenarioJS())
	}
	if len(prepared.ExpectedStepIDs()) != 1 || !strings.HasSuffix(prepared.ExpectedStepIDs()[0], "-delete") {
		t.Fatalf("expected step ids = %v", prepared.ExpectedStepIDs())
	}
}

func TestPrepareDeleteManifestFailureStopsBeforeScenarioGeneration(t *testing.T) {
	source := filepath.Join(t.TempDir(), "delete.csv")
	if err := os.WriteFile(source, []byte(
		"bucket,key,size,version_id\n"+
			"actual,key,1,\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	generated := false
	_, err := prepareRunBundleWithDependencies(
		scenario.Params{
			WorkloadType:    WorkloadTypeDelete,
			RunID:           902,
			ItemsFile:       source,
			Bucket:          "asserted",
			DeleteBatchSize: 1,
		},
		filepath.Join(t.TempDir(), "must-not-exist.js"),
		false,
		runPreparationDependencies{
			PrepareExternal: scenario.PrepareExternalItemFiles,
			GenerateScenario: func(scenario.Params) (string, error) {
				generated = true
				return "", nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "does not match --bucket") {
		t.Fatalf("prepareRunBundleWithDependencies() error = %v", err)
	}
	if generated {
		t.Fatal("scenario generation ran after manifest safety validation failed")
	}
}

func TestPrepareDeleteManifestRejectsUnsafeSourcesBeforeScenarioGeneration(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		batchSize  int
		wantDetail string
	}{
		{
			name:       "empty selection",
			content:    "bucket,key,size,version_id\n",
			batchSize:  1,
			wantDetail: "must contain at least one object identity",
		},
		{
			name: "conflicting identity",
			content: "bucket,key,size,version_id\n" +
				"b,key,1,version\n" +
				"b,key,2,version\n",
			batchSize:  1,
			wantDetail: "conflicting sizes",
		},
		{
			name: "multi-bucket batch",
			content: "bucket,key,size,version_id\n" +
				"b-one,key-one,1,\n" +
				"b-two,key-two,1,\n",
			batchSize:  2,
			wantDetail: "--delete-batch-size=1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "delete.csv")
			if err := os.WriteFile(source, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			generated := false
			_, err := prepareRunBundleWithDependencies(
				scenario.Params{
					WorkloadType:    WorkloadTypeDelete,
					RunID:           903,
					ItemsFile:       source,
					DeleteBatchSize: test.batchSize,
				},
				filepath.Join(t.TempDir(), "must-not-exist.js"),
				false,
				runPreparationDependencies{
					PrepareExternal: scenario.PrepareExternalItemFiles,
					GenerateScenario: func(scenario.Params) (string, error) {
						generated = true
						return "", nil
					},
				},
			)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("prepareRunBundleWithDependencies() error = %v, want %q", err, test.wantDetail)
			}
			if generated {
				t.Fatal("scenario generation ran after manifest validation failed")
			}
		})
	}
}

func TestDeleteManifestFailureStopsBeforeEveryOrchestrationSeam(t *testing.T) {
	t.Chdir(t.TempDir())
	source := filepath.Join(t.TempDir(), "delete.csv")
	if err := os.WriteFile(source, []byte(
		"bucket,key,size,version_id\n"+
			"actual,key,1,\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	previousValidate := validateRunWorkloadTypeFunc
	previousPort := resolvePortConflictFunc
	previousConnect := connectMultiHostOrchestratorFunc
	previousLocal := startLocalHeadlessRunFunc
	previousMulti := startMultiHostHeadlessRunFunc
	previousAutoResults := startAutoResultsFunc
	t.Cleanup(func() {
		validateRunWorkloadTypeFunc = previousValidate
		resolvePortConflictFunc = previousPort
		connectMultiHostOrchestratorFunc = previousConnect
		startLocalHeadlessRunFunc = previousLocal
		startMultiHostHeadlessRunFunc = previousMulti
		startAutoResultsFunc = previousAutoResults
	})
	validateRunWorkloadTypeFunc = func(string) error { return nil }

	var portCalls, connectCalls, localCalls, multiCalls, autoResultsCalls int
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		portCalls++
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		connectCalls++
		return nil
	}
	startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
		localCalls++
		return nil
	}
	startMultiHostHeadlessRunFunc = func(
		*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions,
	) error {
		multiCalls++
		return nil
	}
	startAutoResultsFunc = func(
		context.Context, string, string, string, []string, int64, bool, []*hostparse.HostInfo,
		string, bool, int, string, *runMetadata, io.Writer, io.Writer, string, func(context.Context),
		...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		autoResultsCalls++
		return nil
	}

	for _, hosts := range []string{"127.0.0.1", "remote.example"} {
		for name, value := range map[string]string{
			"test-hosts": hosts, "min-hosts": "1",
			"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
			"bucket": "asserted", "object-count": "0", "duration": "", "threads": "1",
			"headless": "true", "auto-results": "false", "generate-only": "false",
			"items-file": source, "delete-batch-size": "1",
		} {
			setGlobalRunFlagForTest(t, name, value)
		}
		if err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete}); err == nil ||
			!strings.Contains(err.Error(), "does not match --bucket") {
			t.Fatalf("RunE() with hosts %q error = %v", hosts, err)
		}
	}

	if portCalls != 0 || connectCalls != 0 || localCalls != 0 || multiCalls != 0 || autoResultsCalls != 0 {
		t.Fatalf(
			"orchestration calls after unsafe manifest: port=%d connect=%d local=%d multi=%d auto-results=%d",
			portCalls, connectCalls, localCalls, multiCalls, autoResultsCalls,
		)
	}
}
