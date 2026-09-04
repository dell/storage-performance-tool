package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

func TestBuildScenarioParamsResolvesSeededDeleteDefaults(t *testing.T) {
	seeded := deleteValidationCommand(deleteValidationCase{
		bucket: "owned", batchSize: scenario.DefaultDeleteBatchSize, prefix: "team/root/",
	})
	params, err := buildScenarioParams(WorkloadTypeDelete, seeded)
	if err != nil {
		t.Fatal(err)
	}
	if params.ObjectCount != constants.DefaultSeedObjectCount ||
		params.ObjectSize != scenario.DefaultDeleteObjectSize {
		t.Fatalf("seeded DELETE defaults = count %d size %q", params.ObjectCount, params.ObjectSize)
	}
	if params.Prefix != "team/root/" {
		t.Fatalf("seeded DELETE root prefix = %q", params.Prefix)
	}
	if params.FailureBudgetMode != scenario.FailureBudgetModeFixed ||
		params.MaxFailedObjects != scenario.DefaultMaxFailedObjects ||
		params.FailureBudgetGrace != scenario.DefaultFailureBudgetGrace {
		t.Fatalf("seeded DELETE failure budget defaults = %+v", params)
	}

	external := deleteValidationCommand(deleteValidationCase{
		itemsFile: "delete.csv", batchSize: 1,
	})
	externalParams, err := buildScenarioParams(WorkloadTypeDelete, external)
	if err != nil {
		t.Fatal(err)
	}
	if externalParams.ObjectCount != 0 || externalParams.ObjectSize != "" {
		t.Fatalf("external DELETE inherited seed defaults: %+v", externalParams)
	}
}

func TestBuildScenarioParamsSelectsPercentageObjectFailureBudget(t *testing.T) {
	cmd := deleteValidationCommand(deleteValidationCase{
		bucket: "owned", batchSize: scenario.DefaultDeleteBatchSize,
	})
	if err := cmd.Flags().Set(flagMaxFailurePercent, "12.5"); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Flags().Set(flagFailureBudgetGrace, "45s"); err != nil {
		t.Fatal(err)
	}

	params, err := buildScenarioParams(WorkloadTypeDelete, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if params.FailureBudgetMode != scenario.FailureBudgetModePercentage ||
		params.MaxFailurePercent != 12.5 || params.FailureBudgetGrace.String() != "45s" {
		t.Fatalf("percentage failure budget params = %+v", params)
	}
}

func TestBuildScenarioParamsSelectsGuardedExistingPrefixWithoutSeedDefaults(t *testing.T) {
	cmd := deleteValidationCommand(deleteValidationCase{
		bucket: "existing", prefix: "guarded/root/", prefixSet: true,
		deleteExisting: true, batchSize: 2,
	})
	if err := cmd.Flags().Set("object-count", "9"); err != nil {
		t.Fatal(err)
	}
	params, err := buildScenarioParams(WorkloadTypeDelete, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !params.DeleteExisting || params.AllowEmptyPrefix {
		t.Fatalf("existing-prefix controls = deleteExisting %t allowEmptyPrefix %t",
			params.DeleteExisting, params.AllowEmptyPrefix)
	}
	if params.ObjectCount != 9 || params.ObjectSize != "" {
		t.Fatalf("existing-prefix selection inherited seed defaults: %+v", params)
	}
	if params.SelectionOrder != scenario.SelectionOrderCanonical || params.Versions != scenario.VersionsCurrent {
		t.Fatalf("existing-prefix identity = order %q versions %q",
			params.SelectionOrder, params.Versions)
	}
}

func TestBuildSeededDeleteScenarioOwnsNamespaceAndOrdersFinitePhases(t *testing.T) {
	cmd := deleteValidationCommand(deleteValidationCase{
		bucket: "owned", batchSize: 2, prefix: "/team/root/",
	})
	if err := cmd.Flags().Set("object-count", "17"); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Flags().Set("threads", "3"); err != nil {
		t.Fatal(err)
	}
	params, err := buildScenarioParams(WorkloadTypeDelete, cmd)
	if err != nil {
		t.Fatal(err)
	}
	params.RunID = 904
	params.BaseTimestamp = "20260822.120000.000"
	generated, err := scenario.GenerateDeleteScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := scenario.BuildStepPlanFromScenario(generated)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].Op != "seed" || plan.Steps[1].Op != "delete" {
		t.Fatalf("seeded DELETE plan = %+v", plan.Steps)
	}
	for _, want := range []string{
		`"naming": {"prefix": "team/root/spt-delete-904/"}`,
		`"op": {"type": "create", "limit": {"count": 17}}`,
		`"item": {"type": "data", "input": {"file": writtenFile}}`,
	} {
		if !strings.Contains(generated, want) {
			t.Fatalf("seeded DELETE scenario omitted %q:\n%s", want, generated)
		}
	}
	if strings.Contains(generated, `"type": "list"`) || strings.Contains(generated, "ListLoad.config") {
		t.Fatalf("seeded DELETE activated existing-prefix discovery:\n%s", generated)
	}
}

func TestRunCmdGenerateOnlyPublicDeleteContainsPositiveTimedPhase(t *testing.T) {
	t.Chdir(t.TempDir())
	setDeleteRouteFlags(t, "127.0.0.1", "1")
	setGlobalRunFlagForTest(t, "object-count", "0")
	setGlobalRunFlagForTest(t, "object-size", "")
	setGlobalRunFlagForTest(t, flagDeleteBatchSize, fmt.Sprint(scenario.DefaultDeleteBatchSize))
	setGlobalRunFlagForTest(t, "generate-only", "true")

	if err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete}); err != nil {
		t.Fatalf("RunE() public DELETE generate-only error = %v", err)
	}
	paths, err := filepath.Glob("spt-scenario-*.js")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("generated DELETE scenarios = %v, want exactly one", paths)
	}
	content, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	scenarioJS := string(content)
	seed := strings.Index(scenarioJS, "CreateLoad.config")
	deletePhase := strings.Index(scenarioJS, "DeleteLoad.config")
	if seed < 0 || deletePhase <= seed ||
		!strings.Contains(scenarioJS[:deletePhase], fmt.Sprintf(`"limit": {"count": %d}`, constants.DefaultSeedObjectCount)) ||
		!strings.Contains(scenarioJS[deletePhase:], `"standalone": true`) {
		t.Fatalf("public DELETE generate-only scenario lacks the default seed inventory or positive timed phase:\n%s", scenarioJS)
	}
}

func TestRunDeleteHelpDocumentsPublicSafetyAndDefaults(t *testing.T) {
	help := strings.Join(strings.Fields(runCmd.Long), " ")
	for _, want := range []string{
		"delete: Measure DeleteObject or DeleteObjects performance against a frozen inventory.",
		"DELETE is destructive",
		"--delete-existing plus an exact --bucket and explicitly supplied --prefix",
		fmt.Sprintf("defaults to %d seeded %s objects", constants.DefaultSeedObjectCount, scenario.DefaultDeleteObjectSize),
		fmt.Sprintf("batches %d targets per logical request", scenario.DefaultDeleteBatchSize),
		"does not verify removal unless",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("run help omitted public DELETE contract %q:\n%s", want, runCmd.Long)
		}
	}
	seedObjects := runCmd.Flags().Lookup("seed-objects")
	wantSeedDefault := fmt.Sprint(constants.DefaultSeedObjectCount)
	if seedObjects == nil || seedObjects.DefValue != wantSeedDefault ||
		!strings.Contains(runCmd.Flags().FlagUsages(), "(default "+wantSeedDefault+")") {
		t.Fatalf("--seed-objects default/help = %#v, want authoritative seed default %s", seedObjects, wantSeedDefault)
	}
	for _, flag := range []string{
		flagDeleteBatchSize, flagDeleteExisting, flagAllowEmptyPrefix,
		flagMaxFailedObjects, flagMaxFailurePercent, flagValidateInventory,
		flagVerifyDelete, flagVerificationTimeout,
	} {
		if found := runCmd.Flags().Lookup(flag); found == nil || strings.TrimSpace(found.Usage) == "" {
			t.Fatalf("public DELETE flag --%s is missing help", flag)
		}
	}
	rdmaHelp := runCmd.Flags().Lookup("use-rdma")
	if rdmaHelp == nil || !strings.Contains(rdmaHelp.Usage, "driver startup still requires RDMA or --rdma-fallback") {
		t.Fatalf("public S3-RDMA help must distinguish HTTP DELETE from driver startup requirements")
	}
}

func TestDeleteSeedConcurrencyWarningIsBoundedAndReportsFullWaves(t *testing.T) {
	params := scenario.Params{
		WorkloadType:    WorkloadTypeDelete,
		ObjectCount:     7,
		Threads:         4,
		DeleteBatchSize: 2,
	}
	var output bytes.Buffer
	writeDeleteSeedConcurrencyWarning(&output, params)
	writeDeleteSeedConcurrencyWarning(&output, scenario.Params{
		WorkloadType:    WorkloadTypeDelete,
		ObjectCount:     8,
		Threads:         4,
		DeleteBatchSize: 2,
	})
	got := output.String()
	if strings.Count(got, "Warning:") != 1 || !strings.Contains(got, "maximum full request waves: 0") {
		t.Fatalf("DELETE seed concurrency warning = %q", got)
	}
	if !strings.Contains(got, "continues without automatic inventory calibration") {
		t.Fatalf("DELETE seed warning omitted no-calibration contract: %q", got)
	}
}

func TestDeleteDurationSeedConcurrencyWarningUsesSeedInventory(t *testing.T) {
	var output bytes.Buffer
	writeDeleteSeedConcurrencyWarning(&output, scenario.Params{
		WorkloadType: WorkloadTypeDelete,
		Duration:     "1m", SeedCount: 50, Threads: 2, DeleteBatchSize: 100,
	})
	got := output.String()
	if !strings.Contains(got, "seeded DELETE inventory (50 objects)") ||
		!strings.Contains(got, "maximum full request waves: 0") {
		t.Fatalf("duration seed concurrency warning = %q", got)
	}
}

func TestDeleteExistingSafetyWarningNamesExactScopeAndUntimedDiscovery(t *testing.T) {
	var output bytes.Buffer
	writeDeleteExistingSafetyWarning(&output, scenario.Params{
		WorkloadType:   WorkloadTypeDelete,
		Bucket:         "existing",
		Prefix:         "guarded/root/",
		DeleteExisting: true,
	})
	got := output.String()
	for _, want := range []string{
		"DANGER: --delete-existing",
		`bucket "existing" prefix "guarded/root/"`,
		"all discovered identities (unbounded)",
		"Keep the namespace quiescent",
		"Discovery is setup and is excluded from DELETE request timing",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("existing-prefix safety warning omitted %q: %q", want, got)
		}
	}
}

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

func TestPrepareExistingPrefixDeleteBuildsDiscoveryThenTimedDeleteWithoutExternalMounts(t *testing.T) {
	scenarioPath := filepath.Join(t.TempDir(), "delete-existing.js")
	prepared, err := prepareRunBundle(scenario.Params{
		WorkloadType:    WorkloadTypeDelete,
		Endpoint:        "http://127.0.0.1:9000",
		AccessKey:       "access",
		SecretKey:       "secret",
		Bucket:          "existing",
		Prefix:          "guarded/root/",
		ObjectCount:     3,
		Threads:         1,
		DeleteBatchSize: 2,
		DeleteExisting:  true,
		SelectionOrder:  scenario.SelectionOrderCanonical,
		RunID:           905,
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
	if len(prepared.Params().ItemFileMounts) != 0 {
		t.Fatalf("existing-prefix DELETE unexpectedly staged external mounts: %+v",
			prepared.Params().ItemFileMounts)
	}
	if len(prepared.ExpectedStepIDs()) != 2 ||
		!strings.HasSuffix(prepared.ExpectedStepIDs()[0], "-list") ||
		!strings.HasSuffix(prepared.ExpectedStepIDs()[1], "-delete") {
		t.Fatalf("expected existing-prefix step ids = %v", prepared.ExpectedStepIDs())
	}
	generated := string(prepared.ScenarioJS())
	if strings.Index(generated, "Load.config") >= strings.Index(generated, "DeleteLoad.config") {
		t.Fatalf("timed DELETE was not ordered after frozen discovery:\n%s", generated)
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

	previousPort := resolvePortConflictFunc
	previousConnect := connectMultiHostOrchestratorFunc
	previousLocal := startLocalHeadlessRunFunc
	previousMulti := startMultiHostHeadlessRunFunc
	previousAutoResults := startAutoResultsFunc
	t.Cleanup(func() {
		resolvePortConflictFunc = previousPort
		connectMultiHostOrchestratorFunc = previousConnect
		startLocalHeadlessRunFunc = previousLocal
		startMultiHostHeadlessRunFunc = previousMulti
		startAutoResultsFunc = previousAutoResults
	})
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

func TestDeleteExistingSafetyValidationStopsBeforeOrchestrationSideEffects(t *testing.T) {
	cmd := deleteValidationCommand(deleteValidationCase{
		bucket: "existing", prefixSet: true, deleteExisting: true, batchSize: 1,
	})
	cmd.Use = "run <type>"
	cmd.Args = cobra.ExactArgs(1)
	cmd.SilenceUsage = true
	cmd.PreRunE = ValidateRunCommand
	var schemaProbes, scenarioPosts, objectIOStarts int
	cmd.RunE = func(*cobra.Command, []string) error {
		schemaProbes++
		scenarioPosts++
		objectIOStarts++
		return nil
	}
	cmd.SetArgs([]string{WorkloadTypeDelete})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--allow-empty-prefix") {
		t.Fatalf("Execute() error = %v", err)
	}
	if schemaProbes != 0 || scenarioPosts != 0 || objectIOStarts != 0 {
		t.Fatalf("rejected command crossed execution boundary: schema=%d post=%d io=%d",
			schemaProbes, scenarioPosts, objectIOStarts)
	}
}
