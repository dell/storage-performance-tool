package cmd

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

type deleteValidationCase struct {
	name             string
	itemsFile        string
	bucket           string
	batchSize        int
	duration         string
	prefix           string
	prefixSet        bool
	deleteExisting   bool
	allowEmptyPrefix bool
	cleanup          bool
	seedObjects      int
	autoTerminate    int
	wantDetail       string
}

func TestDeleteFailureBudgetValidation(t *testing.T) {
	newCommand := func() *cobra.Command {
		return deleteValidationCommand(deleteValidationCase{bucket: "owned", batchSize: 100})
	}
	for _, test := range []struct {
		name       string
		flags      map[string]string
		workload   string
		wantDetail string
	}{
		{name: "default fixed object policy", workload: WorkloadTypeDelete},
		{name: "strict fixed zero", workload: WorkloadTypeDelete, flags: map[string]string{"max-failed-objects": "0"}},
		{name: "strict percentage zero", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "0"}},
		{name: "percentage inclusive hundred", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "100"}},
		{name: "mutually exclusive", workload: WorkloadTypeDelete, flags: map[string]string{"max-failed-objects": "1", "max-failure-percent": "1"}, wantDetail: "mutually exclusive"},
		{name: "negative fixed", workload: WorkloadTypeDelete, flags: map[string]string{"max-failed-objects": "-1"}, wantDetail: "greater than or equal to zero"},
		{name: "negative percentage", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "-0.1"}, wantDetail: "between 0 and 100"},
		{name: "percentage above hundred", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "100.1"}, wantDetail: "between 0 and 100"},
		{name: "percentage rejects nan", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "NaN"}, wantDetail: "between 0 and 100"},
		{name: "grace with positive percentage", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "5", "failure-budget-grace": "45s"}},
		{name: "grace rejects subsecond precision", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "5", "failure-budget-grace": "500ms"}, wantDetail: "whole number of seconds"},
		{name: "grace rejects fixed", workload: WorkloadTypeDelete, flags: map[string]string{"failure-budget-grace": "45s"}, wantDetail: "positive --max-failure-percent"},
		{name: "grace rejects zero percentage", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "0", "failure-budget-grace": "45s"}, wantDetail: "positive --max-failure-percent"},
		{name: "grace rejects negative duration", workload: WorkloadTypeDelete, flags: map[string]string{"max-failure-percent": "5", "failure-budget-grace": "-1s"}, wantDetail: "greater than or equal to zero"},
		{name: "fixed budget is delete only", workload: WorkloadTypeRead, flags: map[string]string{"max-failed-objects": "1"}, wantDetail: "not supported for read"},
		{name: "percentage budget is delete only", workload: WorkloadTypeRead, flags: map[string]string{"max-failure-percent": "1"}, wantDetail: "not supported for read"},
		{name: "grace is delete only", workload: WorkloadTypeRead, flags: map[string]string{"failure-budget-grace": "45s"}, wantDetail: "not supported for read"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newCommand()
			for flag, value := range test.flags {
				if err := cmd.Flags().Set(flag, value); err != nil {
					t.Fatal(err)
				}
			}
			err := ValidateRunCommand(cmd, []string{test.workload})
			if test.wantDetail == "" && err != nil {
				t.Fatalf("ValidateRunCommand() error = %v", err)
			}
			if test.wantDetail != "" && (err == nil || !strings.Contains(err.Error(), test.wantDetail)) {
				t.Fatalf("ValidateRunCommand() error = %v, want %q", err, test.wantDetail)
			}
		})
	}

	if scenario.DefaultFailureBudgetGrace != 30*time.Second {
		t.Fatalf("default failure budget grace = %s, want 30s", scenario.DefaultFailureBudgetGrace)
	}
}

func TestValidateDeleteManifestFlags(t *testing.T) {
	tests := []deleteValidationCase{
		{name: "seeded default", bucket: "owned", batchSize: 100},
		{name: "seeded cleanup", bucket: "owned", batchSize: 100, cleanup: true},
		{name: "seeded prefix remains owned", bucket: "owned", prefix: "team/root/", batchSize: 100},
		{
			name: "guarded existing prefix", bucket: "existing", prefix: "team/root/", prefixSet: true,
			deleteExisting: true, batchSize: 100,
		},
		{
			name: "existing prefix requires exact bucket", prefix: "team/root/", prefixSet: true,
			deleteExisting: true, batchSize: 100, wantDetail: "--bucket",
		},
		{
			name: "whole bucket double opt in", bucket: "existing", prefixSet: true,
			deleteExisting: true, allowEmptyPrefix: true, batchSize: 100,
		},
		{
			name: "existing prefix requires explicit prefix flag", bucket: "existing",
			deleteExisting: true, batchSize: 100, wantDetail: "requires an explicit --prefix",
		},
		{
			name: "empty prefix requires second opt in", bucket: "existing", prefixSet: true,
			deleteExisting: true, batchSize: 100, wantDetail: "--allow-empty-prefix",
		},
		{
			name: "slash prefix cannot alias whole bucket", bucket: "existing", prefix: "/", prefixSet: true,
			deleteExisting: true, batchSize: 100, wantDetail: "must not start with '/'",
		},
		{
			name: "leading slash cannot change exact prefix", bucket: "existing", prefix: "/team/root/", prefixSet: true,
			deleteExisting: true, allowEmptyPrefix: true, batchSize: 100, wantDetail: "must not start with '/'",
		},
		{
			name: "empty prefix override requires destructive mode", bucket: "owned",
			allowEmptyPrefix: true, batchSize: 100, wantDetail: "requires --delete-existing",
		},
		{
			name: "manifest conflicts with existing source", itemsFile: "delete.csv", bucket: "existing",
			deleteExisting: true, batchSize: 1, wantDetail: "mutually exclusive",
		},
		{
			name: "existing source rejects cleanup", bucket: "existing", prefix: "team/root/", prefixSet: true,
			deleteExisting: true, cleanup: true, batchSize: 1, wantDetail: "cannot be used with --cleanup",
		},
		{
			name: "existing duration", bucket: "existing", prefix: "team/root/", prefixSet: true,
			deleteExisting: true, duration: "1m", batchSize: 1,
		},
		{name: "seeded duration", bucket: "owned", batchSize: 100, duration: "1m", seedObjects: 2500},
		{name: "seeded duration requires inventory", bucket: "owned", batchSize: 100, duration: "1m", seedObjects: -1, wantDetail: "--seed-objects"},
		{name: "standalone delete rejects stable auto termination", bucket: "owned", batchSize: 100, autoTerminate: 5, wantDetail: "--auto-terminate-seconds"},
		{name: "optional bucket assertion omitted", itemsFile: "delete.csv", batchSize: 1},
		{name: "optional bucket assertion present", itemsFile: "delete.csv", bucket: "expected", batchSize: 100},
		{name: "batch low", itemsFile: "delete.csv", batchSize: 0, wantDetail: "between 1 and 1000"},
		{name: "batch high", itemsFile: "delete.csv", batchSize: 1001, wantDetail: "between 1 and 1000"},
		{name: "manifest duration", itemsFile: "delete.csv", batchSize: 1, duration: "1m"},
		{name: "prefix conflicts with manifest", itemsFile: "delete.csv", batchSize: 1, prefix: "unsafe/", wantDetail: "cannot be combined with --prefix"},
		{name: "cleanup rejects external ownership", itemsFile: "delete.csv", batchSize: 1, cleanup: true, wantDetail: "cannot be used with --cleanup"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := deleteValidationCommand(test)
			err := ValidateRunCommand(cmd, []string{WorkloadTypeDelete})
			if test.wantDetail == "" && err != nil {
				t.Fatalf("ValidateRunCommand() error = %v", err)
			}
			if test.wantDetail != "" && (err == nil || !strings.Contains(err.Error(), test.wantDetail)) {
				t.Fatalf("ValidateRunCommand() error = %v, want %q", err, test.wantDetail)
			}
		})
	}
}

func TestDeleteBatchSizeFlagIsDeleteOnly(t *testing.T) {
	cmd := deleteValidationCommand(deleteValidationCase{bucket: "b", batchSize: 2})
	if err := cmd.Flags().Set(flagDeleteBatchSize, "2"); err != nil {
		t.Fatal(err)
	}
	err := ValidateRunCommand(cmd, []string{WorkloadTypeRead})
	if err == nil || !strings.Contains(err.Error(), "not supported for read") {
		t.Fatalf("ValidateRunCommand() error = %v", err)
	}
}

func TestDeleteVerificationFlagValidation(t *testing.T) {
	for _, test := range []struct {
		name       string
		workload   string
		flag       string
		value      string
		wantDetail string
	}{
		{name: "validation enabled", workload: WorkloadTypeDelete, flag: flagValidateInventory, value: "true"},
		{name: "post verification enabled", workload: WorkloadTypeDelete, flag: flagVerifyDelete, value: "true"},
		{name: "millisecond timeout", workload: WorkloadTypeDelete, flag: flagVerificationTimeout, value: "1500ms"},
		{name: "zero timeout", workload: WorkloadTypeDelete, flag: flagVerificationTimeout, value: "0", wantDetail: "greater than zero"},
		{name: "sub-millisecond timeout", workload: WorkloadTypeDelete, flag: flagVerificationTimeout, value: "1500us", wantDetail: "whole milliseconds"},
		{name: "validation is delete only", workload: WorkloadTypeRead, flag: flagValidateInventory, value: "true", wantDetail: "not supported for read"},
		{name: "verification is delete only", workload: WorkloadTypeRead, flag: flagVerifyDelete, value: "true", wantDetail: "not supported for read"},
		{name: "timeout is delete only", workload: WorkloadTypeRead, flag: flagVerificationTimeout, value: "15s", wantDetail: "not supported for read"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := deleteValidationCommand(deleteValidationCase{bucket: "owned", batchSize: 1})
			if err := cmd.Flags().Set(test.flag, test.value); err != nil {
				t.Fatal(err)
			}
			err := ValidateRunCommand(cmd, []string{test.workload})
			if test.wantDetail == "" && err != nil {
				t.Fatalf("ValidateRunCommand() error = %v", err)
			}
			if test.wantDetail != "" && (err == nil || !strings.Contains(err.Error(), test.wantDetail)) {
				t.Fatalf("ValidateRunCommand() error = %v, want %q", err, test.wantDetail)
			}
		})
	}
}

func TestDeleteExistingSourceFlagsAreDeleteOnly(t *testing.T) {
	for _, flag := range []string{flagDeleteExisting, flagAllowEmptyPrefix} {
		cmd := deleteValidationCommand(deleteValidationCase{bucket: "b", batchSize: 2})
		if err := cmd.Flags().Set(flag, "true"); err != nil {
			t.Fatal(err)
		}
		err := ValidateRunCommand(cmd, []string{WorkloadTypeRead})
		if err == nil || !strings.Contains(err.Error(), "not supported for read") {
			t.Fatalf("ValidateRunCommand() for --%s error = %v", flag, err)
		}
	}
}

func TestDeleteExistingRejectsAllVersionAndEmptySelectionOverrides(t *testing.T) {
	for _, test := range []struct {
		name       string
		flag       string
		value      string
		wantDetail string
	}{
		{name: "all versions", flag: flagVersions, value: scenario.VersionsAll, wantDetail: "not supported for delete"},
		{name: "empty selection", flag: "allow-empty-selection", value: "true", wantDetail: "not supported for delete"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := deleteValidationCommand(deleteValidationCase{
				bucket: "existing", prefix: "guarded/", prefixSet: true,
				deleteExisting: true, batchSize: 1,
			})
			if err := cmd.Flags().Set(test.flag, test.value); err != nil {
				t.Fatal(err)
			}
			err := ValidateRunCommand(cmd, []string{WorkloadTypeDelete})
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("ValidateRunCommand() error = %v, want %q", err, test.wantDetail)
			}
		})
	}
}

func deleteValidationCommand(test deleteValidationCase) *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "http://s3.example.com", "")
	cmd.Flags().StringSlice("endpoints", nil, "")
	cmd.Flags().String("access-key", "access", "")
	cmd.Flags().String("secret-key", "secret", "")
	cmd.Flags().String("bucket", test.bucket, "")
	cmd.Flags().String("items-file", test.itemsFile, "")
	cmd.Flags().String("prefix", test.prefix, "")
	if test.prefixSet {
		_ = cmd.Flags().Set("prefix", test.prefix)
	}
	cmd.Flags().Bool("delete-existing", false, "")
	cmd.Flags().Bool("allow-empty-prefix", false, "")
	cmd.Flags().Int64(flagMaxFailedObjects, scenario.DefaultMaxFailedObjects, "")
	cmd.Flags().Float64(flagMaxFailurePercent, 0, "")
	cmd.Flags().Duration(flagFailureBudgetGrace, scenario.DefaultFailureBudgetGrace, "")
	cmd.Flags().Bool(flagValidateInventory, false, "")
	cmd.Flags().Bool(flagVerifyDelete, false, "")
	cmd.Flags().Duration(flagVerificationTimeout, scenario.DefaultDeleteVerificationTimeout, "")
	if test.deleteExisting {
		_ = cmd.Flags().Set("delete-existing", "true")
	}
	if test.allowEmptyPrefix {
		_ = cmd.Flags().Set("allow-empty-prefix", "true")
	}
	cmd.Flags().Int(flagDeleteBatchSize, scenario.DefaultDeleteBatchSize, "")
	cmd.Flags().String(flagVersions, scenario.VersionsCurrent, "")
	cmd.Flags().Bool("allow-empty-selection", false, "")
	_ = cmd.Flags().Set(flagDeleteBatchSize, strconv.Itoa(test.batchSize))
	cmd.Flags().Int("object-count", 0, "")
	cmd.Flags().String("duration", test.duration, "")
	seedObjects := test.seedObjects
	if seedObjects == 0 {
		seedObjects = scenario.DefaultDeleteObjectCount
	}
	cmd.Flags().Int("seed-objects", seedObjects, "")
	cmd.Flags().Int("auto-terminate-seconds", test.autoTerminate, "")
	cmd.Flags().Bool("cleanup", test.cleanup, "")
	cmd.Flags().Int("auth-version", 4, "")
	cmd.Flags().String("part-size", "", "")
	cmd.Flags().String("object-size", "", "")
	cmd.Flags().Int("threads", 1, "")
	cmd.Flags().Bool("object-data-dedupable", true, "")
	cmd.Flags().Int("mpu-concurrent-objects", 0, "")
	cmd.Flags().Int("mpu-concurrent-parts", 0, "")
	cmd.Flags().Float64("object-data-compressibility", 0, "")
	return cmd
}
