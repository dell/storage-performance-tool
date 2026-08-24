package cmd

import (
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func TestBuildScenarioParamsPreservesDeleteVerificationTruthTable(t *testing.T) {
	tests := []struct {
		name              string
		validate          bool
		verifyValue       string
		verifyChanged     bool
		wantPre, wantPost bool
	}{
		{name: "neither"},
		{name: "verify", verifyValue: "true", verifyChanged: true, wantPost: true},
		{name: "validation defaults post", validate: true, wantPre: true, wantPost: true},
		{name: "validation explicit false", validate: true, verifyValue: "false", verifyChanged: true, wantPre: true},
		{name: "validation explicit true", validate: true, verifyValue: "true", verifyChanged: true, wantPre: true, wantPost: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := deleteVerificationCommand()
			if test.validate {
				_ = cmd.Flags().Set(flagValidateInventory, "true")
			}
			if test.verifyChanged {
				_ = cmd.Flags().Set(flagVerifyDelete, test.verifyValue)
			}
			params, err := buildScenarioParams(WorkloadTypeDelete, cmd)
			if err != nil {
				t.Fatal(err)
			}
			if params.ValidateDeleteInventory != test.wantPre || params.VerifyDelete != test.wantPost ||
				params.VerifyDeleteExplicit != test.verifyChanged || params.VerificationTimeout != 30*time.Second {
				t.Fatalf("verification params = %+v", params)
			}
		})
	}
}

func deleteVerificationCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().StringSlice("endpoints", []string{"http://s3.example"}, "")
	cmd.Flags().String("endpoint", "", "")
	cmd.Flags().String("access-key", "access", "")
	cmd.Flags().String("secret-key", "secret", "")
	cmd.Flags().String("bucket", "bucket", "")
	cmd.Flags().String("prefix", "", "")
	cmd.Flags().Int("auth-version", 4, "")
	cmd.Flags().Int("threads", 1, "")
	cmd.Flags().String("object-size", "", "")
	cmd.Flags().String("part-size", "", "")
	cmd.Flags().Int("mpu-concurrent-objects", 0, "")
	cmd.Flags().Int("mpu-concurrent-parts", 0, "")
	cmd.Flags().Int("object-count", 1, "")
	cmd.Flags().Int(flagDeleteBatchSize, 1, "")
	cmd.Flags().String("duration", "", "")
	cmd.Flags().Bool("cleanup", false, "")
	cmd.Flags().String(flagVersions, scenario.VersionsCurrent, "")
	cmd.Flags().Bool(flagDeferVerification, false, "")
	cmd.Flags().Int("seed-objects", scenario.DefaultDeleteObjectCount, "")
	cmd.Flags().Bool("keep-scenario", false, "")
	cmd.Flags().Bool("save-items", false, "")
	cmd.Flags().String("items-file", "", "")
	cmd.Flags().Bool(flagDeleteExisting, false, "")
	cmd.Flags().Bool(flagAllowEmptyPrefix, false, "")
	cmd.Flags().Int64(flagMaxFailedObjects, scenario.DefaultMaxFailedObjects, "")
	cmd.Flags().Float64(flagMaxFailurePercent, 0, "")
	cmd.Flags().Duration(flagFailureBudgetGrace, scenario.DefaultFailureBudgetGrace, "")
	cmd.Flags().Bool(flagValidateInventory, false, "")
	cmd.Flags().Bool(flagVerifyDelete, false, "")
	cmd.Flags().Duration(flagVerificationTimeout, 30*time.Second, "")
	cmd.Flags().Bool("allow-empty-selection", false, "")
	cmd.Flags().Int("integrity-max-console-failures", 20, "")
	cmd.Flags().Bool("shuffle", false, "")
	cmd.Flags().Int("shuffle-batch-size", 0, "")
	cmd.Flags().Int("read-phase-pause-seconds", 0, "")
	cmd.Flags().Int("service-threads", 0, "")
	cmd.Flags().StringArray("engine-override", nil, "")
	cmd.Flags().Int("prefix-shards", 0, "")
	cmd.Flags().Bool("slice-endpoints", false, "")
	cmd.Flags().Bool("object-data-dedupable", true, "")
	cmd.Flags().Float64("object-data-compressibility", 0, "")
	cmd.Flags().String("s3-driver", "", "")
	cmd.Flags().String("rdma-local-ip", "", "")
	cmd.Flags().Int64("rdma-threshold-bytes", 0, "")
	cmd.Flags().Bool("rdma-fallback", false, "")
	cmd.Flags().String("rdma-device", "", "")
	cmd.Flags().String("rdma-log-level", "", "")
	cmd.Flags().Int64("rdma-timeout-ms", 0, "")
	cmd.Flags().String("checksum", "", "")
	return cmd
}
