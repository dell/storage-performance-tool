package cmd

import (
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

type deleteValidationCase struct {
	name       string
	itemsFile  string
	bucket     string
	batchSize  int
	duration   string
	prefix     string
	cleanup    bool
	wantDetail string
}

func TestValidateDeleteManifestFlags(t *testing.T) {
	tests := []deleteValidationCase{
		{name: "optional bucket assertion omitted", itemsFile: "delete.csv", batchSize: 1},
		{name: "optional bucket assertion present", itemsFile: "delete.csv", bucket: "expected", batchSize: 100},
		{name: "batch low", itemsFile: "delete.csv", batchSize: 0, wantDetail: "between 1 and 1000"},
		{name: "batch high", itemsFile: "delete.csv", batchSize: 1001, wantDetail: "between 1 and 1000"},
		{name: "duration deferred", itemsFile: "delete.csv", batchSize: 1, duration: "1m", wantDetail: "finite count mode"},
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

func deleteValidationCommand(test deleteValidationCase) *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "http://s3.example.com", "")
	cmd.Flags().StringSlice("endpoints", nil, "")
	cmd.Flags().String("access-key", "access", "")
	cmd.Flags().String("secret-key", "secret", "")
	cmd.Flags().String("bucket", test.bucket, "")
	cmd.Flags().String("items-file", test.itemsFile, "")
	cmd.Flags().String("prefix", test.prefix, "")
	cmd.Flags().Int(flagDeleteBatchSize, scenario.DefaultDeleteBatchSize, "")
	_ = cmd.Flags().Set(flagDeleteBatchSize, strconv.Itoa(test.batchSize))
	cmd.Flags().Int("object-count", 0, "")
	cmd.Flags().String("duration", test.duration, "")
	cmd.Flags().Bool("cleanup", test.cleanup, "")
	cmd.Flags().Int("auth-version", 4, "")
	cmd.Flags().String("part-size", "", "")
	cmd.Flags().String("object-size", "", "")
	cmd.Flags().Int("mpu-concurrent-objects", 0, "")
	cmd.Flags().Int("mpu-concurrent-parts", 0, "")
	cmd.Flags().Float64("object-data-compressibility", 0, "")
	return cmd
}
