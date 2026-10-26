package cmd

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func bucketCountTestCommand(t *testing.T, value string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().Int(flagBucketCount, 1, "")
	cmd.Flags().Int(flagDeleteBatchSize, scenario.DefaultDeleteBatchSize, "")
	if value != "" {
		if err := cmd.Flags().Set(flagBucketCount, value); err != nil {
			t.Fatal(err)
		}
	}
	return cmd
}

func TestApplyBucketCountRecordsASetForWorkloadsThatCreateObjects(t *testing.T) {
	for _, params := range []scenario.Params{
		{WorkloadType: WorkloadTypeWrite, Bucket: "bench"},
		{WorkloadType: WorkloadTypeWrite, Bucket: "bench", Cleanup: true},
		{WorkloadType: WorkloadTypeWriteVerify, Bucket: "bench"},
		{WorkloadType: WorkloadTypeRead, Bucket: "bench"},
		{WorkloadType: WorkloadTypeMixed, Bucket: "bench"},
		{WorkloadType: WorkloadTypeMixed, Bucket: "bench", ReadItemsFile: "/in/read.csv", DeleteItemsFile: "/in/delete.csv"},
	} {
		got := params
		if err := applyBucketCount(bucketCountTestCommand(t, "4"), &got); err != nil {
			t.Fatalf("%s: %v", params.WorkloadType, err)
		}
		if got.BucketCount != 4 {
			t.Fatalf("%s: BucketCount = %d, want 4", params.WorkloadType, got.BucketCount)
		}
	}
}

func TestApplyBucketCountAllowsSeededDeleteOnlyWithSingleObjectRequests(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeDelete, Bucket: "bench"}
	if err := applyBucketCount(bucketCountTestCommand(t, "4"), &params); err == nil ||
		!strings.Contains(err.Error(), "requires --delete-batch-size=1") {
		t.Fatalf("default batch size: err = %v", err)
	}
	cmd := bucketCountTestCommand(t, "4")
	if err := cmd.Flags().Set(flagDeleteBatchSize, "1"); err != nil {
		t.Fatal(err)
	}
	if err := applyBucketCount(cmd, &params); err != nil || params.BucketCount != 4 {
		t.Fatalf("batch size 1: err = %v, BucketCount = %d", err, params.BucketCount)
	}
}

func TestApplyBucketCountRejectsWorkloadsThatOnlyUseExistingOrListedObjects(t *testing.T) {
	cases := []struct {
		params scenario.Params
		want   string
	}{
		{scenario.Params{WorkloadType: WorkloadTypeRead, Bucket: "bench", ItemsFile: "/in/items.csv"}, "read --items-file"},
		{scenario.Params{WorkloadType: WorkloadTypeDelete, Bucket: "bench", ItemsFile: "/in/items.csv"}, "delete --items-file"},
		{scenario.Params{WorkloadType: WorkloadTypeDelete, Bucket: "bench", DeleteExisting: true}, "delete --delete-existing"},
		{scenario.Params{WorkloadType: WorkloadTypeReadVerify, Bucket: "bench"}, "read-verify"},
		{scenario.Params{WorkloadType: WorkloadTypeList, Bucket: "bench"}, "list"},
		{scenario.Params{WorkloadType: WorkloadTypeTables, Bucket: "bench"}, "tables workload"},
		{scenario.Params{WorkloadType: WorkloadTypeMock, Bucket: "bench"}, "mock workload"},
	}
	for _, c := range cases {
		params := c.params
		err := applyBucketCount(bucketCountTestCommand(t, "4"), &params)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: err = %v, want %q", c.params.WorkloadType, err, c.want)
		}
		if params.BucketCount != 0 {
			t.Fatalf("%s: a rejected count must not be recorded", c.params.WorkloadType)
		}
	}
}

func TestApplyBucketCountKeepsOneBucketForEveryWorkload(t *testing.T) {
	for _, workload := range []string{WorkloadTypeList, WorkloadTypeReadVerify, WorkloadTypeTables, WorkloadTypeMock} {
		for _, value := range []string{"", "1"} {
			params := scenario.Params{WorkloadType: workload, Bucket: "Any_Old.Name"}
			if err := applyBucketCount(bucketCountTestCommand(t, value), &params); err != nil || params.BucketCount != 0 {
				t.Fatalf("%s %q: err = %v, BucketCount = %d", workload, value, err, params.BucketCount)
			}
		}
	}
}

func TestApplyBucketCountRejectsOutOfRangeCountsAndInvalidMemberNames(t *testing.T) {
	for _, value := range []string{"0", "-3", "1001"} {
		params := scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "bench"}
		if err := applyBucketCount(bucketCountTestCommand(t, value), &params); err == nil ||
			!strings.Contains(err.Error(), "between 1 and 1000") {
			t.Fatalf("--bucket-count %s: err = %v", value, err)
		}
	}
	params := scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "xn--bench"}
	if err := applyBucketCount(bucketCountTestCommand(t, "2"), &params); err == nil ||
		!strings.Contains(err.Error(), "reserved prefix") {
		t.Fatalf("invalid member: err = %v", err)
	}
}

func TestApplyBucketCountRejectsEveryEngineOverrideOfTheCount(t *testing.T) {
	for _, override := range []string{"storage.targetBucket.count=4", "storage-targetBucket-count=4", " storage.targetBucket.count = 1"} {
		for _, value := range []string{"", "1", "4"} {
			params := scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "bench", EngineOverrides: []string{override}}
			err := applyBucketCount(bucketCountTestCommand(t, value), &params)
			if err == nil || !strings.Contains(err.Error(), "use --bucket-count") {
				t.Fatalf("override %q with flag %q: err = %v", override, value, err)
			}
		}
	}
}

func TestScenarioSummaryNamesTheFirstAndLastMember(t *testing.T) {
	summary := formatScenarioParams(scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "ps", BucketCount: 12})
	if !strings.Contains(summary, "Bucket Count: 12 (ps-0 ... ps-11)") {
		t.Fatalf("summary:\n%s", summary)
	}
	if strings.Contains(formatScenarioParams(scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "ps"}), "Bucket Count") {
		t.Fatal("one bucket adds no summary line")
	}
}
