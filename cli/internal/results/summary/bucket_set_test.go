package summary

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestWorkloadBucketSetListsEveryMemberOfASet(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		params ScenarioParams
		want   []string
	}{
		{"single bucket", ScenarioParams{Bucket: "ps"}, nil},
		{"count of 1", ScenarioParams{Bucket: "ps", BucketCount: 1}, nil},
		{"generated set", ScenarioParams{Bucket: "ps", BucketCount: 3}, []string{"ps-0", "ps-1", "ps-2"}},
		{"listed set", ScenarioParams{Bucket: "ps-eu", BucketNames: []string{"ps-eu", "PS_US", "ab"}}, []string{"ps-eu", "PS_US", "ab"}},
		{"unusable stored count", ScenarioParams{Bucket: "", BucketCount: 3}, nil},
	} {
		if got := workloadBucketSet(c.params); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%s: workloadBucketSet = %q, want %q", c.name, got, c.want)
		}
		params := c.params
		params.WorkloadType = "write"
		workload, _ := buildWorkloadSummary(&RunData{Params: &RunParams{WorkloadType: "write", ScenarioParams: params}})
		if !reflect.DeepEqual(workload.BucketSet, c.want) || workload.Bucket != c.params.Bucket {
			t.Fatalf("%s: workload summary Bucket %q BucketSet %q", c.name, workload.Bucket, workload.BucketSet)
		}
	}
}

// The run's stored parameters reach the report: a set shows its size and first and last member.
func TestRendererShowsTheWholeBucketSet(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		stored string
		want   string
		absent string
	}{
		{"listed", `{"WorkloadType":"write","Bucket":"ps-eu","BucketNames":["ps-eu","PS_US","ab"]}`, "• Buckets            3 (ps-eu ... ab)", "• Bucket "},
		{"generated", `{"WorkloadType":"write","Bucket":"ps","BucketCount":12}`, "• Buckets            12 (ps-0 ... ps-11)", "• Bucket "},
		{"single, older results", `{"WorkloadType":"write","Bucket":"ps","BucketCount":0}`, "• Bucket             ps", "• Buckets"},
		{"single, before the count", `{"WorkloadType":"write","Bucket":"ps"}`, "• Bucket             ps", "• Buckets"},
	} {
		var params ScenarioParams
		if err := json.Unmarshal([]byte(c.stored), &params); err != nil {
			t.Fatal(err)
		}
		summary := &RunSummary{Workload: WorkloadSummary{Type: "write", Bucket: params.Bucket, BucketSet: workloadBucketSet(params)}}
		report := NewRenderer(RenderOptions{MaxWidth: 120}).FullReport(summary)
		if !strings.Contains(report, c.want) || strings.Contains(report, c.absent) {
			t.Fatalf("%s: want %q and no %q in\n%s", c.name, c.want, c.absent, report)
		}
	}
}
