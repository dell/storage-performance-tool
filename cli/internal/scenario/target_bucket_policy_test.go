package scenario

import (
	"regexp"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

type phasePolicy struct {
	load         string
	existingOnly bool
}

var generatedLoadStart = regexp.MustCompile(`(?m)^\s*(?:var step = |cleanupLoad = )?(\w*Load)\s*(?:\n\s*)?\.config\(`)

// generatedPhasePolicies lists the generated phases in order and whether each one is limited to
// existing buckets (storage.targetBucket.provision: false).
func generatedPhasePolicies(t *testing.T, generated string) []phasePolicy {
	t.Helper()
	starts := generatedLoadStart.FindAllStringSubmatchIndex(generated, -1)
	phases := make([]phasePolicy, 0, len(starts))
	for i, start := range starts {
		end := len(generated)
		if i+1 < len(starts) {
			end = starts[i+1][0]
		}
		segment := generated[start[0]:end]
		phases = append(phases, phasePolicy{
			load: generated[start[2]:start[3]],
			existingOnly: strings.Contains(segment, ".config(existingBucketsOnly)") ||
				strings.Contains(segment, `"targetBucket": {"provision": false}`),
		})
	}
	return phases
}

func TestTargetBucketPhasePolicyMatrix(t *testing.T) {
	const ts = "20261022.120000.000"
	create := func(load string) phasePolicy { return phasePolicy{load: load} }
	existing := func(load string) phasePolicy { return phasePolicy{load: load, existingOnly: true} }
	tests := []struct {
		name     string
		generate func() (string, error)
		want     []phasePolicy
	}{
		{"write count", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Write, Bucket: "b", ObjectSize: "1KiB", ObjectCount: 5, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("Load")}},
		{"write duration", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Write, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("Load")}},
		{"write cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Write, Bucket: "b", ObjectSize: "1KiB", ObjectCount: 5, Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("CreateLoad"), existing("DeleteLoad")}},
		{"write cleanup duration", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Write, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("CreateLoad"), existing("DeleteLoad")}},
		{"write verify", func() (string, error) {
			return GenerateWriteVerifyScenario(Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 501, Bucket: "b", ObjectSize: "1KiB", ObjectCount: 1, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("CreateLoad"), existing("ReadLoad")}},
		{"write verify cleanup", func() (string, error) {
			return GenerateWriteVerifyScenario(Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 501, Bucket: "b", ObjectSize: "1KiB", ObjectCount: 1, Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("CreateLoad"), existing("ReadLoad"), existing("DeleteLoad")}},
		{"seeded read duration cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("ReadLoad"), existing("DeleteLoad")}},
		{"seeded read count cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", ObjectSize: "1KiB", ObjectCount: 3, Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("ReadLoad"), existing("DeleteLoad")}},
		{"seeded read no cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("ReadLoad")}},
		{"seeded read count no cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", ObjectSize: "1KiB", ObjectCount: 3, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("ReadLoad")}},
		{"read items file", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", ItemsFile: "/spt-input/items.csv", Duration: "1s", Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{existing("ReadLoad")}},
		{"read items file cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", ItemsFile: "/spt-input/items.csv", ObjectCount: 3, Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{existing("ReadLoad"), existing("DeleteLoad")}},
		{"read verify discovery", func() (string, error) {
			return GenerateReadVerifyScenario(Params{WorkloadType: WorkloadTypeReadVerify, RunID: 502, Bucket: "b", ObjectCount: 1, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{existing("Load"), existing("ReadLoad")}},
		{"read verify staged", func() (string, error) {
			return GenerateReadVerifyScenario(Params{WorkloadType: WorkloadTypeReadVerify, RunID: 503, Bucket: "b", ObjectCount: 1, Threads: 1, ItemsFile: "/spt-input/items.csv", BaseTimestamp: ts})
		}, []phasePolicy{existing("ReadLoad")}},
		{"list", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.List, Bucket: "b", Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{existing("Load")}},
		{"seeded delete", func() (string, error) {
			return GenerateDeleteScenario(Params{WorkloadType: workload.Delete, RunID: 21, Bucket: "b", SeedCount: 10, Threads: 1, DeleteBatchSize: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("CreateLoad"), existing("DeleteLoad")}},
		{"seeded delete cleanup", func() (string, error) {
			return GenerateDeleteScenario(Params{WorkloadType: workload.Delete, RunID: 21, Bucket: "b", SeedCount: 10, Duration: "1s", Cleanup: true, Threads: 1, DeleteBatchSize: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("CreateLoad"), existing("DeleteLoad"), existing("DeleteLoad")}},
		{"manifest delete", func() (string, error) {
			return GenerateDeleteScenario(Params{WorkloadType: workload.Delete, RunID: 22, ItemsFile: "/input/items.csv", Threads: 1, DeleteBatchSize: 1, BaseTimestamp: ts})
		}, []phasePolicy{existing("DeleteLoad")}},
		{"existing prefix delete", func() (string, error) {
			return GenerateDeleteScenario(Params{WorkloadType: workload.Delete, RunID: 23, Bucket: "b", Prefix: "safe/", DeleteExisting: true, Threads: 1, DeleteBatchSize: 1, BaseTimestamp: ts})
		}, []phasePolicy{existing("Load"), existing("DeleteLoad")}},
		{"seeded mixed", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Mixed, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", SeedCount: 10, GetDistrib: 50, PutDistrib: 30, DeleteDistrib: 10, StatDistrib: 10, Threads: 4, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("MixedLoad")}},
		{"seeded mixed cleanup", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Mixed, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", SeedCount: 10, GetDistrib: 50, PutDistrib: 30, DeleteDistrib: 10, StatDistrib: 10, Cleanup: true, Threads: 4, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("MixedLoad"), existing("DeleteLoad"), existing("DeleteLoad")}},
		{"mixed external items", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Mixed, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", ReadItemsFile: "/spt-input/read.csv", GetDistrib: 70, PutDistrib: 30, Threads: 4, BaseTimestamp: ts})
		}, nil},
		// A bucket set writes its generated members only through PUTs: without a seed phase the
		// measured mixed phase is the first to write them, so its setup may create them.
		{"mixed external items bucket set", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Mixed, Bucket: "b", BucketCount: 4, ObjectSize: "1KiB", Duration: "1s", ReadItemsFile: "/spt-input/read.csv", DeleteItemsFile: "/spt-input/delete.csv", GetDistrib: 60, PutDistrib: 30, DeleteDistrib: 10, Threads: 4, BaseTimestamp: ts})
		}, []phasePolicy{create("MixedLoad")}},
		{"seeded mixed bucket set", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Mixed, Bucket: "b", BucketCount: 4, ObjectSize: "1KiB", Duration: "1s", SeedCount: 10, GetDistrib: 50, PutDistrib: 30, DeleteDistrib: 10, StatDistrib: 10, Cleanup: true, Threads: 4, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("MixedLoad"), existing("DeleteLoad"), existing("DeleteLoad")}},
		{"seeded read bucket set", func() (string, error) {
			return GenerateScenario(Params{WorkloadType: workload.Read, Bucket: "b", BucketCount: 4, ObjectSize: "1KiB", Duration: "1s", Cleanup: true, Threads: 1, BaseTimestamp: ts})
		}, []phasePolicy{create("PreconditionLoad"), existing("ReadLoad"), existing("DeleteLoad")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generated, err := test.generate()
			if err != nil {
				t.Fatal(err)
			}
			phases := generatedPhasePolicies(t, generated)
			if test.want == nil {
				// External items never create: every phase is limited to existing buckets.
				if len(phases) == 0 {
					t.Fatalf("no phases parsed:\n%s", generated)
				}
				for _, phase := range phases {
					if !phase.existingOnly {
						t.Fatalf("phase %s may create a bucket in a flow with external items:\n%s", phase.load, generated)
					}
				}
				return
			}
			if len(phases) != len(test.want) {
				t.Fatalf("phases = %+v, want %+v\n%s", phases, test.want, generated)
			}
			for i := range phases {
				if phases[i] != test.want[i] {
					t.Fatalf("phase %d = %+v, want %+v\n%s", i, phases[i], test.want[i], generated)
				}
			}
		})
	}
}

// Disabling provisioning reaches the engine through the defaults, never by rewriting phases.
func TestProvisionOptOutDoesNotChangeTheScenario(t *testing.T) {
	params := Params{WorkloadType: workload.Read, Bucket: "b", ObjectSize: "1KiB", Duration: "1s", Cleanup: true, Threads: 1, BaseTimestamp: "20261022.120000.000"}
	enabled, err := GenerateScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	params.TargetBucketProvisionDisabled = true
	disabled, err := GenerateScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	if enabled != disabled {
		t.Fatal("the opt-out belongs to the generated defaults, not the scenario")
	}
}
