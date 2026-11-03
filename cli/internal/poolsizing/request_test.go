package poolsizing

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const mib = int64(1) << 20

func rdmaParams(workloadType string) scenario.Params {
	return scenario.Params{
		WorkloadType:       workloadType,
		S3Driver:           scenario.S3DriverRdma,
		Interfaces:         []string{"auto"},
		Threads:            96,
		ObjectSize:         "1GiB",
		RdmaThresholdBytes: mib,
		Endpoints:          []string{"http://192.0.2.1:9000"},
	}
}

func stepsOf(ops ...string) []scenario.StepInfo {
	steps := make([]scenario.StepInfo, 0, len(ops))
	for i, op := range ops {
		steps = append(steps, scenario.StepInfo{ID: op, Number: i + 1, Op: op})
	}
	return steps
}

func noFiles(path string) ItemSizes {
	panic("no file should be read: " + path)
}

func TestSizingAppliesOnlyWithANicSetRdmaAndThePool(t *testing.T) {
	params := rdmaParams("write")
	if !Applies(params) {
		t.Fatal("s3-rdma with a NIC set")
	}
	noSet := rdmaParams("write")
	noSet.Interfaces = nil
	if Applies(noSet) {
		t.Fatal("without a NIC set")
	}
	overrideOnly := noSet
	overrideOnly.HostInterfaces = []nicplan.HostInterfaces{{Host: "h", Value: "ens1", Entries: []string{"ens1"}}}
	if !Applies(overrideOnly) {
		t.Fatal("a host override is a NIC set")
	}
	noneOnly := noSet
	noneOnly.HostInterfaces = []nicplan.HostInterfaces{{Host: "h", Value: "none"}}
	if Applies(noneOnly) {
		t.Fatal("none is no NIC set")
	}
	s3 := rdmaParams("write")
	s3.S3Driver = scenario.S3DriverDefault
	if Applies(s3) {
		t.Fatal("for s3")
	}
	noPool := rdmaParams("write")
	noPool.RdmaDisableBufferPool = true
	if Applies(noPool) {
		t.Fatal("with --rdma-buffer-pool=false")
	}
	overridden := rdmaParams("write")
	overridden.EngineOverrides = []string{"storage.rdma.bufferPool=false"}
	if Applies(overridden) {
		t.Fatal("with the pool off in an engine override")
	}
}

func TestAWriteRunIsOnePut(t *testing.T) {
	request := BuildRequest(rdmaParams("write"), stepsOf("create"), noFiles)
	operation := request.Steps[0].Drivers[0].Operations[0]
	if operation.Kind != kindCreate || operation.Sizes.Range[0] != gib || operation.Sizes.Source != "--object-size" {
		t.Fatalf("%+v", operation)
	}
	driver := request.Steps[0].Drivers[0]
	if driver.Concurrency != 96 || driver.ThresholdBytes != mib {
		t.Fatalf("%+v", driver)
	}
}

func TestAReadRunSizesItsReadsFromTheSeedsObjectSize(t *testing.T) {
	request := BuildRequest(rdmaParams("read"), stepsOf("seed", "read", "delete"), noFiles)
	read := request.Steps[1].Drivers[0].Operations[0]
	if read.Kind != kindRead || read.Sizes.Range[0] != gib || read.Sizes.Source != "seed" {
		t.Fatalf("%+v", read)
	}
	if request.Steps[2].Drivers[0].Operations[0].Kind != kindOther {
		t.Fatalf("a delete has zero demand: %+v", request.Steps[2])
	}
}

func TestASeedWithAPartSizeAndItsReadCarryTheThreshold(t *testing.T) {
	params := rdmaParams("read")
	params.ObjectSize = "8GiB"
	params.PartSize = "64MiB"
	params.MpuObjects, params.MpuParts = 2, 4
	request := BuildRequest(params, stepsOf("seed", "read"), noFiles)
	seed := request.Steps[0].Drivers[0]
	if seed.Operations[0].PartBytes != 64*mib || seed.MultipartObjects != 2 || seed.MultipartParts != 4 {
		t.Fatalf("%+v", seed)
	}
	read := request.Steps[1].Drivers[0].Operations[0]
	if read.PartBytes != 64*mib {
		t.Fatalf("the generated read shares the seed's threshold: %+v", read)
	}
}

func TestAMixedRunHasItsPutsAndItsGetsFromTheSeed(t *testing.T) {
	params := rdmaParams("mixed")
	params.GetDistrib, params.PutDistrib, params.DeleteDistrib = 50, 30, 20
	params.PartSize = "64MiB"
	request := BuildRequest(params, stepsOf("seed", "mixed"), noFiles)
	if request.Steps[0].Drivers[0].Operations[0].PartBytes != 0 {
		t.Fatal("the mixed scenario names no part size")
	}
	operations := request.Steps[1].Drivers[0].Operations
	if len(operations) != 2 || operations[0].Kind != kindRead || operations[1].Kind != kindCreate {
		t.Fatalf("one driver with both: %+v", operations)
	}
	if operations[0].Sizes.Source != "seed" {
		t.Fatalf("%+v", operations[0])
	}
}

func TestAMixedRunWithoutGetsReadsNoFile(t *testing.T) {
	params := rdmaParams("mixed")
	params.PutDistrib, params.DeleteDistrib = 50, 50
	params.ReadItemsFile = "/not/read.csv"
	request := BuildRequest(params, stepsOf("mixed"), noFiles)
	if len(request.Steps[0].Drivers[0].Operations) != 1 {
		t.Fatalf("%+v", request.Steps[0].Drivers[0])
	}
}

// Through the real prepared-file path: the scenario names a container path that is not on this host.
func TestAReadFromItemsFileIsSizedFromTheHostFileThePreparedMountMaps(t *testing.T) {
	for _, name := range []string{"one-class.csv", "manifest.csv"} {
		params := rdmaParams("read")
		params.ItemsFile = filepath.Join(fixtures, "item-files", name)
		prepared, err := scenario.PrepareExternalItemFiles(params)
		if err != nil {
			t.Fatal(err)
		}
		if prepared.ItemsFile == params.ItemsFile {
			t.Fatalf("the path was not rewritten: %q", prepared.ItemsFile)
		}
		if _, err := os.Stat(prepared.ItemsFile); err == nil {
			t.Fatalf("the container path %q should not exist here", prepared.ItemsFile)
		}
		request := BuildRequest(prepared, stepsOf("read"), CachedItemReader(prepared))
		listed := request.Steps[0].Drivers[0].Operations[0].Sizes.Listed
		if listed == nil || !listed.Complete || listed.Items == 0 {
			t.Fatalf("%s: %+v", name, listed)
		}
		if listed.Bounds[0] != [2]int64{3 * mib, 4 * mib} {
			t.Fatalf("%s: %v", name, listed.Bounds)
		}
	}
}

func TestAMixedRunFromReadItemsFileIsSizedFromTheFile(t *testing.T) {
	params := rdmaParams("mixed")
	params.GetDistrib, params.PutDistrib = 50, 50
	params.ReadItemsFile = filepath.Join(fixtures, "item-files", "one-class.csv")
	prepared, err := scenario.PrepareExternalItemFiles(params)
	if err != nil {
		t.Fatal(err)
	}
	request := BuildRequest(prepared, stepsOf("mixed"), CachedItemReader(prepared))
	operations := request.Steps[0].Drivers[0].Operations
	if !operations[0].Sizes.Listed.Complete || operations[1].Sizes.Range[0] != gib {
		t.Fatalf("%+v", operations)
	}
}

func TestAFileThatCannotBeReadInFullIsSentAsUnknownAndThePutsStay(t *testing.T) {
	params := rdmaParams("mixed")
	params.GetDistrib, params.PutDistrib = 50, 50
	params.ReadItemsFile = filepath.Join(fixtures, "item-files", "malformed-size.csv")
	request := BuildRequest(params, stepsOf("mixed"), CachedItemReader(params))
	operations := request.Steps[0].Drivers[0].Operations
	if operations[0].Sizes.Listed.Complete || operations[0].Sizes.Listed.Reason != reasonMalformed {
		t.Fatalf("%+v", operations[0].Sizes.Listed)
	}
	if operations[1].Kind != kindCreate {
		t.Fatalf("%+v", operations[1])
	}
}

func TestEngineOverridesOfWhatTheDefaultsHoldApply(t *testing.T) {
	params := rdmaParams("write")
	params.EngineOverrides = []string{
		"item.data.ranges.threshold=1MiB",
		"storage.rdma.thresholdBytes=4096",
		"storage.driver.limit.multipart.objects=3",
		"storage.driver.limit.multipart.parts=2",
	}
	request := BuildRequest(params, stepsOf("create"), noFiles)
	driver := request.Steps[0].Drivers[0]
	if driver.ThresholdBytes != 4096 || driver.MultipartObjects != 3 || driver.MultipartParts != 2 {
		t.Fatalf("%+v", driver)
	}
	if operation := driver.Operations[0]; operation.PartBytes != mib {
		t.Fatalf("%+v", operation)
	}
}

// A generated step sets its own concurrency and object size after the defaults load.
func TestEngineOverridesOfWhatAStepSetsLose(t *testing.T) {
	params := rdmaParams("write")
	params.EngineOverrides = []string{"storage.driver.limit.concurrency=1", "item.data.size=4MiB"}
	driver := BuildRequest(params, stepsOf("create"), noFiles).Steps[0].Drivers[0]
	if driver.Concurrency != 96 {
		t.Fatalf("the step sets --threads: %+v", driver)
	}
	if operation := driver.Operations[0]; operation.Sizes.Range[0] != 1024*mib || operation.Sizes.Source != "--object-size" {
		t.Fatalf("the step sets --object-size: %+v", operation)
	}
}

func TestADashedEngineOverrideAppliesToo(t *testing.T) {
	params := rdmaParams("write")
	params.EngineOverrides = []string{"storage-rdma-bufferPool=false"}
	if Applies(params) {
		t.Fatal("with the pool off in a dashed engine override")
	}
}

func TestAnExplicitZeroThresholdStaysZero(t *testing.T) {
	params := rdmaParams("write")
	params.RdmaThresholdBytes = 0
	if driver := BuildRequest(params, stepsOf("create"), noFiles).Steps[0].Drivers[0]; driver.ThresholdBytes != 0 {
		t.Fatalf("%+v", driver)
	}
}

func TestAWriteVerifyReadIsSizedForWholeObjects(t *testing.T) {
	params := rdmaParams("write-verify")
	params.PartSize = "64MiB"
	request := BuildRequest(params, stepsOf("create", "read"), noFiles)
	if create := request.Steps[0].Drivers[0].Operations[0]; create.PartBytes != 64*mib {
		t.Fatalf("the seed writes parts: %+v", create)
	}
	if read := request.Steps[1].Drivers[0].Operations[0]; read.PartBytes != 0 {
		t.Fatalf("the verification read sets no threshold: %+v", read)
	}
	params.EngineOverrides = []string{"item.data.ranges.threshold=1MiB"}
	request = BuildRequest(params, stepsOf("create", "read"), noFiles)
	if read := request.Steps[1].Drivers[0].Operations[0]; read.PartBytes != mib {
		t.Fatalf("a threshold in the defaults reaches it: %+v", read)
	}
}

func anyFile(path string) ItemSizes {
	return ItemSizes{Source: path, Complete: true}
}

func TestADefaultsThresholdReachesEveryStepThatInheritsIt(t *testing.T) {
	params := rdmaParams("mixed")
	params.GetDistrib, params.PutDistrib = 1, 1
	params.EngineOverrides = []string{"item.data.ranges.threshold=64MiB"}
	request := BuildRequest(params, stepsOf("seed", "mixed"), noFiles)
	for _, operation := range request.Steps[1].Drivers[0].Operations {
		if operation.PartBytes != 64*mib {
			t.Fatalf("a mixed step inherits it: %+v", operation)
		}
	}
	fromFile := rdmaParams("read")
	fromFile.ItemsFile = "items.csv"
	fromFile.EngineOverrides = params.EngineOverrides
	if read := BuildRequest(fromFile, stepsOf("read"), anyFile).Steps[0].Drivers[0].Operations[0]; read.PartBytes != 64*mib {
		t.Fatalf("a read from a file inherits it: %+v", read)
	}
}

func TestTemplatesThatNameNoPartSizeIgnoreTheFlag(t *testing.T) {
	deleting := rdmaParams("delete")
	deleting.PartSize = "64MiB"
	if seed := BuildRequest(deleting, stepsOf("seed", "delete"), noFiles).Steps[0].Drivers[0].Operations[0]; seed.PartBytes != 0 {
		t.Fatalf("a seeded delete's seed: %+v", seed)
	}
	fromFile := rdmaParams("read")
	fromFile.ItemsFile = "items.csv"
	fromFile.PartSize = "64MiB"
	if read := BuildRequest(fromFile, stepsOf("read"), anyFile).Steps[0].Drivers[0].Operations[0]; read.PartBytes != 0 {
		t.Fatalf("a read from a file: %+v", read)
	}
}

func TestASizeRangeAndARangedReadPassThrough(t *testing.T) {
	params := rdmaParams("write")
	params.ObjectSize = "1MiB-4MiB"
	request := BuildRequest(params, stepsOf("create"), noFiles)
	if got := request.Steps[0].Drivers[0].Operations[0].Sizes.Range; got[0] != mib || got[1] != 4*mib {
		t.Fatalf("%v", got)
	}
	ranged := rdmaParams("read")
	ranged.RangeSize = "1MiB"
	request = BuildRequest(ranged, stepsOf("seed", "read"), noFiles)
	if !request.Steps[1].Drivers[0].Operations[0].Ranged {
		t.Fatalf("%+v", request.Steps[1])
	}
}

func TestAReadVerifyWithoutAFileIsUnknown(t *testing.T) {
	request := BuildRequest(rdmaParams("read-verify"), stepsOf("list", "verify"), noFiles)
	if request.Steps[1].Drivers[0].Operations[0].Sizes.Unknown == "" {
		t.Fatalf("%+v", request.Steps[1].Drivers[0].Operations[0])
	}
}

func TestARequestEncodesForTheCommand(t *testing.T) {
	encoded, err := BuildRequest(rdmaParams("write"), stepsOf("create"), noFiles).Encode()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["steps"]; !ok {
		t.Fatalf("%s", raw)
	}
}

// The shared request holds the JSON shape the engine's PoolDemandRequest reads.
func TestTheSharedRequestIsTheFormBuildRequestWrites(t *testing.T) {
	params := rdmaParams("write")
	params.ObjectSize = "8GiB"
	params.PartSize = "64MiB"
	built, err := json.Marshal(BuildRequest(params, []scenario.StepInfo{{ID: "seed", Op: "create"}}, noFiles))
	if err != nil {
		t.Fatal(err)
	}
	var mine, shared struct {
		Steps []StepRequest `json:"steps"`
	}
	if err := json.Unmarshal(built, &mine); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(fixtures, "multipart-seed-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &shared); err != nil {
		t.Fatal(err)
	}
	want, got := shared.Steps[0].Drivers[0], mine.Steps[0].Drivers[0]
	if got.Concurrency != want.Concurrency || got.ThresholdBytes != want.ThresholdBytes ||
		got.Operations[0].PartBytes != want.Operations[0].PartBytes || *got.Operations[0].Sizes.Range != *want.Operations[0].Sizes.Range {
		t.Fatalf("got %+v, the fixture has %+v", got, want)
	}
}
