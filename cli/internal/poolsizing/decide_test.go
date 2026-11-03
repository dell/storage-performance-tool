package poolsizing

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
)

const gib = int64(1) << 30

func step(id string, limit int64) nicplan.PoolStep {
	return holding(id, limit, limit)
}

// holding is a step whose limit is a ceiling above what its pools hold, as each driver budgets half of the limit.
func holding(id string, limit, pools int64) nicplan.PoolStep {
	return nicplan.PoolStep{ID: id, LimitBytes: limit, LimitForPoolsBytes: limit, LimitForProcessBytes: pools, PoolsNeedBytes: pools, Known: limit > 0}
}

func output(capacity *nicplan.Capacity, steps ...nicplan.PoolStep) *nicplan.Output {
	return &nicplan.Output{PoolDemand: &nicplan.PoolDemand{Steps: steps}, Capacity: capacity, Plan: &nicplan.Plan{NicSet: []string{"ens1", "ens2"}, OK: true}}
}

func capacityOf(bytes int64, source string) *nicplan.Capacity {
	return &nicplan.Capacity{Bytes: bytes, Source: source, MemTotalBytes: bytes, CgroupLimitBytes: -1}
}

func TestAStepsLargestLimitSetsTheValue(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), step("seed", 72*gib), step("read", 192*gib), step("delete", 0))})
	if engine.Mode != ModeSized || engine.NeedBytes != 192*gib || engine.PassedBytes != 192*gib || engine.Value() != "192g" {
		t.Fatalf("a large step after a small one: %+v", engine)
	}
	if !strings.Contains(engine.Message, "-XX:MaxDirectMemorySize=192g") {
		t.Fatalf("the CLI prints the value it set: %q", engine.Message)
	}
	smallAfter := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), step("read", 192*gib), step("seed", 72*gib))})
	if smallAfter.PassedBytes != 192*gib {
		t.Fatalf("a small step after a large one: %+v", smallAfter)
	}
}

func TestANeedBelowTheFloorKeeps64g(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), step("seed", 14*gib))})
	if engine.PassedBytes != FloorBytes || engine.Value() != "64g" || engine.NeedBytes != 14*gib {
		t.Fatalf("%+v", engine)
	}
}

func TestTheFloorHoldsAboveASmallCapacity(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(16*gib, "cgroup"), step("seed", 4*gib))})
	if engine.PassedBytes != FloorBytes || engine.Failure != "" {
		t.Fatalf("64g is a ceiling, not a reservation, so it is never lowered: %+v", engine)
	}
}

func TestPoolsThatFitRunAtTheirCeilingAndWarnWhenItIsAboveCapacity(t *testing.T) {
	for _, shortfall := range []string{ShortfallFail, ShortfallWarn} {
		engine := Decide(DecideInput{Host: "h", Shortfall: shortfall, Output: output(capacityOf(128*gib, "MemTotal"), holding("read", 192*gib, 96*gib))})
		if engine.Failure != "" || engine.PassedBytes != 192*gib || engine.CalculatedBytes != 192*gib || engine.PoolsBytes != 96*gib || !engine.Warning {
			t.Fatalf("%s: %+v", shortfall, engine)
		}
		for _, want := range []string{"-XX:MaxDirectMemorySize=192g", "capacity 120GiB", "above the capacity", "the pools hold 96GiB"} {
			if !strings.Contains(engine.Message, want) {
				t.Errorf("%s: %q is missing %q", shortfall, engine.Message, want)
			}
		}
		plan := Plan{Engines: []Engine{engine}}
		findings := plan.Findings()
		if plan.Failure() != nil || len(findings) != 1 || findings[0].Failure {
			t.Fatalf("%s: a warning, which stops nothing: %+v", shortfall, findings)
		}
	}
}

func TestPoolsAtExactlyTheCapacityFit(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(128*gib, "MemTotal"), holding("read", 192*gib, 120*gib))})
	if engine.Failure != "" || engine.PassedBytes != 192*gib {
		t.Fatalf("%+v", engine)
	}
}

func TestACeilingWithinCapacityNeedsNoWarning(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), holding("read", 192*gib, 96*gib))})
	if engine.Warning || strings.Contains(engine.Message, "above the capacity") {
		t.Fatalf("%+v", engine)
	}
}

func TestThePoolsOfTheLargestStepAreHeldAgainstCapacity(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(64*gib, "MemTotal"), holding("seed", 16*gib, 8*gib), holding("read", 192*gib, 96*gib), step("delete", 0))})
	if engine.Failure == "" || engine.PoolsBytes != 96*gib {
		t.Fatalf("%+v", engine)
	}
}

func TestPoolsThatSkipPreWarmAreStillHeldAgainstCapacity(t *testing.T) {
	cold := holding("read", 192*gib, 96*gib)
	cold.PrewarmResidentBytes = 0
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(64*gib, "MemTotal"), cold)})
	if engine.Failure == "" {
		t.Fatalf("cold pools fill on first use to the same size: %+v", engine)
	}
}

func TestACapacityNotReadComparesNothing(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(-1, "unknown"), holding("read", 192*gib, 96*gib))})
	if engine.Failure != "" || engine.Warning || engine.PassedBytes != 192*gib {
		t.Fatalf("%+v", engine)
	}
}

func TestAnLXCWithNoCapacityLeftRefusesAnyPools(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(8*gib, "MemTotal"), holding("read", 16*gib, 1*gib))})
	if engine.Failure == "" {
		t.Fatalf("8 GiB less the heap and the reserve leaves nothing: %+v", engine)
	}
}

func TestPoolsAboveCapacityFailBeforeLaunchWithBothAmounts(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Shortfall: ShortfallFail, Output: output(capacityOf(128*gib, "MemTotal"), step("read", 192*gib))})
	if engine.Failure == "" {
		t.Fatalf("no failure: %+v", engine)
	}
	for _, want := range []string{"pools of h hold 192GiB", "but it has 120GiB", "128GiB", "4GiB heap", "4GiB reserve", "--rdma-pool-shortfall=warn"} {
		if !strings.Contains(engine.Failure, want) {
			t.Errorf("%q is missing %q", engine.Failure, want)
		}
	}
	plan := Plan{Engines: []Engine{engine}}
	if plan.Failure() == nil {
		t.Error("the plan must stop the run")
	}
}

func TestPoolsAboveCapacityUnderWarnLaunchAtTheCapacity(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Shortfall: ShortfallWarn, Output: output(capacityOf(128*gib, "MemTotal"), step("read", 192*gib))})
	if engine.Failure != "" || engine.PassedBytes != 120*gib || !engine.Warning {
		t.Fatalf("%+v", engine)
	}
	small := Decide(DecideInput{Host: "h", Shortfall: ShortfallWarn, Output: output(capacityOf(32*gib, "cgroup"), step("read", 192*gib))})
	if small.PassedBytes != FloorBytes {
		t.Fatalf("max(64g, capacity): %+v", small)
	}
}

func TestTheHeapComesFromSptJavaOptsOrTheEntrypoint(t *testing.T) {
	defaulted := Decide(DecideInput{Host: "h", Shortfall: ShortfallWarn, Output: output(capacityOf(100*gib, "MemTotal"), step("read", 192*gib))})
	if defaulted.Capacity.EffectiveBytes != 92*gib || defaulted.Capacity.HeapBytes != DefaultHeapBytes {
		t.Fatalf("%+v", defaulted.Capacity)
	}
	custom := Decide(DecideInput{Host: "h", Shortfall: ShortfallWarn, SptJavaOpts: "-Xmx2g -Xmx16g", Output: output(capacityOf(100*gib, "MemTotal"), step("read", 192*gib))})
	if custom.Capacity.EffectiveBytes != 80*gib || custom.Capacity.HeapBytes != 16*gib {
		t.Fatalf("the last -Xmx wins: %+v", custom.Capacity)
	}
}

func TestMaxHeapSizeIsTheHeapToo(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Shortfall: ShortfallWarn, SptJavaOpts: "-Xmx2g -XX:MaxHeapSize=16g", Output: output(capacityOf(100*gib, "MemTotal"), step("read", 192*gib))})
	if engine.Capacity.HeapBytes != 16*gib {
		t.Fatalf("the JVM takes the last of either: %+v", engine.Capacity)
	}
}

// Zero is a value the JVM accepts: the limit then follows the heap. The engine checks it.
func TestAnExplicitZeroDirectLimitIsStillTheUsersChoice(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Shortfall: ShortfallFail, SptJavaOpts: "-XX:MaxDirectMemorySize=0", Output: output(capacityOf(100*gib, "MemTotal"), step("read", 192*gib))})
	if engine.Mode != ModeExplicit || engine.Failure != "" {
		t.Fatalf("%+v", engine)
	}
}

func TestChecksAtAnExplicitZeroLimitAreNotShownAtTheFloor(t *testing.T) {
	var text strings.Builder
	renderChecks(&text, Engine{Mode: ModeExplicit}, nil)
	if !strings.Contains(text.String(), "Checks at "+formatForJVM(0)) || strings.Contains(text.String(), formatForJVM(FloorBytes)) {
		t.Fatal(text.String())
	}
}

// Without the image's answer, the run must say that no cgroup limit was checked.
func TestACapacityNoteReachesWhatARunPrints(t *testing.T) {
	bounded := capacityOf(100*gib, "MemTotal")
	bounded.Note = "no cgroup limit was checked"
	if engine := Decide(DecideInput{Host: "h", Shortfall: ShortfallFail, Output: output(bounded, step("read", 10*gib))}); !strings.Contains(engine.Message, bounded.Note) {
		t.Fatalf("sized: %+v", engine)
	}
	if engine := Decide(DecideInput{Host: "h", Shortfall: ShortfallFail, Output: output(bounded, step("read", 192*gib))}); !strings.Contains(engine.Failure, bounded.Note) {
		t.Fatalf("refused: %+v", engine)
	}
}

func TestAnExplicitValueStaysAndItsDemandIsStillRecorded(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", SptJavaOpts: "-XX:+UseZGC -XX:MaxDirectMemorySize=128g", Output: output(capacityOf(1024*gib, "MemTotal"), step("read", 192*gib))})
	if engine.Mode != ModeExplicit || engine.ExplicitBytes != 128*gib || engine.CalculatedBytes != 0 || engine.NeedBytes != 192*gib {
		t.Fatalf("%+v", engine)
	}
	if LaunchOptionsOf(engine).JavaOpts != "" {
		t.Fatal("the CLI adds nothing beside a user's value")
	}
	if engine.Failure != "" {
		t.Fatal("spt run leaves a short explicit value to the engine's check")
	}
	findings := Plan{Engines: []Engine{engine}}.Findings()
	if len(findings) != 1 || !findings[0].Failure {
		t.Fatalf("verify fails it under fail: %+v", findings)
	}
	warned := Decide(DecideInput{Host: "h", Shortfall: ShortfallWarn, SptJavaOpts: "-XX:MaxDirectMemorySize=128g", Output: output(nil, step("read", 192*gib))})
	findings = Plan{Engines: []Engine{warned}}.Findings()
	if len(findings) != 1 || findings[0].Failure {
		t.Fatalf("verify warns under warn: %+v", findings)
	}
}

func TestAnAttachedWorkerIsNotSized(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Attached: true, Output: output(nil, step("read", 192*gib))})
	if engine.Mode != ModeAttached || engine.PassedBytes != 0 || !strings.Contains(engine.Message, "cannot size") {
		t.Fatalf("%+v", engine)
	}
	if options := LaunchOptionsOf(engine); options.JavaOpts != "" || options.Estimate != "" {
		t.Fatalf("%+v", options)
	}
}

func TestAKnownStepSurvivesAnUnknownOne(t *testing.T) {
	unknown := nicplan.PoolStep{ID: "read", Unknown: []string{"GET: items.csv: a malformed item"}}
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), step("seed", 192*gib), unknown)})
	if engine.PassedBytes != 192*gib || !engine.Warning || !strings.Contains(engine.Message, "GET: items.csv: a malformed item") {
		t.Fatalf("a known step followed by an unknown one: %+v", engine)
	}
}

func TestAMixedRunSizedForItsPutsNamesItsUnknownGets(t *testing.T) {
	mixed := nicplan.PoolStep{ID: "mixed", LimitBytes: 96 * gib, Known: true, Partial: true, Unknown: []string{"GET: mixed-read-items.csv: a malformed item"}}
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), mixed)})
	if engine.PassedBytes != 96*gib || !strings.Contains(engine.Message, "this is unknown: GET: mixed-read-items.csv") {
		t.Fatalf("%+v", engine)
	}
}

func TestAPlanWithNoKnownContributionKeepsTheDefaultWithAMessage(t *testing.T) {
	unknown := nicplan.PoolStep{ID: "read", Unknown: []string{"GET: its items come from a bucket listing"}}
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), unknown)})
	if engine.Mode != ModeDefault || engine.PassedBytes != 0 || !strings.Contains(engine.Message, "keeps the default limit") {
		t.Fatalf("%+v", engine)
	}
	if LaunchOptionsOf(engine).JavaOpts != "" {
		t.Fatal("the shipped default stays")
	}
}

func TestZeroDemandChangesNothing(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), step("delete", 0))})
	if engine.Mode != ModeZero || engine.Message != "" || LaunchOptionsOf(engine).JavaOpts != "" {
		t.Fatalf("%+v", engine)
	}
}

func TestAHostThatCannotBeAskedKeepsTheDefaultAndStillRuns(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", ProbeError: errors.New("ssh failed")})
	if engine.Mode != ModeUnprobed || !engine.Warning || engine.Failure != "" || !strings.Contains(engine.Message, "ssh failed") {
		t.Fatalf("%+v", engine)
	}
}

func TestAHostWhosePlanFailsStillShowsItsCapacity(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", PlanFailure: "no RDMA device", Output: &nicplan.Output{Capacity: capacityOf(64*gib, "cgroup")}})
	if engine.Capacity == nil || engine.Capacity.Source != "cgroup" || engine.Capacity.MemoryBytes != 64*gib {
		t.Fatalf("%+v", engine.Capacity)
	}
	if !strings.Contains(engine.Message, "no RDMA device") {
		t.Fatalf("%q", engine.Message)
	}
}

func TestAHostWithoutANicSetIsLeftAlone(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", NoNicSet: true})
	if engine.Mode != ModeNoNicSet || engine.Message != "" {
		t.Fatalf("%+v", engine)
	}
}

func TestLaunchOptionsHoldTheValueAndTheEstimate(t *testing.T) {
	engine := Decide(DecideInput{Host: "h", Output: output(capacityOf(1024*gib, "MemTotal"), step("mt-001-seed", 72*gib), step("mt-002-read", 192*gib))})
	options := LaunchOptionsOf(engine)
	if options.JavaOpts != "-XX:MaxDirectMemorySize=192g" {
		t.Fatalf("%q", options.JavaOpts)
	}
	want := "mt-001-seed=" + itoa(72*gib) + ",mt-002-read=" + itoa(192*gib)
	if options.Estimate != want {
		t.Fatalf("%q, want %q", options.Estimate, want)
	}
}

func itoa(value int64) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

// The engine's own response for the shared request, parsed as the CLI reads it.
func TestTheSharedResponseComposesAsTheEngineMeant(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(fixtures, "multipart-seed-response.json"))
	if err != nil {
		t.Fatal(err)
	}
	var demand nicplan.PoolDemand
	if err := json.Unmarshal(raw, &demand); err != nil {
		t.Fatal(err)
	}
	if demand.HeadroomBytes != 8*gib {
		t.Fatalf("headroom = %d", demand.HeadroomBytes)
	}
	composed := compose(demand.Steps)
	if !composed.known || composed.need != 14*gib || composed.resident != 6*gib || composed.pools != 6*gib {
		t.Fatalf("%+v", composed)
	}
	seed := demand.Steps[0]
	if seed.Drivers[0].Operations[0].Op != "PUT" || seed.Drivers[0].Operations[0].SizeClass != 64<<20 || seed.Drivers[0].Nics[0].Count != 24 {
		t.Fatalf("%+v", seed.Drivers[0])
	}
	read := demand.Steps[1]
	if read.Known || read.Drivers[0].Demand != "zero" || read.LimitBytes != 0 {
		t.Fatalf("whole-object GET of 8 GiB is zero: %+v", read)
	}
	partial := demand.Steps[3]
	if !partial.Partial || len(partial.Unknown) != 1 {
		t.Fatalf("%+v", partial)
	}
}

func TestRenderGivesTheVerifyLines(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(fixtures, "multipart-seed-response.json"))
	if err != nil {
		t.Fatal(err)
	}
	var demand nicplan.PoolDemand
	if err := json.Unmarshal(raw, &demand); err != nil {
		t.Fatal(err)
	}
	out := &nicplan.Output{PoolDemand: &demand, Capacity: capacityOf(1024*gib, "MemTotal"), Plan: &nicplan.Plan{OK: true}}
	engine := Decide(DecideInput{Host: "wrk1", Shortfall: ShortfallWarn, Output: out})
	plan := Plan{Engines: []Engine{engine}, Outputs: map[string]*nicplan.Output{"wrk1": out}}
	var text strings.Builder
	plan.Render(&text)
	for _, want := range []string{
		"RDMA pools on wrk1 (shortfall: warn):",
		"Step seed: needs 14GiB",
		"Driver 1 PUT: 64MiB in the 64MiB class, from --object-size",
		"NIC nic-a: minimum 24, cap 25, need 1.5GiB (24 x 64MiB)",
		"Driver 1 GET: zero, from seed",
		"Driver 1 GET: unknown: items.csv: a malformed item",
		"Checks at 64g: each pool within its budget: passes (needs 12GiB); the process limit: passes (needs 14GiB)",
		"Pools hold: 6GiB, which is what capacity is compared with",
		"Capacity: 1016GiB from host memory of 1024GiB, less 4GiB heap and a 4GiB reserve",
		"MaxDirectMemorySize: the CLI would set 64g",
		"Pre-warm: makes 6GiB resident",
		"Pre-warm skipped, cold pools: mixed-partial: its demand is unknown",
	} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("missing %q in:\n%s", want, text.String())
		}
	}
}
