package poolsizing

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
)

const (
	// FloorBytes is 5.16's shipped ceiling, which a ceiling not being a reservation is never lowered (D51).
	FloorBytes int64 = 64 * constants.BytesPerGiB
	// NonDirectReserveBytes is what capacity keeps for the JVM's metaspace, code cache, thread stacks
	// and GC structures beyond the heap (N6). It only limits what the CLI adds above the floor.
	NonDirectReserveBytes int64 = 4 * constants.BytesPerGiB
	// DefaultHeapBytes is the entrypoint's -Xmx.
	DefaultHeapBytes int64 = 4 * constants.BytesPerGiB

	// ShortfallFail stops a run whose pools do not fit.
	ShortfallFail = "fail"
	// ShortfallWarn lets a run whose pools do not fit go on, with a warning.
	ShortfallWarn = "warn"

	directMemoryOption = "-XX:MaxDirectMemorySize="
)

// How an engine's value was decided.
const (
	// ModeSized means the CLI set the value.
	ModeSized = "sized"
	// ModeExplicit means SPT_JAVA_OPTS set it, and the engine's check applies.
	ModeExplicit = "explicit"
	// ModeAttached means the engine is an attached worker, whose options the CLI cannot change (D52).
	ModeAttached = "attached"
	// ModeDefault means no contribution is known, so the shipped default stays (T3).
	ModeDefault = "default"
	// ModeZero means no pool needs a buffer, which changes nothing.
	ModeZero = "zero"
	// ModeUnprobed means the host could not be asked, so the shipped default stays.
	ModeUnprobed = "unprobed"
	// ModeNoNicSet means the host has no NIC set.
	ModeNoNicSet = "no-nic-set"
)

// StepLimit is the limit one step needs on one engine.
type StepLimit struct {
	ID         string `json:"id"`
	LimitBytes int64  `json:"limitBytes"`
	Partial    bool   `json:"partial,omitempty"`
}

// CapacityRecord is an engine's capacity with the inputs it took off.
type CapacityRecord struct {
	EffectiveBytes int64  `json:"effectiveBytes"`
	MemoryBytes    int64  `json:"memoryBytes"`
	Source         string `json:"source"`
	Cgroup         string `json:"cgroup,omitempty"`
	HeapBytes      int64  `json:"heapBytes"`
	ReserveBytes   int64  `json:"reserveBytes"`
	Note           string `json:"note,omitempty"`
}

// Engine is what the CLI decided for one engine, as run metadata records it (item 17). The engine's
// own effective value is in its report, which nicEngines stores per step.
type Engine struct {
	Host            string          `json:"host"`
	Mode            string          `json:"mode"`
	ShortfallMode   string          `json:"shortfallMode"`
	ExplicitBytes   int64           `json:"explicitBytes,omitempty"`
	NeedBytes       int64           `json:"needBytes,omitempty"`
	CalculatedBytes int64           `json:"calculatedBytes,omitempty"`
	PassedBytes     int64           `json:"passedBytes,omitempty"`
	Threads         int             `json:"threads"`
	Nics            []string        `json:"nics,omitempty"`
	Steps           []StepLimit     `json:"steps,omitempty"`
	Unknown         []string        `json:"unknown,omitempty"`
	ResidentBytes   int64           `json:"prewarmResidentBytes,omitempty"`
	Capacity        *CapacityRecord `json:"capacity,omitempty"`
	Message         string          `json:"message,omitempty"`
	Warning         bool            `json:"warning,omitempty"`
	// Failure, when set, stops the run before launch.
	Failure string `json:"failure,omitempty"`
}

// Value is the option for the JVM, in the form the JVM takes it.
func (e Engine) Value() string {
	return formatForJVM(e.PassedBytes)
}

// DecideInput is one engine's inputs.
type DecideInput struct {
	Host     string
	Threads  int
	Attached bool
	// NoNicSet is a host whose value is none.
	NoNicSet bool
	// SptJavaOpts is the user's SPT_JAVA_OPTS.
	SptJavaOpts string
	Shortfall   string
	// Output is the image command's answer, or nil with ProbeError.
	Output     *nicplan.Output
	ProbeError error
	// PlanFailure is the host's NIC plan failing, which the engine also reports at the barrier.
	PlanFailure string
}

// composite is a run's steps put together on one engine.
type composite struct {
	need     int64
	known    bool
	steps    []StepLimit
	unknown  []string
	resident int64
}

// compose takes the largest limit across sequential steps. A known step's limit survives an unknown
// step before or after it (T3). Concurrent drivers are already added within each step.
func compose(steps []nicplan.PoolStep) composite {
	var c composite
	seen := map[string]bool{}
	for _, step := range steps {
		c.steps = append(c.steps, StepLimit{ID: step.ID, LimitBytes: step.LimitBytes, Partial: step.Partial})
		c.known = c.known || step.Known
		if step.LimitBytes > c.need {
			c.need = step.LimitBytes
		}
		if step.PrewarmResidentBytes > c.resident {
			c.resident = step.PrewarmResidentBytes
		}
		for _, reason := range step.Unknown {
			if !seen[reason] {
				seen[reason] = true
				c.unknown = append(c.unknown, reason)
			}
		}
	}
	return c
}

// Decide applies D51 to one engine: max(64g, min(need, capacity)).
func Decide(in DecideInput) Engine {
	engine := Engine{Host: in.Host, ShortfallMode: shortfallOf(in.Shortfall), Threads: in.Threads}
	if in.NoNicSet {
		engine.Mode = ModeNoNicSet
		return engine
	}
	if in.Output != nil && in.Output.Capacity != nil {
		engine.Capacity = capacityRecord(in.Output.Capacity, in.SptJavaOpts)
	}
	if in.Attached {
		engine.Mode = ModeAttached
		engine.Message = fmt.Sprintf("RDMA direct memory: %s is an attached worker, which the CLI cannot size; "+
			"its engine reports its limit and checks its pools", in.Host)
		return engine
	}
	explicit := explicitBytes(in.SptJavaOpts)
	if HasExplicitLimit(in.SptJavaOpts) {
		engine.Mode = ModeExplicit
		engine.ExplicitBytes = explicit
		engine.PassedBytes = explicit
	}
	if in.ProbeError != nil || in.Output == nil || in.Output.PoolDemand == nil {
		return unprobed(engine, in)
	}
	if in.Output.Plan != nil {
		engine.Nics = append(engine.Nics, in.Output.Plan.NicSet...)
	}
	demand := compose(in.Output.PoolDemand.Steps)
	engine.Steps = demand.steps
	engine.Unknown = demand.unknown
	engine.ResidentBytes = demand.resident
	engine.NeedBytes = demand.need
	if engine.Mode == ModeExplicit {
		engine.Message = fmt.Sprintf("RDMA direct memory: %s keeps %s from %s", in.Host, formatForJVM(explicit), constants.EnvSptJavaOpts)
		return engine
	}
	switch {
	case !demand.known && len(demand.unknown) > 0:
		engine.Mode = ModeDefault
		engine.Warning = true
		engine.Message = fmt.Sprintf("RDMA direct memory: %s keeps the default limit: its pool demand is unknown (%s)",
			in.Host, strings.Join(demand.unknown, "; "))
		return engine
	case demand.need == 0:
		engine.Mode = ModeZero
		return engine
	}
	return sized(engine, demand)
}

// unprobed is an engine with no estimate: it keeps the shipped default, and its engine checks its pools.
func unprobed(engine Engine, in DecideInput) Engine {
	if engine.Mode == "" {
		engine.Mode = ModeUnprobed
	}
	switch {
	case in.PlanFailure != "":
		engine.Message = fmt.Sprintf("RDMA direct memory: %s has no NIC plan, so its pools were not sized: %s", in.Host, in.PlanFailure)
	case in.ProbeError != nil:
		engine.Message = fmt.Sprintf("RDMA direct memory: could not size %s: %v; it keeps the default limit, and its engine checks its pools",
			in.Host, in.ProbeError)
	default:
		engine.Message = fmt.Sprintf("RDMA direct memory: could not size %s: the image gave no estimate; "+
			"it keeps the default limit, and its engine checks its pools", in.Host)
	}
	engine.Warning = true
	return engine
}

// sized sets the value for a known need, and applies the shortfall policy to a need above capacity.
func sized(engine Engine, demand composite) Engine {
	engine.Mode = ModeSized
	capacity := int64(-1)
	if engine.Capacity != nil {
		capacity = engine.Capacity.EffectiveBytes
	}
	engine.CalculatedBytes = value(demand.need, capacity)
	engine.PassedBytes = engine.CalculatedBytes
	if capacity >= 0 && demand.need > capacity {
		short := fmt.Sprintf("%s needs %s of direct memory for its RDMA pools, but has %s: %s", engine.Host, format(demand.need), format(capacity), describeCapacity(engine.Capacity))
		if engine.ShortfallMode == ShortfallFail {
			engine.Failure = short + fmt.Sprintf(". Lower the threads or object size, or set --rdma-pool-shortfall=%s to run with per-operation buffers", ShortfallWarn)
			return engine
		}
		engine.Warning = true
		engine.Message = "RDMA direct memory: " + short + fmt.Sprintf("; it runs with -XX:MaxDirectMemorySize=%s under --rdma-pool-shortfall=%s", engine.Value(), ShortfallWarn)
		return engine
	}
	engine.Message = sizedMessage(engine, demand, capacity)
	engine.Warning = len(demand.unknown) > 0
	return engine
}

func sizedMessage(engine Engine, demand composite, capacity int64) string {
	message := fmt.Sprintf("RDMA direct memory: %s: -XX:MaxDirectMemorySize=%s (its steps need %s", engine.Host, engine.Value(), format(demand.need))
	if engine.Capacity != nil && capacity >= 0 {
		message += ", capacity " + format(capacity) + " from " + engine.Capacity.Source
		if engine.Capacity.Note != "" {
			message += " (" + engine.Capacity.Note + ")"
		}
	} else {
		message += ", capacity not read"
	}
	if demand.resident > 0 {
		message += ", pre-warm makes " + format(demand.resident) + " resident"
	}
	message += ")"
	if len(demand.unknown) > 0 {
		message += "; sized from the known demand, and this is unknown: " + strings.Join(demand.unknown, "; ")
	}
	return message
}

// value is max(64g, min(need, capacity)). A capacity that could not be read limits nothing.
func value(need, capacity int64) int64 {
	limited := need
	if capacity >= 0 && capacity < limited {
		limited = capacity
	}
	if limited < FloorBytes {
		return FloorBytes
	}
	return limited
}

func shortfallOf(value string) string {
	if value == ShortfallWarn {
		return ShortfallWarn
	}
	return ShortfallFail
}

func capacityRecord(capacity *nicplan.Capacity, sptJavaOpts string) *CapacityRecord {
	record := &CapacityRecord{
		MemoryBytes:  capacity.Bytes,
		Source:       capacity.Source,
		Cgroup:       capacity.Cgroup,
		HeapBytes:    heapBytes(sptJavaOpts),
		ReserveBytes: NonDirectReserveBytes,
		Note:         capacity.Note,
	}
	record.EffectiveBytes = -1
	if capacity.Bytes >= 0 {
		record.EffectiveBytes = capacity.Bytes - record.HeapBytes - record.ReserveBytes
		if record.EffectiveBytes < 0 {
			record.EffectiveBytes = 0
		}
	}
	return record
}

func describeCapacity(capacity *CapacityRecord) string {
	source := "host memory"
	if capacity.Source == "cgroup" {
		source = "the cgroup limit"
		if capacity.Cgroup != "" {
			source += " on " + capacity.Cgroup
		}
	}
	described := fmt.Sprintf("%s of %s, less %s heap and a %s reserve", source, format(capacity.MemoryBytes), format(capacity.HeapBytes), format(capacity.ReserveBytes))
	if capacity.Note != "" {
		described += " (" + capacity.Note + ")"
	}
	return described
}

var (
	explicitOption = regexp.MustCompile(`(?:^|\s)` + regexp.QuoteMeta(directMemoryOption) + `(\S+)`)
	heapOption     = regexp.MustCompile(`(?:^|\s)(?:-Xmx|-XX:MaxHeapSize=)(\S+)`)
)

// HasExplicitLimit reports whether SPT_JAVA_OPTS sets -XX:MaxDirectMemorySize, the last of which wins.
func HasExplicitLimit(sptJavaOpts string) bool {
	return explicitOption.MatchString(sptJavaOpts)
}

func explicitBytes(sptJavaOpts string) int64 {
	return lastOption(explicitOption, sptJavaOpts)
}

func heapBytes(sptJavaOpts string) int64 {
	if heap := lastOption(heapOption, sptJavaOpts); heap > 0 {
		return heap
	}
	return DefaultHeapBytes
}

func lastOption(pattern *regexp.Regexp, options string) int64 {
	matches := pattern.FindAllStringSubmatch(options, -1)
	if len(matches) == 0 {
		return 0
	}
	parsed, err := parseJVMSize(matches[len(matches)-1][1])
	if err != nil {
		return 0
	}
	return parsed
}

// parseJVMSize reads a JVM size: a number with an optional k, m, g or t.
func parseJVMSize(value string) (int64, error) {
	if plain, err := strconv.ParseInt(value, 10, 64); err == nil {
		return plain, nil
	}
	return sizeparse.Parse(value)
}

func formatForJVM(bytes int64) string {
	if bytes > 0 && bytes%constants.BytesPerGiB == 0 {
		return fmt.Sprintf("%dg", bytes/constants.BytesPerGiB)
	}
	return strconv.FormatInt(bytes, 10)
}

// format gives bytes in the form the engine's messages use.
func format(bytes int64) string {
	switch {
	case bytes >= constants.BytesPerGiB && bytes%constants.BytesPerGiB == 0:
		return fmt.Sprintf("%dGiB", bytes/constants.BytesPerGiB)
	case bytes >= constants.BytesPerGiB:
		return fmt.Sprintf("%.1fGiB", float64(bytes)/float64(constants.BytesPerGiB))
	case bytes >= constants.BytesPerMiB:
		return fmt.Sprintf("%dMiB", bytes/constants.BytesPerMiB)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}
