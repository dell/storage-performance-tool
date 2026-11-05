package poolsizing

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// EstimateEnv carries the CLI's per-step estimate to an engine, which only logs a difference from
// its own figure. It must match PoolPreparation.ESTIMATE_ENV in the engine.
const EstimateEnv = "SPT_RDMA_POOL_ESTIMATE"

// probeTimeout bounds one host's planner and capacity commands.
const probeTimeout = 2 * time.Minute

// hostMemoryTimeout bounds reading a host's own MemTotal; a variable for a test of a host that hangs.
var hostMemoryTimeout = 20 * time.Second

// Input is what a run or a verify gives the plan.
type Input struct {
	Params    scenario.Params
	Steps     []scenario.StepInfo
	Hosts     []*hostparse.HostInfo
	Image     string
	SkipPull  bool
	Attached  bool
	Shortfall string
	// SptJavaOpts is the user's SPT_JAVA_OPTS.
	SptJavaOpts string
	Executor    command.CommandExecutor
}

// Plan is the decision for every engine of a run.
type Plan struct {
	Engines []Engine `json:"engines"`
	// Outputs are the hosts' answers, for verify's lines.
	Outputs map[string]*nicplan.Output `json:"-"`
}

// memTotalField is the host memory's name in /proc/meminfo, and a capacity's source when it is that alone.
const memTotalField = "MemTotal"

// Failure returns the failures that stop a run before launch, or nil.
func (p Plan) Failure() error {
	var failures []error
	for _, engine := range p.Engines {
		if engine.Failure != "" {
			failures = append(failures, errors.New(engine.Failure))
		}
	}
	return errors.Join(failures...)
}

// BuildPlan asks every host's image for its NIC plan and capacity, on that host, before anything
// starts, and decides each engine's value. A host it cannot ask keeps the shipped default: the
// engine still checks its pools, so this never stops a run by itself.
func BuildPlan(ctx context.Context, in Input) (Plan, error) {
	request := BuildRequest(in.Params, in.Steps, CachedItemReader(in.Params))
	encoded, err := request.Encode()
	if err != nil {
		return Plan{}, err
	}
	engines := make([]Engine, len(in.Hosts))
	outputs := make([]*nicplan.Output, len(in.Hosts))
	var group sync.WaitGroup
	for i, host := range in.Hosts {
		group.Add(1)
		go func(i int, host *hostparse.HostInfo) {
			defer group.Done()
			engines[i], outputs[i] = planHost(ctx, in, i, host, encoded)
		}(i, host)
	}
	group.Wait()
	plan := Plan{Engines: engines, Outputs: make(map[string]*nicplan.Output, len(in.Hosts))}
	for i, host := range in.Hosts {
		plan.Outputs[host.Host] = outputs[i]
	}
	return plan, nil
}

func planHost(ctx context.Context, in Input, slice int, host *hostparse.HostInfo, encoded string) (Engine, *nicplan.Output) {
	interfaces := in.Params.Interfaces
	if override, ok := nicplan.ForHost(in.Params.HostInterfaces, host.Host); ok {
		interfaces = override.Entries
	}
	decide := DecideInput{
		Host:        host.Host,
		Threads:     effective(in.Params).concurrency,
		Attached:    in.Attached && slice > 0,
		NoNicSet:    len(interfaces) == 0,
		SptJavaOpts: in.SptJavaOpts,
		Shortfall:   in.Shortfall,
	}
	// An attached worker is not sized, so it is not asked either: it may not be reachable at all.
	if decide.NoNicSet || decide.Attached {
		return Decide(decide), nil
	}
	endpoints := hostEndpoints(in, slice)
	// The command passes through a remote shell, so nothing unchecked goes on its line.
	if err := nicplan.ValidateEndpoints(endpoints); err != nil {
		decide.ProbeError = err
		return Decide(decide), nil
	}
	askHost(ctx, in, host, PlanArgs(interfaces, endpoints, encoded), &decide)
	return Decide(decide), decide.Output
}

// askHost fills in the host's answers. Its capacity is asked apart, as the command with --rdma needs
// an RDMA device: a host without one fails that command and still shows its capacity.
func askHost(ctx context.Context, in Input, host *hostparse.HostInfo, planArgs []string, decide *DecideInput) {
	output, err := probe(ctx, in, host, planArgs)
	switch {
	case err != nil:
		decide.ProbeError = err
	case output.Plan != nil && !output.Plan.OK:
		decide.PlanFailure = nicplan.Summary(output.Plan)
		decide.Output = output
	default:
		decide.Output = output
	}
	var found *nicplan.Capacity
	if capacity, err := probe(ctx, in, host, CapacityArgs()); err == nil && capacity != nil {
		found = capacity.Capacity
	}
	found = boundedByHostMemory(found, hostMemTotal(ctx, in, host))
	if found == nil {
		return
	}
	if decide.Output == nil {
		decide.Output = &nicplan.Output{}
	}
	decide.Output.Capacity = found
}

// boundedByHostMemory applies what the host itself reports. A Proxmox LXC's limit sits above its
// cgroup namespace, where nothing inside can read it, and lxcfs shows it only in the LXC's own
// /proc/meminfo, which a container on it does not get. So the probe alone reports the machine's.
// Without the probe's answer the host's figure is only an upper bound, and its note says so.
func boundedByHostMemory(capacity *nicplan.Capacity, hostBytes int64) *nicplan.Capacity {
	if hostBytes <= 0 {
		// With no figure from the image either, there is nothing to call an upper bound.
		if capacity == nil || capacity.Bytes < 0 {
			return capacity
		}
		// An LXC's limit shows nowhere else, so what the image reports is only an upper bound.
		unbounded := *capacity
		note := "the host's own MemTotal could not be read, so an LXC's limit was not checked"
		if unbounded.Note != "" {
			note = unbounded.Note + "; " + note
		}
		unbounded.Note = note
		return &unbounded
	}
	if capacity == nil {
		return &nicplan.Capacity{
			Bytes: hostBytes, Source: memTotalField, MemTotalBytes: hostBytes, CgroupLimitBytes: -1,
			Note: "the image's capacity probe gave no answer, so this is the host's MemTotal alone and no cgroup limit was checked",
		}
	}
	if capacity.Bytes >= 0 && capacity.Bytes <= hostBytes {
		return capacity
	}
	note := "the host's own /proc/meminfo reports less than a container on it sees"
	if capacity.Bytes < 0 {
		note = "the image could not read its memory, so this is the host's MemTotal alone"
	}
	if capacity.Note != "" {
		note = capacity.Note + "; " + note
	}
	bounded := *capacity
	bounded.Bytes, bounded.Source, bounded.MemTotalBytes, bounded.Cgroup, bounded.Note = hostBytes, memTotalField, hostBytes, "", note
	return &bounded
}

// hostMemTotal reads the host's own MemTotal, outside any container; 0 when it cannot. Waiting is
// bounded here, as a command can outlive its context while a proxy holds its output open: without
// this figure the probe's stands, so nothing is worth holding up a run for.
func hostMemTotal(ctx context.Context, in Input, host *hostparse.HostInfo) int64 {
	ctx, cancel := context.WithTimeout(ctx, hostMemoryTimeout)
	defer cancel()
	result := make(chan int64, 1)
	go func() {
		stdout, _, err := in.Executor.ExecuteCommand(ctx, host, []string{"grep", memTotalField, "/proc/meminfo"})
		if err != nil {
			result <- 0
			return
		}
		result <- parseMemTotal(stdout)
	}()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		select {
		case value := <-result:
			return value // It answered in time; only this wait was late.
		default:
			return 0
		}
	}
}

// parseMemTotal finds a /proc/meminfo line, such as "MemTotal:  8388608 kB", in a command's output,
// which a remote login banner can precede; 0 when there is none.
func parseMemTotal(output string) int64 {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "MemTotal:" || fields[2] != "kB" {
			continue
		}
		if kilobytes, err := strconv.ParseInt(fields[1], 10, 64); err == nil && kilobytes > 0 {
			return kilobytes * 1024
		}
	}
	return 0
}

// hostEndpoints gives a host its slice of the endpoints, as the engine's drivers name them.
func hostEndpoints(in Input, slice int) []string {
	urls := in.Params.Endpoints
	if len(urls) == 0 && in.Params.Endpoint != "" {
		urls = []string{in.Params.Endpoint}
	}
	nodes, err := scenario.ParseNodeEndpoints(urls)
	if err != nil {
		return urls
	}
	endpoints := nodes.DriverEndpoints(nodes.Addrs)
	if in.Params.SliceEndpoints {
		return nicplan.SliceEndpoints(endpoints, len(in.Hosts), slice)
	}
	return endpoints
}

// CapacityArgs are the command's arguments for a host's capacity alone: it takes no RDMA device, so
// a host whose NIC plan fails, such as one without a device, still shows it.
func CapacityArgs() []string {
	return []string{"--capacity"}
}

// PlanArgs are the command's arguments for a host's plan and the workload's demand on it.
func PlanArgs(interfaces, endpoints []string, encoded string) []string {
	args := []string{"--rdma", "--interfaces", strings.Join(interfaces, ",")}
	for _, endpoint := range endpoints {
		args = append(args, "--endpoint", endpoint)
	}
	return append(args, "--pool-request", encoded)
}

// probe runs the image's command in a short-lived container on the host. The capacity probe runs in
// the host's cgroup namespace, so /proc/self/cgroup names the whole chain the engine's own container
// would sit on, ancestors included, such as an LXC container's limit (N6). Without that, a
// container's own namespace hides them.
func probe(ctx context.Context, in Input, host *hostparse.HostInfo, args []string) (*nicplan.Output, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	command := ProbeCommand(in.Image, in.SkipPull, len(args) > 0 && args[0] == "--rdma", args)
	stdout, stderr, err := in.Executor.ExecuteCommand(ctx, host, command)
	if err != nil {
		if nicplan.CommandMissing(stderr) || nicplan.CommandMissing(stdout) {
			return nil, nicplan.ErrImagePredatesNicPlan
		}
		return nil, fmt.Errorf("%s on %s failed: %w%s", constants.SptNicPlanCommand, host.Host, err, firstLine(stderr))
	}
	return nicplan.Parse([]byte(stdout))
}

// ProbeCommand is the docker run that asks the image on one host.
func ProbeCommand(image string, skipPull, rdma bool, args []string) []string {
	command := []string{constants.DockerCommand, "run", "--rm", "--network", "host"}
	if rdma {
		command = append(command, "--device", constants.RdmaDevicePath, "--cap-add", constants.RdmaCapIpcLock, "--ulimit", constants.RdmaUlimitMemlock)
	} else {
		// Only the capacity probe needs the host's view of the cgroups, and a daemon that refuses the
		// flag then loses only that answer.
		command = append(command, "--cgroupns=host")
	}
	command = append(command, "--entrypoint", constants.SptNicPlanCommand)
	if skipPull {
		command = append(command, "--pull", "never")
	}
	command = append(command, image)
	return append(command, args...)
}

func firstLine(stderr string) string {
	for _, line := range strings.Split(stderr, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return ": " + line
		}
	}
	return ""
}

// LaunchOptions are what an engine's container gets from the plan.
type LaunchOptions struct {
	// JavaOpts goes in the container's JAVA_OPTS, which the entrypoint places before SPT_JAVA_OPTS,
	// so a user's value wins unchanged.
	JavaOpts string
	// Estimate is the value of EstimateEnv.
	Estimate string
}

// Environment returns the extra environment of a container, without JAVA_OPTS.
func (o LaunchOptions) Environment() map[string]string {
	if o.Estimate == "" {
		return nil
	}
	return map[string]string{EstimateEnv: o.Estimate}
}

// LaunchOptionsOf gives the engine's launch options, or none for an engine the CLI did not size.
func LaunchOptionsOf(engine Engine) LaunchOptions {
	if engine.Mode != ModeSized && engine.Mode != ModeExplicit {
		return LaunchOptions{}
	}
	options := LaunchOptions{Estimate: estimateValue(engine.Steps)}
	if engine.Mode == ModeSized {
		options.JavaOpts = directMemoryOption + engine.Value()
	}
	return options
}

func estimateValue(steps []StepLimit) string {
	entries := make([]string, 0, len(steps))
	for _, step := range steps {
		entries = append(entries, fmt.Sprintf("%s=%d", step.ID, step.LimitBytes))
	}
	return strings.Join(entries, ",")
}

// LocalHostKey names the engine that DockerManager starts on the CLI's own host.
const LocalHostKey = "local"

var (
	launchMu      sync.RWMutex
	launchOptions = map[string]LaunchOptions{}
)

// SetLaunchOptions records what each engine gets, for the launch paths that build its container.
// One process runs one run, as SPT_RDMA's environment switch assumes too.
func SetLaunchOptions(plan Plan, hosts []*hostparse.HostInfo) {
	launchMu.Lock()
	defer launchMu.Unlock()
	launchOptions = map[string]LaunchOptions{}
	byHost := make(map[string]*hostparse.HostInfo, len(hosts))
	for _, host := range hosts {
		byHost[host.Host] = host
	}
	for _, engine := range plan.Engines {
		options := LaunchOptionsOf(engine)
		launchOptions[engine.Host] = options
		if host := byHost[engine.Host]; host != nil && host.IsLocal {
			launchOptions[LocalHostKey] = options
		}
	}
}

// ClearLaunchOptions forgets them; tests call it.
func ClearLaunchOptions() {
	launchMu.Lock()
	defer launchMu.Unlock()
	launchOptions = map[string]LaunchOptions{}
}

// LaunchOptionsFor gives the options recorded for a host; none when it was not sized.
func LaunchOptionsFor(host string) LaunchOptions {
	launchMu.RLock()
	defer launchMu.RUnlock()
	return launchOptions[host]
}

// Hosts names the engines the plan sized, for a message.
func (p Plan) Hosts() []string {
	hosts := make([]string, 0, len(p.Engines))
	for _, engine := range p.Engines {
		hosts = append(hosts, engine.Host)
	}
	sort.Strings(hosts)
	return hosts
}
