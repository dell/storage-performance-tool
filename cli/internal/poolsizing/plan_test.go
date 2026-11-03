package poolsizing

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
)

// fakeHosts answers each docker run as the image would on that host.
type fakeHosts struct {
	command.CommandExecutor
	mu       sync.Mutex
	commands map[string][][]string
	reply    func(host string, args []string) (string, error)
}

func (f *fakeHosts) ExecuteCommand(_ context.Context, host *hostparse.HostInfo, cmd []string) (string, string, error) {
	f.mu.Lock()
	if f.commands == nil {
		f.commands = map[string][][]string{}
	}
	f.commands[host.Host] = append(f.commands[host.Host], cmd)
	f.mu.Unlock()
	out, err := f.reply(host.Host, cmd)
	return out, "", err
}

func nicOutput(t *testing.T, value nicplan.Output) string {
	t.Helper()
	value.Format, value.Version = nicplan.Format, nicplan.Version
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestPlanAsksEachHostsImageForItsPlanAndCapacity(t *testing.T) {
	hosts := []*hostparse.HostInfo{{Host: "entry", IsLocal: true}, {Host: "wrk1"}, {Host: "wrk2"}}
	params := rdmaParams("write")
	params.Interfaces = []string{"auto"}
	params.HostInterfaces = []nicplan.HostInterfaces{{Host: "wrk1", Value: "ens9", Entries: []string{"ens9"}}, {Host: "wrk2", Value: "none"}}
	params.Endpoints = []string{"http://192.0.2.1:9000", "http://192.0.2.2:9000"}
	params.SliceEndpoints = true
	fake := &fakeHosts{}
	fake.reply = func(host string, cmd []string) (string, error) {
		if strings.Contains(strings.Join(cmd, " "), "--capacity") {
			return nicOutput(t, nicplan.Output{Capacity: capacityOf(1024*gib, "MemTotal")}), nil
		}
		return nicOutput(t, nicplan.Output{
			Plan:       &nicplan.Plan{OK: true, NicSet: []string{"ens1", "ens2"}},
			PoolDemand: &nicplan.PoolDemand{Steps: []nicplan.PoolStep{step("create", 192*gib)}},
		}), nil
	}
	plan, err := BuildPlan(context.Background(), Input{
		Params: params, Steps: stepsOf("create"), Hosts: hosts, Image: "spt:test", Executor: fake, Shortfall: ShortfallFail,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Engines[0].Mode != ModeSized || plan.Engines[1].Mode != ModeSized || plan.Engines[2].Mode != ModeNoNicSet {
		t.Fatalf("%+v", plan.Engines)
	}
	if len(fake.commands["wrk2"]) != 0 {
		t.Fatalf("a none host is not asked: %v", fake.commands["wrk2"])
	}
	worker := strings.Join(fake.commands["wrk1"][0], " ")
	if strings.Contains(worker, "--cgroupns") {
		t.Errorf("only the capacity probe needs the host's cgroup namespace: %q", worker)
	}
	for _, want := range []string{"docker run --rm --network host", "--entrypoint /opt/spt/spt-nic-plan",
		"--device /dev/infiniband/ --cap-add IPC_LOCK --ulimit memlock=-1:-1", "spt:test", "--rdma", "--interfaces ens9", "--pool-request "} {
		if !strings.Contains(worker, want) {
			t.Errorf("%q is missing %q", worker, want)
		}
	}
	capacity := strings.Join(fake.commands["wrk1"][1], " ")
	if strings.Contains(capacity, "--device") || !strings.Contains(capacity, "--cgroupns=host") {
		t.Errorf("the capacity probe takes no RDMA device and needs the host's cgroup namespace: %q", capacity)
	}
	// The entry node holds the first slice, and this worker the second.
	if !strings.Contains(strings.Join(fake.commands["entry"][0], " "), "--endpoint 192.0.2.1:9000") {
		t.Errorf("%v", fake.commands["entry"][0])
	}
	if !strings.Contains(worker, "--endpoint 192.0.2.2:9000") {
		t.Errorf("%q", worker)
	}
}

func TestPlanLeavesAHostItCannotAskToItsEngine(t *testing.T) {
	fake := &fakeHosts{reply: func(string, []string) (string, error) { return "", errors.New("ssh failed") }}
	plan, err := BuildPlan(context.Background(), Input{
		Params: rdmaParams("write"), Steps: stepsOf("create"), Hosts: []*hostparse.HostInfo{{Host: "wrk1"}}, Executor: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Engines[0].Mode != ModeUnprobed || plan.Failure() != nil {
		t.Fatalf("%+v", plan.Engines[0])
	}
}

// The command runs through a remote shell, so an endpoint it could expand never reaches one.
func TestAnEndpointWithShellCharactersIsNeverSentToAHost(t *testing.T) {
	for _, endpoint := range []string{"http://$(uname):9000", "http://`uname`:9000", "http://a;b:9000"} {
		fake := &fakeHosts{reply: func(string, []string) (string, error) { return "", errors.New("unexpected") }}
		params := rdmaParams("write")
		params.Endpoints = []string{endpoint}
		plan, err := BuildPlan(context.Background(), Input{
			Params: params, Steps: stepsOf("create"), Hosts: []*hostparse.HostInfo{{Host: "wrk1"}}, Executor: fake,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(fake.commands) != 0 {
			t.Fatalf("%s: %v", endpoint, fake.commands)
		}
		if plan.Engines[0].Mode != ModeUnprobed {
			t.Fatalf("%s: %+v", endpoint, plan.Engines[0])
		}
	}
}

func TestAWorkerWithoutAnRdmaDeviceStillShowsItsCapacity(t *testing.T) {
	fake := &fakeHosts{}
	fake.reply = func(_ string, cmd []string) (string, error) {
		if strings.Contains(strings.Join(cmd, " "), "--capacity") {
			return nicOutput(t, nicplan.Output{Capacity: &nicplan.Capacity{Bytes: 32 * gib, Source: "cgroup", Cgroup: "/lxc/1"}}), nil
		}
		return "", errors.New("docker: error gathering device information: no such file or directory")
	}
	plan, err := BuildPlan(context.Background(), Input{
		Params: rdmaParams("write"), Steps: stepsOf("create"), Hosts: []*hostparse.HostInfo{{Host: "wrk1"}}, Executor: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := plan.Engines[0]
	if engine.Capacity == nil || engine.Capacity.Source != "cgroup" || engine.Capacity.Cgroup != "/lxc/1" {
		t.Fatalf("%+v", engine)
	}
}

// A Proxmox LXC's limit is above its cgroup namespace: a container on it reports the machine's memory,
// and only the LXC's own /proc/meminfo, through lxcfs, shows the limit.
func TestAnLXCsOwnMemInfoBoundsWhatAContainerOnItReports(t *testing.T) {
	for _, tc := range []struct {
		name    string
		probe   *nicplan.Capacity
		host    string
		want    int64
		source  string
		noteHas string
	}{
		{"an LXC below the machine", capacityOf(264*gib, "MemTotal"), "MemTotal:    8388608 kB\n", 8 * gib, "MemTotal", "reports less"},
		{"a cgroup below the host stays", &nicplan.Capacity{Bytes: 2 * gib, Source: "cgroup", Cgroup: "/docker/id", CgroupLimitBytes: 2 * gib}, "MemTotal:    8388608 kB\n", 2 * gib, "cgroup", ""},
		{"no answer from the host", capacityOf(264*gib, "MemTotal"), "not a meminfo line", 264 * gib, "MemTotal", ""},
		{"no answer from the image", nil, "MemTotal:    8388608 kB\n", 8 * gib, "MemTotal", "no cgroup limit was checked"},
		{"a login banner before the line", capacityOf(264*gib, "MemTotal"), "Welcome to wrk1\nMemTotal:    8388608 kB\n", 8 * gib, "MemTotal", "reports less"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHosts{}
			fake.reply = func(_ string, cmd []string) (string, error) {
				text := strings.Join(cmd, " ")
				switch {
				case strings.Contains(text, "/proc/meminfo"):
					return tc.host, nil
				case strings.Contains(text, "--capacity") && tc.probe != nil:
					return nicOutput(t, nicplan.Output{Capacity: tc.probe}), nil
				}
				return "", errors.New("no RDMA device")
			}
			plan, err := BuildPlan(context.Background(), Input{
				Params: rdmaParams("write"), Steps: stepsOf("create"), Hosts: []*hostparse.HostInfo{{Host: "wrk1"}}, Executor: fake,
			})
			if err != nil {
				t.Fatal(err)
			}
			capacity := plan.Engines[0].Capacity
			if capacity == nil || capacity.MemoryBytes != tc.want || capacity.Source != tc.source || !strings.Contains(capacity.Note, tc.noteHas) {
				t.Fatalf("%+v", capacity)
			}
		})
	}
}

// A command can outlive its context while a proxy holds its output open.
func TestAHostThatHangsOnItsMemTotalDoesNotHoldUpTheRun(t *testing.T) {
	previous := hostMemoryTimeout
	hostMemoryTimeout = 50 * time.Millisecond
	defer func() { hostMemoryTimeout = previous }()
	release := make(chan struct{})
	defer close(release)
	fake := &fakeHosts{}
	fake.reply = func(_ string, cmd []string) (string, error) {
		text := strings.Join(cmd, " ")
		if strings.Contains(text, "/proc/meminfo") {
			<-release
			return "", nil
		}
		if strings.Contains(text, "--capacity") {
			return nicOutput(t, nicplan.Output{Capacity: capacityOf(32*gib, "MemTotal")}), nil
		}
		return "", errors.New("no RDMA device")
	}
	done := make(chan Plan, 1)
	go func() {
		plan, err := BuildPlan(context.Background(), Input{
			Params: rdmaParams("write"), Steps: stepsOf("create"), Hosts: []*hostparse.HostInfo{{Host: "wrk1"}}, Executor: fake,
		})
		if err != nil {
			t.Error(err)
		}
		done <- plan
	}()
	select {
	case plan := <-done:
		if capacity := plan.Engines[0].Capacity; capacity == nil || capacity.MemoryBytes != 32*gib {
			t.Fatalf("the image's figure stands: %+v", capacity)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run waited for the hung command")
	}
}

func TestAttachedWorkersAreNotSizedButTheEntryNodeIs(t *testing.T) {
	fake := &fakeHosts{}
	fake.reply = func(_ string, cmd []string) (string, error) {
		if strings.Contains(strings.Join(cmd, " "), "--capacity") {
			return nicOutput(t, nicplan.Output{Capacity: capacityOf(1024*gib, "MemTotal")}), nil
		}
		return nicOutput(t, nicplan.Output{Plan: &nicplan.Plan{OK: true}, PoolDemand: &nicplan.PoolDemand{Steps: []nicplan.PoolStep{step("create", 192*gib)}}}), nil
	}
	plan, err := BuildPlan(context.Background(), Input{
		Params: rdmaParams("write"), Steps: stepsOf("create"), Hosts: []*hostparse.HostInfo{{Host: "entry", IsLocal: true}, {Host: "wrk1"}},
		Attached: true, Executor: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Engines[0].Mode != ModeSized || plan.Engines[1].Mode != ModeAttached {
		t.Fatalf("%+v", plan.Engines)
	}
	if len(fake.commands) != 1 || len(fake.commands["entry"]) == 0 {
		t.Fatalf("an attached worker is not asked, as it may not be reachable: %v", fake.commands)
	}
	var text strings.Builder
	plan.Render(&text)
	if !strings.Contains(text.String(), "wrk1 is an attached worker") || strings.Contains(text.String(), "No estimate") {
		t.Fatal(text.String())
	}
}

func TestLaunchOptionsAreKeptPerHostAndForTheLocalManager(t *testing.T) {
	t.Cleanup(ClearLaunchOptions)
	sized := Decide(DecideInput{Host: "127.0.0.1", Output: output(capacityOf(1024*gib, "MemTotal"), step("create", 192*gib))})
	worker := Decide(DecideInput{Host: "wrk1", Output: output(capacityOf(1024*gib, "MemTotal"), step("create", 128*gib))})
	SetLaunchOptions(Plan{Engines: []Engine{sized, worker}}, []*hostparse.HostInfo{{Host: "127.0.0.1", IsLocal: true}, {Host: "wrk1"}})
	if LaunchOptionsFor(LocalHostKey).JavaOpts != "-XX:MaxDirectMemorySize=192g" {
		t.Fatalf("%+v", LaunchOptionsFor(LocalHostKey))
	}
	if LaunchOptionsFor("wrk1").JavaOpts != "-XX:MaxDirectMemorySize=128g" {
		t.Fatalf("%+v", LaunchOptionsFor("wrk1"))
	}
	if LaunchOptionsFor("elsewhere").JavaOpts != "" {
		t.Fatal("an unsized host gets nothing")
	}
}
