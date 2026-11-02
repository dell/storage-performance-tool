package poolsizing

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

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
