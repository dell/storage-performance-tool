package verification

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
)

const (
	nicContainer     = "abc123"
	nicInventoryCmd  = "docker exec abc123 /opt/spt/spt-nic-plan"
	nicPolicyRuleCmd = "ip -4 rule show"
	missingNicPlan   = `OCI runtime exec failed: exec failed: unable to start container process: exec: "/opt/spt/spt-nic-plan": stat /opt/spt/spt-nic-plan: no such file or directory: unknown`
)

var nicEndpoints = []string{"192.0.2.101:9000", "192.0.2.102:9000", "192.0.2.103:9000", "192.0.2.104:9000"}

func nicFixture(t *testing.T, name string) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate nic-plan fixtures from test source")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "..", "..", "..", "test-fixtures", "nic-plan", name))
	if err != nil {
		t.Fatalf("read nic-plan fixture: %v", err)
	}
	return string(data)
}

// planWith returns the SC26 plan fixture after edit changes its plan.
func planWith(t *testing.T, edit func(*nicplan.Plan)) string {
	t.Helper()
	out, err := nicplan.Parse([]byte(nicFixture(t, "sc26-auto-rdma.json")))
	if err != nil {
		t.Fatal(err)
	}
	edit(out.Plan)
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func planCommand(endpoints ...string) string {
	command := nicInventoryCmd + " --interfaces auto"
	for _, endpoint := range endpoints {
		command += " --endpoint " + endpoint
	}
	return command
}

func nicVerifier(config Config, commands map[string]MockCommandResponse, hosts ...*hostparse.HostInfo) (*Verifier, *MockCommandExecutor) {
	if len(hosts) == 0 {
		hosts = []*hostparse.HostInfo{{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}}
	}
	mockCmd := &MockCommandExecutor{Commands: commands}
	return NewVerifierWithDeps(hosts, config, mockCmd, &MockNetworkChecker{}, NewMockTimeProvider(time.Now())), mockCmd
}

func planConfig() Config {
	return Config{NetworkMode: constants.HostNetworkMode, Interfaces: []string{"auto"}, Endpoints: nicEndpoints, Threads: 64}
}

// readyResult is a node whose critical checks passed.
func readyResult() *NodeResult {
	return &NodeResult{
		SSHConnectivity:  Check{Passed: true},
		DockerAvailable:  Check{Passed: true},
		ContainerStart:   Check{Passed: true},
		PortsAccessible:  Check{Passed: true},
		ContainerCleanup: Check{Passed: true},
		MetricsEndpoint:  Check{Passed: true},
		ControlEndpoint:  Check{Passed: true},
		containerID:      nicContainer,
	}
}

func executed(mockCmd *MockCommandExecutor, command string) bool {
	for _, execution := range mockCmd.ExecutedCommands {
		if strings.Join(execution.Command, " ") == command {
			return true
		}
	}
	return false
}

func TestListNicsWithoutInterfacesIsInformation(t *testing.T) {
	v, mockCmd := nicVerifier(Config{NetworkMode: constants.HostNetworkMode}, map[string]MockCommandResponse{
		nicInventoryCmd: {Stdout: nicFixture(t, "sc26-inventory.json")},
	})
	result := readyResult()
	v.stepListNics(v.hosts[0], result)

	if result.NICs == nil || result.NICs.Output == nil || result.NICs.Output.Plan != nil {
		t.Fatalf("NICs = %+v", result.NICs)
	}
	if result.NicPlan.Message != "" {
		t.Fatalf("no NIC Plan check without --interfaces, got %+v", result.NicPlan)
	}
	if executed(mockCmd, nicPolicyRuleCmd) {
		t.Fatal("policy rules are read only for a plan")
	}
	if overall := v.assessNode(result); !overall.Passed {
		t.Fatalf("Overall = %+v", overall)
	}
}

func TestListNicsWithRdmaProbes(t *testing.T) {
	v, mockCmd := nicVerifier(Config{NetworkMode: constants.HostNetworkMode, UseRdma: true}, map[string]MockCommandResponse{
		nicInventoryCmd + " --rdma": {Stdout: nicFixture(t, "sc26-inventory.json")},
	})
	listing, check := v.listNics(v.hosts[0], nicContainer)
	if !check.Passed || check.Message != "11 interfaces listed" || listing.Output == nil {
		t.Fatalf("listNics = %+v, %+v", listing, check)
	}
	if !executed(mockCmd, nicInventoryCmd+" --rdma") {
		t.Fatal("--use-rdma passes --rdma")
	}
}

// Docker can put the runtime's diagnostic on stdout and leave stderr empty, as it does for a
// missing executable with exit status 127.
func TestListNicsOlderImage(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		response := MockCommandResponse{Stdout: missingNicPlan, Error: errors.New("exit status 127")}
		if stream == "stderr" {
			response = MockCommandResponse{Stderr: missingNicPlan, Error: errors.New("exit status 126")}
		}
		commands := map[string]MockCommandResponse{nicInventoryCmd: response, planCommand(nicEndpoints...): response}

		t.Run(stream+", listing only", func(t *testing.T) {
			v, _ := nicVerifier(Config{NetworkMode: constants.HostNetworkMode}, commands)
			result := readyResult()
			v.stepListNics(v.hosts[0], result)
			if result.NICs == nil || result.NICs.Note != nicplan.ErrImagePredatesNicPlan.Error() {
				t.Fatalf("NICs = %+v", result.NICs)
			}
			if result.NicPlan.Message != "" {
				t.Fatalf("NicPlan = %+v", result.NicPlan)
			}
			if overall := v.assessNode(result); !overall.Passed {
				t.Fatalf("an older image only lists no NICs, got Overall %+v", overall)
			}
		})

		t.Run(stream+", planning", func(t *testing.T) {
			v, _ := nicVerifier(planConfig(), commands)
			result := readyResult()
			v.stepListNics(v.hosts[0], result)
			if result.NicPlan.Passed || result.NicPlan.Warning || !errors.Is(result.NicPlan.Error, nicplan.ErrImagePredatesNicPlan) {
				t.Fatalf("NicPlan = %+v", result.NicPlan)
			}
			overall := v.assessNode(result)
			if overall.Passed || overall.Message != "NIC plan: "+nicplan.ErrImagePredatesNicPlan.Error() {
				t.Fatalf("Overall = %+v", overall)
			}
		})
	}
}

func TestListNicsCommandFailure(t *testing.T) {
	tests := []struct {
		name     string
		response MockCommandResponse
		want     string
	}{
		{"stderr", MockCommandResponse{Stderr: "\nError: cannot read /sys/class/net\n", Error: errors.New("exit status 1")},
			"spt-nic-plan failed: exit status 1: Error: cannot read /sys/class/net"},
		{"stdout when stderr is empty", MockCommandResponse{Stdout: "Error response from daemon: container abc123 is not running\n", Error: errors.New("exit status 1")},
			"spt-nic-plan failed: exit status 1: Error response from daemon: container abc123 is not running"},
		{"stderr before stdout", MockCommandResponse{Stdout: "partial\n", Stderr: "Error: cannot read /sys/class/net", Error: errors.New("exit status 1")},
			"spt-nic-plan failed: exit status 1: Error: cannot read /sys/class/net"},
		{"another missing file is not an older image", MockCommandResponse{Stdout: "open /sys/class/net/x: no such file or directory", Error: errors.New("exit status 1")},
			"spt-nic-plan failed: exit status 1: open /sys/class/net/x: no such file or directory"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := nicVerifier(Config{NetworkMode: constants.HostNetworkMode}, map[string]MockCommandResponse{nicInventoryCmd: tc.response})
			listing, check := v.listNics(v.hosts[0], nicContainer)
			if check.Passed || listing.Note != tc.want || errors.Is(check.Error, nicplan.ErrImagePredatesNicPlan) {
				t.Fatalf("listNics = %+v, %+v", listing, check)
			}
		})
	}
}

// A command that succeeds is read as its output, whatever its text holds.
func TestListNicsSucceedsWhateverItsOutputSays(t *testing.T) {
	v, _ := nicVerifier(Config{NetworkMode: constants.HostNetworkMode}, map[string]MockCommandResponse{
		nicInventoryCmd: {Stdout: missingNicPlan},
	})
	listing, check := v.listNics(v.hosts[0], nicContainer)
	if check.Passed || errors.Is(check.Error, nicplan.ErrImagePredatesNicPlan) || !strings.HasPrefix(listing.Note, "unreadable spt.nic-plan output") {
		t.Fatalf("listNics = %+v, %+v", listing, check)
	}
}

func TestListNicsWithoutContainer(t *testing.T) {
	v, mockCmd := nicVerifier(planConfig(), nil)
	listing, check := v.listNics(v.hosts[0], "")
	if check.Passed || listing.Note != "No container to list NICs" || len(mockCmd.ExecutedCommands) != 0 {
		t.Fatalf("listNics = %+v, %+v", listing, check)
	}
}

func TestPlanPasses(t *testing.T) {
	v, mockCmd := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: nicFixture(t, "sc26-auto-rdma.json")},
		nicPolicyRuleCmd:             {Stdout: "0:\tfrom all lookup local\n100:\tfrom 192.0.2.11 lookup 101\n32766:\tfrom all lookup main\n32767:\tfrom all lookup default\n"},
	})
	result := readyResult()
	v.stepListNics(v.hosts[0], result)

	if !result.NicPlan.Passed || result.NicPlan.Message != "auto: 4 NICs, 4 Paths" {
		t.Fatalf("NicPlan = %+v", result.NicPlan)
	}
	if !executed(mockCmd, nicPolicyRuleCmd) {
		t.Fatal("a plan reads the policy rules")
	}
	options := result.NICs.Options
	if strings.Join(options.Endpoints, ",") != strings.Join(nicEndpoints, ",") || options.Threads != 64 || options.SliceCount != 0 {
		t.Fatalf("Options = %+v", options)
	}
	if len(options.PolicyRules) != 1 || options.PolicyRules[0] != "100: from 192.0.2.11 lookup 101" {
		t.Fatalf("PolicyRules = %v", options.PolicyRules)
	}
	if overall := v.assessNode(result); !overall.Passed {
		t.Fatalf("Overall = %+v", overall)
	}
}

func TestPlanPolicyRulesUnread(t *testing.T) {
	v, _ := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: nicFixture(t, "sc26-auto-rdma.json")},
	})
	listing, check := v.listNics(v.hosts[0], nicContainer)
	if !check.Passed || !listing.Options.PolicyRulesUnread {
		t.Fatalf("listNics = %+v, %+v", listing.Options, check)
	}
}

func TestPlanFailureFailsTheHost(t *testing.T) {
	failed := planWith(t, func(plan *nicplan.Plan) {
		plan.OK = false
		plan.Failures = []nicplan.Finding{{Code: "endpoint_unserved", Text: "no chosen NIC serves 192.0.2.104:9000"}}
	})
	v, _ := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: failed},
		nicPolicyRuleCmd:             {},
	})
	result := readyResult()
	v.stepListNics(v.hosts[0], result)

	if result.NicPlan.Passed || result.NicPlan.Warning {
		t.Fatalf("NicPlan = %+v", result.NicPlan)
	}
	overall := v.assessNode(result)
	if overall.Passed || overall.Message != "NIC plan: 1 failure: no chosen NIC serves 192.0.2.104:9000" {
		t.Fatalf("Overall = %+v", overall)
	}
}

func TestPlanNotOKFailsTheHost(t *testing.T) {
	notOK := planWith(t, func(plan *nicplan.Plan) { plan.OK = false })
	v, _ := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: notOK},
		nicPolicyRuleCmd:             {},
	})
	_, check := v.listNics(v.hosts[0], nicContainer)
	if check.Passed || check.Warning {
		t.Fatalf("a plan that is not ok fails, got %+v", check)
	}
}

func TestPlanWarningWarns(t *testing.T) {
	warned := planWith(t, func(plan *nicplan.Plan) {
		plan.Warnings = []nicplan.Finding{{Code: "uneven_load", Text: "group 1 loads its NICs unevenly"}}
	})
	v, _ := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: warned},
		nicPolicyRuleCmd:             {},
	})
	result := readyResult()
	v.stepListNics(v.hosts[0], result)

	if result.NicPlan.Passed || !result.NicPlan.Warning || result.NicPlan.Message != "auto: 4 NICs, 4 Paths, 1 warning" {
		t.Fatalf("NicPlan = %+v", result.NicPlan)
	}
	if overall := v.assessNode(result); !overall.Passed {
		t.Fatalf("a warning does not fail the host, got %+v", overall)
	}
}

func TestPlanWithoutPlanFails(t *testing.T) {
	v, _ := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: nicFixture(t, "sc26-inventory.json")},
	})
	_, check := v.listNics(v.hosts[0], nicContainer)
	if check.Passed || check.Message != "spt-nic-plan returned no plan" {
		t.Fatalf("check = %+v", check)
	}
}

func TestPlanSlicesEndpointsInHostOrder(t *testing.T) {
	entry := &hostparse.HostInfo{Host: "client-a", Original: "client-a"}
	worker := &hostparse.HostInfo{Host: "client-b", Original: "client-b"}
	config := planConfig()
	config.SliceEndpoints = true
	plan := nicFixture(t, "sc26-auto-rdma.json")
	v, mockCmd := nicVerifier(config, map[string]MockCommandResponse{
		planCommand(nicEndpoints[:2]...): {Stdout: plan},
		planCommand(nicEndpoints[2:]...): {Stdout: plan},
		nicPolicyRuleCmd:                 {},
	}, entry, worker)

	for i, host := range []*hostparse.HostInfo{entry, worker} {
		listing, check := v.listNics(host, nicContainer)
		if listing.Output == nil {
			t.Fatalf("host %d: %+v", i, check)
		}
		if listing.Options.Slice != i || listing.Options.SliceCount != 2 {
			t.Fatalf("host %d: Options = %+v", i, listing.Options)
		}
	}
	if !executed(mockCmd, planCommand(nicEndpoints[:2]...)) || !executed(mockCmd, planCommand(nicEndpoints[2:]...)) {
		t.Fatalf("each host plans its slice; executed %v", mockCmd.ExecutedCommands)
	}
}

func TestPlanWithoutSlicingGivesEveryHostAllEndpoints(t *testing.T) {
	entry := &hostparse.HostInfo{Host: "client-a", Original: "client-a"}
	worker := &hostparse.HostInfo{Host: "client-b", Original: "client-b"}
	v, _ := nicVerifier(planConfig(), map[string]MockCommandResponse{
		planCommand(nicEndpoints...): {Stdout: nicFixture(t, "sc26-auto-rdma.json")},
		nicPolicyRuleCmd:             {},
	}, entry, worker)
	for _, host := range []*hostparse.HostInfo{entry, worker} {
		if _, check := v.listNics(host, nicContainer); !check.Passed {
			t.Fatalf("%s: %+v", host.Host, check)
		}
	}
}

func TestBridgeNoticeReachesTheListing(t *testing.T) {
	v, _ := nicVerifier(Config{NetworkMode: constants.BridgeNetworkMode}, map[string]MockCommandResponse{
		nicInventoryCmd: {Stdout: nicFixture(t, "sc26-inventory.json")},
	})
	listing, _ := v.listNics(v.hosts[0], nicContainer)
	if !listing.Options.Bridge {
		t.Fatalf("Options = %+v", listing.Options)
	}
}

func TestPrintNodeResultShowsNics(t *testing.T) {
	out, err := nicplan.Parse([]byte(nicFixture(t, "sc26-auto-rdma.json")))
	if err != nil {
		t.Fatal(err)
	}
	result := readyResult()
	result.Host = "client-a"
	result.NicPlan = Check{Passed: true, Message: "auto: 4 NICs, 4 Paths"}
	result.NICs = &NicListing{Output: out, Options: nicplan.RenderOptions{Endpoints: nicEndpoints, Threads: 64}}
	result.Overall = Check{Passed: true, Message: "READY"}
	output := printReport(t, result)
	for _, want := range []string{"NIC Plan", "auto: 4 NICs, 4 Paths", "NICs (engine 5.17.0):", "NIC plan (--interfaces auto, RDMA): ok", "Permits at --threads 64: 16 per NIC"} {
		if !strings.Contains(output, want) {
			t.Errorf("missing %q in:\n%s", want, output)
		}
	}
}

func TestPrintNodeResultShowsNicNote(t *testing.T) {
	result := readyResult()
	result.Host = "client-a"
	result.NICs = &NicListing{Note: nicplan.ErrImagePredatesNicPlan.Error()}
	result.Overall = Check{Passed: true, Message: "READY"}
	output := printReport(t, result)
	if !strings.Contains(output, "NICs: not listed: this image predates NIC inventory") || strings.Contains(output, "NIC Plan") {
		t.Errorf("unexpected report:\n%s", output)
	}
}

func printReport(t *testing.T, result *NodeResult) string {
	t.Helper()
	report := &Report{Results: map[string]*NodeResult{result.Host: result}, Duration: time.Second, Config: Config{MinHosts: 1}}
	report.assess()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	report.Print()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	return buf.String()
}

func TestRdmaReadinessCoversEveryDevice(t *testing.T) {
	commands := map[string]MockCommandResponse{
		"docker exec abc123 ls /sys/class/infiniband/": {Stdout: "mlx5_0\nmlx5_1\nbnxt_re0\n"},
	}
	for _, device := range []string{"mlx5_0", "mlx5_1", "bnxt_re0"} {
		commands["docker exec abc123 cat /sys/class/infiniband/"+device+"/ports/1/state"] = MockCommandResponse{Stdout: "4: ACTIVE\n"}
		commands["docker exec abc123 cat /sys/class/infiniband/"+device+"/ports/1/link_layer"] = MockCommandResponse{Stdout: "Ethernet\n"}
	}
	v, _ := nicVerifier(Config{UseRdma: true}, commands)

	check := v.checkRdmaReadiness(v.hosts[0], nicContainer)
	if !check.Passed || check.Message != "Port 1 ACTIVE on all 3 devices: mlx5_0 (Ethernet), mlx5_1 (Ethernet), bnxt_re0 (Ethernet)" {
		t.Fatalf("check = %+v", check)
	}

	commands["docker exec abc123 cat /sys/class/infiniband/mlx5_1/ports/1/state"] = MockCommandResponse{Stdout: "1: DOWN\n"}
	delete(commands, "docker exec abc123 cat /sys/class/infiniband/bnxt_re0/ports/1/state")
	check = v.checkRdmaReadiness(v.hosts[0], nicContainer)
	if check.Passed || !check.Warning ||
		check.Message != "RDMA port 1 not active on 2 of 3 devices: mlx5_1 (1: DOWN), bnxt_re0 (port state unreadable)" {
		t.Fatalf("check = %+v", check)
	}
}
