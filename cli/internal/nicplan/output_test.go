package nicplan

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate nic-plan fixtures from test source")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "..", "..", "..", "test-fixtures", "nic-plan", name))
	if err != nil {
		t.Fatalf("read nic-plan fixture: %v", err)
	}
	return data
}

func parseFixture(t *testing.T, name string) *Output {
	t.Helper()
	out, err := Parse(fixture(t, name))
	if err != nil {
		t.Fatalf("Parse(%s): %v", name, err)
	}
	return out
}

func TestParseReadsThePlanFixture(t *testing.T) {
	out := parseFixture(t, "sc26-auto-rdma.json")
	if out.Engine.Version != "5.17.0" || out.Host != "client-a" {
		t.Fatalf("engine/host = %+v %q", out.Engine, out.Host)
	}
	if len(out.Inventory.Nics) != 11 || len(out.Inventory.RdmaDevices) != 8 {
		t.Fatalf("inventory has %d NICs and %d RDMA devices", len(out.Inventory.Nics), len(out.Inventory.RdmaDevices))
	}
	if out.Plan == nil {
		t.Fatal("plan is missing")
	}
	if got := strings.Join(out.Plan.NicSet, ","); got != "ens2np0,ens10np0,ens33np0,ens101np0" {
		t.Fatalf("nicSet = %s", got)
	}
	if !out.Plan.OK || len(out.Plan.Groups) != 1 || len(out.Plan.Groups[0].Paths) != 4 {
		t.Fatalf("plan = %+v", out.Plan)
	}
}

func TestParseKeepsUnknownValuesNull(t *testing.T) {
	out := parseFixture(t, "sc26-inventory.json")
	if out.Plan != nil {
		t.Fatal("an inventory without --interfaces has no plan")
	}
	for _, device := range out.Inventory.RdmaDevices {
		if device.DcSupported != nil || device.ActiveMTU != nil {
			t.Fatalf("%s: DC and MTU are read only with --rdma, got %v %v", device.Name, device.DcSupported, device.ActiveMTU)
		}
	}
	for _, nic := range out.Inventory.Nics {
		if nic.Name == "lo" && (nic.SpeedMbps != nil || nic.NumaNode != nil) {
			t.Fatalf("lo has no speed or NUMA node, got %v %v", nic.SpeedMbps, nic.NumaNode)
		}
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name, stdout, want string
	}{
		{"garbage", "Exception in thread main", "unreadable spt.nic-plan output"},
		{"another format", `{"format":"spt.other","version":1}`, `unexpected output format "spt.other"`},
		{"newer version", `{"format":"spt.nic-plan","version":2}`, "image reports nic-plan version 2; this CLI reads 1; update the CLI"},
		{"no version", `{"format":"spt.nic-plan"}`, "unexpected nic-plan version 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.stdout))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Parse error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestParseIgnoresSurroundingWhitespace(t *testing.T) {
	if _, err := Parse([]byte("\n  {\"format\":\"spt.nic-plan\",\"version\":1,\"future\":true}\n")); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestCommandMissing(t *testing.T) {
	tests := []struct {
		stderr string
		want   bool
	}{
		{`OCI runtime exec failed: exec failed: unable to start container process: exec: "/opt/spt/spt-nic-plan": stat /opt/spt/spt-nic-plan: no such file or directory: unknown`, true},
		{`exec: "/opt/spt/spt-nic-plan": executable file not found in $PATH`, true},
		{"Error response from daemon: container abc is not running", false},
		{"cat: /sys/class/net/eth0/speed: No such file or directory", false},
		{"/opt/spt/spt-nic-plan: 18: exec: java: not found", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := CommandMissing(tt.stderr); got != tt.want {
			t.Errorf("CommandMissing(%q) = %v, want %v", tt.stderr, got, tt.want)
		}
	}
}
