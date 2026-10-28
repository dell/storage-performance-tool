package nicplan

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

var sc26Endpoints = []string{"192.0.2.101:9000", "192.0.2.102:9000", "192.0.2.103:9000", "192.0.2.104:9000"}

var columnGap = regexp.MustCompile(`(\S) {2,}`)

// render returns the output with the gaps between columns collapsed to one space, so
// assertions keep the indentation but not the column widths.
func render(out *Output, opts RenderOptions) string {
	var buf bytes.Buffer
	Render(&buf, out, opts)
	return columnGap.ReplaceAllString(buf.String(), "$1 ")
}

func assertLines(t *testing.T, text string, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing line %q in:\n%s", line, text)
		}
	}
}

func TestRenderInventory(t *testing.T) {
	text := render(parseFixture(t, "sc26-inventory.json"), RenderOptions{})
	assertLines(t, text,
		"  NICs (engine 5.17.0):",
		"    INTERFACE STATE ADDRESS SPEED MTU NUMA RDMA PORT GID DC ROCE MTU SHARES PORT WITH",
		"    docker0 down 172.17.0.1/16 ? 1500 ? - - - - - -",
		"    eno8303 up 198.51.100.10/24 1G 1500 0 - - - - - -",
		"    ens2np0 up 192.0.2.11/24 400G 9000 0 mlx5_0 1 3 ? ? -",
		"    ens6f1np1 up 192.0.2.24/24 200G 9000 1 bnxt_re3 1 3 ? ? -",
		"    DC support and the RoCE MTU are read with --use-rdma.",
	)
	if strings.Contains(text, "NIC plan") || strings.Contains(text, "Bridge network") {
		t.Errorf("an inventory alone has no plan and no bridge notice:\n%s", text)
	}
}

func TestRenderBridgeNotice(t *testing.T) {
	text := render(parseFixture(t, "sc26-inventory.json"), RenderOptions{Bridge: true})
	assertLines(t, text, "    Bridge network: verify sees only the container's interfaces, not the host's.")
}

func TestRenderPlan(t *testing.T) {
	text := render(parseFixture(t, "sc26-auto-rdma.json"), RenderOptions{Endpoints: sc26Endpoints, Threads: 64})
	assertLines(t, text,
		"    ens2np0 up 192.0.2.11/24 400G 9000 0 mlx5_0 1 3 yes 4096 -",
		"    ens5f0np0 up 192.0.2.21/24 200G 9000 0 bnxt_re0 1 3 no 4096 -",
		"  NIC plan (--interfaces auto, RDMA): ok",
		"    Endpoints: 192.0.2.101:9000, 192.0.2.102:9000, 192.0.2.103:9000, 192.0.2.104:9000",
		"    NIC set: ens2np0, ens10np0, ens33np0, ens101np0",
		"    Permits at --threads 64: 16 per NIC",
		"    docker0 not used: virtual",
		"    eno8303 not used: no endpoint on its subnet",
		"    ens2np0 used from 192.0.2.11 (GID 3): 1 Path, 16 permits; serves all 4 endpoints",
		"    ens5f0np0 not used: RDMA device without DC support",
		"    Group 1: ens2np0, ens10np0, ens33np0, ens101np0; 4 endpoints, 4 Paths",
		"      ens2np0 → 192.0.2.101:9000",
		"      ens101np0 → 192.0.2.104:9000",
	)
	if strings.Contains(text, "--use-rdma") || strings.Contains(text, "Policy routing") {
		t.Errorf("unexpected footnote:\n%s", text)
	}
}

func TestRenderSlicedEndpoints(t *testing.T) {
	out := parseFixture(t, "sc26-auto-rdma.json")
	assertLines(t, render(out, RenderOptions{Endpoints: sc26Endpoints[:2], Slice: 0, SliceCount: 2, Threads: 8}),
		"    Endpoints (slice 1 of 2, entry node): 192.0.2.101:9000, 192.0.2.102:9000",
		"    Permits at --threads 8: 2 per NIC")
	assertLines(t, render(out, RenderOptions{Endpoints: sc26Endpoints[2:], Slice: 1, SliceCount: 2, Threads: 10}),
		"    Endpoints (slice 2 of 2, worker): 192.0.2.103:9000, 192.0.2.104:9000",
		"    Permits at --threads 10: 2 or 3 per NIC, the first 2 NICs taking one more",
		"    ens2np0 used from 192.0.2.11 (GID 3): 1 Path, 3 permits; serves all 4 endpoints",
		"    ens33np0 used from 192.0.2.13 (GID 3): 1 Path, 2 permits; serves all 4 endpoints")
}

func TestRenderSharedPermits(t *testing.T) {
	out := parseFixture(t, "sc26-auto-rdma.json")
	text := render(out, RenderOptions{Endpoints: sc26Endpoints, Threads: 2})
	assertLines(t, text,
		"    Permits at --threads 2: 2 permits shared by 4 NICs in rotation, fewer than one per NIC",
		"    ens2np0 used from 192.0.2.11 (GID 3): 1 Path; serves all 4 endpoints")
	assertLines(t, render(out, RenderOptions{Endpoints: sc26Endpoints, Threads: 0}),
		"    Permits at --threads 0: unlimited, shared by the NICs in rotation")
}

func TestRenderFindingsAndPolicyRules(t *testing.T) {
	out := &Output{
		Engine: Engine{Version: "5.17.0"},
		Inventory: Inventory{Nics: []Nic{
			{Name: "ens1f0", Up: true, Addresses: []string{"192.0.2.11/24"}, Port: "0000:17:00.0/0"},
			{Name: "ens1f0v1", Up: true, Addresses: []string{"192.0.2.12/24"}, Port: "0000:17:00.0/0"},
			{Name: "ens2", Up: true, Addresses: []string{"203.0.113.5/24"}},
		}},
		Plan: &Plan{
			Interfaces: []string{"ens1f0", "ens1f0v1", "ens2"},
			Mode:       "list",
			NicSet:     []string{"ens1f0", "ens2"},
			Nics: []PlannedNic{
				{Name: "ens1f0", Used: true, Address: "192.0.2.11", Serves: []string{"192.0.2.101:9000"}, Paths: 1},
				{Name: "ens1f0v1", Reason: &Reason{Code: "shares_port", Text: "shares a physical port", With: "ens1f0"}},
				{Name: "ens2", Used: true},
			},
			Endpoints: []PlannedEndpoint{{Endpoint: "192.0.2.101:9000"}, {Endpoint: "192.0.2.102:9000"}},
			Groups: []Group{{Nics: []string{"ens1f0"}, Endpoints: []string{"192.0.2.101:9000"},
				Paths: []Path{{Nic: "ens1f0", Endpoint: "192.0.2.101:9000"}}}},
			Failures: []Finding{
				{Code: "nic_serves_no_endpoint", Text: "ens2 serves no endpoint", Nic: "ens2", Fix: "remove ens2 from --interfaces"},
				{Code: "endpoint_unserved", Text: "no chosen NIC serves 192.0.2.102:9000"},
			},
			Warnings: []Finding{{Code: "future_code"}},
		},
	}
	text := render(out, RenderOptions{Endpoints: []string{"192.0.2.101:9000", "192.0.2.102:9000"}, Threads: 4,
		PolicyRules: []string{"100: from 192.0.2.11 lookup 101"}})
	assertLines(t, text,
		"    ens1f0 up 192.0.2.11/24 ? ? ? - - - - - ens1f0v1",
		"    ens1f0v1 up 192.0.2.12/24 ? ? ? - - - - - ens1f0",
		"    ens2 up 203.0.113.5/24 ? ? ? - - - - - -",
		"  NIC plan (--interfaces ens1f0,ens1f0v1,ens2): 2 failures",
		"    ens1f0 used from 192.0.2.11: 1 Path, 2 permits; serves 192.0.2.101:9000",
		"    ens1f0v1 not used: shares a physical port with ens1f0",
		"    ens2 used: 0 Paths, 2 permits",
		"    Group 1: ens1f0; 1 endpoint, 1 Path",
		"    ✗ ens2 serves no endpoint (fix: remove ens2 from --interfaces)",
		"    ✗ no chosen NIC serves 192.0.2.102:9000",
		"    ⚠ future_code",
		"    Policy routing: 1 rule besides the defaults. NIC choice follows connected subnets and the main table only:",
		"      100: from 192.0.2.11 lookup 101",
	)
	if strings.Contains(text, "--use-rdma") {
		t.Errorf("no RDMA device needs the probe footnote:\n%s", text)
	}
	if got := Summary(out.Plan); got != "2 failures: ens2 serves no endpoint (fix: remove ens2 from --interfaces)" {
		t.Errorf("Summary = %q", got)
	}
}

func TestRenderPolicyRulesUnread(t *testing.T) {
	text := render(parseFixture(t, "sc26-auto-rdma.json"), RenderOptions{Endpoints: sc26Endpoints, Threads: 4, PolicyRulesUnread: true})
	assertLines(t, text, "    Policy routing: not checked; `ip -4 rule show` failed on the host.")
}

func TestRenderNoInterfaces(t *testing.T) {
	text := render(&Output{}, RenderOptions{})
	assertLines(t, text, "  NICs (engine unknown version):", "    (no interfaces)")
}

func TestSummary(t *testing.T) {
	plan := parseFixture(t, "sc26-auto-rdma.json").Plan
	if got := Summary(plan); got != "auto: 4 NICs, 4 Paths" {
		t.Errorf("Summary = %q", got)
	}
	plan.Warnings = []Finding{{Code: "uneven_load", Text: "uneven load"}, {Code: "x"}}
	if got := Summary(plan); got != "auto: 4 NICs, 4 Paths, 2 warnings" {
		t.Errorf("Summary with warnings = %q", got)
	}
}

func TestSpeed(t *testing.T) {
	for mbps, want := range map[int]string{400000: "400G", 2500: "2.5G", 1000: "1G", 100: "100M"} {
		if got := speed(&mbps); got != want {
			t.Errorf("speed(%d) = %s, want %s", mbps, got, want)
		}
	}
}

// The planner can choose any address of an interface; the plan, not the inventory, names the
// chosen source address and its GID.
func TestRenderSelectedSourceOnMultiAddressNic(t *testing.T) {
	speed, mtu, numa, active, gid := 400000, 9000, 0, 4096, 7
	dc := true
	endpoint := "192.0.2.101:9000"
	out := &Output{
		Engine: Engine{Version: "5.17.0"},
		Inventory: Inventory{
			Nics: []Nic{{Name: "ens2np0", Up: true, Addresses: []string{"192.0.2.11/24", "192.0.2.12/24"},
				SpeedMbps: &speed, MTU: &mtu, NumaNode: &numa, RdmaDevice: "mlx5_0",
				Gids: []Gid{
					{Device: "mlx5_0", Port: 1, Index: 2, Address: "192.0.2.11"},
					{Device: "mlx5_0", Port: 1, Index: 3, Address: "192.0.2.11", RoceV2: true},
					{Device: "mlx5_0", Port: 1, Index: 6, Address: "192.0.2.12"},
					{Device: "mlx5_0", Port: 1, Index: 7, Address: "192.0.2.12", RoceV2: true},
				}}},
			RdmaDevices: []RdmaDevice{{Name: "mlx5_0", DcSupported: &dc, ActiveMTU: &active}},
		},
		Plan: &Plan{
			Interfaces: []string{"192.0.2.12"}, Mode: "list", Rdma: true, OK: true, NicSet: []string{"ens2np0"},
			Nics: []PlannedNic{{Name: "ens2np0", Used: true, Address: "192.0.2.12", GidIndex: &gid,
				Serves: []string{endpoint}, Paths: 1}},
			Endpoints: []PlannedEndpoint{{Endpoint: endpoint, Address: "192.0.2.101"}},
			Groups:    []Group{{Nics: []string{"ens2np0"}, Endpoints: []string{endpoint}, Paths: []Path{{Nic: "ens2np0", Endpoint: endpoint}}}},
		},
	}
	assertLines(t, render(out, RenderOptions{Endpoints: []string{endpoint}, Threads: 8}),
		"    ens2np0 up 192.0.2.11/24,192.0.2.12/24 400G 9000 0 mlx5_0 1 3,7 yes 4096 -",
		"  NIC plan (--interfaces 192.0.2.12, RDMA): ok",
		"    ens2np0 used from 192.0.2.12 (GID 7): 1 Path, 8 permits; serves 192.0.2.101:9000",
	)

	out.Plan = nil
	text := render(out, RenderOptions{})
	assertLines(t, text, "    ens2np0 up 192.0.2.11/24,192.0.2.12/24 400G 9000 0 mlx5_0 1 3,7 yes 4096 -")
	if strings.Contains(text, "used") {
		t.Errorf("an inventory alone chooses no source:\n%s", text)
	}
}

func TestRoceV2Gids(t *testing.T) {
	nic := Nic{Addresses: []string{"192.0.2.11/24", "192.0.2.12/24"}, Gids: []Gid{
		{Port: 2, Index: 5, Address: "192.0.2.11", RoceV2: true},
		{Port: 1, Index: 2, Address: "192.0.2.11", RoceV2: false},
		{Port: 1, Index: 3, Address: "192.0.2.11", RoceV2: true},
		{Port: 1, Index: 7, Address: "192.0.2.99", RoceV2: true},
	}}
	tests := []struct {
		name           string
		gids           []Gid
		ports, indexes string
	}{
		{"port 1 preferred, an address without one shown as none", nic.Gids, "1", "3,-"},
		{"another port without port 1", nic.Gids[:2], "2", "5,-"},
		{"a RoCE v1 GID is not a RoCE v2 GID", nic.Gids[1:2], "", ""},
		{"an address of another interface is not this one's", nic.Gids[3:], "", ""},
		{"each address its own GID", []Gid{
			{Port: 1, Index: 3, Address: "192.0.2.11", RoceV2: true},
			{Port: 1, Index: 7, Address: "192.0.2.12", RoceV2: true},
		}, "1", "3,7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nic.Gids = tc.gids
			ports, indexes := roceV2Gids(nic)
			if got := strings.Join(ports, ","); got != tc.ports {
				t.Errorf("ports = %q, want %q", got, tc.ports)
			}
			if got := strings.Join(indexes, ","); got != tc.indexes {
				t.Errorf("indexes = %q, want %q", got, tc.indexes)
			}
		})
	}
}
