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
		"    ens2np0 used: 1 Path, 16 permits; serves all 4 endpoints",
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
		"    ens2np0 used: 1 Path, 3 permits; serves all 4 endpoints",
		"    ens33np0 used: 1 Path, 2 permits; serves all 4 endpoints")
}

func TestRenderSharedPermits(t *testing.T) {
	out := parseFixture(t, "sc26-auto-rdma.json")
	text := render(out, RenderOptions{Endpoints: sc26Endpoints, Threads: 2})
	assertLines(t, text,
		"    Permits at --threads 2: 2 permits shared by 4 NICs in rotation, fewer than one per NIC",
		"    ens2np0 used: 1 Path; serves all 4 endpoints")
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
				{Name: "ens1f0", Used: true, Serves: []string{"192.0.2.101:9000"}, Paths: 1},
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
		"    ens1f0 used: 1 Path, 2 permits; serves 192.0.2.101:9000",
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

func TestRoceV2GidPrefersPortOne(t *testing.T) {
	nic := Nic{Addresses: []string{"192.0.2.11/24"}, Gids: []Gid{
		{Port: 2, Index: 5, Address: "192.0.2.11", RoceV2: true},
		{Port: 1, Index: 2, Address: "192.0.2.11", RoceV2: false},
		{Port: 1, Index: 3, Address: "192.0.2.11", RoceV2: true},
		{Port: 1, Index: 7, Address: "192.0.2.99", RoceV2: true},
	}}
	if gid, ok := roceV2Gid(nic); !ok || gid.Port != 1 || gid.Index != 3 {
		t.Errorf("roceV2Gid = %+v, %v", gid, ok)
	}
	nic.Gids = nic.Gids[:2]
	if gid, ok := roceV2Gid(nic); !ok || gid.Port != 2 || gid.Index != 5 {
		t.Errorf("roceV2Gid without port 1 = %+v, %v", gid, ok)
	}
	nic.Gids = nic.Gids[1:]
	if _, ok := roceV2Gid(nic); ok {
		t.Error("a RoCE v1 GID is not a RoCE v2 GID")
	}
}
