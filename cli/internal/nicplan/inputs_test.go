package nicplan

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseInterfaces(t *testing.T) {
	tests := []struct {
		value string
		want  []string
	}{
		{"", nil},
		{"  ", nil},
		{"auto", []string{"auto"}},
		{" ens2np0, 192.0.2.12 ,mlx5_2 ", []string{"ens2np0", "192.0.2.12", "mlx5_2"}},
		{"bond0.100,eth-data", []string{"bond0.100", "eth-data"}},
	}
	for _, tt := range tests {
		got, err := ParseInterfaces(tt.value)
		if err != nil || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("ParseInterfaces(%q) = %v, %v; want %v", tt.value, got, err, tt.want)
		}
	}
}

func TestParseInterfacesRejects(t *testing.T) {
	tests := []struct {
		value, want string
	}{
		{"ens2np0,,ens10np0", `--interfaces "ens2np0,,ens10np0" has an empty entry`},
		{"ens2np0,", "has an empty entry"},
		{"ens2np0;reboot", `--interfaces entry "ens2np0;reboot" is not an interface name, IPv4 address or RDMA device`},
		{"$(id)", "is not an interface name"},
		{"eth 0", "is not an interface name"},
		{"auto,ens2np0", `--interfaces "auto,ens2np0": auto must be the only entry`},
		{"ens2np0, auto", "auto must be the only entry"},
	}
	for _, tt := range tests {
		_, err := ParseInterfaces(tt.value)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("ParseInterfaces(%q) error = %v, want %q", tt.value, err, tt.want)
		}
	}
}

func TestValidateEndpoints(t *testing.T) {
	if err := ValidateEndpoints([]string{"192.0.2.101:9000", "s3.example.com:443", "[2001:db8::1]:9000", "obs-1_a:80"}); err != nil {
		t.Fatalf("ValidateEndpoints: %v", err)
	}
	for _, endpoint := range []string{"192.0.2.101:9000&&true", "host name:80", "`id`:80", "a'b:80"} {
		err := ValidateEndpoints([]string{"192.0.2.101:9000", endpoint})
		if err == nil || !strings.Contains(err.Error(), "verify cannot pass to the NIC planner") {
			t.Errorf("ValidateEndpoints(%q) error = %v", endpoint, err)
		}
	}
}

func TestArgs(t *testing.T) {
	endpoints := []string{"192.0.2.101:9000", "192.0.2.102:9000"}
	tests := []struct {
		name       string
		interfaces []string
		rdma       bool
		want       []string
	}{
		{"inventory", nil, false, []string{"/opt/spt/spt-nic-plan"}},
		{"inventory with RDMA", nil, true, []string{"/opt/spt/spt-nic-plan", "--rdma"}},
		{"plan", []string{"auto"}, false, []string{"/opt/spt/spt-nic-plan", "--interfaces", "auto",
			"--endpoint", "192.0.2.101:9000", "--endpoint", "192.0.2.102:9000"}},
		{"RDMA plan", []string{"ens2np0", "mlx5_1"}, true, []string{"/opt/spt/spt-nic-plan", "--rdma", "--interfaces", "ens2np0,mlx5_1",
			"--endpoint", "192.0.2.101:9000", "--endpoint", "192.0.2.102:9000"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Args(tt.interfaces, endpoints, tt.rdma); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Args = %v, want %v", got, tt.want)
			}
		})
	}
}

// The rows of the engine's ConfigSliceUtilTest: slice count, endpoint count, and each slice's
// endpoint indexes.
func TestSliceEndpointsMatchesTheEngine(t *testing.T) {
	all := []string{"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000", "10.0.0.4:9000", "10.0.0.5:9000", "10.0.0.6:9000"}
	tests := []struct {
		slices, endpoints int
		want              [][]int
	}{
		{1, 1, [][]int{{0}}},
		{2, 4, [][]int{{0, 1}, {2, 3}}},
		{4, 2, [][]int{{0}, {0}, {1}, {1}}},
		{4, 4, [][]int{{0}, {1}, {2}, {3}}},
		{4, 6, [][]int{{0, 1, 2}, {0, 1, 2}, {3, 4, 5}, {3, 4, 5}}},
		{3, 2, [][]int{{0, 1}, {0, 1}, {0, 1}}},
		{2, 3, [][]int{{0, 1, 2}, {0, 1, 2}}},
	}
	for _, tt := range tests {
		endpoints := all[:tt.endpoints]
		for slice, indexes := range tt.want {
			want := make([]string, 0, len(indexes))
			for _, i := range indexes {
				want = append(want, endpoints[i])
			}
			if got := SliceEndpoints(endpoints, tt.slices, slice); !reflect.DeepEqual(got, want) {
				t.Errorf("%d slices, %d endpoints, slice %d: got %v, want %v", tt.slices, tt.endpoints, slice, got, want)
			}
		}
	}
}

func TestSliceEndpointsOutOfRangeKeepsAll(t *testing.T) {
	endpoints := []string{"10.0.0.1:9000", "10.0.0.2:9000"}
	for _, slice := range []int{-1, 2} {
		if got := SliceEndpoints(endpoints, 2, slice); !reflect.DeepEqual(got, endpoints) {
			t.Errorf("slice %d: got %v", slice, got)
		}
	}
}

func TestPermitShares(t *testing.T) {
	tests := []struct {
		limit, nics int
		want        Permits
	}{
		{64, 4, Permits{Limit: 64, PerNic: []int{16, 16, 16, 16}}},
		{10, 4, Permits{Limit: 10, PerNic: []int{3, 3, 2, 2}}},
		{4, 4, Permits{Limit: 4, PerNic: []int{1, 1, 1, 1}}},
		{3, 4, Permits{Limit: 3, Shared: true}},
		{0, 4, Permits{Limit: 0, Shared: true}},
		{-1, 2, Permits{Limit: -1, Shared: true}},
		{8, 0, Permits{Limit: 8}},
	}
	for _, tt := range tests {
		if got := PermitShares(tt.limit, tt.nics); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("PermitShares(%d, %d) = %+v, want %+v", tt.limit, tt.nics, got, tt.want)
		}
	}
}

func TestNonDefaultPolicyRules(t *testing.T) {
	defaults := "0:\tfrom all lookup local\n32766:\tfrom all lookup main\n32767:\tfrom all lookup default\n"
	if got := NonDefaultPolicyRules(defaults); got != nil {
		t.Fatalf("default rules gave %v", got)
	}
	custom := "0:\tfrom all lookup local\n100:\tfrom 192.0.2.11 lookup 101\n32766:\tfrom all lookup main\n200:  from all fwmark 0x1   lookup 200 \n32767:\tfrom all lookup default\n"
	want := []string{"100: from 192.0.2.11 lookup 101", "200: from all fwmark 0x1 lookup 200"}
	if got := NonDefaultPolicyRules(custom); !reflect.DeepEqual(got, want) {
		t.Fatalf("NonDefaultPolicyRules = %v, want %v", got, want)
	}
}
