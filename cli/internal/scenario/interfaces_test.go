package scenario

import (
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
	"gopkg.in/yaml.v3"
)

func TestValidateInterfacesAcceptsTheDefaultS3Driver(t *testing.T) {
	for _, driver := range []string{"", S3DriverDefault, S3DriverNetty} {
		if err := ValidateInterfaces([]string{"auto"}, InterfacesTarget{S3Driver: driver, WorkloadType: workload.Read}); err != nil {
			t.Fatalf("--s3-driver %q: %v", driver, err)
		}
	}
	if err := ValidateInterfaces(nil, InterfacesTarget{S3Driver: S3DriverAws, WorkloadType: workload.Tables}); err != nil {
		t.Fatalf("no NIC set is always valid: %v", err)
	}
}

func TestValidateInterfacesRejectsWhatANicSetCannotServe(t *testing.T) {
	roundRobin := EndpointSelection{Mode: EndpointSelectionRoundRobin}
	tests := map[string]struct {
		target InterfacesTarget
		want   string
	}{
		"tables": {InterfacesTarget{WorkloadType: workload.Tables},
			"--interfaces does not support the tables workload"},
		"mock": {InterfacesTarget{WorkloadType: workload.Mock},
			"--interfaces does not support the mock workload"},
		"aws": {InterfacesTarget{S3Driver: S3DriverAws, WorkloadType: workload.Write},
			"--interfaces requires the default Netty S3 driver or --s3-driver rdma, not --s3-driver aws"},
		"round-robin": {InterfacesTarget{WorkloadType: workload.Mixed, EndpointSelection: roundRobin},
			"--interfaces supports only --endpoint-selection default, not round-robin"},
		"the environment's value": {InterfacesTarget{Source: "SPT_INTERFACES (environment or .env)", S3Driver: S3DriverAws},
			"SPT_INTERFACES (environment or .env) requires the default Netty S3 driver or --s3-driver rdma, not --s3-driver aws"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateInterfaces([]string{"ens1f0"}, tc.target); err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRejectInterfacesOverrideCoversEverySpelling(t *testing.T) {
	for _, override := range []string{
		"storage.net.interfaces=auto",
		"storage-net-interfaces=[ens1f0]",
		" storage.net.interfaces = [] ",
		"storage.net={interfaces: [auto]}",
		"storage={net: {interfaces: auto}}",
	} {
		err := RejectInterfacesOverride(override)
		if err == nil || !strings.Contains(err.Error(), "which is not supported; use --interfaces") {
			t.Fatalf("override %q: err = %v", override, err)
		}
	}
	for _, override := range []string{
		"storage.net.nodeInterfaces=[auto, none]",
		"storage-net-nodeInterfaces=[auto]",
		"storage.net.nodeInterfaces.entry=none",
		"storage-net-nodeInterfaces-entry=none",
		"storage.net={nodeInterfaces: {entry: none}}",
	} {
		err := RejectInterfacesOverride(override)
		if err == nil || !strings.Contains(err.Error(), "which is not supported; use --host-interfaces") {
			t.Fatalf("override %q: err = %v", override, err)
		}
	}
	err := RejectInterfacesOverride("storage.net.interfaces=auto")
	want := `engine override "storage.net.interfaces=auto" sets storage.net.interfaces, which is not supported; use --interfaces`
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	for _, override := range []string{"storage.net.ssl.enabled=true", "storage.net={ssl: {enabled: true}}", "storage.driver.threads=6"} {
		if err := RejectInterfacesOverride(override); err != nil {
			t.Fatalf("override %q leaves the NIC set alone: %v", override, err)
		}
	}
}

func generatedNet(t *testing.T, params Params) map[string]any {
	t.Helper()
	data, err := GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Storage struct {
			Net map[string]any `yaml:"net"`
		} `yaml:"storage"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	return root.Storage.Net
}

func interfacesTestParams(interfaces []string, overrides ...string) Params {
	return Params{
		WorkloadType: workload.Read, Endpoints: []string{"http://192.0.2.10:9000"}, AccessKey: "a", SecretKey: "s",
		Bucket: "b", Threads: 4, ObjectSize: "1KiB", Interfaces: interfaces, EngineOverrides: overrides,
	}
}

func TestGenerateDefaultsWritesTheNicSetOnlyWithInterfaces(t *testing.T) {
	without, err := GenerateDefaults(interfacesTestParams(nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(without), "interfaces") {
		t.Fatalf("defaults without a NIC set mention interfaces:\n%s", without)
	}
	empty, err := GenerateDefaults(interfacesTestParams([]string{}))
	if err != nil || string(empty) != string(without) {
		t.Fatalf("an empty NIC set must generate the same defaults: %v\n%s", err, empty)
	}

	for _, interfaces := range [][]string{{"auto"}, {"ens1f0", "192.0.2.21", "mlx5_1"}} {
		got := generatedNet(t, interfacesTestParams(interfaces))["interfaces"]
		want := make([]any, 0, len(interfaces))
		for _, entry := range interfaces {
			want = append(want, entry)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("storage.net.interfaces = %#v, want %#v", got, want)
		}
	}
}

func TestTheNicSetSurvivesOverridesThatReplaceItsAncestors(t *testing.T) {
	for _, override := range []string{"storage.net={ssl: {enabled: false}}", "storage={region: us-east-1}", "storage-net={}"} {
		got := generatedNet(t, interfacesTestParams([]string{"ens1f0", "ens1f1"}, override))["interfaces"]
		if !reflect.DeepEqual(got, []any{"ens1f0", "ens1f1"}) {
			t.Fatalf("override %q: storage.net.interfaces = %#v", override, got)
		}
	}
	if _, ok := generatedNet(t, interfacesTestParams(nil, "storage.net={ssl: {enabled: false}}"))["interfaces"]; ok {
		t.Fatal("an override never adds a NIC set the run did not ask for")
	}
}

func TestNodeInterfacesArgs(t *testing.T) {
	overrides := []nicplan.HostInterfaces{
		{Host: "entry.example", Value: "none"},
		{Host: "wrk2", Value: "mlx5_0,mlx5_1", Entries: []string{"mlx5_0", "mlx5_1"}},
		{Host: "wrk-dropped", Value: "auto", Entries: []string{"auto"}},
	}
	got := NodeInterfacesArgs(overrides, "entry.example", []WorkerNode{
		{Host: "wrk2", Addr: "198.51.100.12:1099"},
		{Host: "wrk3", Addr: "198.51.100.13:1099"},
	})
	want := []string{
		"--storage-net-nodeInterfaces-entry=none",
		"--storage-net-nodeInterfaces-198.51.100.12:1099=mlx5_0,mlx5_1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
	if got := NodeInterfacesArgs(nil, "entry.example", []WorkerNode{{Host: "wrk2", Addr: "a:1099"}}); len(got) != 0 {
		t.Fatalf("no overrides, args = %q", got)
	}
}
