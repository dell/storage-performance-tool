package nicplan

import (
	"reflect"
	"testing"
)

func TestParseHostInterfaces(t *testing.T) {
	got, err := ParseHostInterfaces(
		[]string{"root@wrk1=none", " wrk2 = mlx5_0,mlx5_1 ", "localhost=auto"},
		[]string{"wrk1", "wrk2", "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	want := []HostInterfaces{
		{Host: "wrk1", Value: "none"},
		{Host: "wrk2", Value: "mlx5_0,mlx5_1", Entries: []string{"mlx5_0", "mlx5_1"}},
		{Host: "127.0.0.1", Value: "auto", Entries: []string{"auto"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if entries := AnyNicSet(got); !reflect.DeepEqual(entries, []string{"mlx5_0", "mlx5_1"}) {
		t.Fatalf("AnyNicSet = %v", entries)
	}
	if override, ok := ForHost(got, "wrk1"); !ok || override.Value != None {
		t.Fatalf("ForHost(wrk1) = %+v, %v", override, ok)
	}
	if _, ok := ForHost(got, "wrk9"); ok {
		t.Fatal("wrk9 has no value")
	}
}

func TestAnyNicSetIgnoresNone(t *testing.T) {
	if entries := AnyNicSet([]HostInterfaces{{Host: "wrk1", Value: None}}); entries != nil {
		t.Fatalf("AnyNicSet = %v, want none", entries)
	}
}

func TestParseHostInterfacesRejects(t *testing.T) {
	hosts := []string{"wrk1", "wrk2"}
	tests := map[string]struct {
		values []string
		want   string
	}{
		"a host not in --test-hosts": {[]string{"wrk9=auto"}, "--host-interfaces wrk9=auto: wrk9 is not in --test-hosts"},
		"a host given twice":         {[]string{"wrk2=auto", "root@wrk2=none"}, "--host-interfaces names root@wrk2 twice"},
		"an empty value":             {[]string{"wrk2="}, "--host-interfaces wrk2= has an empty value"},
		"no value at all":            {[]string{"wrk2"}, `--host-interfaces "wrk2" is not <host>=<value>`},
		"no host":                    {[]string{"=auto"}, `--host-interfaces "=auto" is not <host>=<value>`},
		"auto beside another":        {[]string{"wrk2=auto,eth0"}, `--host-interfaces wrk2=auto,eth0: --interfaces "auto,eth0": auto must be the only entry`},
		"none beside another":        {[]string{"wrk2=none,eth0"}, `--host-interfaces wrk2=none,eth0: --interfaces entry "none" applies only to --host-interfaces`},
		"a bad entry":                {[]string{"wrk2=eth0;id"}, `--host-interfaces wrk2=eth0;id: --interfaces entry "eth0;id" is not an interface name, IPv4 address or RDMA device`},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseHostInterfaces(tc.values, hosts)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseInterfacesRejectsNone(t *testing.T) {
	_, err := ParseInterfaces("none")
	if want := `--interfaces entry "none" applies only to --host-interfaces`; err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
}
