package containerutil

import "testing"

func TestPortsUseAnyTarget(t *testing.T) {
	tests := []struct {
		name    string
		ports   []string
		targets []int
		want    bool
	}{
		{"host-mapped exact", []string{"0.0.0.0:9999->9999/tcp"}, []int{9999}, true},
		{"host-mapped different internal", []string{"127.0.0.1:443->8443/tcp"}, []int{443}, true},
		{"no host mapping (container-only)", []string{"9999/tcp"}, []int{9999}, false},
		{"no matches", []string{"foo"}, []int{80}, false},
		{"ipv6-style mapping", []string{":::9999->9999/tcp"}, []int{9999}, true},
		{"multiple targets, one match", []string{"0.0.0.0:8080->80/tcp"}, []int{9999, 8080}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PortsUseAnyTarget(tt.ports, tt.targets)
			if got != tt.want {
				t.Fatalf("PortsUseAnyTarget(%v, %v) = %v; want %v", tt.ports, tt.targets, got, tt.want)
			}
		})
	}
}

func TestIsSptLike(t *testing.T) {
	if !IsSptLike("spt-verify-abc", "busybox:latest") {
		t.Fatalf("expected spt-verify name to be spt-like")
	}
	if !IsSptLike("worker-1", "spt:latest") {
		t.Fatalf("expected spt image to be spt-like")
	}
	if IsSptLike("worker-1", "ubuntu:22.04") {
		t.Fatalf("did not expect non-spt name/image to be spt-like")
	}
}

type testContainer struct{ Name, Image string }

func TestAnySpt(t *testing.T) {
	// Values slice
	vals := []testContainer{{"a", "ubuntu:22.04"}, {"spt-verify-1", "nginx:alpine"}}
	if !AnySpt(vals, func(v testContainer) string { return v.Name }, func(v testContainer) string { return v.Image }) {
		t.Fatalf("expected AnySpt true for values slice")
	}
	if AnySpt([]testContainer{{"a", "ubuntu"}}, func(v testContainer) string { return v.Name }, func(v testContainer) string { return v.Image }) {
		t.Fatalf("did not expect AnySpt true for values slice without match")
	}

	// Pointers slice
	p1 := &testContainer{"x", "ubuntu"}
	p2 := &testContainer{"y", "spt:latest"}
	ptrs := []*testContainer{p1, p2}
	if !AnySpt(ptrs, func(v *testContainer) string { return v.Name }, func(v *testContainer) string { return v.Image }) {
		t.Fatalf("expected AnySpt true for pointers slice")
	}

	// Empty/nil slices
	var nilVals []testContainer
	if AnySpt(nilVals, func(v testContainer) string { return v.Name }, func(v testContainer) string { return v.Image }) {
		t.Fatalf("did not expect AnySpt true for nil slice")
	}
}

func TestHasConflicts(t *testing.T) {
	if HasConflicts(nil) {
		t.Fatalf("expected nil slice -> no conflicts")
	}
	if HasConflicts([]int{}) {
		t.Fatalf("expected empty slice -> no conflicts")
	}
	if !HasConflicts([]int{1099}) {
		t.Fatalf("expected non-empty slice -> conflicts")
	}
}
