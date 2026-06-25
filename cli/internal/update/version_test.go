package update

import "testing"

func TestCanSelfUpdateCurrentBuild(t *testing.T) {
	tests := []struct {
		version string
		wantOK  bool
	}{
		{"", false},
		{"dev", false},
		{"5.10.4-dev+abc1234", false},
		{"5.10.4-SNAPSHOT", false},
		{"not-semver", false},
		{"5.10.4", true},
		{"5.11.0-rc.1", true},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			gotOK, reason := CanSelfUpdateCurrentBuild(tt.version)
			if gotOK != tt.wantOK {
				t.Fatalf("CanSelfUpdateCurrentBuild(%q) ok = %v, want %v (reason %q)", tt.version, gotOK, tt.wantOK, reason)
			}
			if !gotOK && reason == "" {
				t.Fatalf("CanSelfUpdateCurrentBuild(%q) returned empty rejection reason", tt.version)
			}
		})
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		left  string
		right string
		want  int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.0.1", "1.0.0", 1},
		{"1.2.0", "1.10.0", -1},
		{"2.0.0", "1.99.99", 1},
		{"1.0.0-rc.1", "1.0.0", -1},
		{"1.0.0", "1.0.0-rc.1", 1},
		{"1.0.0-rc.2", "1.0.0-rc.1", 1},
		{"1.0.0-rc.10", "1.0.0-rc.2", 1},
		{"1.0.0-alpha", "1.0.0-alpha.1", -1},
		{"1.0.0-alpha.1", "1.0.0-alpha.beta", -1},
		{"1.0.0-alpha.beta", "1.0.0-beta", -1},
		{"1.0.0-beta", "1.0.0-beta.2", -1},
		{"1.0.0-beta.11", "1.0.0-rc.1", -1},
		{"1.0.0-1", "1.0.0-alpha", -1},
	}

	for _, tt := range tests {
		t.Run(tt.left+"_vs_"+tt.right, func(t *testing.T) {
			left := mustParseVersion(t, tt.left)
			right := mustParseVersion(t, tt.right)
			got := left.Compare(right)
			if got != tt.want {
				t.Fatalf("Compare(%q, %q) = %d, want %d", tt.left, tt.right, got, tt.want)
			}
		})
	}
}

func TestParseTagRequiresVPrefix(t *testing.T) {
	if _, err := ParseTag("5.10.4"); err == nil {
		t.Fatal("ParseTag accepted bare version")
	}
	got, err := ParseTag("v5.10.4-rc.1")
	if err != nil {
		t.Fatalf("ParseTag returned error: %v", err)
	}
	if got.String() != "5.10.4-rc.1" {
		t.Fatalf("ParseTag string = %q", got.String())
	}
}

func mustParseVersion(t *testing.T, s string) Version {
	t.Helper()
	v, err := ParseVersion(s)
	if err != nil {
		t.Fatalf("ParseVersion(%q): %v", s, err)
	}
	return v
}
