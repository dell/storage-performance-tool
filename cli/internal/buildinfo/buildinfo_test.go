package buildinfo

import "testing"

func TestIsRelease(t *testing.T) {
	cases := []struct {
		version string
		want    bool
	}{
		{"5.10.3", true},
		{"5.10.0-rc.1", true},
		{"5.9.0", true},
		{"dev", false},
		{"", false},
		{"5.10.3-dev+abc1234", false},
		{"0.0.0-dev+local", false},
		{"not-a-version", false},
	}
	restore := Version
	t.Cleanup(func() { Version = restore })
	for _, c := range cases {
		Version = c.version
		if got := IsRelease(); got != c.want {
			t.Errorf("IsRelease() with Version=%q = %v, want %v", c.version, got, c.want)
		}
	}
}
