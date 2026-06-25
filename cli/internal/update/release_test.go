package update

import "testing"

func TestSelectLatestRelease(t *testing.T) {
	releases := []Release{
		{TagName: "v5.10.4"},
		{TagName: "v5.11.0-rc.1", Prerelease: true},
		{TagName: "v5.11.0"},
		{TagName: "v5.12.0-rc.1", Prerelease: true},
		{TagName: "v99.0.0", Draft: true},
		{TagName: "not-semver"},
	}

	stable, err := SelectLatestRelease(releases, ChannelStable)
	if err != nil {
		t.Fatalf("SelectLatestRelease stable: %v", err)
	}
	if stable.TagName != "v5.11.0" {
		t.Fatalf("stable tag = %q, want v5.11.0", stable.TagName)
	}

	pre, err := SelectLatestRelease(releases, ChannelPrerelease)
	if err != nil {
		t.Fatalf("SelectLatestRelease prerelease: %v", err)
	}
	if pre.TagName != "v5.12.0-rc.1" {
		t.Fatalf("pre tag = %q, want v5.12.0-rc.1", pre.TagName)
	}
}

func TestSelectLatestReleaseSkipsPrereleaseFlagMismatch(t *testing.T) {
	releases := []Release{
		{TagName: "v5.12.0", Prerelease: true},
		{TagName: "v5.11.0"},
		{TagName: "v5.13.0-rc.1", Prerelease: false},
		{TagName: "v5.10.0-rc.1", Prerelease: true},
	}

	stable, err := SelectLatestRelease(releases, ChannelStable)
	if err != nil {
		t.Fatalf("SelectLatestRelease stable: %v", err)
	}
	if stable.TagName != "v5.11.0" {
		t.Fatalf("stable tag = %q, want v5.11.0", stable.TagName)
	}

	pre, err := SelectLatestRelease(releases, ChannelPrerelease)
	if err != nil {
		t.Fatalf("SelectLatestRelease prerelease: %v", err)
	}
	if pre.TagName != "v5.11.0" {
		t.Fatalf("pre tag = %q, want v5.11.0", pre.TagName)
	}
}

func TestAvailable(t *testing.T) {
	tests := []struct {
		current string
		latest  string
		want    bool
	}{
		{"5.10.4", "5.10.4", false},
		{"5.10.4", "5.10.5", true},
		{"5.10.4", "5.10.3", false},
		{"5.11.0-rc.1", "5.11.0-rc.2", true},
		{"5.11.0-rc.2", "5.11.0", true},
	}

	for _, tt := range tests {
		t.Run(tt.current+"_to_"+tt.latest, func(t *testing.T) {
			current := mustParseVersion(t, tt.current)
			latest := mustParseVersion(t, tt.latest)
			if got := Available(current, latest); got != tt.want {
				t.Fatalf("Available(%q, %q) = %v, want %v", tt.current, tt.latest, got, tt.want)
			}
		})
	}
}
