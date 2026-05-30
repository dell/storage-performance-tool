package constants

import (
	"os"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/buildinfo"
)

func TestEffectiveSptImage(t *testing.T) {
	t.Run("SPT_IMAGE override is used verbatim", func(t *testing.T) {
		t.Setenv(EnvSptImage, "myregistry.example.com/spt:custom")
		if got := EffectiveSptImage(); got != "myregistry.example.com/spt:custom" {
			t.Fatalf("EffectiveSptImage() = %q, want override value", got)
		}
	})

	t.Run("override is trimmed", func(t *testing.T) {
		t.Setenv(EnvSptImage, "  myregistry.example.com/spt:custom  ")
		if got := EffectiveSptImage(); got != "myregistry.example.com/spt:custom" {
			t.Fatalf("EffectiveSptImage() = %q, want trimmed override", got)
		}
	})

	t.Run("release build maps to v-prefixed version tag", func(t *testing.T) {
		t.Setenv(EnvSptImage, "")
		restore := buildinfo.Version
		buildinfo.Version = "5.10.3"
		t.Cleanup(func() { buildinfo.Version = restore })
		want := DefaultSptImage + ":v5.10.3"
		if got := EffectiveSptImage(); got != want {
			t.Fatalf("EffectiveSptImage() = %q, want %q", got, want)
		}
	})

	t.Run("pre-release build maps to v-prefixed tag", func(t *testing.T) {
		t.Setenv(EnvSptImage, "")
		restore := buildinfo.Version
		buildinfo.Version = "5.10.0-rc.1"
		t.Cleanup(func() { buildinfo.Version = restore })
		want := DefaultSptImage + ":v5.10.0-rc.1"
		if got := EffectiveSptImage(); got != want {
			t.Fatalf("EffectiveSptImage() = %q, want %q", got, want)
		}
	})

	t.Run("bare dev default maps to spt_dev tag", func(t *testing.T) {
		t.Setenv(EnvSptImage, "")
		restore := buildinfo.Version
		buildinfo.Version = "dev"
		t.Cleanup(func() { buildinfo.Version = restore })
		want := DefaultSptImage + ":" + DevImageTag
		if got := EffectiveSptImage(); got != want {
			t.Fatalf("EffectiveSptImage() = %q, want %q", got, want)
		}
	})

	t.Run("dev-marked local build maps to spt_dev tag", func(t *testing.T) {
		t.Setenv(EnvSptImage, "")
		restore := buildinfo.Version
		buildinfo.Version = "5.10.3-dev+abc1234"
		t.Cleanup(func() { buildinfo.Version = restore })
		want := DefaultSptImage + ":" + DevImageTag
		if got := EffectiveSptImage(); got != want {
			t.Fatalf("EffectiveSptImage() = %q, want %q", got, want)
		}
	})
}

func TestIsDevImage(t *testing.T) {
	cases := []struct {
		ref  string
		want bool
	}{
		{"ghcr.io/dell/storage-performance-tool:spt_dev", true},
		{"ghcr.io/dell/storage-performance-tool:spt_dev-baseline", true},
		{"ghcr.io/dell/storage-performance-tool:v5.10.3", false},
		{"ghcr.io/dell/storage-performance-tool:latest", false},
		{"ghcr.io/dell/storage-performance-tool", false},
		{"localhost:5000/spt:spt_dev", true},
		{"localhost:5000/spt:v5.10.3", false},
	}
	for _, c := range cases {
		if got := IsDevImage(c.ref); got != c.want {
			t.Errorf("IsDevImage(%q) = %v, want %v", c.ref, got, c.want)
		}
	}
}
func TestIsRdmaEnabled(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		unset    bool // if true, unset the env var entirely
		want     bool
	}{
		// Truthy values
		{name: "true", envValue: "true", want: true},
		{name: "TRUE", envValue: "TRUE", want: true},
		{name: "True", envValue: "True", want: true},
		{name: "1", envValue: "1", want: true},
		{name: "yes", envValue: "yes", want: true},
		{name: "YES", envValue: "YES", want: true},
		{name: "Yes", envValue: "Yes", want: true},

		// Falsy values
		{name: "false", envValue: "false", want: false},
		{name: "FALSE", envValue: "FALSE", want: false},
		{name: "0", envValue: "0", want: false},
		{name: "empty string", envValue: "", want: false},
		{name: "unset", unset: true, want: false},

		// Edge cases
		{name: "whitespace true", envValue: "  true  ", want: true},
		{name: "whitespace 1", envValue: " 1 ", want: true},
		{name: "whitespace yes", envValue: " yes ", want: true},
		{name: "garbage string", envValue: "garbage", want: false},
		{name: "no", envValue: "no", want: false},
		{name: "NO", envValue: "NO", want: false},
		{name: "2", envValue: "2", want: false},
		{name: "enabled", envValue: "enabled", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				os.Unsetenv(EnvRdmaEnabled)
			} else {
				os.Setenv(EnvRdmaEnabled, tt.envValue)
			}
			t.Cleanup(func() { os.Unsetenv(EnvRdmaEnabled) })

			got := IsRdmaEnabled()
			if got != tt.want {
				t.Errorf("IsRdmaEnabled() with SPT_RDMA=%q = %v, want %v", tt.envValue, got, tt.want)
			}
		})
	}
}
