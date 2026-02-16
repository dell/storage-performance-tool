package constants

import (
	"os"
	"testing"
)

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
		{name: "2", envValue: "2", want: false},             // strconv.ParseBool rejects "2"
		{name: "enabled", envValue: "enabled", want: false}, // only "yes" is special-cased
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

func TestEffectiveSptImage(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		unset    bool
		want     string
	}{
		{name: "default when unset", unset: true, want: DefaultSptImage},
		{name: "default when empty", envValue: "", want: DefaultSptImage},
		{name: "custom image", envValue: "myregistry/spt:v2", want: "myregistry/spt:v2"},
		{name: "whitespace trimmed", envValue: "  myregistry/spt:v3  ", want: "myregistry/spt:v3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				os.Unsetenv(EnvSptImage)
			} else {
				os.Setenv(EnvSptImage, tt.envValue)
			}
			t.Cleanup(func() { os.Unsetenv(EnvSptImage) })

			got := EffectiveSptImage()
			if got != tt.want {
				t.Errorf("EffectiveSptImage() = %q, want %q", got, tt.want)
			}
		})
	}
}
