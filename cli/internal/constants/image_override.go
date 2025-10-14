package constants

import (
	"os"
	"strings"
)

// EffectiveSptImage returns the image to use for running/pulling Spt.
// If the environment variable SPT_IMAGE is set and non-empty, it overrides
// the built-in default. Otherwise, DefaultSptImage is returned.
func EffectiveSptImage() string {
	if v := strings.TrimSpace(os.Getenv(EnvSptImage)); v != "" {
		return v
	}
	return DefaultSptImage
}
