package constants

import (
	"os"
	"strconv"
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

// IsRdmaEnabled returns true when SPT_RDMA is set to a truthy value (1, true, yes).
// When enabled, container launches include RDMA device passthrough, IPC_LOCK
// capability, and unlimited memlock ulimits.
func IsRdmaEnabled() bool {
	v := strings.TrimSpace(os.Getenv(EnvRdmaEnabled))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return strings.EqualFold(v, "yes")
	}
	return b
}
