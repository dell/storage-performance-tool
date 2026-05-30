package constants

import (
	"os"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/buildinfo"
)

// DevImageTag is the tag used for local, build-only engine images that are never
// published to a registry. Produced by `make docker-local` and distributed to
// workers with engine/tools/push-worker-image.sh.
const DevImageTag = "spt_dev"

// EffectiveSptImage returns the image ref to run/pull for the engine.
//
// Precedence:
//  1. $SPT_IMAGE override — used verbatim (keeps the comparison harness / cli/.env
//     and the --spt-image flag working).
//  2. Release build → DefaultSptImage:v<version>, matching the published tag scheme
//     (see .github/workflows/docker-publish.yaml). There is deliberately no
//     fallback to ":latest": a missing release tag fails loudly at pull time rather
//     than silently running a different engine version.
//  3. Dev/local build → DefaultSptImage:spt_dev (local-only image, see IsDevImage).
func EffectiveSptImage() string {
	if v := strings.TrimSpace(os.Getenv(EnvSptImage)); v != "" {
		return v
	}
	if buildinfo.IsRelease() {
		return DefaultSptImage + ":v" + buildinfo.Version
	}
	return DefaultSptImage + ":" + DevImageTag
}

// IsDevImage reports whether ref points at a local-only dev image that must never
// be pulled from a registry. It matches the DevImageTag exactly as well as
// discriminator variants like "spt_dev-baseline" used for dev-vs-dev comparisons.
func IsDevImage(ref string) bool {
	tagPart := ref
	if slash := strings.LastIndex(ref, "/"); slash >= 0 {
		tagPart = ref[slash:]
	}
	colon := strings.LastIndex(tagPart, ":")
	if colon < 0 {
		return false
	}
	tag := tagPart[colon+1:]
	return tag == DevImageTag || strings.HasPrefix(tag, DevImageTag+"-")
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
