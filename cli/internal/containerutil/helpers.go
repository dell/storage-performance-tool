package containerutil

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// PortsUseAnyTarget returns true if any host port mapping in the provided
// container ports slice matches one of the target ports.
// Accepts mappings like "0.0.0.0:9999->9999/tcp" or "9999/tcp".
func PortsUseAnyTarget(ports []string, targets []int) bool {
	for _, portMapping := range ports {
		for _, targetPort := range targets {
			if strings.Contains(portMapping, fmt.Sprintf(":%d->", targetPort)) ||
				strings.Contains(portMapping, fmt.Sprintf(":%d/", targetPort)) {
				return true
			}
		}
	}
	return false
}

// IsSptLike reports whether a container name/image appears to be
// related to Spt or spt verification containers.
func IsSptLike(name, image string) bool {
	lowerName := strings.ToLower(name)
	lowerImage := strings.ToLower(image)

	if strings.Contains(lowerImage, "spt") || strings.HasPrefix(lowerName, "spt-verify") {
		return true
	}

	normalizedImage := strings.TrimSpace(image)
	if normalizedImage == constants.DefaultSptImage || normalizedImage == constants.LegacyMongooseImage {
		return true
	}

	return false
}

// AnySpt reports whether any item in a list appears to be a Spt
// container according to the provided name and image accessors.
func AnySpt[T any](list []T, name func(T) string, image func(T) string) bool {
	for _, v := range list {
		if IsSptLike(name(v), image(v)) {
			return true
		}
	}
	return false
}

// HasConflicts returns true when any ports are marked conflicted.
func HasConflicts(ports []int) bool { return len(ports) > 0 }
