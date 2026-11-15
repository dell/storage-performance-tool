/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"

	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/preflight"
)

// errNoPortCheckResult reports a port checker that returned neither a result nor an error.
var errNoPortCheckResult = errors.New("port checker returned no result")

// portCheckResult turns a nil result with no error into an error, so callers can read the
// result whenever the error is nil.
func portCheckResult(info *dockerconf.ConflictInfo, err error) (*dockerconf.ConflictInfo, error) {
	if err == nil && info == nil {
		return nil, errNoPortCheckResult
	}
	return info, err
}

// recheckPortsAfterCleanup checks the ports again after --force stopped conflicting containers.
// It returns nil when they are free. Otherwise it returns the host error and the line for the
// connect-error summary: a failed re-check when the checker errors or returns no result, or the
// ports still in use.
func recheckPortsAfterCleanup(
	ctx context.Context,
	checker preflight.Checker,
	host *hostparse.HostInfo,
	registryPort, apiPort int,
) (summary string, err error) {
	info, err := portCheckResult(checker.CheckPorts(ctx, host, registryPort, apiPort))
	if err != nil {
		return fmt.Sprintf("  ❌ %s: Port re-check failed after cleanup: %v", host.Original, err),
			fmt.Errorf("port re-check failed after cleanup: %w", err)
	}
	if len(info.ConflictPorts) > 0 {
		return fmt.Sprintf("  ❌ %s: Ports still in use: %v", host.Original, info.ConflictPorts),
			fmt.Errorf("ports still in use after cleanup: %v", info.ConflictPorts)
	}
	return "", nil
}
