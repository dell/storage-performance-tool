package docker

import (
	"context"
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// isPortAvailable is a test-only helper moved from production to satisfy unused lints.
func (pc *PortChecker) isPortAvailable(ctx context.Context, port int) (bool, error) {
	cmd := []string{"sh", "-c", fmt.Sprintf(constants.PortCheckCommand, port, port)}

	stdout, stderr, err := pc.cmdExecutor.ExecuteCommand(ctx, pc.host, cmd)

	if err != nil {
		if strings.TrimSpace(stdout) == "" && strings.TrimSpace(stderr) == "" {
			return true, nil
		}
		if strings.Contains(stderr, "exit status 1") {
			return true, nil
		}
	}

	if err != nil && (strings.Contains(stderr, "ss: not found") || strings.Contains(stderr, "netstat: not found") || strings.Contains(stderr, "command not found")) {
		logging.LogDebug("docker-conflicts", "port check tools missing; assuming available",
			"host", pc.host.Host, "port", port, "stderr", stderr)
		return true, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to check port %d: %w (stderr: %s)", port, err, stderr)
	}

	if strings.TrimSpace(stdout) != "" {
		logging.LogDebug("docker-conflicts", "port in use",
			"host", pc.host.Host, "port", port, "output", stdout)
		return false, nil
	}

	return true, nil
}
