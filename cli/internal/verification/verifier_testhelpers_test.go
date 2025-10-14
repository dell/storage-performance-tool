package verification

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// checkLocalDocker is a test-only helper retained for verifier unit tests.
func (v *Verifier) checkLocalDocker(host *hostparse.HostInfo, start time.Time) Check {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DockerDaemonTimeoutSecs*time.Second)
	defer cancel()

	stdout, stderr, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat})
	if err != nil {
		return Check{Passed: false, Message: "Local Docker not accessible", Duration: v.timeProvider.Since(start), Error: fmt.Errorf("docker version failed: %w (stderr: %s)", err, stderr)}
	}
	version := strings.TrimSpace(stdout)
	return Check{Passed: true, Message: fmt.Sprintf("Local Docker %s accessible", version), Duration: v.timeProvider.Since(start)}
}

// checkRemoteDocker is a test-only helper retained for verifier unit tests.
func (v *Verifier) checkRemoteDocker(host *hostparse.HostInfo, start time.Time) Check {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DockerDaemonTimeoutSecs*time.Second)
	defer cancel()

	stdout, stderr, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat})
	if err != nil {
		if strings.Contains(stderr, "Connection") || strings.Contains(stderr, "ssh:") {
			return Check{Passed: false, Message: "SSH connection failed", Duration: v.timeProvider.Since(start), Error: fmt.Errorf("cannot SSH to %s: %s", host.GetSSHTarget(), stderr)}
		}
		return Check{Passed: false, Message: "Docker not accessible on remote host", Duration: v.timeProvider.Since(start), Error: fmt.Errorf("remote docker version failed: %w (stderr: %s)", err, stderr)}
	}
	version := strings.TrimSpace(stdout)
	return Check{Passed: true, Message: fmt.Sprintf("Docker %s accessible via SSH", version), Duration: v.timeProvider.Since(start)}
}
