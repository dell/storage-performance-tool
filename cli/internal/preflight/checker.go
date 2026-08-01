/*
Copyright © 2025 Dell Technologies
*/

package preflight

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// Checker defines preflight validation operations for a host
type Checker interface {
	// CheckDocker validates that Docker is accessible and returns the server version
	CheckDocker(ctx context.Context, host *hostparse.HostInfo) (string, error)
	// EnsureImage ensures the image exists locally (pulls if missing)
	EnsureImage(ctx context.Context, host *hostparse.HostInfo, image string) error
	// CheckPorts verifies required ports are available and reports conflicts
	CheckPorts(ctx context.Context, host *hostparse.HostInfo, apiPort int, rmiPortStart, rmiPortCount int) (*dockerconf.ConflictInfo, error)
}

// ImageIdentity is immutable Docker image evidence resolved on one participant.
type ImageIdentity struct {
	ID          string   `json:"id"`
	RepoDigests []string `json:"repoDigests,omitempty"`
}

// ImageIdentityChecker is the optional strict identity capability required by distributed
// persisted-data verification. Keeping it separate preserves existing readiness-check fakes.
type ImageIdentityChecker interface {
	InspectImageIdentity(ctx context.Context, host *hostparse.HostInfo, image string) (ImageIdentity, error)
}

// PayloadIdentityChecker proves the canonical relative-path content hash of the shipped engine
// payload. It is required only for controlled comparison and release-evidence verification runs.
type PayloadIdentityChecker interface {
	InspectPayloadIdentity(ctx context.Context, host *hostparse.HostInfo, image string) (string, error)
}

// RunningPayloadIdentityChecker measures the payload of the actual started engine container.
type RunningPayloadIdentityChecker interface {
	InspectRunningPayloadIdentity(ctx context.Context, host *hostparse.HostInfo, containerID string) (string, error)
}

// DefaultChecker implements Checker using the command-layer (SSH/local)
type DefaultChecker struct {
	exec command.CommandExecutor
}

// NewDefaultChecker creates a default preflight checker
func NewDefaultChecker() *DefaultChecker {
	return &DefaultChecker{exec: command.NewCommandExecutor()}
}

// NewCheckerWithExecutor creates a preflight checker that uses the provided
// command executor (useful for tests so mocks see the same calls).
func NewCheckerWithExecutor(exec command.CommandExecutor) *DefaultChecker {
	if exec == nil {
		exec = command.NewCommandExecutor()
	}
	return &DefaultChecker{exec: exec}
}

// withTimeout derives a context with timeout
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, d) // #nosec G118 -- cancel is called by all callers
}

// CheckDocker runs `docker version --format '{{.Server.Version}}'`
func (c *DefaultChecker) CheckDocker(ctx context.Context, host *hostparse.HostInfo) (string, error) {
	tctx, cancel := withTimeout(ctx, time.Duration(constants.DockerDaemonTimeoutSecs)*time.Second)
	defer cancel()
	stdout, stderr, err := c.exec.ExecuteCommand(tctx, host, []string{constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat})
	if err != nil {
		return "", fmt.Errorf("docker not accessible on %s: %w (stderr: %s)", host.Original, err, stderr)
	}
	return stdout, nil
}

// EnsureImage checks if image exists and pulls when missing
func (c *DefaultChecker) EnsureImage(ctx context.Context, host *hostparse.HostInfo, image string) error {
	ops := command.NewDockerOperations(c.exec, host)
	tctx, cancel := withTimeout(ctx, 3*time.Minute)
	defer cancel()
	available, err := ops.IsImageAvailable(tctx, image)
	if err != nil {
		return err
	}
	if available {
		return nil
	}
	if constants.IsDevImage(image) {
		return fmt.Errorf("dev image %s not present on %s; build it with `make docker-local` and distribute it with engine/tools/push-worker-image.sh", image, host.Original)
	}
	_, err = ops.PullImage(tctx, image)
	return err
}

// CheckPorts verifies that the provided ports are not in use
func (c *DefaultChecker) CheckPorts(ctx context.Context, host *hostparse.HostInfo, apiPort int, rmiPortStart, rmiPortCount int) (*dockerconf.ConflictInfo, error) {
	ports := []int{constants.DefaultRMIRegistryPort, apiPort}
	for i := 0; i < rmiPortCount; i++ {
		ports = append(ports, rmiPortStart+i)
	}
	checker := dockerconf.NewPortChecker(host, c.exec, ports)
	tctx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()
	return checker.CheckForConflicts(tctx)
}

// InspectImageIdentity resolves the content-addressed Docker image ID and any registry digests.
func (c *DefaultChecker) InspectImageIdentity(
	ctx context.Context,
	host *hostparse.HostInfo,
	image string,
) (ImageIdentity, error) {
	var identity ImageIdentity
	tctx, cancel := withTimeout(ctx, time.Duration(constants.DockerDaemonTimeoutSecs)*time.Second)
	defer cancel()
	stdout, stderr, err := c.exec.ExecuteCommand(tctx, host, []string{
		constants.DockerCommand,
		constants.DockerCmdImage,
		constants.DockerCmdInspect,
		image,
	})
	if err != nil {
		return identity, fmt.Errorf("inspect image identity on %s: %w (stderr: %s)", host.Original, err, stderr)
	}
	var records []struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
	}
	if err := json.Unmarshal([]byte(stdout), &records); err != nil || len(records) != 1 {
		return identity, fmt.Errorf("inspect image identity on %s returned invalid JSON evidence", host.Original)
	}
	identity.ID = strings.TrimSpace(records[0].ID)
	encodedID := strings.TrimPrefix(identity.ID, "sha256:")
	if len(encodedID) != 64 {
		return ImageIdentity{}, fmt.Errorf("inspect image identity on %s returned invalid ID %q", host.Original, identity.ID)
	}
	if _, err := hex.DecodeString(encodedID); err != nil {
		return ImageIdentity{}, fmt.Errorf("inspect image identity on %s returned invalid ID %q", host.Original, identity.ID)
	}
	for _, digest := range records[0].RepoDigests {
		if digest = strings.TrimSpace(digest); digest != "" {
			identity.RepoDigests = append(identity.RepoDigests, digest)
		}
	}
	sort.Strings(identity.RepoDigests)
	return identity, nil
}

const integrityPayloadHashScript = "set -eu; cd " + constants.IntegrityPayloadRoot +
	"; find . -type f -printf '%P\\0' | LC_ALL=C sort -z | xargs -0 -r sha256sum | sha256sum"

// InspectPayloadIdentity starts the already-resolved image without network or writable rootfs,
// hashes each regular file under /opt/spt in canonical relative-path order, and returns the hash
// of that path-and-content digest stream. The temporary container is removed by Docker.
func (c *DefaultChecker) InspectPayloadIdentity(
	ctx context.Context,
	host *hostparse.HostInfo,
	image string,
) (string, error) {
	tctx, cancel := withTimeout(ctx, time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()
	scriptArg := integrityPayloadHashScript
	if host != nil && !host.IsLocal {
		// OpenSSH joins command arguments into one remote shell command. Preserve the fixed script
		// as one `sh -c` argument, including its quoted find format.
		scriptArg = quoteRemoteShellArg(scriptArg)
	}
	stdout, stderr, err := c.exec.ExecuteCommand(tctx, host, []string{
		constants.DockerCommand,
		constants.DockerCmdRun,
		constants.DockerFlagRemove,
		constants.DockerFlagReadOnly,
		constants.DockerFlagNetwork,
		"none",
		constants.DockerFlagEntrypoint,
		"sh",
		image,
		"-c",
		scriptArg,
	})
	if err != nil {
		return "", fmt.Errorf("inspect payload identity on %s: %w (stderr: %s)", host.Original, err, stderr)
	}
	fields := strings.Fields(stdout)
	if len(fields) == 0 || len(fields[0]) != 64 {
		return "", fmt.Errorf("inspect payload identity on %s returned invalid SHA-256 evidence", host.Original)
	}
	digest := strings.ToLower(fields[0])
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("inspect payload identity on %s returned invalid SHA-256 evidence", host.Original)
	}
	return digest, nil
}

// InspectRunningPayloadIdentity hashes the payload in the actual container selected for a run.
func (c *DefaultChecker) InspectRunningPayloadIdentity(
	ctx context.Context,
	host *hostparse.HostInfo,
	containerID string,
) (string, error) {
	if strings.TrimSpace(containerID) == "" {
		return "", fmt.Errorf("running payload identity requires a container ID on %s", host.Original)
	}
	tctx, cancel := withTimeout(ctx, time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()
	scriptArg := integrityPayloadHashScript
	if host != nil && !host.IsLocal {
		scriptArg = quoteRemoteShellArg(scriptArg)
	}
	stdout, stderr, err := c.exec.ExecuteCommand(tctx, host, []string{
		constants.DockerCommand, constants.DockerCmdExec, containerID, "sh", "-c", scriptArg,
	})
	if err != nil {
		return "", fmt.Errorf("inspect running payload identity on %s: %w (stderr: %s)", host.Original, err, stderr)
	}
	fields := strings.Fields(stdout)
	if len(fields) == 0 || len(fields[0]) != 64 {
		return "", fmt.Errorf("inspect running payload identity on %s returned invalid SHA-256 evidence", host.Original)
	}
	digest := strings.ToLower(fields[0])
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("inspect running payload identity on %s returned invalid SHA-256 evidence", host.Original)
	}
	return digest, nil
}

func quoteRemoteShellArg(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
