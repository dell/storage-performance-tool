package command

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
)

// APIWaitLogLines is how many of a container's last log lines a failed engine API wait shows.
const APIWaitLogLines = 20

// containerStateFormat has no space, comma or pipe, so it passes through the remote shell
// that an SSH command runs in unchanged.
const containerStateFormat = "{{.State.Status}}:{{.State.ExitCode}}:{{.State.OOMKilled}}"

// ContainerState is the state of a container as docker inspect reports it.
type ContainerState struct {
	Status    string // docker's State.Status: running, exited, created, restarting, dead...
	ExitCode  int
	OOMKilled bool
	Missing   bool // docker has no container with this ID
}

// String describes the state for a person reading a failure report.
func (s ContainerState) String() string {
	if s.Missing {
		return "missing (no such container)"
	}
	status := strings.TrimSpace(s.Status)
	if status == "" {
		status = "unknown"
	}
	if status != "exited" && status != "dead" {
		return status
	}
	description := fmt.Sprintf("%s with exit code %d", status, s.ExitCode)
	if s.OOMKilled {
		description += " (killed for running out of memory)"
	}
	return description
}

// InspectContainerState reads a container's state with docker inspect on the host. A
// container docker no longer knows is reported as Missing, not as an error.
func InspectContainerState(ctx context.Context, executor CommandExecutor, host *hostparse.HostInfo, containerID string) (ContainerState, error) {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdInspect, constants.DockerFlagFormat, containerStateFormat, containerID}
	stdout, stderr, err := executor.ExecuteCommand(ctx, host, dockerCmd)
	if err != nil {
		if isNoSuchContainer(stderr) || isNoSuchContainer(err.Error()) {
			return ContainerState{Missing: true}, nil
		}
		if detail := strings.TrimSpace(stderr); detail != "" {
			return ContainerState{}, fmt.Errorf("%w (stderr: %s)", err, detail)
		}
		return ContainerState{}, err
	}
	return ParseContainerState(stdout)
}

// ParseContainerState parses the output of containerStateFormat.
func ParseContainerState(output string) (ContainerState, error) {
	fields := strings.Split(strings.TrimSpace(output), ":")
	if len(fields) != 3 || strings.TrimSpace(fields[0]) == "" {
		return ContainerState{}, fmt.Errorf("unexpected docker inspect output %q", strings.TrimSpace(output))
	}
	exitCode, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return ContainerState{}, fmt.Errorf("unexpected exit code in docker inspect output %q", strings.TrimSpace(output))
	}
	return ContainerState{
		Status:    strings.TrimSpace(fields[0]),
		ExitCode:  exitCode,
		OOMKilled: strings.TrimSpace(fields[2]) == "true",
	}, nil
}

func isNoSuchContainer(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "no such object") || strings.Contains(lower, "no such container")
}

// ContainerLogTail returns a container's last maxLines log lines, stdout and stderr
// together, read with GetContainerLogs.
func ContainerLogTail(ctx context.Context, ops DockerOperations, containerID string, maxLines int) (string, error) {
	var lines []string
	collect := func(line string) { lines = append(lines, line) }
	if err := ops.GetContainerLogs(ctx, containerID, false, collect, collect); err != nil {
		return "", err
	}
	if maxLines > 0 && len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n"), nil
}

// APIWaitReport describes a container whose engine API never became ready: its state and
// its last log lines, or why either could not be read. The text is redacted with
// secretmask.Text and secretmask.EnvironmentValues. It has no indentation; callers indent it.
func APIWaitReport(containerID string, state ContainerState, stateErr error, logs string, logErr error) string {
	var b strings.Builder
	id := containerID
	if len(id) > constants.ContainerIDDisplayLength {
		id = id[:constants.ContainerIDDisplayLength]
	}
	if stateErr != nil {
		fmt.Fprintf(&b, "could not read container state: %v", stateErr)
	} else {
		fmt.Fprintf(&b, "Container %s: %s", id, state)
	}
	switch {
	case logErr != nil:
		fmt.Fprintf(&b, "\ncould not read container logs: %v", logErr)
	case strings.TrimSpace(logs) == "":
		b.WriteString("\nContainer logs: (empty)")
	default:
		lines := strings.Split(strings.TrimRight(logs, "\n"), "\n")
		fmt.Fprintf(&b, "\nLast %d container log lines:", len(lines))
		for _, line := range lines {
			b.WriteString("\n  ")
			b.WriteString(strings.TrimRight(line, "\r"))
		}
	}
	return secretmask.EnvironmentValues(secretmask.Text(b.String()))
}

// IndentLines prefixes every line of text with prefix.
func IndentLines(text, prefix string) string {
	if text == "" {
		return ""
	}
	return prefix + strings.ReplaceAll(text, "\n", "\n"+prefix)
}
