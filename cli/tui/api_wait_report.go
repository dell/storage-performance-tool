package tui

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
)

// apiWaitReportTimeout bounds reading a container's state and logs after its API wait failed,
// so a slow Docker or SSH host adds little to the failure it is explaining.
const apiWaitReportTimeout = 10 * time.Second

// apiWaitReporter is implemented by the Docker managers that can describe a container whose
// engine API never became ready: its state and last log lines (command.APIWaitReport).
type apiWaitReporter interface {
	apiWaitReport(ctx context.Context, containerID string) string
}

// containerAPIWaitReport describes containerID after a failed API wait, or returns "" when
// there is no container to describe, the manager cannot describe one, or ctx has ended.
func containerAPIWaitReport(ctx context.Context, dm DockerInterface, containerID string) string {
	if dm == nil || containerID == "" {
		return ""
	}
	reporter, ok := dm.(apiWaitReporter)
	if !ok {
		return ""
	}
	ctx = normalizeContext(ctx)
	if ctx.Err() != nil {
		return ""
	}
	reportCtx, cancel := context.WithTimeout(ctx, apiWaitReportTimeout)
	defer cancel()
	return reporter.apiWaitReport(reportCtx, containerID)
}

// withAPIWaitReport appends report, indented, to the wait failure err; err stays first and
// wrapped.
func withAPIWaitReport(err error, report string) error {
	if err == nil || report == "" {
		return err
	}
	return fmt.Errorf("%w\n%s", err, command.IndentLines(report, "  "))
}

// containerStateReader is implemented by the Docker managers that can read a container's state.
type containerStateReader interface {
	containerState(ctx context.Context, containerID string) (command.ContainerState, error)
}

// containerStateDiagnostic is the single-host run's state line after a failed API wait:
// the container's state, or why it could not be read; "" when the manager cannot read one.
func containerStateDiagnostic(ctx context.Context, dm DockerInterface, containerID string) string {
	reader, ok := dm.(containerStateReader)
	if !ok || containerID == "" {
		return ""
	}
	ctx = normalizeContext(ctx)
	if ctx.Err() != nil {
		return ""
	}
	stateCtx, cancel := context.WithTimeout(ctx, apiWaitReportTimeout)
	defer cancel()
	state, err := reader.containerState(stateCtx, containerID)
	if err != nil {
		return fmt.Sprintf("could not read container state: %v", err)
	}
	return fmt.Sprintf("Container state: %s", state)
}

func (dm *DockerManager) apiWaitReport(ctx context.Context, containerID string) string {
	if dm.remote != nil {
		return dm.remote.apiWaitReport(ctx, containerID)
	}
	state, stateErr := dm.containerState(ctx, containerID)
	logs, logErr := "", errNoDockerClient
	if dm.client != nil {
		logs, logErr = dm.ContainerLogTailContext(ctx, containerID, command.APIWaitLogLines)
	}
	return command.APIWaitReport(containerID, state, stateErr, logs, logErr)
}

var errNoDockerClient = errors.New("no Docker client")

func (dm *DockerManager) containerState(ctx context.Context, containerID string) (command.ContainerState, error) {
	if dm.remote != nil {
		return dm.remote.containerState(ctx, containerID)
	}
	if dm.client == nil {
		return command.ContainerState{}, errNoDockerClient
	}
	inspect, err := dm.client.ContainerInspect(ctx, containerID)
	if err != nil {
		if isNoSuchContainer(err) {
			return command.ContainerState{Missing: true}, nil
		}
		return command.ContainerState{}, err
	}
	if inspect.ContainerJSONBase == nil || inspect.State == nil {
		return command.ContainerState{}, errors.New("docker inspect returned no state")
	}
	return command.ContainerState{
		Status:    inspect.State.Status,
		ExitCode:  inspect.State.ExitCode,
		OOMKilled: inspect.State.OOMKilled,
	}, nil
}

func (m *RemoteDockerManager) apiWaitReport(ctx context.Context, containerID string) string {
	state, stateErr := m.containerState(ctx, containerID)
	logs, logErr := command.ContainerLogTail(ctx, m.ops, containerID, command.APIWaitLogLines)
	return command.APIWaitReport(containerID, state, stateErr, logs, logErr)
}

func (m *RemoteDockerManager) containerState(ctx context.Context, containerID string) (command.ContainerState, error) {
	return command.InspectContainerState(ctx, m.exec, m.host, containerID)
}
