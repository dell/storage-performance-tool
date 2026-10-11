package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// dockerLogsFollower is a narrow seam for tests; it matches the single method we need.
type dockerLogsFollower interface {
	FollowContainerLogs(ctx context.Context, containerID string, since time.Time, stdoutCallback, stderrCallback func(string)) error
}

// remoteLogFetcher follows a remote container's stdout over one long-lived
// session instead of opening a new SSH session per poll. If the session drops,
// it resumes just after the newest relayed timestamp.
type remoteLogFetcher struct {
	ops          dockerLogsFollower
	containerID  string
	reconnectMin time.Duration
	reconnectMax time.Duration
}

const dockerSinceInclusiveSkew = time.Nanosecond

func newRemoteLogFetcher(ops command.DockerOperations, containerID string) *remoteLogFetcher {
	return &remoteLogFetcher{
		ops:          ops,
		containerID:  containerID,
		reconnectMin: constants.EntryLogRelayReconnectMin,
		reconnectMax: constants.EntryLogRelayReconnectMax,
	}
}

func (f *remoteLogFetcher) Stream(ctx context.Context, onLine func(string)) error {
	if f.ops == nil || f.containerID == "" {
		return fmt.Errorf("invalid remoteLogFetcher configuration")
	}
	var since time.Time
	delay := f.reconnectMin
	warned := false
	for {
		relayed := false
		diagnostic := ""
		err := f.ops.FollowContainerLogs(ctx, f.containerID, since, func(line string) {
			text, ts, ok := splitDockerTimestamp(line)
			// docker logs --since is inclusive, so resume just past the newest relayed line.
			if resume := ts.Add(dockerSinceInclusiveSkew); ok && resume.After(since) {
				since = resume
			}
			onLine(text)
			relayed = true
		}, func(line string) {
			// Only stdout is relayed: it arrives in timestamp order, while stderr
			// comes through a separate pipe, so one resume point cannot cover both.
			// Timestamped stderr is the engine's (JVM and logging warnings); the rest
			// comes from ssh or the docker CLI.
			if text, _, ok := splitDockerTimestamp(line); ok {
				logging.LogDebug("entry-log-relay", "remote engine stderr", "line", text)
				return
			}
			diagnostic = line
		})
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			// docker logs --follow exits cleanly once the container stops.
			return nil
		}
		if relayed {
			delay = f.reconnectMin
		}
		if !warned {
			logging.LogWarn("entry-log-relay", "remote log stream dropped; resuming",
				"error", err.Error(), "detail", diagnostic, "retry_in", delay.String())
			warned = true
		} else {
			logging.LogDebug("entry-log-relay", "remote log stream dropped; resuming",
				"error", err.Error(), "detail", diagnostic, "retry_in", delay.String())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(2*delay, f.reconnectMax)
	}
}

// splitDockerTimestamp separates the RFC3339Nano prefix that docker logs
// --timestamps adds to each line. ok is false when the line has no timestamp.
func splitDockerTimestamp(line string) (text string, ts time.Time, ok bool) {
	prefix, rest, found := strings.Cut(line, " ")
	if !found {
		return line, time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, prefix)
	if err != nil {
		return line, time.Time{}, false
	}
	return rest, ts, true
}
