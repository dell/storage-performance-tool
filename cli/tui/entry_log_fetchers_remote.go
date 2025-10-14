package tui

import (
	"context"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
)

// remoteLogFetcher polls docker logs via CLI on a remote host with --since and --timestamps.
// dockerLogsSince is a narrow seam for tests; it matches the single method we need.
type dockerLogsSince interface {
	GetContainerLogsSince(ctx context.Context, containerID string, since time.Time, timestamps bool, stdoutCallback, stderrCallback func(string)) error
}

type remoteLogFetcher struct {
	ops         dockerLogsSince
	containerID string
}

const dockerSinceInclusiveSkew = time.Nanosecond

func newRemoteLogFetcher(ops command.DockerOperations, containerID string) *remoteLogFetcher {
	return &remoteLogFetcher{ops: ops, containerID: containerID}
}

func (f *remoteLogFetcher) Stream(context.Context, func(string)) error { return nil }

func (f *remoteLogFetcher) Poll(ctx context.Context, since time.Time) ([]string, time.Time, error) {
	if f.ops == nil || f.containerID == "" {
		return nil, since, nil
	}
	// Collect lines into a slice
	lines := make([]string, 0, 64)
	_ = f.ops.GetContainerLogsSince(ctx, f.containerID, since, true,
		func(line string) { lines = append(lines, line) },
		func(line string) { lines = append(lines, line) },
	)

	// Parse timestamps and strip them from output
	var maxTS time.Time
	for i, ln := range lines {
		// Expect: "<RFC3339...> <rest>"
		sp := strings.SplitN(ln, " ", 2)
		if len(sp) == 2 {
			if ts, err := time.Parse(time.RFC3339Nano, sp[0]); err == nil {
				if ts.After(maxTS) {
					maxTS = ts
				}
				lines[i] = sp[1]
				continue
			}
		}
		// No timestamp: keep as-is
	}
	if maxTS.IsZero() {
		return lines, since, nil
	}
	// docker logs --since is inclusive, so advance past the last timestamp to avoid re-emitting the final line
	next := maxTS.Add(dockerSinceInclusiveSkew)
	return lines, next, nil
}
