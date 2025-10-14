package tui

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/docker/docker/api/types/container"
)

// sdkLogFetcher uses the local Docker SDK client to stream logs.
type sdkLogFetcher struct {
	dm          *DockerManager
	containerID string
}

func newSDKLogFetcher(dm *DockerManager, containerID string) *sdkLogFetcher {
	return &sdkLogFetcher{dm: dm, containerID: containerID}
}

func (f *sdkLogFetcher) Stream(ctx context.Context, onLine func(string)) error {
	if f.dm == nil || f.dm.client == nil || f.containerID == "" {
		return fmt.Errorf("invalid sdkLogFetcher configuration")
	}

	opts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: false,
	}
	rc, err := f.dm.client.ContainerLogs(ctx, f.containerID, opts)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			slog.Warn("Failed to close container log stream", "component", "entry-log", "error", err)
		}
	}()

	buf := make([]byte, 4096)
	for {
		n, rerr := rc.Read(buf)
		if rerr != nil {
			if rerr != io.EOF {
				return rerr
			}
			return nil
		}
		if n <= 0 {
			continue
		}
		// Docker SDK uses multiplexed headers; reuse DockerManager parsing.
		parsed := f.dm.processDockerOutput(buf[:n])
		for _, s := range parsed.StdoutLines {
			onLine(s)
		}
		for _, s := range parsed.StderrLines {
			onLine(s)
		}
	}
}

func (f *sdkLogFetcher) Poll(_ context.Context, _ time.Time) ([]string, time.Time, error) {
	// Polling is not used for SDK fetcher.
	return nil, time.Time{}, nil
}
