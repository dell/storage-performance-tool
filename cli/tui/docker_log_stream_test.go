/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// cancelAwareLogReader returns its frames, then blocks until ctx is cancelled
// and fails with the context's error, as the Docker follow stream does when
// DockerManager.Close cancels it.
type cancelAwareLogReader struct {
	ctx    context.Context
	frames [][]byte
}

func (r *cancelAwareLogReader) Read(p []byte) (int, error) {
	if len(r.frames) > 0 {
		n := copy(p, r.frames[0])
		r.frames = r.frames[1:]
		return n, nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

type failingLogReader struct {
	frames [][]byte
	err    error
}

func (r *failingLogReader) Read(p []byte) (int, error) {
	if len(r.frames) > 0 {
		n := copy(p, r.frames[0])
		r.frames = r.frames[1:]
		return n, nil
	}
	return 0, r.err
}

type logLines struct {
	mu     sync.Mutex
	stdout []string
	stderr []string
}

func (l *logLines) onStdout(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stdout = append(l.stdout, line)
}

func (l *logLines) onStderr(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stderr = append(l.stderr, line)
}

func TestReadContainerLogs_ManagerCancellationEndsQuietly(t *testing.T) {
	dm := &DockerManager{}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelAwareLogReader{ctx: ctx, frames: [][]byte{muxFrame(1, "engine line\n")}}
	lines := &logLines{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		dm.readContainerLogs(ctx, reader, lines.onStdout, lines.onStderr)
	}()
	cancel()
	<-done

	lines.mu.Lock()
	defer lines.mu.Unlock()
	if len(lines.stdout) != 1 || lines.stdout[0] != "engine line" {
		t.Fatalf("stdout lines = %q, want [\"engine line\"]", lines.stdout)
	}
	if len(lines.stderr) != 0 {
		t.Fatalf("stderr callback got %q after the manager's own cancellation, want none", lines.stderr)
	}
}

func TestReadContainerLogs_ReportsOtherReadErrors(t *testing.T) {
	dm := &DockerManager{}
	reader := &failingLogReader{
		frames: [][]byte{muxFrame(2, "engine warning\n")},
		err:    errors.New("connection reset by peer"),
	}
	lines := &logLines{}

	dm.readContainerLogs(context.Background(), reader, lines.onStdout, lines.onStderr)

	if len(lines.stderr) != 2 || lines.stderr[0] != "engine warning" {
		t.Fatalf("stderr lines = %q, want the container line and one read error", lines.stderr)
	}
	if want := "Error reading container logs: connection reset by peer"; lines.stderr[1] != want {
		t.Fatalf("read error line = %q, want %q", lines.stderr[1], want)
	}
}

func TestReadContainerLogs_EOFEndsQuietly(t *testing.T) {
	dm := &DockerManager{}
	reader := strings.NewReader(string(muxFrame(1, "last line\n")))
	lines := &logLines{}

	dm.readContainerLogs(context.Background(), reader, lines.onStdout, lines.onStderr)

	if len(lines.stdout) != 1 || len(lines.stderr) != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want one stdout line and no error", lines.stdout, lines.stderr)
	}
}

func TestReportContainerLogsOpenError(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want []string
	}{
		{
			name: "manager context cancelled",
			ctx:  cancelled,
			err:  errors.New("error during connect: context canceled"),
		},
		{
			name: "wrapped context.Canceled",
			ctx:  context.Background(),
			err:  errors.Join(errors.New("open logs"), context.Canceled),
		},
		{
			name: "other error",
			ctx:  context.Background(),
			err:  errors.New("No such container: abc"),
			want: []string{"Error getting container logs: No such container: abc"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			reportContainerLogsOpenError(tt.ctx, tt.err, func(line string) { got = append(got, line) })
			if strings.Join(got, "\n") != strings.Join(tt.want, "\n") {
				t.Fatalf("stderr callback got %q, want %q", got, tt.want)
			}
		})
	}
}
