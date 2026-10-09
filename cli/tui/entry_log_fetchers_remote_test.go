package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// followSession scripts one FollowContainerLogs call.
type followSession struct {
	stdout []string
	stderr []string
	err    error
	block  bool // block until ctx is cancelled after emitting
}

// scriptedFollower replays one session per call and blocks once the script runs out.
type scriptedFollower struct {
	mu       sync.Mutex
	sessions []followSession
	sinces   []time.Time
	called   chan struct{}
}

func (f *scriptedFollower) FollowContainerLogs(ctx context.Context, _ string, since time.Time, stdoutCallback, stderrCallback func(string)) error {
	f.mu.Lock()
	f.sinces = append(f.sinces, since)
	n := len(f.sinces)
	f.mu.Unlock()
	if f.called != nil {
		f.called <- struct{}{}
	}
	if n > len(f.sessions) {
		<-ctx.Done()
		return ctx.Err()
	}
	s := f.sessions[n-1]
	for _, line := range s.stdout {
		stdoutCallback(line)
	}
	for _, line := range s.stderr {
		stderrCallback(line)
	}
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.err
}

func (f *scriptedFollower) recordedSinces() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.sinces...)
}

func newTestRemoteLogFetcher(ops dockerLogsFollower, reconnect time.Duration) *remoteLogFetcher {
	return &remoteLogFetcher{ops: ops, containerID: "abc", reconnectMin: reconnect, reconnectMax: reconnect}
}

var errSessionDropped = errors.New("exit status 255")

func TestRemoteLogFetcher_Stream_RelaysStdoutAndEndsWithContainer(t *testing.T) {
	follower := &scriptedFollower{sessions: []followSession{{
		stdout: []string{
			"2025-09-14T12:00:01.000000000Z first line",
			"2025-09-14T12:00:02.500000000Z second line",
		},
		stderr: []string{
			"2025-09-14T12:00:03.000000000Z engine warning",
			"Connection to worker-1 closed.",
		},
	}}}
	f := newTestRemoteLogFetcher(follower, time.Millisecond)

	var got []string
	if err := f.Stream(context.Background(), func(s string) { got = append(got, s) }); err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}
	if want := "first line|second line"; strings.Join(got, "|") != want {
		t.Fatalf("relayed lines = %q, want %q (stderr, including ssh/docker diagnostics, must not be relayed)", got, want)
	}
	if sinces := follower.recordedSinces(); len(sinces) != 1 || !sinces[0].IsZero() {
		t.Fatalf("expected one session from container start, got %v", sinces)
	}
}

func TestRemoteLogFetcher_Stream_ResumesPastLastTimestamp(t *testing.T) {
	last := time.Date(2025, 9, 24, 18, 12, 8, 188000000, time.UTC)
	follower := &scriptedFollower{sessions: []followSession{
		{stdout: []string{"2025-09-24T18:12:07.000000000Z a", last.Format(time.RFC3339Nano) + " b"}, err: errSessionDropped},
		{err: errSessionDropped},
		{stdout: []string{"2025-09-24T18:12:09.000000000Z c"}},
	}}
	f := newTestRemoteLogFetcher(follower, time.Millisecond)

	var got []string
	if err := f.Stream(context.Background(), func(s string) { got = append(got, s) }); err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}
	if strings.Join(got, "|") != "a|b|c" {
		t.Fatalf("relayed lines = %q", got)
	}
	sinces := follower.recordedSinces()
	if len(sinces) != 3 {
		t.Fatalf("expected three sessions, got %d", len(sinces))
	}
	// docker logs --since is inclusive; resuming exactly past the last line avoids duplicates,
	// and a session that relayed nothing must not move the watermark.
	want := last.Add(dockerSinceInclusiveSkew)
	if !sinces[1].Equal(want) || !sinces[2].Equal(want) {
		t.Fatalf("resume watermarks = %v, want %v for both", sinces[1:], want)
	}
}

func TestRemoteLogFetcher_Stream_ResumePointOnlyMovesForward(t *testing.T) {
	newest := time.Date(2025, 9, 14, 12, 0, 5, 0, time.UTC)
	follower := &scriptedFollower{sessions: []followSession{
		{
			// Stdout and stderr arrive through separate pipes, so older stderr can
			// follow newer stdout; a skewed stdout timestamp must not rewind either.
			stdout: []string{newest.Format(time.RFC3339Nano) + " newer", "2025-09-14T12:00:03.000000000Z skewed"},
			stderr: []string{"2025-09-14T12:00:04.000000000Z older stderr"},
			err:    errSessionDropped,
		},
		{stdout: []string{"2025-09-14T12:00:06.000000000Z next"}},
	}}
	f := newTestRemoteLogFetcher(follower, time.Millisecond)

	var got []string
	if err := f.Stream(context.Background(), func(s string) { got = append(got, s) }); err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}
	if strings.Join(got, "|") != "newer|skewed|next" {
		t.Fatalf("relayed lines = %q", got)
	}
	sinces := follower.recordedSinces()
	if len(sinces) != 2 || !sinces[1].Equal(newest.Add(dockerSinceInclusiveSkew)) {
		t.Fatalf("resume watermarks = %v, want just past %v", sinces, newest)
	}
}

func TestRemoteLogFetcher_Stream_ReturnsWhenCancelledDuringBackoff(t *testing.T) {
	follower := &scriptedFollower{
		sessions: []followSession{{err: errSessionDropped}},
		called:   make(chan struct{}, 1),
	}
	f := newTestRemoteLogFetcher(follower, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- f.Stream(ctx, func(string) {}) }()

	<-follower.called
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("expected nil after cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not return while waiting to reconnect")
	}
	if n := len(follower.recordedSinces()); n != 1 {
		t.Fatalf("expected no reconnect after cancellation, got %d sessions", n)
	}
}

func TestRemoteLogFetcher_Stream_RejectsMissingContainer(t *testing.T) {
	f := &remoteLogFetcher{ops: &scriptedFollower{}}
	if err := f.Stream(context.Background(), func(string) {}); err == nil {
		t.Fatal("expected an error without a container ID")
	}
}

// streamingRemoteExecutor records remote commands and holds each stream open until cancelled.
type streamingRemoteExecutor struct {
	mu       sync.Mutex
	streams  [][]string
	executed [][]string
}

func (e *streamingRemoteExecutor) ExecuteCommand(_ context.Context, _ *hostparse.HostInfo, command []string) (string, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executed = append(e.executed, command)
	return "", "", nil
}

func (e *streamingRemoteExecutor) CopyFile(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (e *streamingRemoteExecutor) CopyFromHost(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (e *streamingRemoteExecutor) StreamCommand(ctx context.Context, _ *hostparse.HostInfo, command []string, stdoutLine, _ func(string)) error {
	e.mu.Lock()
	e.streams = append(e.streams, command)
	e.mu.Unlock()
	stdoutLine("2025-09-14T12:00:01.000000000Z engine ready")
	<-ctx.Done()
	return ctx.Err()
}

func (e *streamingRemoteExecutor) counts() (streams, executed int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.streams), len(e.executed)
}

func newRelayTestOrchestrator(t *testing.T, dm *DockerManager) (*MultiHostTestOrchestrator, chan string) {
	t.Helper()
	host := &hostparse.HostInfo{Host: "worker-1", User: "spt", Original: "spt@worker-1"}
	m := &MultiHostTestOrchestrator{multiHost: &MultiHostOrchestrator{hosts: []*HostConnection{
		{Info: host, DockerManager: dm, ContainerID: "entry123"},
	}}}
	lines := make(chan string, 16)
	m.messageSink = func(s string) { lines <- s }
	return m, lines
}

func TestStartEntryLogRelay_RemoteEntryUsesOneSSHStream(t *testing.T) {
	exec := &streamingRemoteExecutor{}
	rdm, err := newRemoteDockerManagerWithExecutor(&hostparse.HostInfo{Host: "worker-1", User: "spt"}, exec)
	if err != nil {
		t.Fatal(err)
	}
	m, lines := newRelayTestOrchestrator(t, &DockerManager{remote: rdm})

	m.startEntryLogRelay(context.Background())
	if _, ok := m.entryRelay.fetcher.(*remoteLogFetcher); !ok {
		t.Fatalf("remote entry node should use remoteLogFetcher, got %T", m.entryRelay.fetcher)
	}
	select {
	case line := <-lines:
		if line != "[SPT] engine ready" {
			t.Fatalf("unexpected relayed line %q", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no line relayed from the remote stream")
	}
	// Span several of the former 500ms poll intervals: the stream must stay a single session.
	time.Sleep(1200 * time.Millisecond)
	m.entryRelay.Stop()

	streams, executed := exec.counts()
	if streams != 1 || executed != 0 {
		t.Fatalf("expected exactly one streamed session and no one-shot commands, got streams=%d executed=%d", streams, executed)
	}
	if got := strings.Join(exec.streams[0], " "); got != "docker logs --follow --timestamps entry123" {
		t.Fatalf("unexpected streamed command %q", got)
	}
}

func TestStartEntryLogRelay_LocalEntryUsesDockerAPINotSSH(t *testing.T) {
	exec := &streamingRemoteExecutor{}
	rdm, err := newRemoteDockerManagerWithExecutor(&hostparse.HostInfo{Host: "worker-1", User: "spt"}, exec)
	if err != nil {
		t.Fatal(err)
	}
	m, lines := newRelayTestOrchestrator(t, &DockerManager{client: &fakeDockerClient{}, remote: rdm})

	m.startEntryLogRelay(context.Background())
	if _, ok := m.entryRelay.fetcher.(*sdkLogFetcher); !ok {
		t.Fatalf("entry node with a Docker API client should use sdkLogFetcher, got %T", m.entryRelay.fetcher)
	}
	for _, want := range []string{"[SPT] log1", "[SPT] log2"} {
		select {
		case line := <-lines:
			if line != want {
				t.Fatalf("relayed %q, want %q", line, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no line relayed, want %q", want)
		}
	}
	m.entryRelay.Stop()
	if streams, executed := exec.counts(); streams != 0 || executed != 0 {
		t.Fatalf("Docker API route must not use SSH, got streams=%d executed=%d", streams, executed)
	}
}
