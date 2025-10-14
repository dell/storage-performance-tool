package tui

import (
	"context"
	"strings"
	"testing"
	"time"
)

// fakeLogsSince implements only the dockerLogsSince method required by remoteLogFetcher
type fakeLogsSince struct {
	lines string
	err   error
}

func (f *fakeLogsSince) GetContainerLogsSince(ctx context.Context, containerID string, since time.Time, timestamps bool, stdoutCallback, stderrCallback func(string)) error {
	_ = ctx
	_ = containerID
	_ = since
	_ = timestamps
	for _, line := range strings.Split(f.lines, "\n") {
		if line != "" {
			stdoutCallback(line)
		}
	}
	return f.err
}

type inclusiveRecordingLogsSince struct {
	timestamp time.Time
	calls     []time.Time
}

func (f *inclusiveRecordingLogsSince) GetContainerLogsSince(ctx context.Context, containerID string, since time.Time, timestamps bool, stdoutCallback, stderrCallback func(string)) error {
	_ = ctx
	_ = containerID
	_ = timestamps
	f.calls = append(f.calls, since)
	if since.IsZero() || !since.After(f.timestamp) {
		stdoutCallback(f.timestamp.Format(time.RFC3339Nano) + " inclusive line")
	}
	return nil
}

func TestRemoteLogFetcher_Poll_StripsTimestampAndAdvances(t *testing.T) {
	ts1 := "2025-09-14T12:00:01.000000000Z"
	ts2 := "2025-09-14T12:00:02.500000000Z"
	fls := &fakeLogsSince{lines: ts1 + " first line\n" + ts2 + " second line"}
	f := &remoteLogFetcher{ops: fls, containerID: "abc"}

	ctx := context.Background()
	since := time.Time{}
	lines, next, err := f.Poll(ctx, since)
	if err != nil {
		t.Fatalf("Poll returned error: %v", err)
	}
	if len(lines) == 0 || lines[0] != "first line" || lines[1] != "second line" {
		t.Fatalf("unexpected lines: %#v", lines)
	}
	if next.IsZero() {
		t.Fatalf("expected next watermark advanced, got zero")
	}
}

func TestRemoteLogFetcher_Poll_NoTimestampKeepsSince(t *testing.T) {
	fls := &fakeLogsSince{lines: "no timestamp line"}
	f := &remoteLogFetcher{ops: fls, containerID: "abc"}
	ctx := context.Background()
	s := time.Date(2025, 9, 14, 12, 0, 0, 0, time.UTC)
	lines, next, err := f.Poll(ctx, s)
	if err != nil {
		t.Fatalf("Poll error: %v", err)
	}
	if len(lines) != 1 || lines[0] != "no timestamp line" {
		t.Fatalf("unexpected lines: %#v", lines)
	}
	if !next.Equal(s) {
		t.Fatalf("expected watermark unchanged; want %v got %v", s, next)
	}
}

func TestRemoteLogFetcher_Poll_AdvancesPastInclusiveBoundary(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2025, 9, 24, 18, 12, 8, 188000000, time.UTC)
	fls := &inclusiveRecordingLogsSince{timestamp: ts}
	f := &remoteLogFetcher{ops: fls, containerID: "abc"}

	lines, next, err := f.Poll(ctx, time.Time{})
	if err != nil {
		t.Fatalf("first poll error: %v", err)
	}
	if len(lines) != 1 || lines[0] != "inclusive line" {
		t.Fatalf("unexpected first poll lines: %#v", lines)
	}
	if !next.After(ts) {
		t.Fatalf("expected watermark after last timestamp; want > %v got %v", ts, next)
	}
	if len(fls.calls) != 1 {
		t.Fatalf("expected single call recorded, got %d", len(fls.calls))
	}

	lines, _, err = f.Poll(ctx, next)
	if err != nil {
		t.Fatalf("second poll error: %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("expected no lines on second poll, got %#v", lines)
	}
	if len(fls.calls) != 2 {
		t.Fatalf("expected two calls recorded, got %d", len(fls.calls))
	}
	if !fls.calls[1].After(ts) {
		t.Fatalf("expected second call to pass since > timestamp; want > %v got %v", ts, fls.calls[1])
	}
}
