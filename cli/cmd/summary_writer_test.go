package cmd

import (
	"strings"
	"sync"
	"testing"
	"time"
)

type sinkCollector struct {
	mu    sync.Mutex
	lines []string
}

func (s *sinkCollector) append(line string) {
	s.mu.Lock()
	s.lines = append(s.lines, line)
	s.mu.Unlock()
}

func (s *sinkCollector) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.lines))
	copy(out, s.lines)
	return out
}

func waitForLines(t *testing.T, sink *sinkCollector, want int) []string {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		lines := sink.snapshot()
		if len(lines) >= want {
			return lines
		}
		time.Sleep(5 * time.Millisecond)
	}
	return sink.snapshot()
}

func TestSummaryMessageWriterBuffersBeforeSink(t *testing.T) {
	w := newSummaryMessageWriter()
	if _, err := w.Write([]byte("line1\nline2\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if len(w.buffer) != 2 {
		t.Fatalf("expected buffer length 2, got %d", len(w.buffer))
	}
	sink := &sinkCollector{}
	w.SetSink(sink.append)
	lines := waitForLines(t, sink, 2)
	if len(lines) != 2 {
		t.Fatalf("expected sink to receive 2 lines, got %d", len(lines))
	}
	if lines[0] != "line1" || lines[1] != "line2" {
		t.Fatalf("unexpected sink lines: %v", lines)
	}
}

func TestSummaryMessageWriterFlushesPartial(t *testing.T) {
	w := newSummaryMessageWriter()
	sink := &sinkCollector{}
	w.SetSink(sink.append)
	if _, err := w.Write([]byte("line1\nline2")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	lines := waitForLines(t, sink, 1)
	if len(lines) != 1 || lines[0] != "line1" {
		t.Fatalf("unexpected sink lines after write: %v", lines)
	}
	w.Flush()
	lines = waitForLines(t, sink, 2)
	if len(lines) != 2 {
		t.Fatalf("expected flush to emit second line, got %v", lines)
	}
	if lines[1] != "line2" {
		t.Fatalf("expected \"line2\", got %q", lines[1])
	}
}

func TestSummaryMessageWriterTrimsBlankAndCR(t *testing.T) {
	w := newSummaryMessageWriter()
	sink := &sinkCollector{}
	w.SetSink(sink.append)
	if _, err := w.Write([]byte("line1\r\n\n   \nline2\r\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	lines := waitForLines(t, sink, 2)
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if !strings.EqualFold(lines[0], "line1") {
		t.Fatalf("expected first line line1, got %q", lines[0])
	}
	if !strings.EqualFold(lines[1], "line2") {
		t.Fatalf("expected second line line2, got %q", lines[1])
	}
}
