/*
Copyright © 2026 Dell Technologies
*/

package headless

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func captureHeadlessStderr(t *testing.T, run func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stderr
	os.Stderr = writer
	output := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(reader)
		output <- data
	}()
	run()
	os.Stderr = previous
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := <-output
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func newTraceRunner(t *testing.T) (*HeadlessRunner, string) {
	t.Helper()
	tracePath := filepath.Join(t.TempDir(), "trace.log")
	runner, err := NewHeadlessRunner(nil, HeadlessOptions{TraceFile: tracePath})
	if err != nil {
		t.Fatal(err)
	}
	return runner, tracePath
}

func TestHeadlessRunnerOutputAfterCloseSkipsTraceFile(t *testing.T) {
	runner, tracePath := newTraceRunner(t)

	var stdout string
	stderr := captureHeadlessStderr(t, func() {
		stdout = captureHeadlessStdout(t, func() {
			runner.output("SPT", "before close")
			if err := runner.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			runner.output("ERROR", "late stream line")
			runner.outputJSONLine([]byte(`{"late":true}`))
		})
	})

	if stderr != "" {
		t.Fatalf("stderr = %q, want empty (no write to the closed trace file)", stderr)
	}
	if !strings.Contains(stdout, "late stream line") || !strings.Contains(stdout, `{"late":true}`) {
		t.Fatalf("stdout = %q, want the late lines still printed", stdout)
	}
	trace, err := os.ReadFile(tracePath) // #nosec G304 -- test temp file
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(trace), "before close") {
		t.Fatalf("trace = %q, want the line written before Close", trace)
	}
	if strings.Contains(string(trace), "late stream line") || strings.Contains(string(trace), `{"late":true}`) {
		t.Fatalf("trace = %q, want no line written after Close", trace)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestHeadlessRunnerOutputConcurrentWithClose runs output on another goroutine
// while Close runs, as the Docker log stream does at the end of a run; with
// -race it fails if the trace file is not guarded.
func TestHeadlessRunnerOutputConcurrentWithClose(t *testing.T) {
	runner, _ := newTraceRunner(t)

	stderr := captureHeadlessStderr(t, func() {
		_ = captureHeadlessStdout(t, func() {
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 200; i++ {
					runner.output("SPT", "stream line %d", i)
					runner.outputJSONLine([]byte(`{"stream":true}`))
				}
			}()
			close(start)
			if err := runner.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			wg.Wait()
		})
	})

	if stderr != "" {
		lines := strings.Split(strings.TrimSpace(stderr), "\n")
		t.Fatalf("stderr has %d lines, first %q, want empty", len(lines), lines[0])
	}
}
