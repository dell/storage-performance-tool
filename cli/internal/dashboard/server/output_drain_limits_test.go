package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Intentionally implements only Reader, so io.Copy cannot bypass accounting
// through strings.Reader.WriteTo or bytes.Buffer.ReadFrom.
type countedLogReader struct {
	data     []byte
	chunk    int
	consumed int
	finalErr error
}

func (r *countedLogReader) Read(p []byte) (int, error) {
	if r.consumed == len(r.data) {
		if r.finalErr != nil {
			return 0, r.finalErr
		}
		return 0, io.EOF
	}
	n := min(len(p), r.chunk, len(r.data)-r.consumed)
	copy(p, r.data[r.consumed:r.consumed+n])
	r.consumed += n
	return n, nil
}

func TestOutputDrainByteBoundaries(t *testing.T) {
	for _, size := range []int{0, maxLogBytes - 1, maxLogBytes, maxLogBytes + 1, maxLogBytes + 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte("log-data\n"), size/9+1)[:size]
			reader := &countedLogReader{data: payload, chunk: 4093}
			path := filepath.Join(t.TempDir(), "console.log")
			drainOutput(reader, path)
			if reader.consumed != size {
				t.Fatalf("drained %d bytes; want all %d even after truncation", reader.consumed, size)
			}
			data, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, payload[:min(size, maxLogBytes)]) {
				t.Fatalf("log did not preserve exactly the bounded prefix: bytes=%d, error=%v", len(data), err)
			}
			if runtime.GOOS != "windows" {
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("log permissions: %v, %v", info, err)
				}
			}
		})
	}
}

func TestOutputDrainOpenFailuresStillConsumeInput(t *testing.T) {
	for _, kind := range []string{"existing log", "directory", "missing parent"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "console.log")
			sentinel := []byte("previous log must survive\n")
			switch kind {
			case "existing log":
				if err := os.WriteFile(path, sentinel, 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "missing parent":
				path = filepath.Join(root, "missing", "console.log")
			}
			reader := &countedLogReader{data: bytes.Repeat([]byte("x"), maxLogBytes+1024), chunk: 8191}
			drainOutput(reader, path)
			if reader.consumed != len(reader.data) {
				t.Fatal("failed log creation stopped consuming child output")
			}
			if kind == "existing log" {
				data, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(data, sentinel) {
					t.Fatal("existing log was overwritten")
				}
			}
			if kind == "missing parent" {
				if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
					t.Fatal("best-effort logging created an unexpected parent directory")
				}
			}
		})
	}
}

type logWriterFunc func([]byte) (int, error)

func (f logWriterFunc) Write(p []byte) (int, error) { return f(p) }

func TestOutputLimitedWriterConsumesAfterSinkFailure(t *testing.T) {
	for _, kind := range []string{"write error", "short write", "normal"} {
		t.Run(kind, func(t *testing.T) {
			var attempted []byte
			writer := logWriterFunc(func(p []byte) (int, error) {
				attempted = append(attempted, p...)
				switch kind {
				case "write error":
					return 0, errors.New("simulated full disk")
				case "short write":
					return len(p) / 2, nil
				default:
					return len(p), nil
				}
			})
			bounded := &limitedWriter{w: writer, left: 5}
			for _, chunk := range [][]byte{nil, []byte("ab"), []byte("cdef"), []byte("gh"), nil} {
				if n, err := bounded.Write(chunk); n != len(chunk) || err != nil {
					t.Fatalf("best-effort Write consumed %d, %v; want %d", n, err, len(chunk))
				}
			}
			if string(attempted) != "abcde" {
				t.Fatalf("sink received %q; want only the bounded prefix", attempted)
			}
		})
	}
}

func TestOutputDrainKeepsDataBeforeReaderFailure(t *testing.T) {
	reader := &countedLogReader{data: []byte("last useful output"), chunk: 3, finalErr: errors.New("simulated pipe failure")}
	path := filepath.Join(t.TempDir(), "console.log")
	drainOutput(reader, path)
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, reader.data) || reader.consumed != len(reader.data) {
		t.Fatalf("pipe error discarded preceding output: %q, %v", data, err)
	}
}

func TestOutputLimitDoesNotBlockChildCompletion(t *testing.T) {
	a, _, _ := prepareShutdownTest(t)
	payload := bytes.Repeat([]byte("large console output\n"), maxLogBytes/21+1000)
	input := filepath.Join(a.root, "large-output")
	if err := os.WriteFile(input, payload, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DASHBOARD_TEST_BULK_OUTPUT", input)
	body := "#!/bin/sh\n/bin/cat \"$DASHBOARD_TEST_BULK_OUTPUT\"\nprintf 'stderr after cap\\n' >&2\nexit 7\n"
	if err := os.WriteFile(a.sptBin, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	w := submitRecoveryRun(a)
	if w.Code != http.StatusAccepted {
		t.Fatalf("capture child launch = %d %s", w.Code, w.Body.String())
	}
	waitForRecoveryRun(t, a)
	if a.activeJob[keyStatus] != jobFailed || a.activeJob["returncode"] != 7 || a.activeCmd != nil || a.activeLock != nil || a.lockBusy() {
		t.Fatal("bounded logging prevented child completion or lost the final exit status")
	}
	data, err := os.ReadFile(filepath.Join(a.stateDir, str(a.activeJob[keyRunID])+".log"))
	if err != nil || !bytes.Equal(data, payload[:maxLogBytes]) {
		t.Fatalf("child log did not preserve the bounded prefix: bytes=%d, %v", len(data), err)
	}
}
