//go:build unix

package command

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestSCPCommandCarriesKeepalives(t *testing.T) {
	got := scpCommand(context.Background(), "/local/items.csv", "root@remote.example.com:/tmp/items.csv").Args
	want := []string{
		constants.SCPCommand,
		"-o", "ConnectTimeout=10",
		"-o", "BatchMode=yes",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"/local/items.csv", "root@remote.example.com:/tmp/items.csv",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("scp args = %q, want %q", got, want)
	}
}

// Both copy directions run the scp that scpCommand builds.
func TestCopy_PassesKeepalivesToSCP(t *testing.T) {
	target := CreateRemoteHost("remote.example.com").GetSSHTarget()
	for _, tc := range []struct {
		name         string
		copy         func(ctx context.Context, r *RealCommandExecutor) error
		source, dest string
	}{
		{"CopyFile", copyCalls[0].copy, "/local/items.csv", target + ":/tmp/items.csv"},
		{"CopyFromHost", copyCalls[1].copy, target + ":/tmp/diag.tar", "/local/diag.tar"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			argsFile := filepath.Join(t.TempDir(), "args")
			fakeOnPath(t, constants.SCPCommand, "printf '%s\\n' \"$@\" > '"+argsFile+"'")
			if err := tc.copy(context.Background(), NewCommandExecutor()); err != nil {
				t.Fatal(err)
			}
			recorded, err := os.ReadFile(argsFile) // #nosec G304 -- test temp dir
			if err != nil {
				t.Fatal(err)
			}
			got := strings.Fields(string(recorded))
			want := []string{
				"-o", "ConnectTimeout=10", "-o", "BatchMode=yes",
				"-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
				tc.source, tc.dest,
			}
			if !slices.Equal(got, want) {
				t.Fatalf("scp args = %q, want %q", got, want)
			}
		})
	}
}

// signalCause stands in for signal.NotifyContext's cause, which matches
// context.Canceled.
type signalCause struct{}

func (signalCause) Error() string        { return "interrupt signal received" }
func (signalCause) Is(target error) bool { return target == context.Canceled }

// A copy that ctx ended says it was stopped and why, and still matches the
// context's error, instead of reading as a bare "signal: killed". It returns
// within the wait delay although scp's ssh child holds the pipe.
func TestCopy_StoppedCopySaysWhy(t *testing.T) {
	unrelated := errors.New("launch abandoned")
	ends := []struct {
		name   string
		ctx    func() (context.Context, context.CancelFunc)
		is     []error
		reason string
	}{
		{"deadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 500*time.Millisecond)
		}, []error{context.DeadlineExceeded}, "stopped: context deadline exceeded ("},
		{"cancel", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(500*time.Millisecond, cancel)
			return ctx, cancel
		}, []error{context.Canceled}, "stopped: context canceled ("},
		{"signal", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancelCause(context.Background())
			time.AfterFunc(500*time.Millisecond, func() { cancel(signalCause{}) })
			return ctx, func() { cancel(nil) }
		}, []error{context.Canceled, signalCause{}}, "stopped: interrupt signal received ("},
		{"cancel with another cause", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancelCause(context.Background())
			time.AfterFunc(500*time.Millisecond, func() { cancel(unrelated) })
			return ctx, func() { cancel(nil) }
		}, []error{context.Canceled, unrelated}, "stopped: context canceled: launch abandoned ("},
	}
	for _, call := range copyCalls {
		for _, end := range ends {
			t.Run(call.name+"/"+end.name, func(t *testing.T) {
				fakeOnPath(t, constants.SCPCommand, pipeHolder(t, "")+"; echo 'Warning: Permanently added' >&2; exec sleep 10")
				ctx, cancel := end.ctx()
				defer cancel()
				started := time.Now()
				err := call.copy(ctx, NewCommandExecutor())
				requireReturnedBeforeHolder(t, started)
				requireKilled(t, err)
				for _, target := range end.is {
					if !errors.Is(err, target) {
						t.Fatalf("error %q does not match %v", err, target)
					}
				}
				msg := err.Error()
				if !strings.Contains(msg, end.reason) || !strings.Contains(msg, "(scp: signal: killed: Warning: Permanently added)") {
					t.Fatalf("error %q, want %q and the scp detail", msg, end.reason)
				}
				if !strings.HasPrefix(msg, "copy ") || !strings.Contains(msg, "remote.example.com") {
					t.Fatalf("error %q does not name the copy and its host", msg)
				}
			})
		}
	}
}

func TestCopy_FailureNamesTheCopyAndWhatScpSaid(t *testing.T) {
	fakeOnPath(t, constants.SCPCommand, "printf 'Timeout, server remote.example.com not responding.\\r\\nlost connection\\n' >&2; exit 1")
	err := copyCalls[0].copy(context.Background(), NewCommandExecutor())
	want := "copy /local/items.csv to remote.example.com:/tmp/items.csv failed: exit status 1: " +
		"Timeout, server remote.example.com not responding.\r\nlost connection"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a copy that failed on its own must not read as stopped: %v", err)
	}

	fakeOnPath(t, constants.SCPCommand, "exit 1")
	err = copyCalls[0].copy(context.Background(), NewCommandExecutor())
	if want := "copy /local/items.csv to remote.example.com:/tmp/items.csv failed: exit status 1"; err == nil || err.Error() != want {
		t.Fatalf("error without scp output = %q, want %q", err, want)
	}
}
