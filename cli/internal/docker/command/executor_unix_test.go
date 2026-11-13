//go:build unix

package command

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

func TestSSHAcceptsOption_ProbesWithoutUserConfig(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	fakeSSH := func(status int) string {
		path := filepath.Join(dir, "ssh-"+strconv.Itoa(status))
		script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + argsFile + "'\nexit " + strconv.Itoa(status) + "\n"
		if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}

	if !sshAcceptsOption(fakeSSH(0), constants.SSHStdinNullNo) {
		t.Fatal("an ssh client that exits 0 accepts the option")
	}
	recorded, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	// An empty config file keeps the probe independent of user config, and -G
	// prints the resolved config instead of connecting.
	if got, want := strings.Fields(string(recorded)), []string{"-G", "-F", os.DevNull, "-o", "StdinNull=no", "spt-ssh-probe.invalid"}; !slices.Equal(got, want) {
		t.Fatalf("probe args = %q, want %q", got, want)
	}
	// OpenSSH < 8.7 exits 255 with "Bad configuration option: stdinnull".
	if sshAcceptsOption(fakeSSH(255), constants.SSHStdinNullNo) {
		t.Fatal("an ssh client that rejects the option does not accept it")
	}
	if sshAcceptsOption(filepath.Join(dir, "missing"), constants.SSHStdinNullNo) {
		t.Fatal("a missing ssh client does not accept the option")
	}
}

func TestSSHAcceptsOption_RealClient(t *testing.T) {
	if _, err := exec.LookPath(constants.SSHCommand); err != nil {
		t.Skip("no ssh client on PATH")
	}
	if !sshAcceptsOption(constants.SSHCommand, constants.SSHBatchMode) {
		t.Fatal("the local ssh client should accept BatchMode=yes")
	}
	if sshAcceptsOption(constants.SSHCommand, "SptNoSuchOption=yes") {
		t.Fatal("the local ssh client should reject an unknown option")
	}
}

// startGuardedSession runs a streamed remote command the way sshd does, with
// the command line parsed by a login shell. Its stdin is a pipe that stands in
// for the SSH session: closing the returned writer simulates the session closing.
func startGuardedSession(t *testing.T, command []string) (*exec.Cmd, *bufio.Reader, *os.File) {
	t.Helper()
	host := CreateRemoteHost("remote.example.com")
	args := streamHostCommand(context.Background(), host, command, false).Args
	target := slices.Index(args, host.GetSSHTarget())
	if target < 0 {
		t.Fatalf("no ssh target in %q", args)
	}
	sessionIn, session, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", strings.Join(args[target+1:], " "))
	cmd.Stdin = sessionIn
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = sessionIn.Close()
	t.Cleanup(func() {
		_ = session.Close()
		_ = cmd.Process.Kill()
	})
	return cmd, bufio.NewReader(stdout), session
}

// finishGuardedSession drains stdout and waits for the guarded command.
func finishGuardedSession(t *testing.T, cmd *exec.Cmd, stdout io.Reader) (string, error) {
	t.Helper()
	type result struct {
		out string
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, _ := io.ReadAll(stdout)
		done <- result{string(out), cmd.Wait()}
	}()
	select {
	case r := <-done:
		return r.out, r.err
	case <-time.After(5 * time.Second):
		t.Fatal("guarded command did not exit")
		return "", nil
	}
}

func TestRemoteStreamGuard_EndsQuietCommandWhenSessionCloses(t *testing.T) {
	cmd, stdout, session := startGuardedSession(t, []string{"sh", "-c", "'echo $$; exec sleep 30'"})
	line, err := stdout.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the command's pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("unexpected first line %q", line)
	}

	_ = session.Close()
	if _, err := finishGuardedSession(t, cmd, stdout); err == nil {
		t.Fatal("expected the killed command's status")
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("quiet command %d outlived its session (kill -0: %v)", pid, err)
	}
}

func TestRemoteStreamGuard_ExitsWithCommandWhileSessionStaysOpen(t *testing.T) {
	cmd, stdout, session := startGuardedSession(t, []string{"sh", "-c", "'echo done; exit 3'"})
	out, err := finishGuardedSession(t, cmd, stdout)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("expected the command's exit status 3, got %v", err)
	}
	if out != "done\n" {
		t.Fatalf("unexpected output %q", out)
	}
	// The stdin watcher must not outlive the command, or it could later kill a
	// recycled pid. Once it is gone the session pipe has no reader.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := session.Write([]byte("\n")); errors.Is(err, syscall.EPIPE) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the guard's stdin watcher outlived the command")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pipeHolderLife is how long a test's background process holds the output
// pipes it inherited. heldPipeBound is the wide margin a call must return
// within: far below the holder's life, so a call that waits for the holder
// fails, and far above deadline plus WaitDelay, so a loaded runner does not.
const (
	pipeHolderLife = 10 * time.Second
	heldPipeBound  = 5 * time.Second
)

// pipeHolder returns a shell fragment that leaves a background process holding
// the inherited output pipes after the shell moves on. redirect selects the
// held pipes: "" holds both, ">/dev/null" only stderr (as an ssh ProxyJump
// child does), "2>/dev/null" only stdout. The holder is killed at cleanup.
func pipeHolder(t *testing.T, redirect string) string {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "holder.pid")
	t.Cleanup(func() {
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	return "sleep " + strconv.Itoa(int(pipeHolderLife/time.Second)) + " " + redirect + " & echo $! > '" + pidFile + "'"
}

// fakeOnPath puts an executable shell script called name first on PATH.
func fakeOnPath(t *testing.T, name, body string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// captureDebugLog routes the shared logger into a buffer until the test ends.
func captureDebugLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := logging.GetLogger()
	logging.SetLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { logging.SetLogger(previous) })
	return &buf
}

func requireReturnedBeforeHolder(t *testing.T, started time.Time) {
	t.Helper()
	if elapsed := time.Since(started); elapsed > heldPipeBound {
		t.Fatalf("call returned after %v, waiting on the process that held its output pipe (bound %v)", elapsed, heldPipeBound)
	}
}

// requireKilled checks that err is the deadline kill of the direct child, not
// a WaitDelay overrun.
func requireKilled(t *testing.T, err error) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected the kill's exit error, got %v", err)
	}
	if status, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("expected signal: killed, got %v", err)
	}
	if errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("a deadline kill must not report a WaitDelay overrun: %v", err)
	}
}

func TestExecuteCommand_HeldPipeAfterExitZeroIsSuccess(t *testing.T) {
	for _, tc := range []struct{ name, redirect string }{
		{"stdout and stderr held", ""},
		{"stderr held", ">/dev/null"},
		{"stdout held", "2>/dev/null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureDebugLog(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			stdout, stderr, err := NewCommandExecutor().ExecuteCommand(ctx, CreateLocalHost(),
				[]string{"sh", "-c", "echo answer; " + pipeHolder(t, tc.redirect)})
			requireReturnedBeforeHolder(t, started)
			if err != nil {
				t.Fatalf("a command that exited 0 must succeed while a descendant holds its pipe, got %v", err)
			}
			if stdout != "answer" || stderr != "" {
				t.Fatalf("output = %q, %q; want the collected output", stdout, stderr)
			}
			if !strings.Contains(logs.String(), "held the output pipe") {
				t.Fatalf("expected a debug log about the held pipe, got %q", logs.String())
			}
		})
	}
}

func TestExecuteCommand_RemoteHeldPipeAfterExitZeroIsSuccess(t *testing.T) {
	// An ssh ControlPersist master started with -v keeps the first call's stderr.
	fakeOnPath(t, constants.SSHCommand, "echo answer; "+pipeHolder(t, ">/dev/null"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	stdout, _, err := NewCommandExecutor().ExecuteCommand(ctx, CreateRemoteHost("remote.example.com"), []string{"docker", "version"})
	requireReturnedBeforeHolder(t, started)
	if err != nil || stdout != "answer" {
		t.Fatalf("got %q, %v; want answer and no error", stdout, err)
	}
}

func TestExecuteCommand_HeldPipeKeepsExitError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	stdout, stderr, err := NewCommandExecutor().ExecuteCommand(ctx, CreateLocalHost(),
		[]string{"sh", "-c", "echo partial; echo failed >&2; " + pipeHolder(t, "") + "; exit 3"})
	requireReturnedBeforeHolder(t, started)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("expected exit status 3, got %v", err)
	}
	if stdout != "partial\n" || stderr != "failed\n" {
		t.Fatalf("output = %q, %q", stdout, stderr)
	}
}

func TestExecuteCommand_NoHeldPipeReturnsAtOnce(t *testing.T) {
	logs := captureDebugLog(t)
	stdout, _, err := NewCommandExecutor().ExecuteCommand(context.Background(), CreateLocalHost(), []string{"sh", "-c", "echo answer"})
	if err != nil || stdout != "answer" {
		t.Fatalf("got %q, %v", stdout, err)
	}
	// A command that closes its pipes on exit never reaches the delay. The log
	// shows it without timing the call, which a loaded runner can slow.
	if strings.Contains(logs.String(), "held the output pipe") {
		t.Fatalf("a command that closed its pipes reached the wait delay: %q", logs.String())
	}
}

func TestExecuteCommand_DeadlineKillReturnsDespiteHeldPipe(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		started := time.Now()
		_, _, err := NewCommandExecutor().ExecuteCommand(ctx, CreateLocalHost(),
			[]string{"sh", "-c", pipeHolder(t, "") + "; sleep 10"})
		requireReturnedBeforeHolder(t, started)
		requireKilled(t, err)
	})
	t.Run("ssh with a jump child holding stderr", func(t *testing.T) {
		// Go kills ssh at the deadline; its ProxyJump child (ssh -W) keeps stderr.
		fakeOnPath(t, constants.SSHCommand, pipeHolder(t, ">/dev/null")+"; exec sleep 10")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		started := time.Now()
		_, _, err := NewCommandExecutor().ExecuteCommand(ctx, CreateRemoteHost("remote.example.com"), []string{"docker", "version"})
		requireReturnedBeforeHolder(t, started)
		requireKilled(t, err)
	})
}

// copyCalls runs each executor copy against a remote host, so it goes through scp.
var copyCalls = []struct {
	name string
	copy func(ctx context.Context, r *RealCommandExecutor) error
}{
	{"CopyFile", func(ctx context.Context, r *RealCommandExecutor) error {
		return r.CopyFile(ctx, CreateRemoteHost("remote.example.com"), "/local/items.csv", "/tmp/items.csv")
	}},
	{"CopyFromHost", func(ctx context.Context, r *RealCommandExecutor) error {
		return r.CopyFromHost(ctx, CreateRemoteHost("remote.example.com"), "/tmp/diag.tar", "/local/diag.tar")
	}},
}

func TestCopy_DeadlineKillReturnsDespiteHeldPipe(t *testing.T) {
	for _, call := range copyCalls {
		t.Run(call.name, func(t *testing.T) {
			// Go kills scp at the deadline; its ssh child keeps the pipe.
			fakeOnPath(t, constants.SCPCommand, pipeHolder(t, "")+"; exec sleep 10")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			err := call.copy(ctx, NewCommandExecutor())
			requireReturnedBeforeHolder(t, started)
			requireKilled(t, err)
		})
	}
}

func TestCopy_HeldPipeAfterExitZeroIsSuccess(t *testing.T) {
	for _, call := range copyCalls {
		t.Run(call.name, func(t *testing.T) {
			fakeOnPath(t, constants.SCPCommand, pipeHolder(t, ">/dev/null")+"; exit 0")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			err := call.copy(ctx, NewCommandExecutor())
			requireReturnedBeforeHolder(t, started)
			if err != nil {
				t.Fatalf("a copy that exited 0 must succeed while a descendant holds its pipe, got %v", err)
			}
		})
	}
}

func TestCopy_HeldPipeKeepsExitError(t *testing.T) {
	for _, call := range copyCalls {
		t.Run(call.name, func(t *testing.T) {
			fakeOnPath(t, constants.SCPCommand, "echo 'scp: permission denied' >&2; "+pipeHolder(t, "")+"; exit 1")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			err := call.copy(ctx, NewCommandExecutor())
			requireReturnedBeforeHolder(t, started)
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("expected exit status 1, got %v", err)
			}
			if !strings.Contains(err.Error(), "scp: permission denied") {
				t.Fatalf("the error must keep the copy's output, got %v", err)
			}
		})
	}
}

func TestStreamCommand_HeldPipeAfterExitZeroIsSuccess(t *testing.T) {
	for _, tc := range []struct{ name, redirect string }{
		{"stdout and stderr held", ""},
		{"stderr held", ">/dev/null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureDebugLog(t)
			var stdout []string
			started := time.Now()
			err := NewCommandExecutor().StreamCommand(context.Background(), CreateLocalHost(),
				[]string{"sh", "-c", "printf 'first\\nlast'; " + pipeHolder(t, tc.redirect)},
				func(line string) { stdout = append(stdout, line) }, func(string) {})
			requireReturnedBeforeHolder(t, started)
			if err != nil {
				t.Fatalf("a stream that exited 0 must succeed while a descendant holds its pipe, got %v", err)
			}
			// Success flushes the final unterminated line.
			if strings.Join(stdout, "|") != "first|last" {
				t.Fatalf("stdout lines = %q", stdout)
			}
			if !strings.Contains(logs.String(), "held the output pipe") {
				t.Fatalf("expected a debug log about the held pipe, got %q", logs.String())
			}
		})
	}
}

func TestStreamCommand_HeldPipeKeepsExitError(t *testing.T) {
	var stdout []string
	started := time.Now()
	err := NewCommandExecutor().StreamCommand(context.Background(), CreateLocalHost(),
		[]string{"sh", "-c", "printf 'complete\\ninterrup'; " + pipeHolder(t, "") + "; exit 255"},
		func(line string) { stdout = append(stdout, line) }, func(string) {})
	requireReturnedBeforeHolder(t, started)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 255 {
		t.Fatalf("expected exit status 255, got %v", err)
	}
	// A failure still drops the fragment.
	if strings.Join(stdout, "|") != "complete" {
		t.Fatalf("stdout lines = %q", stdout)
	}
}

// TestStreamCommand_CutDuringCallbackReportsWaitDelay holds a line callback
// past the wait delay while the command writes its last output and exits 0,
// with no descendant holding the pipe. The cut can leave that output unread, so
// the stream must report exec.ErrWaitDelay for the caller to replay, not
// succeed. The streams share one emit mutex, so a blocked stderr callback also
// holds stdout's copy.
func TestStreamCommand_CutDuringCallbackReportsWaitDelay(t *testing.T) {
	tests := []struct {
		name string
		// script writes "first" on the blocked stream; exited is appended.
		script      string
		blockStderr bool
		// complete is every stream's lines when the copy drained in time.
		complete string
	}{
		{name: "stdout callback", script: "echo first; sleep 0.1; echo final", complete: "out:first|out:final"},
		{name: "stderr callback", script: "echo first >&2; sleep 0.1; echo final >&2", blockStderr: true, complete: "err:first|err:final"},
		{name: "stdout behind a stderr callback", script: "echo first >&2; sleep 0.1; echo out1; sleep 0.1; echo out2",
			blockStderr: true, complete: "err:first|out:out1|out:out2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			exited := filepath.Join(t.TempDir(), "exited")
			var lines []string
			callback := func(prefix string, blocks bool) func(string) {
				return func(line string) {
					lines = append(lines, prefix+line)
					if !blocks || line != "first" {
						return
					}
					// Model a blocked consumer: hold the copy until the delay has cut it.
					for deadline := time.Now().Add(heldPipeBound); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
						if _, err := os.Stat(exited); err == nil {
							break
						}
					}
					time.Sleep(constants.StreamCommandWaitDelay + 500*time.Millisecond)
				}
			}
			err := NewCommandExecutor().StreamCommand(context.Background(), CreateLocalHost(),
				[]string{"sh", "-c", tt.script + "; : > '" + exited + "'"},
				callback("out:", !tt.blockStderr), callback("err:", tt.blockStderr))
			slices.Sort(lines)
			switch {
			case errors.Is(err, exec.ErrWaitDelay):
			case err == nil && strings.Join(lines, "|") == tt.complete:
				// The delay fired late enough for the copy to drain: nothing was lost.
				t.Log("the wait delay did not cut the copy on this run")
			default:
				t.Fatalf("a cut during a callback must report exec.ErrWaitDelay, got %v with lines %q", err, lines)
			}
		})
	}
}

func TestLineWriter_CutMidWrite(t *testing.T) {
	var mu sync.Mutex
	w := &lineWriter{mu: &mu}
	if w.cutMidWrite() {
		t.Fatal("a writer that never wrote cannot be cut mid-write")
	}
	before := time.Now()
	if _, err := w.Write([]byte("line\n")); err != nil {
		t.Fatal(err)
	}
	if w.lastWrite.Before(before) {
		t.Fatalf("Write must record when it returned, got %v before %v", w.lastWrite, before)
	}
	w.lastWrite = time.Now().Add(-streamWriteSettle / 5)
	if !w.cutMidWrite() {
		t.Fatal("a Write that returned just before the cut must count as caught by it")
	}
	w.lastWrite = time.Now().Add(-constants.StreamCommandWaitDelay)
	if w.cutMidWrite() {
		t.Fatal("a Write that returned a wait delay ago must count as idle")
	}
}
