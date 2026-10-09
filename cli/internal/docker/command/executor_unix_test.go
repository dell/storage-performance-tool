//go:build unix

package command

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
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
