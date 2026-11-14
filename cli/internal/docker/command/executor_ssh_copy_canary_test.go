//go:build unix && ssh_wait_delay_canary

package command

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// Bounds for the stalled-copy canary. scp's keepalives give up about 60 s
// after the peer's last reply (59.3 s measured in the triage); the window
// leaves room for a loaded host on both sides. A cancel returns within the
// wait delay, with the same margin as TestSSHWaitDelayCanary.
const (
	stalledCopyKeepaliveMin = 30 * time.Second
	stalledCopyKeepaliveMax = 90 * time.Second
	stalledCopyFileSize     = 64 << 20
	stalledCopyRateKbit     = "16000" // 2 MB/s: the copy takes 32 s unless it stalls
	stalledCopyInFlight     = 1 << 20
)

// TestSSHStalledCopyCanary runs CopyFile through real scp against a throwaway
// loopback sshd (see TestSSHWaitDelayCanary) and stops the server's session
// processes with SIGSTOP mid-copy, as the triage's probe did: the peer stops
// answering while TCP stays up. With no deadline, scp's keepalives end the
// copy in about 60 s; a cancel ends a stalled or a live copy within the wait
// delay, with an error that says the copy was stopped. The frozen sessions are
// killed at the end, and no ssh client or session process may outlive them.
//
//	go test -tags ssh_wait_delay_canary ./internal/docker/command -run SSHStalledCopyCanary -v
func TestSSHStalledCopyCanary(t *testing.T) {
	env := startCanarySSHD(t)
	env.useLimitedSCP(t, stalledCopyRateKbit)
	src := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(src, make([]byte, stalledCopyFileSize), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("keepalives end a stalled copy with no deadline", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "items.csv")
		done := startCopy(context.Background(), canaryHost("spt-copy"), src, dest)
		waitInFlight(t, dest, done)
		frozenAt := env.freezeSessions(t)
		err, elapsed := awaitCopy(t, done, frozenAt, stalledCopyKeepaliveMax)
		requireEndedOnKeepalives(t, err, elapsed)
		t.Logf("returned %v after the freeze: %v", elapsed.Round(100*time.Millisecond), err)
	})

	t.Run("a cancel ends a stalled copy", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "items.csv")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startCopy(ctx, canaryHost("spt-copy"), src, dest)
		waitInFlight(t, dest, done)
		env.freezeSessions(t)
		time.Sleep(2 * time.Second) // let the client block on the frozen peer
		cancel()
		requireStoppedByCancel(t, done, time.Now())
	})

	t.Run("a cancel ends a live copy", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "items.csv")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startCopy(ctx, canaryHost("spt-copy"), src, dest)
		waitInFlight(t, dest, done)
		cancel()
		requireStoppedByCancel(t, done, time.Now())
		env.requireNoClientsLeft(t)
	})
}

// TestSSHSharedConnectionCopyCanary is the keepalive case of
// TestSSHStalledCopyCanary through a shared connection, as a user's config
// with ControlMaster auto makes it: spt-mux shares connections through a
// ControlPersist process and sets no keepalives of its own. An ExecuteCommand
// opens the control process, the copy rides it (sshd accepts no new
// connection), and the sessions are frozen mid-copy. scp's ssh, a client of
// the shared connection, ignores its own keepalives: the copy ends only
// because the control process carries hostCommand's, and its exit takes the
// copy down with it.
//
//	go test -tags ssh_wait_delay_canary ./internal/docker/command -run SSHSharedConnectionCopyCanary -v
func TestSSHSharedConnectionCopyCanary(t *testing.T) {
	const alias = "spt-mux"
	env := startCanarySSHD(t)
	env.useLimitedSCP(t, stalledCopyRateKbit)
	env.requireSharedWithoutKeepalives(t, alias)
	src := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(src, make([]byte, stalledCopyFileSize), 0o600); err != nil {
		t.Fatal(err)
	}
	host := canaryHost(alias)
	if stdout, err := runWithin(t, 10*time.Second, host, []string{"echo", "ready"}); err != nil || stdout != "ready" {
		t.Fatalf("opening the control process = %q, %v", stdout, err)
	}
	env.requireControlProcess(t, alias)
	before := env.acceptedSessions(t)

	dest := filepath.Join(t.TempDir(), "items.csv")
	done := startCopy(context.Background(), host, src, dest)
	waitInFlight(t, dest, done)
	if after := env.acceptedSessions(t); after != before {
		t.Fatalf("sshd accepted %d connection(s) for the copy; it did not ride the control process", after-before)
	}
	frozenAt := env.freezeSessions(t)
	err, elapsed := awaitCopy(t, done, frozenAt, stalledCopyKeepaliveMax)
	requireEndedOnKeepalives(t, err, elapsed)
	if out, checkErr := env.controlCheck(alias); checkErr == nil {
		t.Fatalf("the control process outlived the copy it should have ended: %s", out)
	}
	t.Logf("returned %v after the freeze, with the control process gone: %v", elapsed.Round(100*time.Millisecond), err)
}

func startCopy(ctx context.Context, host *hostparse.HostInfo, src, dest string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- NewCommandExecutor().CopyFile(ctx, host, src, dest) }()
	return done
}

// requireEndedOnKeepalives checks that a stalled copy failed on its own, with
// ssh's exit status once the keepalives went unanswered, and not before they
// could have.
func requireEndedOnKeepalives(t *testing.T, err error, elapsed time.Duration) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() <= 0 {
		t.Fatalf("want scp's own exit status after the keepalives failed, got %v", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), " failed: ") {
		t.Fatalf("a copy that ended on keepalives must read as failed, not stopped: %v", err)
	}
	if elapsed < stalledCopyKeepaliveMin {
		t.Fatalf("the copy ended %v after the freeze, before the keepalives could fail: %v", elapsed, err)
	}
}

// requireSharedWithoutKeepalives checks that the canary's client config shares
// connections for alias and sets no keepalives, so only SPT's can end a
// stalled session within the canary's bound. It resolves the config without
// SPT's options: on Debian and Ubuntu, BatchMode=yes alone sets a 300 s
// ServerAliveInterval, which still ends a stalled session only long after
// that bound.
func (e canarySSHD) requireSharedWithoutKeepalives(t *testing.T, alias string) {
	t.Helper()
	out, err := exec.Command("/usr/bin/ssh", "-G", "-F", e.config, alias).Output() // #nosec G204 -- test-only canary
	if err != nil {
		t.Fatalf("ssh -G %s: %v", alias, err)
	}
	resolved := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if key, value, ok := strings.Cut(line, " "); ok {
			resolved[key] = value
		}
	}
	for key, want := range map[string]string{"controlmaster": "auto", "serveraliveinterval": "0"} {
		if resolved[key] != want {
			t.Fatalf("the client config sets %s %q for %s, want %q", key, resolved[key], alias, want)
		}
	}
	if !strings.HasPrefix(resolved["controlpath"], e.dir) || resolved["controlpersist"] == "" || resolved["controlpersist"] == "no" {
		t.Fatalf("want a ControlPath in %s and ControlPersist for %s, got %q and %q",
			e.dir, alias, resolved["controlpath"], resolved["controlpersist"])
	}
}

// waitInFlight waits until the copy has written stalledCopyInFlight bytes.
func waitInFlight(t *testing.T, dest string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("the copy ended before it was in flight: %v", err)
		default:
		}
		if info, err := os.Stat(dest); err == nil && info.Size() >= stalledCopyInFlight {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy did not get under way")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// awaitCopy waits up to bound after since for the copy to return.
func awaitCopy(t *testing.T, done <-chan error, since time.Time, bound time.Duration) (error, time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the copy completed; the peer never stalled it")
		}
		return err, time.Since(since)
	case <-time.After(bound - time.Since(since)):
		t.Fatalf("the copy was still blocked %v after %v", bound, since.Format(time.TimeOnly))
		return nil, 0
	}
}

func requireStoppedByCancel(t *testing.T, done <-chan error, cancelledAt time.Time) {
	t.Helper()
	err, elapsed := awaitCopy(t, done, cancelledAt, constants.CommandWaitDelay+3*time.Second)
	requireKilled(t, err)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), " stopped: context canceled (scp: signal: killed") {
		t.Fatalf("want the copy stopped by the cancel, got %v", err)
	}
	t.Logf("returned %v after the cancel: %v", elapsed.Round(10*time.Millisecond), err)
}

// useLimitedSCP is usePath with scp limited to rateKbit, so a copy is still
// running when the test acts on it.
func (e canarySSHD) useLimitedSCP(t *testing.T, rateKbit string) {
	t.Helper()
	bin := t.TempDir()
	for name, script := range map[string]string{
		constants.SSHCommand: "exec /usr/bin/ssh -F '" + e.config + "' \"$@\"\n",
		constants.SCPCommand: "exec /usr/bin/scp -l " + rateKbit + " -F '" + e.config + "' \"$@\"\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// freezeSessions stops every process the canary sshd started for its
// connections, so the peer stops answering while TCP stays up. The test's
// cleanup kills them and then checks that no client is left.
func (e canarySSHD) freezeSessions(t *testing.T) time.Time {
	t.Helper()
	sessions := descendantsOf(e.pid)
	if len(sessions) == 0 {
		t.Fatal("the canary sshd has no session processes to freeze")
	}
	for _, pid := range sessions {
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			t.Fatalf("freeze session process %d: %v", pid, err)
		}
	}
	frozenAt := time.Now()
	t.Cleanup(func() {
		for _, pid := range sessions {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		e.requireNoClientsLeft(t)
	})
	t.Logf("froze %d session process(es)", len(sessions))
	return frozenAt
}

// requireNoClientsLeft waits for every ssh or scp client of the canary's
// config, and every session process of its sshd, to exit.
func (e canarySSHD) requireNoClientsLeft(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		left := append(processesMentioning(e.config), descendantsOf(e.pid)...)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, pid := range left {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
			t.Fatalf("processes outlived the copy: %v", left)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// descendantsOf lists root's live descendants from /proc.
func descendantsOf(root int) []int {
	children := map[int][]int{}
	for pid := range procPIDs() {
		if parent := procParent(pid); parent > 0 {
			children[parent] = append(children[parent], pid)
		}
	}
	var out []int
	queue := []int{root}
	for len(queue) > 0 {
		next := children[queue[0]]
		queue = append(queue[1:], next...)
		out = append(out, next...)
	}
	return out
}

// processesMentioning lists this user's processes whose command line contains s.
func processesMentioning(s string) []int {
	var out []int
	for pid := range procPIDs() {
		cmdline, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline")) // #nosec G304 -- /proc of this host
		if err == nil && strings.Contains(string(cmdline), s) {
			out = append(out, pid)
		}
	}
	return out
}

func procPIDs() map[int]struct{} {
	entries, _ := os.ReadDir("/proc")
	pids := make(map[int]struct{}, len(entries))
	for _, entry := range entries {
		if pid, err := strconv.Atoi(entry.Name()); err == nil && pid != os.Getpid() {
			pids[pid] = struct{}{}
		}
	}
	return pids
}

func procParent(pid int) int {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat")) // #nosec G304 -- /proc of this host
	if err != nil {
		return 0
	}
	// The command name in field 2 may hold spaces; the parent is the second field after it.
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 2 {
		return 0
	}
	parent, _ := strconv.Atoi(fields[1])
	return parent
}
