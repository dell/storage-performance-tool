//go:build unix && ssh_wait_delay_canary

package command

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// silentHold is how long the silent listener keeps each connection open
// without sending a banner. A call that waits for it fails the bound.
const silentHold = 20 * time.Second

// TestSSHWaitDelayCanary runs the real executor through OpenSSH against a
// throwaway sshd on a loopback port, with throwaway keys and a client config
// in the test's temp directory, so it reads no user SSH config or keys. It
// checks that the executor's WaitDelay leaves ControlPersist processes and mux
// reuse working, and that it bounds the two measured deadline hangs: a
// ProxyJump child and scp's ssh child holding the pipe.
//
//	go test -tags ssh_wait_delay_canary ./internal/docker/command -run SSHWaitDelayCanary -v
//
// SPT_TEST_SSHD overrides the sshd path (default /usr/sbin/sshd).
func TestSSHWaitDelayCanary(t *testing.T) {
	env := startCanarySSHD(t)

	t.Run("ControlPersist process and mux reuse", func(t *testing.T) {
		env.usePath(t, "")
		logs := captureDebugLog(t)
		host := canaryHost("spt-mux")
		before := env.acceptedSessions(t)

		started := time.Now()
		stdout, err := runWithin(t, 10*time.Second, host, []string{"echo", "answer"})
		if err != nil || stdout != "answer" {
			t.Fatalf("call 1 (creates the control process) = %q, %v", stdout, err)
		}
		t.Logf("call 1 created the control process in %v", time.Since(started).Round(time.Millisecond))
		env.requireControlProcess(t, "spt-mux")
		// Without -v the control process's fds 0-2 are /dev/null, so the delay never engages.
		if strings.Contains(logs.String(), "held the output pipe") {
			t.Fatalf("a ControlPersist process without -v held the pipe:\n%s", logs.String())
		}

		for call := 2; call <= 3; call++ {
			started := time.Now()
			stdout, err := runWithin(t, 20*time.Second, host, []string{"seq", "1", "300000"})
			if err != nil {
				t.Fatalf("call %d: %v", call, err)
			}
			lines := strings.Split(stdout, "\n")
			if len(lines) != 300000 || lines[len(lines)-1] != "300000" {
				t.Fatalf("call %d returned %d lines (%d bytes), last %q", call, len(lines), len(stdout), lines[len(lines)-1])
			}
			t.Logf("call %d: %d bytes through the mux in %v", call, len(stdout), time.Since(started).Round(time.Millisecond))
		}
		env.requireControlProcess(t, "spt-mux")
		if after := env.acceptedSessions(t); after != before+1 {
			t.Fatalf("sshd accepted %d connections for 3 calls; the mux was not reused", after-before)
		}
	})

	t.Run("ControlPersist process started with -v keeps stderr", func(t *testing.T) {
		env.usePath(t, "-v")
		logs := captureDebugLog(t)
		host := canaryHost("spt-mux-verbose")
		before := env.acceptedSessions(t)

		started := time.Now()
		stdout, err := runWithin(t, 10*time.Second, host, []string{"echo", "answer"})
		elapsed := time.Since(started)
		if err != nil || stdout != "answer" {
			t.Fatalf("call 1 = %q, %v; a held pipe after exit 0 is success", stdout, err)
		}
		if bound := constants.CommandWaitDelay + 3*time.Second; elapsed > bound {
			t.Fatalf("call 1 took %v, past %v: it waited on the control process's stderr", elapsed, bound)
		}
		if !strings.Contains(logs.String(), "held the output pipe") {
			t.Fatalf("expected the control process to hold stderr (the case under test):\n%s", logs.String())
		}
		t.Logf("call 1 returned in %v with the control process holding stderr", elapsed.Round(time.Millisecond))

		// The control process survives the closed pipe (ssh ignores SIGPIPE) and serves the next call.
		env.requireControlProcess(t, "spt-mux-verbose")
		stdout, err = runWithin(t, 10*time.Second, host, []string{"echo", "again"})
		if err != nil || stdout != "again" {
			t.Fatalf("call 2 through the control process = %q, %v", stdout, err)
		}
		env.requireControlProcess(t, "spt-mux-verbose")
		if after := env.acceptedSessions(t); after != before+1 {
			t.Fatalf("sshd accepted %d connections for 2 calls; the mux was not reused", after-before)
		}
	})

	t.Run("deadline kill on a ProxyJump route to a silent target", func(t *testing.T) {
		env.usePath(t, "")
		ctxTimeout := 3 * time.Second
		started := time.Now()
		_, err := runWithin(t, ctxTimeout, canaryHost("spt-jumped-silent"), []string{"echo", "answer"})
		elapsed := time.Since(started)
		requireKilled(t, err)
		if bound := ctxTimeout + constants.CommandWaitDelay + 2*time.Second; elapsed > bound {
			t.Fatalf("returned after %v, past %v: it waited on the ProxyJump child", elapsed, bound)
		}
		t.Logf("returned %v after the start with %v", elapsed.Round(time.Millisecond), err)
	})

	t.Run("deadline kill of scp to a silent target", func(t *testing.T) {
		env.usePath(t, "")
		src := filepath.Join(t.TempDir(), "src")
		if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
		ctxTimeout := 3 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer cancel()
		started := time.Now()
		err := NewCommandExecutor().CopyFile(ctx, canaryHost("spt-silent"), src, "/tmp/never")
		elapsed := time.Since(started)
		requireKilled(t, err)
		// scp's ssh child would hold the pipe until its ConnectTimeout (10 s).
		if bound := ctxTimeout + constants.CommandWaitDelay + 2*time.Second; elapsed > bound {
			t.Fatalf("returned after %v, past %v: it waited on scp's ssh child", elapsed, bound)
		}
		t.Logf("returned %v after the start with %v", elapsed.Round(time.Millisecond), err)
	})

	t.Run("scp round trip", func(t *testing.T) {
		env.usePath(t, "")
		dir := t.TempDir()
		payload := bytes.Repeat([]byte("spt-canary\n"), 100000)
		src, remote, back := filepath.Join(dir, "src"), filepath.Join(dir, "remote"), filepath.Join(dir, "back")
		if err := os.WriteFile(src, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		executor := NewCommandExecutor()
		if err := executor.CopyFile(ctx, canaryHost("spt-copy"), src, remote); err != nil {
			t.Fatal(err)
		}
		if err := executor.CopyFromHost(ctx, canaryHost("spt-copy"), remote, back); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(back)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("round trip returned %d bytes, %v; want %d", len(got), err, len(payload))
		}
	})
}

func canaryHost(name string) *hostparse.HostInfo {
	return &hostparse.HostInfo{Host: name, Original: name}
}

// runWithin runs command on host through ExecuteCommand under a fresh timeout
// and returns its stdout.
func runWithin(t *testing.T, timeout time.Duration, host *hostparse.HostInfo, command []string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stdout, _, err := NewCommandExecutor().ExecuteCommand(ctx, host, command)
	return stdout, err
}

type canarySSHD struct {
	dir     string
	config  string
	sshdLog string
	pid     int
}

// usePath puts ssh and scp wrappers that read only the canary's client config
// first on PATH; sshFlags adds flags to every ssh client, such as -v.
func (e canarySSHD) usePath(t *testing.T, sshFlags string) {
	t.Helper()
	bin := t.TempDir()
	for name, script := range map[string]string{
		constants.SSHCommand: "exec /usr/bin/ssh " + sshFlags + " -F '" + e.config + "' \"$@\"\n",
		constants.SCPCommand: "exec /usr/bin/scp -F '" + e.config + "' \"$@\"\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// requireControlProcess checks that a ControlPersist process serves host.
func (e canarySSHD) requireControlProcess(t *testing.T, host string) {
	t.Helper()
	out, err := exec.Command("/usr/bin/ssh", "-F", e.config, "-O", "check", host).CombinedOutput() // #nosec G204 -- test-only canary
	if err != nil {
		t.Fatalf("no ControlPersist process for %s: %v: %s", host, err, out)
	}
}

// acceptedSessions counts the connections sshd has authenticated.
func (e canarySSHD) acceptedSessions(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile(e.sshdLog)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(data), "Accepted publickey")
}

func startCanarySSHD(t *testing.T) canarySSHD {
	t.Helper()
	sshd := os.Getenv("SPT_TEST_SSHD")
	if sshd == "" {
		sshd = "/usr/sbin/sshd"
	}
	for _, tool := range []string{sshd, "/usr/bin/ssh", "/usr/bin/scp", "/usr/bin/ssh-keygen"} {
		if _, err := os.Stat(tool); err != nil {
			t.Fatalf("ssh wait delay canary requires %s: %v", tool, err)
		}
	}
	dir := t.TempDir()
	for _, key := range []string{"host_key", "user_key"} {
		if out, err := exec.Command("/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", filepath.Join(dir, key)).CombinedOutput(); err != nil { // #nosec G204 -- test-only canary
			t.Fatalf("ssh-keygen: %v: %s", err, out)
		}
	}
	pub, err := os.ReadFile(filepath.Join(dir, "user_key.pub"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "authorized_keys"), pub, 0o600); err != nil {
		t.Fatal(err)
	}

	sshdPort := freeLoopbackPort(t)
	silentPort := startSilentListener(t)
	sshdConfig := filepath.Join(dir, "sshd_config")
	writeLines(t, sshdConfig,
		"Port "+sshdPort,
		"ListenAddress 127.0.0.1",
		"HostKey "+filepath.Join(dir, "host_key"),
		"PidFile "+filepath.Join(dir, "sshd.pid"),
		"AuthorizedKeysFile "+filepath.Join(dir, "authorized_keys"),
		"PasswordAuthentication no",
		"KbdInteractiveAuthentication no",
		"UsePAM no",
		"StrictModes no",
		"AllowTcpForwarding yes",
		"Subsystem sftp internal-sftp",
		"LogLevel INFO",
	)
	clientConfig := filepath.Join(dir, "ssh_config")
	// ssh uses the first value it finds, so host blocks precede the defaults.
	writeLines(t, clientConfig,
		"Host spt-mux spt-mux-verbose",
		"  ControlMaster auto",
		// %n, the alias, keeps one control process per host: %C hashes the shared HostName.
		"  ControlPath "+filepath.Join(dir, "cm-%n"),
		"  ControlPersist 60",
		"Host spt-jumped-silent",
		"  Port "+silentPort,
		"  ProxyJump spt-jump",
		"Host spt-silent",
		"  Port "+silentPort,
		"Host *",
		"  HostName 127.0.0.1",
		"  Port "+sshdPort,
		"  IdentityFile "+filepath.Join(dir, "user_key"),
		"  IdentitiesOnly yes",
		"  IdentityAgent none",
		"  UserKnownHostsFile "+filepath.Join(dir, "known_hosts"),
		"  GlobalKnownHostsFile /dev/null",
		"  StrictHostKeyChecking no",
		"  LogLevel ERROR",
		"  ControlMaster no",
	)

	sshdLog := filepath.Join(dir, "sshd.log")
	logFile, err := os.Create(sshdLog) // #nosec G304 -- test temp dir
	if err != nil {
		t.Fatal(err)
	}
	server := exec.Command(sshd, "-D", "-e", "-f", sshdConfig) // #nosec G204 -- test-only canary
	server.Stdout, server.Stderr = logFile, logFile
	if err := server.Start(); err != nil {
		t.Fatalf("start sshd: %v", err)
	}
	env := canarySSHD{dir: dir, config: clientConfig, sshdLog: sshdLog, pid: server.Process.Pid}
	t.Cleanup(func() {
		for _, host := range []string{"spt-mux", "spt-mux-verbose"} {
			_ = exec.Command("/usr/bin/ssh", "-F", clientConfig, "-O", "exit", host).Run() // #nosec G204 -- test-only canary
		}
		_ = server.Process.Kill()
		_ = server.Wait()
		_ = logFile.Close()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+sshdPort, time.Second)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(sshdLog) // #nosec G304 -- test temp dir
			t.Fatalf("sshd did not listen on %s: %s", sshdPort, log)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return env
}

// startSilentListener accepts connections and holds each for silentHold
// without sending an SSH banner, like a host that stopped answering.
func startSilentListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				time.Sleep(silentHold)
				_ = conn.Close()
			}()
		}
	}()
	return strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
}

func freeLoopbackPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	_ = listener.Close()
	return port
}

func writeLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}
