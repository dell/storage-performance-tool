/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// Executor defines the interface for executing commands (SSH/local)
type Executor interface {
	// ExecuteCommand executes a command on the given host (local or remote via SSH)
	// Returns stdout, stderr, and error
	ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error)

	// CopyFile copies a local file to the given host path.
	CopyFile(ctx context.Context, host *hostparse.HostInfo, localPath, remotePath string) error

	// CopyFromHost copies a file from the given host path to a local path.
	CopyFromHost(ctx context.Context, host *hostparse.HostInfo, remotePath, localPath string) error
}

// LineStreamer is implemented by executors that can run a long-lived command
// and deliver its output line by line while it runs, over a single SSH session
// for remote hosts.
type LineStreamer interface {
	// StreamCommand runs command on host until it exits or ctx is cancelled.
	// Callbacks receive complete lines without line terminators and are never
	// invoked concurrently. A final unterminated line is delivered only when the
	// command succeeds, since after a failure it may be a fragment.
	StreamCommand(ctx context.Context, host *hostparse.HostInfo, command []string, stdoutLine, stderrLine func(string)) error
}

// RealCommandExecutor implements Executor using actual system commands
type RealCommandExecutor struct{}

// NewCommandExecutor creates a new RealCommandExecutor
func NewCommandExecutor() *RealCommandExecutor {
	return &RealCommandExecutor{}
}

// CommandExecutor is a compatibility alias for Executor during migration.
// TODO: remove after call sites are updated.
//
//nolint:revive // compatibility alias for pre-migration identifiers
type CommandExecutor = Executor

// ExecuteCommand runs a command locally or via SSH on the given host and
// returns stdout, stderr (when available), and an error value. It returns
// within constants.CommandWaitDelay of the command exiting or ctx ending, even
// when a descendant keeps the output pipes open; see endHeldPipeWait.
func (r *RealCommandExecutor) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error) {
	if len(command) == 0 {
		return "", "", fmt.Errorf("empty command")
	}
	cmd := hostCommand(ctx, host, command)
	cmd.WaitDelay = constants.CommandWaitDelay

	stdoutBytes, err := cmd.Output()
	err = endHeldPipeWait(cmd, host, err)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return string(stdoutBytes), string(exitErr.Stderr), err
		}
		return string(stdoutBytes), "", err
	}

	return strings.TrimSpace(string(stdoutBytes)), "", nil
}

// StreamCommand runs a long-lived command locally or over one SSH session and
// delivers each output line as it arrives. Cancelling ctx kills the local
// process; a remote command runs under remoteStreamGuard, which ends it when
// the session closes. A descendant that keeps the output pipes open delays
// the return by at most constants.StreamCommandWaitDelay; see endHeldPipeWait.
// If a line callback is still running when that delay cuts the pipes, output
// may have been left unread, so the cut is returned as exec.ErrWaitDelay.
func (r *RealCommandExecutor) StreamCommand(ctx context.Context, host *hostparse.HostInfo, command []string, stdoutLine, stderrLine func(string)) error {
	if len(command) == 0 {
		return fmt.Errorf("empty command")
	}
	cmd := streamHostCommand(ctx, host, command, !host.IsLocal && sshSupportsStdinNull())
	if !host.IsLocal {
		// Hold the session's stdin open until the command ends: the remote guard
		// treats its EOF as the session closing.
		if _, err := cmd.StdinPipe(); err != nil {
			return err
		}
	}
	var emitMu sync.Mutex
	stdout := &lineWriter{mu: &emitMu, emit: stdoutLine}
	stderr := &lineWriter{mu: &emitMu, emit: stderrLine}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Bound the wait if a descendant (for example an ssh ProxyCommand) keeps the
	// output pipes open after the process exits or is killed.
	cmd.WaitDelay = constants.StreamCommandWaitDelay
	err := cmd.Run()
	if errors.Is(err, exec.ErrWaitDelay) && (stdout.cutMidWrite() || stderr.cutMidWrite()) {
		// The cut caught a callback holding up the copy, so output the command
		// wrote before it exited may have been left unread: report the cut so the
		// caller can replay, as it did before held pipes counted as success.
		logging.LogDebug("command-executor", "output pipes cut while a line callback was still running; reporting the wait delay",
			"host", host.Host, "command", cmd.Args[0], "wait_delay", cmd.WaitDelay.String())
		return err
	}
	if err := endHeldPipeWait(cmd, host, err); err != nil {
		// A trailing unterminated line may have been cut off mid-write; callers
		// that resume replay it whole instead.
		return err
	}
	stdout.flush()
	stderr.flush()
	return nil
}

// streamWriteSettle is how soon before Wait returns a lineWriter's last Write
// may have ended and still count as caught by the pipe cut. Wait closes the
// pipes and then joins the copy goroutines, so a Write that was running at the
// cut ends just before Wait returns. An idle copy's last Write ended once the
// callbacks for the last output written had returned: with prompt callbacks,
// about cmd.WaitDelay before the cut. A descendant that wrote, or a callback
// that returned, within the window before the cut makes a false cut. Well below
// the delay and above a CPU-throttling period: a false cut only makes the
// caller replay, while a missed one loses output.
const streamWriteSettle = 250 * time.Millisecond

// endHeldPipeWait maps the bare exec.ErrWaitDelay that Wait returns when the
// process exited 0 on its own but a descendant still held its output pipes
// once cmd.WaitDelay expired: the command succeeded, so it returns nil, and
// the caller keeps the output collected so far. Output the descendant writes
// after the delay is discarded. That is acceptable for SPT's commands: ssh,
// scp and the docker CLI write their own output before they exit, and the
// usual holder is an ssh child (a ProxyJump "ssh -W", scp's ssh, or a
// ControlPersist process started with -v) that writes only diagnostics.
// Exit errors and the context kill are returned unchanged. The holder itself
// is left running: killing the process group would change how Ctrl-C reaches
// ssh.
func endHeldPipeWait(cmd *exec.Cmd, host *hostparse.HostInfo, err error) error {
	if !errors.Is(err, exec.ErrWaitDelay) || cmd.ProcessState == nil || !cmd.ProcessState.Success() {
		return err
	}
	logging.LogDebug("command-executor", "command exited 0 but a descendant held the output pipe; stopped reading after the wait delay",
		"host", host.Host, "command", cmd.Args[0], "wait_delay", cmd.WaitDelay.String())
	return nil
}

// remoteStreamGuard runs "$@" in the background and kills it when stdin
// reaches EOF, which happens when the SSH session closes. Without it a quiet
// remote command outlives a cancelled or dropped session, because sshd does not
// signal commands that run without a pty. When the command ends first, the
// guard stops its watcher and exits with the command's status. The watcher
// reads a copy of stdin on fd 3 because sh gives background jobs /dev/null.
const remoteStreamGuard = `exec 3<&0 </dev/null; "$@" & p=$!; ` +
	`{ while read -r line; do :; done <&3; kill "$p"; } >/dev/null 2>&1 & w=$!; ` +
	`wait "$p"; s=$?; kill "$w" 2>/dev/null; exit "$s"`

// streamHostCommand builds a streamed command: run directly when local, or over
// SSH under remoteStreamGuard. The SSH session has keepalives, no pty (which
// would merge stderr into stdout), and, when the client supports the option, an
// explicit StdinNull=no so client config cannot close the stdin the guard
// watches. The single-quoted guard reaches sh as one word after the remote
// login shell parses the command line.
func streamHostCommand(ctx context.Context, host *hostparse.HostInfo, command []string, supportsStdinNull bool) *exec.Cmd {
	if host.IsLocal {
		return hostCommand(ctx, host, command)
	}
	sshFlags := []string{constants.SSHFlagNoTTY,
		"-o", constants.SSHServerAliveInterval, "-o", constants.SSHServerAliveCountMax}
	if supportsStdinNull {
		sshFlags = append(sshFlags, "-o", constants.SSHStdinNullNo)
	}
	guarded := append([]string{"sh", "-c", "'" + remoteStreamGuard + "'", "sh"}, command...)
	return hostCommand(ctx, host, guarded, sshFlags...)
}

// sshSupportsStdinNull reports, once per process, whether the local ssh client
// accepts StdinNull (OpenSSH 8.7+). Older clients cannot null stdin from config
// either, so they keep it open without the option.
var sshSupportsStdinNull = sync.OnceValue(func() bool {
	return sshAcceptsOption(constants.SSHCommand, constants.SSHStdinNullNo)
})

// sshAcceptsOption reports whether the ssh client accepts option. It only
// prints the configuration for a placeholder host from an empty config file,
// so it neither reads user config nor connects.
func sshAcceptsOption(sshCommand, option string) bool {
	// #nosec G204: probes the configured ssh client with a constant option
	return exec.Command(sshCommand, "-G", "-F", os.DevNull, "-o", option, "spt-ssh-probe.invalid").Run() == nil
}

// hostCommand builds command for local execution or wraps it in ssh for a
// remote host. sshFlags go after the default options, before the target.
func hostCommand(ctx context.Context, host *hostparse.HostInfo, command []string, sshFlags ...string) *exec.Cmd {
	if host.IsLocal {
		// #nosec G204: command and arguments originate from trusted spt constructors
		return exec.CommandContext(ctx, command[0], command[1:]...)
	}
	sshArgs := make([]string, 0, 5+len(sshFlags)+len(command))
	sshArgs = append(sshArgs, "-o", constants.SSHConnectTimeout, "-o", constants.SSHBatchMode)
	sshArgs = append(sshArgs, sshFlags...)
	sshArgs = append(sshArgs, host.GetSSHTarget())
	sshArgs = append(sshArgs, command...)
	// #nosec G204: SSH used intentionally with constructed args
	return exec.CommandContext(ctx, constants.SSHCommand, sshArgs...)
}

// lineWriter splits a byte stream into lines and emits each complete line
// under a mutex shared with its sibling stream.
type lineWriter struct {
	mu      *sync.Mutex
	emit    func(string)
	pending []byte
	// lastWrite is when the latest Write returned. Only os/exec's copy
	// goroutine writes it; read it only after Wait has joined that goroutine.
	lastWrite time.Time
}

func (w *lineWriter) Write(p []byte) (int, error) {
	defer func() { w.lastWrite = time.Now() }()
	w.pending = append(w.pending, p...)
	for {
		i := bytes.IndexByte(w.pending, '\n')
		if i < 0 {
			break
		}
		w.emitLine(w.pending[:i])
		w.pending = w.pending[i+1:]
	}
	return len(p), nil
}

// flush emits a trailing line that had no terminator. Call it only after the
// stream completed successfully.
func (w *lineWriter) flush() {
	if len(w.pending) > 0 {
		w.emitLine(w.pending)
		w.pending = nil
	}
}

// cutMidWrite reports whether the latest Write returned within
// streamWriteSettle, which after Wait returned exec.ErrWaitDelay means the cut
// found the copy inside a Write, with output possibly still unread in the pipe.
// A copy starved of CPU for longer than streamWriteSettle between a Write and
// its next read goes unseen. Call it only after Wait returns.
func (w *lineWriter) cutMidWrite() bool {
	return !w.lastWrite.IsZero() && time.Since(w.lastWrite) < streamWriteSettle
}

func (w *lineWriter) emitLine(line []byte) {
	if w.emit == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.emit(string(bytes.TrimSuffix(line, []byte{'\r'})))
}

// CopyFile copies a local file to the target host path. It returns within
// constants.CommandWaitDelay of the copy exiting or ctx ending, even when
// scp's ssh child keeps the output pipe open; see endHeldPipeWait.
func (r *RealCommandExecutor) CopyFile(ctx context.Context, host *hostparse.HostInfo, localPath, remotePath string) error {
	var cmd *exec.Cmd
	if host.IsLocal {
		cmd = exec.CommandContext(ctx, "cp", localPath, remotePath) // #nosec G204 -- paths are user-selected SPT artifacts.
	} else {
		sshTarget := host.GetSSHTarget() + ":" + remotePath
		scpArgs := []string{
			"-o", constants.SSHConnectTimeout,
			"-o", constants.SSHBatchMode,
			localPath,
			sshTarget,
		}
		cmd = exec.CommandContext(ctx, constants.SCPCommand, scpArgs...) // #nosec G204 -- SCP target is built from parsed host info.
	}
	cmd.WaitDelay = constants.CommandWaitDelay
	out, err := cmd.CombinedOutput()
	if err = endHeldPipeWait(cmd, host, err); err != nil {
		return fmt.Errorf("copy %s to %s:%s failed: %w: %s", localPath, host.Original, remotePath, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// CopyFromHost copies a file from the target host to a local path. It returns
// within constants.CommandWaitDelay of the copy exiting or ctx ending, even
// when scp's ssh child keeps the output pipe open; see endHeldPipeWait.
func (r *RealCommandExecutor) CopyFromHost(ctx context.Context, host *hostparse.HostInfo, remotePath, localPath string) error {
	var cmd *exec.Cmd
	if host.IsLocal {
		cmd = exec.CommandContext(ctx, "cp", remotePath, localPath) // #nosec G204 -- paths are user-selected SPT artifacts.
	} else {
		sshSource := host.GetSSHTarget() + ":" + remotePath
		scpArgs := []string{
			"-o", constants.SSHConnectTimeout,
			"-o", constants.SSHBatchMode,
			sshSource,
			localPath,
		}
		cmd = exec.CommandContext(ctx, constants.SCPCommand, scpArgs...) // #nosec G204 -- SCP source is built from parsed host info.
	}
	cmd.WaitDelay = constants.CommandWaitDelay
	out, err := cmd.CombinedOutput()
	if err = endHeldPipeWait(cmd, host, err); err != nil {
		return fmt.Errorf("copy %s:%s to %s failed: %w: %s", host.Original, remotePath, localPath, err, strings.TrimSpace(string(out)))
	}
	return nil
}
