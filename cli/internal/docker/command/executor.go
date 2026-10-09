/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
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
// returns stdout, stderr (when available), and an error value.
func (r *RealCommandExecutor) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error) {
	if len(command) == 0 {
		return "", "", fmt.Errorf("empty command")
	}
	cmd := hostCommand(ctx, host, command)

	stdoutBytes, err := cmd.Output()
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
// the session closes.
func (r *RealCommandExecutor) StreamCommand(ctx context.Context, host *hostparse.HostInfo, command []string, stdoutLine, stderrLine func(string)) error {
	if len(command) == 0 {
		return fmt.Errorf("empty command")
	}
	cmd := streamHostCommand(ctx, host, command)
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
	if err := cmd.Run(); err != nil {
		// A trailing unterminated line may have been cut off mid-write; callers
		// that resume replay it whole instead.
		return err
	}
	stdout.flush()
	stderr.flush()
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
// SSH with keepalives under remoteStreamGuard. The single-quoted guard reaches
// sh as one word after the remote login shell parses the command line.
func streamHostCommand(ctx context.Context, host *hostparse.HostInfo, command []string) *exec.Cmd {
	if host.IsLocal {
		return hostCommand(ctx, host, command)
	}
	guarded := append([]string{"sh", "-c", "'" + remoteStreamGuard + "'", "sh"}, command...)
	return hostCommand(ctx, host, guarded, constants.SSHServerAliveInterval, constants.SSHServerAliveCountMax)
}

// hostCommand builds command for local execution or wraps it in ssh for a
// remote host. extraSSHOptions are appended as additional -o options.
func hostCommand(ctx context.Context, host *hostparse.HostInfo, command []string, extraSSHOptions ...string) *exec.Cmd {
	if host.IsLocal {
		// #nosec G204: command and arguments originate from trusted spt constructors
		return exec.CommandContext(ctx, command[0], command[1:]...)
	}
	sshOptions := append([]string{constants.SSHConnectTimeout, constants.SSHBatchMode}, extraSSHOptions...)
	sshArgs := make([]string, 0, 2*len(sshOptions)+1+len(command))
	for _, option := range sshOptions {
		sshArgs = append(sshArgs, "-o", option)
	}
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
}

func (w *lineWriter) Write(p []byte) (int, error) {
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

func (w *lineWriter) emitLine(line []byte) {
	if w.emit == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.emit(string(bytes.TrimSuffix(line, []byte{'\r'})))
}

// CopyFile copies a local file to the target host path.
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
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("copy %s to %s:%s failed: %w: %s", localPath, host.Original, remotePath, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// CopyFromHost copies a file from the target host to a local path.
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
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("copy %s:%s to %s failed: %w: %s", host.Original, remotePath, localPath, err, strings.TrimSpace(string(out)))
	}
	return nil
}
