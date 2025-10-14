/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"errors"
	"os/exec"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// Executor defines the interface for executing commands (SSH/local)
type Executor interface {
	// ExecuteCommand executes a command on the given host (local or remote via SSH)
	// Returns stdout, stderr, and error
	ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error)
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
	var cmd *exec.Cmd

	if host.IsLocal {
		// Local execution
		// #nosec G204: command and arguments originate from trusted spt constructors
		cmd = exec.CommandContext(ctx, command[0], command[1:]...)
	} else {
		// Remote execution via SSH
		sshTarget := host.GetSSHTarget()
		sshArgs := []string{
			"-o", constants.SSHConnectTimeout,
			"-o", constants.SSHBatchMode,
			sshTarget,
		}
		sshArgs = append(sshArgs, command...)
		// #nosec G204: SSH used intentionally with constructed args
		cmd = exec.CommandContext(ctx, constants.SSHCommand, sshArgs...)
	}

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
