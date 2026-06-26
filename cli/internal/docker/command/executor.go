/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"errors"
	"fmt"
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

	// CopyFile copies a local file to the given host path.
	CopyFile(ctx context.Context, host *hostparse.HostInfo, localPath, remotePath string) error
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

	if len(command) == 0 {
		return "", "", fmt.Errorf("empty command")
	}

	if host.IsLocal {
		// Local execution
		// #nosec G204: command and arguments originate from trusted spt constructors
		cmd = exec.CommandContext(ctx, command[0], command[1:]...)
	} else {
		// Remote execution via SSH
		sshTarget := host.GetSSHTarget()
		sshArgs := make([]string, 0, 5+len(command))
		sshArgs = append(sshArgs,
			"-o", constants.SSHConnectTimeout,
			"-o", constants.SSHBatchMode,
			sshTarget,
		)
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
