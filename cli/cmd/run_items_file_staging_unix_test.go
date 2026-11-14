//go:build unix

/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

// installFakeSSHAndSCP puts ssh and scp scripts first on PATH that log one line
// per call to the returned files and never connect. ssh answers docker version,
// lists the image as present and refuses docker run, so a remote launch stops
// after staging; every other command succeeds silently.
func installFakeSSHAndSCP(t *testing.T) (sshLog, scpLog string) {
	t.Helper()
	dir := t.TempDir()
	sshLog, scpLog = filepath.Join(dir, "ssh.log"), filepath.Join(dir, "scp.log")
	scripts := map[string]string{
		constants.SSHCommand: `echo "$*" >> '` + sshLog + `'
case "$*" in
  *"docker version"*) echo 27.0.0 ;;
  *"docker images -q"*) echo 0123456789ab ;;
  *"docker run"*) echo 'fake docker run refused' >&2; exit 1 ;;
esac
exit 0
`,
		constants.SCPCommand: `echo "$*" >> '` + scpLog + `'
exit 0
`,
	}
	for name, body := range scripts {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o700); err != nil { // #nosec G306 -- test fake must be executable
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return sshLog, scpLog
}

func readLogLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path) // #nosec G304 -- test temp dir
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

// A read from --items-file copies the file over scp to every remote host,
// through the real orchestrator and remote Docker manager, with keepalives on
// the copy and on every ssh command; the local route copies nothing over SSH
// and hands the file to the local launcher instead.
func TestRunCmdItemsFileStagesOverSCPOnlyOnRemoteHosts(t *testing.T) {
	tests := []struct {
		name, hosts, minHosts string
		wantLocal             int
		wantCopiedTo          []string
	}{
		{name: "local Docker controller", hosts: "127.0.0.1", minHosts: "1", wantLocal: 1},
		{name: "lone remote orchestrator", hosts: "entry.example", minHosts: "1", wantCopiedTo: []string{"entry.example"}},
		{name: "distributed entry workers", hosts: "entry.example,worker.example", minHosts: "2",
			wantCopiedTo: []string{"entry.example", "worker.example"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			sshLog, scpLog := installFakeSSHAndSCP(t)
			itemsFile := filepath.Join(t.TempDir(), "list.csv")
			if err := os.WriteFile(itemsFile, []byte("bucket/item-1,0,1024\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			for name, value := range map[string]string{
				"test-hosts": test.hosts, "min-hosts": test.minHosts,
				"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
				"bucket": "qualification", "object-size": "1KiB", "duration": "5s", "threads": "1",
				"headless": "true", "auto-results": "false", "shutdown-on-complete": "false",
				"generate-only": "false", "items-file": itemsFile, "skip-image-pull": "true",
			} {
				setGlobalRunFlagForTest(t, name, value)
			}

			previousPort := resolvePortConflictFunc
			previousLocal := startLocalHeadlessRunFunc
			t.Cleanup(func() {
				resolvePortConflictFunc = previousPort
				startLocalHeadlessRunFunc = previousLocal
			})
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			errLocalRoute := errors.New("local route inspected")
			var localCalls int
			startLocalHeadlessRunFunc = func(_ string, _ string, params scenario.Params, _ headless.HeadlessOptions) error {
				localCalls++
				if len(params.ItemFileMounts) != 1 || params.ItemFileMounts[0].HostPath != itemsFile {
					t.Fatalf("local route item mounts = %+v, want %s", params.ItemFileMounts, itemsFile)
				}
				return errLocalRoute
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeRead})
			sshCalls, scpCalls := readLogLines(t, sshLog), readLogLines(t, scpLog)
			if localCalls != test.wantLocal {
				t.Fatalf("local route calls = %d, want %d", localCalls, test.wantLocal)
			}
			if test.wantLocal > 0 {
				if !errors.Is(err, errLocalRoute) {
					t.Fatalf("RunE() error = %v, want the local route", err)
				}
				if len(sshCalls) != 0 || len(scpCalls) != 0 {
					t.Fatalf("the local route ran ssh %q and scp %q", sshCalls, scpCalls)
				}
				return
			}
			// The fake host stages but cannot start an engine, so the launch fails after staging.
			if err == nil {
				t.Fatal("RunE() succeeded on a host that cannot start an engine")
			}
			const keepalives = "-o ConnectTimeout=10 -o BatchMode=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 "
			for _, call := range sshCalls {
				if !strings.HasPrefix(call, keepalives) || strings.Count(call, "ServerAlive") != 2 {
					t.Fatalf("ssh call %q, want the keepalives once at the front", call)
				}
			}
			if len(scpCalls) != len(test.wantCopiedTo) {
				t.Fatalf("scp calls = %q, want one per remote host %q", scpCalls, test.wantCopiedTo)
			}
			for i, host := range test.wantCopiedTo {
				copyArgs := regexp.MustCompile(`^` + regexp.QuoteMeta(keepalives+itemsFile) +
					` \S*` + regexp.QuoteMeta(host) + `:(/tmp/spt-items-\d+)/read-items\.csv$`)
				match := copyArgs.FindStringSubmatch(scpCalls[i])
				if match == nil {
					t.Fatalf("scp call %d = %q, want the items file copied to %s with keepalives", i, scpCalls[i], host)
				}
				stagingDir := match[1]
				for _, want := range []string{"mkdir -p " + stagingDir, "chmod 0444 " + stagingDir + "/read-items.csv", "rm -rf " + stagingDir} {
					if !containsLineWithSuffix(sshCalls, host+" "+want) {
						t.Fatalf("ssh calls lack %q on %s:\n%s", want, host, strings.Join(sshCalls, "\n"))
					}
				}
			}
		})
	}
}

func containsLineWithSuffix(lines []string, suffix string) bool {
	for _, line := range lines {
		if strings.HasSuffix(line, suffix) {
			return true
		}
	}
	return false
}
