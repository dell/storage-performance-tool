/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// An attach run with an items file stages the file only on the entry node, which spt starts:
// the attached worker runs an engine someone else started, with no bind mount for the file.
// The end of the run removes the entry's container and staging, and touches nothing on the
// attached worker, so its engine keeps running and leaves no staging directory behind.
func TestAttachRunStagesItemFilesOnEntryOnly(t *testing.T) {
	// The attached worker's RMI registry, on a free port rather than the default.
	registry, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	registryPort := registry.Addr().(*net.TCPAddr).Port

	itemsFile := filepath.Join(t.TempDir(), "read-items.csv")
	if err := os.WriteFile(itemsFile, []byte("bucket/key,0,1024\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	entryExec := &startFailureExecutor{copy: func(context.Context) error { return nil }}
	workerExec := &startFailureExecutor{copy: func(context.Context) error { return nil }}
	infos := []*hostparse.HostInfo{
		{Host: "host0.example.com", Original: "host0.example.com"},
		{Host: "127.0.0.1", Original: "worker"},
	}
	orchestrator := NewMultiHostOrchestrator(infos, len(infos))
	orchestrator.SetNotifier(func(string) {})
	orchestrator.SetAttachExistingWorkers(true)
	orchestrator.SetRMIRegistryPort(registryPort)
	orchestrator.SetAPIPort(strconv.Itoa(registryPort + 1))
	orchestrator.rollbackTimeout = 5 * time.Second
	orchestrator.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) {
		return "192.0.2.10", nil
	}
	managers := make([]*RemoteDockerManager, len(infos))
	for i, exec := range []*startFailureExecutor{entryExec, workerExec} {
		mgr, mgrErr := newRemoteDockerManagerWithExecutor(infos[i], exec)
		if mgrErr != nil {
			t.Fatal(mgrErr)
		}
		managers[i] = mgr
		orchestrator.hosts[i].DockerManager = mgr
		orchestrator.hosts[i].SetStatus(HostStatusReady)
	}
	entry, worker := orchestrator.hosts[0], orchestrator.hosts[1]

	params := scenario.ScenarioParams{
		WorkloadType: scenario.WorkloadTypeRead, Threads: 1, ObjectSize: "1KB", Bucket: "bucket",
		ItemFileMounts: []scenario.FileMount{{HostPath: itemsFile, ContainerPath: "/tmp/spt-items/read-items.csv"}},
	}
	if err := orchestrator.StartDistributedTestWithContent(
		context.Background(), "test-image", params, []byte(`Load.run({});`)); err != nil {
		t.Fatalf("attach start error = %v", err)
	}
	if worker.IsManaged() || !entry.IsManaged() {
		t.Fatalf("managed entry=%t worker=%t, want the entry only", entry.IsManaged(), worker.IsManaged())
	}
	if got := workerExec.commands(""); len(got) != 0 {
		t.Fatalf("attached worker commands = %q, want none", got)
	}
	if managers[1].HasManagedResources() {
		t.Fatalf("attached worker's manager owns staging=%q", managers[1].stagingDir)
	}
	entryStaging := stagingDirOf(t, entryExec)
	if got := entryExec.commands("copy " + itemsFile + " " + entryStaging); len(got) != 1 {
		t.Fatalf("entry item copies = %q, want one into %s", got, entryStaging)
	}

	outcome := orchestrator.FinalizeDiagnosticsAndCleanupOutcome(context.Background())
	if err := outcome.Error(); err != nil {
		t.Fatalf("end-of-run outcome error = %v", err)
	}
	if outcome.Resources != runcontrol.ResourceDispositionRemoved {
		t.Fatalf("end-of-run resources = %v, want removed", outcome.Resources)
	}
	if got := workerExec.commands(""); len(got) != 0 {
		t.Fatalf("end of run sent the attached worker %q, want nothing", got)
	}
	if worker.IsManaged() {
		t.Fatal("the attached worker became managed at the end of the run")
	}
	if got := entryExec.liveCommands("rm -rf " + entryStaging); len(got) != 1 {
		t.Fatalf("entry staging removals = %q, want one", got)
	}
	if got := entryExec.liveCommands("docker rm -f " + startFailureContainerID); len(got) != 1 {
		t.Fatalf("entry container removals = %q, want one", got)
	}
	assertNothingLeft(t, entry, managers[0])
}
