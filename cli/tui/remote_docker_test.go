/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func newTestRemoteManager(t *testing.T) (*RemoteDockerManager, *command.MockCommandExecutor, *hostparse.HostInfo) {
	t.Helper()
	host := &hostparse.HostInfo{User: "root", Host: "worker1.example.com", Original: "root@worker1.example.com", IsLocal: false}
	mock := command.NewMockCommandExecutor()
	// Make any docker command succeed and return a fake container ID
	mock.DefaultResponse = command.MockResponse{Stdout: "abc123def456", Stderr: "", Error: nil}
	mgr, err := newRemoteDockerManagerWithExecutor(host, mock)

	if err != nil {
		t.Fatalf("failed to create remote docker manager: %v", err)
	}
	return mgr, mock, host
}

type blockingRemoteExecutor struct {
	started chan struct{}
}

func (e *blockingRemoteExecutor) signalStarted() {
	select {
	case e.started <- struct{}{}:
	default:
	}
}

func (e *blockingRemoteExecutor) ExecuteCommand(
	ctx context.Context, _ *hostparse.HostInfo, _ []string,
) (string, string, error) {
	e.signalStarted()
	<-ctx.Done()
	return "", "", ctx.Err()
}

func (e *blockingRemoteExecutor) CopyFile(
	ctx context.Context, _ *hostparse.HostInfo, _, _ string,
) error {
	e.signalStarted()
	<-ctx.Done()
	return ctx.Err()
}

func (e *blockingRemoteExecutor) CopyFromHost(
	ctx context.Context, _ *hostparse.HostInfo, _, _ string,
) error {
	e.signalStarted()
	<-ctx.Done()
	return ctx.Err()
}

func TestRemoteDocker_StartContainerInNodeMode_RespectsPort(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	id, err := mgr.StartContainerInNodeMode(constants.DefaultSptImage, "10080", constants.BridgeNetworkMode, []string{"--load-service-threads=4"})
	if err != nil {
		t.Fatalf("StartContainerInNodeMode error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	// Verify a docker run with the expected port mapping and args was executed
	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"-p 10080:10080/tcp",
		constants.DefaultSptImage,
		"--run-node=true",
		"--run-port=10080",
		"--load-service-threads=4",
		"--label spt.host=worker1.example.com",
		"--label spt.managed=true",
		"--label spt.role=node",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected docker run command to contain %q, got: %s", s, cmd)
		}
	}
	if !strings.Contains(cmd, "--name spt-node-") {
		t.Errorf("expected docker run command to include node name prefix, got: %s", cmd)
	}
}

func TestRemoteDockerCleanupStopFailureRetainsContainerAndStagingForRetry(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	mgr.containerID = "retry-container"
	mgr.stagingDir = "/tmp/retry-staging"
	mock.SetCommandFailure("docker rm -f retry-container", "busy", errors.New("remove failed"))

	if err := mgr.CleanupContext(context.Background()); err == nil {
		t.Fatal("CleanupContext() error = nil, want container removal failure")
	}
	if mgr.containerID != "retry-container" || mgr.stagingDir != "/tmp/retry-staging" {
		t.Fatalf("failed cleanup lost ownership: container=%q staging=%q", mgr.containerID, mgr.stagingDir)
	}

	mock.SetCommandSuccess("docker rm -f retry-container", "removed")
	mock.SetCommandSuccess("rm -rf /tmp/retry-staging", "")
	if err := mgr.CleanupContext(context.Background()); err != nil {
		t.Fatalf("CleanupContext() retry error = %v", err)
	}
	if mgr.containerID != "" || mgr.stagingDir != "" {
		t.Fatalf("successful retry retained ownership: container=%q staging=%q", mgr.containerID, mgr.stagingDir)
	}
}

func TestRemoteDockerCleanupStagingFailureRemainsRetryable(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	mgr.containerID = "removed-container"
	mgr.stagingDir = "/tmp/retry-staging"
	mock.SetCommandSuccess("docker rm -f removed-container", "removed")
	mock.SetCommandFailure("rm -rf /tmp/retry-staging", "permission denied", errors.New("rm failed"))

	if err := mgr.CleanupContext(context.Background()); err == nil {
		t.Fatal("CleanupContext() error = nil, want staging removal failure")
	}
	if mgr.containerID != "" || mgr.stagingDir != "/tmp/retry-staging" {
		t.Fatalf("staging failure state = container %q staging %q", mgr.containerID, mgr.stagingDir)
	}

	mock.SetCommandSuccess("rm -rf /tmp/retry-staging", "")
	if err := mgr.CleanupContext(context.Background()); err != nil {
		t.Fatalf("staging cleanup retry error = %v", err)
	}
	if mgr.stagingDir != "" {
		t.Fatalf("successful staging retry retained path %q", mgr.stagingDir)
	}
}

func TestRemoteDockerPreservedDiagnosticsRemainManagedAndRetryCopy(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	mgr.containerID = "diagnostics-container"
	mgr.stagingDir = "/tmp/diagnostics-staging"
	mgr.diagnosticsDir = "/tmp/spt-diagnostics/run/worker"
	mgr.diagnosticsRole = constants.DockerRoleWorker
	mgr.diagnosticsRoot = t.TempDir()
	mgr.diagnosticsStopDone = true
	mock.SetCommandSuccess(
		"find "+mgr.diagnosticsDir+" -maxdepth 1 -type f -print",
		mgr.diagnosticsDir+"/spt.jfr\n",
	)
	mock.FailureMode = "copy_from_failure"

	if err := mgr.CleanupContext(context.Background()); err == nil || !strings.Contains(err.Error(), "copy from host failed") {
		t.Fatalf("CleanupContext() error = %v, want diagnostics copy failure", err)
	}
	if mgr.containerID != "" || mgr.stagingDir != "" || mgr.diagnosticsDir == "" || !mgr.HasManagedResources() {
		t.Fatalf("copy failure ownership: container=%q staging=%q diagnostics=%q managed=%t",
			mgr.containerID, mgr.stagingDir, mgr.diagnosticsDir, mgr.HasManagedResources())
	}

	mock.FailureMode = ""
	if err := mgr.CleanupContext(context.Background()); err != nil {
		t.Fatalf("diagnostics copy retry error = %v", err)
	}
	if mgr.diagnosticsDir != "" || mgr.HasManagedResources() {
		t.Fatalf("successful copy retry retained diagnostics ownership: dir=%q managed=%t",
			mgr.diagnosticsDir, mgr.HasManagedResources())
	}
	if record := mgr.diagnosticsRecord(); record == nil || !record.RemoteDirRemoved || record.PreservedRemoteDir {
		t.Fatalf("successful copy retry record = %+v", record)
	}
}

func TestRemoteDockerPreservedDiagnosticsRetryRemoteRemoval(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	mgr.containerID = "diagnostics-container"
	mgr.diagnosticsDir = "/tmp/spt-diagnostics/run/worker"
	mgr.diagnosticsRole = constants.DockerRoleWorker
	mgr.diagnosticsRoot = t.TempDir()
	mgr.diagnosticsStopDone = true
	mock.SetCommandSuccess(
		"find "+mgr.diagnosticsDir+" -maxdepth 1 -type f -print", "",
	)
	mock.SetCommandFailure("rm -rf "+mgr.diagnosticsDir, "permission denied", errors.New("rm failed"))

	if err := mgr.CleanupContext(context.Background()); err == nil || !strings.Contains(err.Error(), "remove remote diagnostics") {
		t.Fatalf("CleanupContext() error = %v, want remote diagnostics removal failure", err)
	}
	if mgr.containerID != "" || mgr.diagnosticsDir == "" || !mgr.HasManagedResources() {
		t.Fatalf("remove failure ownership: container=%q diagnostics=%q managed=%t",
			mgr.containerID, mgr.diagnosticsDir, mgr.HasManagedResources())
	}

	mock.SetCommandSuccess("rm -rf "+mgr.diagnosticsDir, "")
	if err := mgr.CleanupContext(context.Background()); err != nil {
		t.Fatalf("remote diagnostics removal retry error = %v", err)
	}
	if mgr.diagnosticsDir != "" || mgr.HasManagedResources() {
		t.Fatalf("successful removal retry retained ownership: dir=%q managed=%t",
			mgr.diagnosticsDir, mgr.HasManagedResources())
	}
}

func TestRemoteDockerCleanupCancellationDuringStopRetainsOwnershipForRetry(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	mgr.containerID = "cancel-container"
	mgr.stagingDir = "/tmp/cancel-staging"
	mock.SetCommandResponse("docker rm -f cancel-container", command.MockResponse{
		Stdout: "removed",
		Delay:  100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := mgr.CleanupContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CleanupContext() error = %v, want context deadline", err)
	}
	if mgr.containerID != "cancel-container" || mgr.stagingDir != "/tmp/cancel-staging" {
		t.Fatalf("mid-cleanup cancellation lost ownership: container=%q staging=%q",
			mgr.containerID, mgr.stagingDir)
	}

	mock.SetCommandSuccess("docker rm -f cancel-container", "removed")
	mock.SetCommandSuccess("rm -rf /tmp/cancel-staging", "")
	if err := mgr.CleanupContext(context.Background()); err != nil {
		t.Fatalf("CleanupContext() retry error = %v", err)
	}
	if mgr.containerID != "" || mgr.stagingDir != "" {
		t.Fatalf("successful retry retained ownership: container=%q staging=%q",
			mgr.containerID, mgr.stagingDir)
	}
}

func TestRemoteDocker_StartContainerInNodeMode_StagesAndMountsItemFiles(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	localItems := filepath.Join(t.TempDir(), "items.csv")
	mounts := []scenario.FileMount{{HostPath: localItems, ContainerPath: "/spt-input/items/read-items.csv"}}
	if err := mgr.SetFileMounts(mounts); err != nil {
		t.Fatalf("SetFileMounts() error = %v", err)
	}

	id, err := mgr.StartContainerInNodeMode(constants.DefaultSptImage, "10080", constants.BridgeNetworkMode, nil)
	if err != nil {
		t.Fatalf("StartContainerInNodeMode error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}
	if len(mock.CopiedFiles) != 1 {
		t.Fatalf("copied files = %d, want 1", len(mock.CopiedFiles))
	}
	if mock.CopiedFiles[0].LocalPath != localItems {
		t.Fatalf("copied local path = %q, want %q", mock.CopiedFiles[0].LocalPath, localItems)
	}
	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	wantBind := mock.CopiedFiles[0].RemotePath + ":/spt-input/items/read-items.csv:ro"
	if !strings.Contains(cmd, "-v "+wantBind) {
		t.Fatalf("docker run missing item bind %q: %s", wantBind, cmd)
	}
}

func TestRemoteDocker_StartContainerInNodeMode_HostNetwork(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	id, err := mgr.StartContainerInNodeMode(constants.DefaultSptImage, "10080", constants.HostNetworkMode, nil)
	if err != nil {
		t.Fatalf("StartContainerInNodeMode error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"--network host",
		constants.DefaultSptImage,
		"--run-node=true",
		"--run-port=10080",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected docker run command to contain %q, got: %s", s, cmd)
		}
	}
	if strings.Contains(cmd, "-p 10080:10080/tcp") {
		t.Fatalf("host-network node should not publish bridge ports: %s", cmd)
	}
}

func TestRemoteDocker_StartWorkerNodeContainer_BuildsExpectedCommand(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	id, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3, []string{"--load-service-threads=4"})
	if err != nil {
		t.Fatalf("StartWorkerNodeContainer error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"-e JAVA_OPTS=-Djava.rmi.server.hostname=10.0.0.10",
		"-e JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=10.0.0.10",
		"--network host",
		constants.DefaultSptImage,
		"--run-node=true",
		"--run-port=9999",
		"--load-step-node-port=1099",
		"--load-service-threads=4",
		"--label spt.host=worker1.example.com",
		"--label spt.managed=true",
		"--label spt.role=worker",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected worker run command to contain %q, got: %s", s, cmd)
		}
	}
	if !strings.Contains(cmd, "--name spt-worker-") {
		t.Errorf("expected worker run command to include worker name prefix, got: %s", cmd)
	}
}

func TestRemoteDocker_StartWorkerNodeContainer_DiagnosticsEnvFileAndMount(t *testing.T) {
	javaOpts := "-Xlog:gc*,safepoint:file=/spt-diagnostics/spt-gc-%p.log:time,uptime,level,tags -XX:StartFlightRecording=dumponexit=true,maxsize=512m,filename=/spt-diagnostics/spt-%p.jfr,settings=profile"
	t.Setenv(constants.EnvSptJavaOpts, javaOpts)
	mgr, mock, _ := newTestRemoteManager(t)
	resultsRoot := filepath.Join(t.TempDir(), "mt-20260705.120000.000")
	mgr.setDiagnosticsResultsRoot(resultsRoot)

	id, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3, nil)
	if err != nil {
		t.Fatalf("StartWorkerNodeContainer error = %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	var envCopy *command.CopiedFile
	for i := range mock.CopiedFiles {
		if strings.HasSuffix(mock.CopiedFiles[i].RemotePath, "/worker.env") {
			envCopy = &mock.CopiedFiles[i]
			break
		}
	}
	if envCopy == nil {
		t.Fatalf("expected staged worker env-file, copied files = %+v", mock.CopiedFiles)
	}
	wantContent := constants.EnvSptJavaOpts + "=" + javaOpts + "\n"
	if envCopy.Content != wantContent {
		t.Fatalf("env-file content = %q, want %q", envCopy.Content, wantContent)
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected docker run command")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	if strings.Contains(cmd, javaOpts) {
		t.Fatalf("docker run command should not inline multi-flag SPT_JAVA_OPTS: %s", cmd)
	}
	mustContain := []string{
		"--env-file " + envCopy.RemotePath,
		"-v " + mgr.diagnosticsDir + ":" + dockerDiagnosticsMount,
		"--network host",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Fatalf("docker run missing %q: %s", s, cmd)
		}
	}
}

func TestRemoteDocker_CleanupCollectsDiagnosticsBeforeStagingCleanup(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc*:file=/spt-diagnostics/spt-gc-%p.log")
	mgr, mock, _ := newTestRemoteManager(t)
	resultsRoot := filepath.Join(t.TempDir(), "mt-20260705.120000.000")
	mgr.setDiagnosticsResultsRoot(resultsRoot)

	if _, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3, nil); err != nil {
		t.Fatalf("StartWorkerNodeContainer error = %v", err)
	}
	remoteDir := mgr.diagnosticsDir
	stagingDir := mgr.stagingDir
	mock.SetCommandSuccess(
		"find "+remoteDir+" -maxdepth 1 -type f -print",
		remoteDir+"/spt-gc-123.log\n"+remoteDir+"/spt-123.jfr\n",
	)

	if err := mgr.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if mgr.diagnosticsDir != "" || mgr.HasManagedResources() {
		t.Fatalf("successful diagnostics cleanup retained ownership: dir=%q managed=%t",
			mgr.diagnosticsDir, mgr.HasManagedResources())
	}
	if len(mock.CopiedFromFiles) != 2 {
		t.Fatalf("copied diagnostics = %d, want 2", len(mock.CopiedFromFiles))
	}
	localDir := diagnosticsLocalDir(resultsRoot, mgr.host.Host, constants.DockerRoleWorker)
	for _, name := range []string{"spt-gc-123.log", "spt-123.jfr", dockerDiagnosticsManifestFileName} {
		if _, err := os.Stat(filepath.Join(localDir, name)); err != nil {
			t.Fatalf("expected local diagnostics file %s: %v", name, err)
		}
	}

	stopIdx := commandIndex(mock, "docker stop -t 60")
	findIdx := commandIndex(mock, "find "+remoteDir)
	diagRmIdx := commandIndex(mock, "rm -rf "+remoteDir)
	containerRmIdx := commandIndex(mock, "docker rm -f")
	stagingRmIdx := commandIndex(mock, "rm -rf "+stagingDir)
	for name, idx := range map[string]int{
		"docker stop":           stopIdx,
		"find diagnostics":      findIdx,
		"remove diagnostics":    diagRmIdx,
		"remove container":      containerRmIdx,
		"remove staging folder": stagingRmIdx,
	} {
		if idx < 0 {
			t.Fatalf("missing %s command; commands = %+v", name, mock.GetExecutedCommands())
		}
	}
	if !(stopIdx < findIdx && findIdx < diagRmIdx && diagRmIdx < containerRmIdx && containerRmIdx < stagingRmIdx) {
		t.Fatalf("unexpected cleanup order: stop=%d find=%d diagRm=%d containerRm=%d stagingRm=%d", stopIdx, findIdx, diagRmIdx, containerRmIdx, stagingRmIdx)
	}
}

func TestRemoteDocker_CleanupWithJavaOptsAndNoResultsRootRemovesRemoteDiagnostics(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xmx8g")
	mgr, mock, _ := newTestRemoteManager(t)

	if _, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3, nil); err != nil {
		t.Fatalf("StartWorkerNodeContainer error = %v", err)
	}
	remoteDir := mgr.diagnosticsDir
	stagingDir := mgr.stagingDir

	if err := mgr.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(mock.CopiedFromFiles) != 0 {
		t.Fatalf("copied diagnostics = %d, want 0 without results root", len(mock.CopiedFromFiles))
	}
	record := mgr.diagnosticsRecord()
	if record == nil {
		t.Fatal("expected diagnostics record")
	}
	if record.LocalDir != "" {
		t.Fatalf("record LocalDir = %q, want empty", record.LocalDir)
	}
	if !record.RemoteDirRemoved || record.PreservedRemoteDir {
		t.Fatalf("record remote cleanup = removed:%v preserved:%v, want removed only", record.RemoteDirRemoved, record.PreservedRemoteDir)
	}

	stopIdx := commandIndex(mock, "docker stop -t 60")
	diagRmIdx := commandIndex(mock, "rm -rf "+remoteDir)
	containerRmIdx := commandIndex(mock, "docker rm -f")
	stagingRmIdx := commandIndex(mock, "rm -rf "+stagingDir)
	findIdx := commandIndex(mock, "find "+remoteDir)
	if findIdx >= 0 {
		t.Fatalf("unexpected diagnostics copy scan without results root; commands = %+v", mock.GetExecutedCommands())
	}
	for name, idx := range map[string]int{
		"docker stop":           stopIdx,
		"remove diagnostics":    diagRmIdx,
		"remove container":      containerRmIdx,
		"remove staging folder": stagingRmIdx,
	} {
		if idx < 0 {
			t.Fatalf("missing %s command; commands = %+v", name, mock.GetExecutedCommands())
		}
	}
	if !(stopIdx < diagRmIdx && diagRmIdx < containerRmIdx && containerRmIdx < stagingRmIdx) {
		t.Fatalf("unexpected cleanup order: stop=%d diagRm=%d containerRm=%d stagingRm=%d", stopIdx, diagRmIdx, containerRmIdx, stagingRmIdx)
	}
}

func commandIndex(mock *command.MockCommandExecutor, contains string) int {
	for i, executed := range mock.GetExecutedCommands() {
		if strings.Contains(strings.Join(executed.Command, " "), contains) {
			return i
		}
	}
	return -1
}

func TestRemoteDocker_StartEntryNodeContainer_IncludesWorkerAddrsAndArgs(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	workerAddrs := []string{"w1:1099", "w2:1099"}
	extra := []string{"--storage-driver-type=s3", "--storage-net-node-addrs", "minio:9000"}

	id, err := mgr.StartEntryNodeContainer(constants.DefaultSptImage, workerAddrs, extra, constants.DefaultNetworkMode)
	if err != nil {
		t.Fatalf("StartEntryNodeContainer error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"--network host",
		constants.DefaultSptImage,
		"--load-step-node-addrs=w1:1099,w2:1099",
		"--run-node=true",
		"--run-port=9999",
		"--storage-driver-type=s3",
		"--storage-net-node-addrs minio:9000",
		"--label spt.host=worker1.example.com",
		"--label spt.managed=true",
		"--label spt.role=entry",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected entry run command to contain %q, got: %s", s, cmd)
		}
	}
	if !strings.Contains(cmd, "--name spt-entry-") {
		t.Errorf("expected entry run command to include entry name prefix, got: %s", cmd)
	}
}

// TestRemoteDocker_CollectDiagnosticsPreservesRemoteDirWhenStopNotConfirmed guards
// against collecting (and then deleting) JFR/GC artifacts before the container's
// JVM has had a chance to exit and flush dumponexit files. This exercises
// collectDiagnostics directly, bypassing gracefulStopForDiagnostics/Cleanup, the
// same way MultiHostOrchestrator.CollectDiagnostics used to before it was fixed
// to stop first.
func TestRemoteDocker_CollectDiagnosticsPreservesRemoteDirWhenStopNotConfirmed(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc*:file=/spt-diagnostics/spt-gc-%p.log")
	mgr, mock, _ := newTestRemoteManager(t)
	resultsRoot := filepath.Join(t.TempDir(), "mt-20260705.120000.000")
	mgr.setDiagnosticsResultsRoot(resultsRoot)

	if _, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3, nil); err != nil {
		t.Fatalf("StartWorkerNodeContainer error = %v", err)
	}
	remoteDir := mgr.diagnosticsDir
	mock.SetCommandSuccess(
		"find "+remoteDir+" -maxdepth 1 -type f -print",
		remoteDir+"/spt-gc-123.log\n",
	)

	record, err := mgr.collectDiagnostics(context.Background())
	if err == nil {
		t.Fatal("expected an error when stop was never confirmed before collection")
	}
	if record == nil || !record.Collected {
		t.Fatalf("expected best-effort collection despite unconfirmed stop, record = %+v", record)
	}
	if !record.PreservedRemoteDir {
		t.Fatal("expected remote dir to be preserved when stop was not confirmed")
	}
	if idx := commandIndex(mock, "rm -rf "+remoteDir); idx >= 0 {
		t.Fatalf("remote diagnostics dir should not be removed when stop was not confirmed; commands = %+v", mock.GetExecutedCommands())
	}
}

// TestRemoteDocker_CleanupCollectsEntryNodeDiagnostics covers the entry-node
// diagnostics path specifically: prior coverage exercised worker-role Cleanup
// only, but the plan requires diagnostics on both worker and entry containers.
func TestRemoteDocker_CleanupCollectsEntryNodeDiagnostics(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc*:file=/spt-diagnostics/spt-gc-%p.log")
	mgr, mock, _ := newTestRemoteManager(t)
	resultsRoot := filepath.Join(t.TempDir(), "mt-20260705.120000.000")
	mgr.setDiagnosticsResultsRoot(resultsRoot)

	if _, err := mgr.StartEntryNodeContainer(constants.DefaultSptImage, []string{"w1:1099"}, nil, constants.DefaultNetworkMode); err != nil {
		t.Fatalf("StartEntryNodeContainer error = %v", err)
	}
	remoteDir := mgr.diagnosticsDir
	mock.SetCommandSuccess(
		"find "+remoteDir+" -maxdepth 1 -type f -print",
		remoteDir+"/spt-gc-123.log\n",
	)

	if err := mgr.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	record := mgr.diagnosticsRecord()
	if record == nil {
		t.Fatal("expected diagnostics record")
	}
	if record.Role != constants.DockerRoleEntry {
		t.Fatalf("record role = %q, want %q", record.Role, constants.DockerRoleEntry)
	}
	if !record.Collected || !record.RemoteDirRemoved {
		t.Fatalf("expected entry diagnostics to be collected and remote dir removed, record = %+v", record)
	}
	localDir := diagnosticsLocalDir(resultsRoot, mgr.host.Host, constants.DockerRoleEntry)
	if _, err := os.Stat(filepath.Join(localDir, "spt-gc-123.log")); err != nil {
		t.Fatalf("expected local entry diagnostics file: %v", err)
	}
}

// TestRemoteDocker_CleanupCollectsNodeModeDiagnostics covers the standalone
// node-mode diagnostics path (StartContainerInNodeMode), the third of the
// three container roles diagnostics must be wired for.
func TestRemoteDocker_CleanupCollectsNodeModeDiagnostics(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc*:file=/spt-diagnostics/spt-gc-%p.log")
	mgr, mock, _ := newTestRemoteManager(t)
	resultsRoot := filepath.Join(t.TempDir(), "mt-20260705.120000.000")
	mgr.setDiagnosticsResultsRoot(resultsRoot)

	if _, err := mgr.StartContainerInNodeMode(constants.DefaultSptImage, "10080", constants.BridgeNetworkMode, nil); err != nil {
		t.Fatalf("StartContainerInNodeMode error = %v", err)
	}
	remoteDir := mgr.diagnosticsDir
	mock.SetCommandSuccess(
		"find "+remoteDir+" -maxdepth 1 -type f -print",
		remoteDir+"/spt-gc-123.log\n",
	)

	if err := mgr.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	record := mgr.diagnosticsRecord()
	if record == nil {
		t.Fatal("expected diagnostics record")
	}
	if record.Role != constants.DockerRoleNode {
		t.Fatalf("record role = %q, want %q", record.Role, constants.DockerRoleNode)
	}
	if !record.Collected || !record.RemoteDirRemoved {
		t.Fatalf("expected node-mode diagnostics to be collected and remote dir removed, record = %+v", record)
	}
}

func TestRemoteDockerSetFileMountsContextCancelsStaging(t *testing.T) {
	host := &hostparse.HostInfo{
		User: "root", Host: "worker.example.com", Original: "root@worker.example.com", IsLocal: false,
	}
	exec := &blockingRemoteExecutor{started: make(chan struct{}, 1)}
	mgr, err := newRemoteDockerManagerWithExecutor(host, exec)
	if err != nil {
		t.Fatalf("new remote manager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- mgr.SetFileMountsContext(ctx, []scenario.FileMount{{
			HostPath: "/host/items.csv", ContainerPath: "/spt-input/items.csv",
		}})
	}()
	select {
	case <-exec.started:
	case <-time.After(time.Second):
		t.Fatal("remote staging command did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("staging error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote staging did not return after cancellation")
	}
}

func TestRemoteDockerStartContextCancelsRemoteCommand(t *testing.T) {
	host := &hostparse.HostInfo{
		User: "root", Host: "worker.example.com", Original: "root@worker.example.com", IsLocal: false,
	}
	exec := &blockingRemoteExecutor{started: make(chan struct{}, 1)}
	mgr, err := newRemoteDockerManagerWithExecutor(host, exec)
	if err != nil {
		t.Fatalf("new remote manager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := mgr.StartContainerInNodeModeContext(
			ctx, constants.DefaultSptImage, "10080", constants.BridgeNetworkMode, nil)
		done <- err
	}()
	select {
	case <-exec.started:
	case <-time.After(time.Second):
		t.Fatal("remote Docker command did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("remote start error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote Docker start did not return after cancellation")
	}
}

func TestRemoteDockerWorkerReadinessUsesLaunchContext(t *testing.T) {
	mgr, _, _ := newTestRemoteManager(t)
	readinessStarted := make(chan struct{})
	mgr.proberRun = func(ctx context.Context, _ string, _ time.Duration) error {
		close(readinessStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := mgr.StartWorkerNodeContainerContext(
			ctx, constants.DefaultSptImage, "192.0.2.10", 1099, 1, nil)
		done <- err
	}()
	select {
	case <-readinessStarted:
	case <-time.After(time.Second):
		t.Fatal("worker readiness did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("readiness error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker readiness did not return after cancellation")
	}
	if mgr.containerID == "" {
		t.Fatal("canceled readiness lost ownership of the started container")
	}
}
