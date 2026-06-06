package tui

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// fakeDockerClient implements dockerClient for tests
type fakeDockerClient struct {
	pulled           bool
	stopped, removed int
	ver              types.Version
	inspectErr       error
	logBody          []byte
	logOptions       container.LogsOptions
	stopErr          error
	removeErr        error
}

func (f *fakeDockerClient) ImageInspect(ctx context.Context, imageID string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
	return image.InspectResponse{}, f.inspectErr
}
func (f *fakeDockerClient) ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
	f.pulled = true
	return io.NopCloser(strings.NewReader("ok")), nil
}
func (f *fakeDockerClient) ContainerLogs(ctx context.Context, container string, options container.LogsOptions) (io.ReadCloser, error) {
	f.logOptions = options
	if f.logBody != nil {
		return io.NopCloser(bytes.NewReader(f.logBody)), nil
	}
	var logs bytes.Buffer
	_, _ = stdcopy.NewStdWriter(&logs, stdcopy.Stdout).Write([]byte("log1\nlog2\n"))
	return io.NopCloser(bytes.NewReader(logs.Bytes())), nil
}
func (f *fakeDockerClient) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
	return container.CreateResponse{ID: "abc"}, nil
}
func (f *fakeDockerClient) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	return nil
}
func (f *fakeDockerClient) ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error {
	f.stopped++
	return f.stopErr
}
func (f *fakeDockerClient) ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error {
	f.removed++
	return f.removeErr
}
func (f *fakeDockerClient) ServerVersion(ctx context.Context) (types.Version, error) {
	return f.ver, nil
}
func (f *fakeDockerClient) Close() error { return nil }

func TestEnsureImageAvailable_PullsWhenMissing(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "false")
	dm := &DockerManager{client: &fakeDockerClient{inspectErr: io.EOF}, ctx: context.Background()}
	if err := dm.ensureImageAvailable("myimg:latest"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dm.client.(*fakeDockerClient).pulled {
		t.Fatalf("expected image to be pulled")
	}
}

func TestEnsureImageAvailable_SkipPullWhenPresent(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	dm := &DockerManager{client: &fakeDockerClient{}, ctx: context.Background()}
	if err := dm.ensureImageAvailable("present"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dm.client.(*fakeDockerClient).pulled {
		t.Fatalf("did not expect pull when image present")
	}
}

func TestEnsureImageAvailable_DevImagePresent(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "false")
	devImage := constants.DefaultSptImage + ":" + constants.DevImageTag
	dm := &DockerManager{client: &fakeDockerClient{}, ctx: context.Background()}
	if err := dm.ensureImageAvailable(devImage); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dm.client.(*fakeDockerClient).pulled {
		t.Fatalf("did not expect pull for present dev image")
	}
}

func TestEnsureImageAvailable_DevImageMissing(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "false")
	devImage := constants.DefaultSptImage + ":" + constants.DevImageTag
	dm := &DockerManager{client: &fakeDockerClient{inspectErr: io.EOF}, ctx: context.Background()}
	err := dm.ensureImageAvailable(devImage)
	if err == nil {
		t.Fatal("expected error when dev image is missing locally")
	}
	if !strings.Contains(err.Error(), "dev image") {
		t.Errorf("unexpected error message: %v", err)
	}
	if dm.client.(*fakeDockerClient).pulled {
		t.Fatalf("did not expect pull attempt for missing dev image")
	}
}

func TestCleanup_UsesClientToStopAndRemove(t *testing.T) {
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, containerID: "xyz", ctx: context.Background()}
	if err := dm.Cleanup(); err != nil {
		t.Fatalf("cleanup error: %v", err)
	}
	if f.stopped != 1 || f.removed != 1 {
		t.Fatalf("expected stop+remove once, got %d/%d", f.stopped, f.removed)
	}
}

func TestCleanupIgnoresAlreadyGoneContainer(t *testing.T) {
	f := &fakeDockerClient{stopErr: fmt.Errorf("Error response from daemon: No such container: gone")}
	dm := &DockerManager{client: f, containerID: "gone", ctx: context.Background()}
	if err := dm.Cleanup(); err != nil {
		t.Fatalf("cleanup error = %v, want nil for already-gone container", err)
	}
	if f.stopped != 1 || f.removed != 1 {
		t.Fatalf("expected stop+remove once, got %d/%d", f.stopped, f.removed)
	}
}

func TestGetDockerVersion_Delegates(t *testing.T) {
	f := &fakeDockerClient{ver: types.Version{Version: "28.0.0"}}
	dm := &DockerManager{client: f}
	v, err := dm.GetDockerVersion(context.Background())
	if err != nil || v != "28.0.0" {
		t.Fatalf("unexpected version: %q err=%v", v, err)
	}
}

func TestStopAndRemoveDelegation(t *testing.T) {
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, ctx: context.Background()}
	if err := dm.StopContainer("abc", 5); err != nil {
		t.Fatalf("stop err: %v", err)
	}
	if f.stopped != 1 {
		t.Fatalf("expected stop count 1, got %d", f.stopped)
	}
	if err := dm.RemoveContainer("abc"); err != nil {
		t.Fatalf("remove err: %v", err)
	}
	if f.removed != 1 {
		t.Fatalf("expected remove count 1, got %d", f.removed)
	}
}

func TestStreamOutput_RawTextLines(t *testing.T) {
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, ctx: context.Background()}
	got := make(chan struct{}, 1)
	dm.StreamOutput("cid", func(s string) { got <- struct{}{} }, func(string) {})
	select {
	case <-got:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected stdout lines from stream")
	}
}

func TestContainerDiagnosticTailIncludesDockerAndEngineErrors(t *testing.T) {
	var dockerLogs bytes.Buffer
	_, _ = stdcopy.NewStdWriter(&dockerLogs, stdcopy.Stdout).Write([]byte("console-start --access-key LOCALACCESS\n"))
	_, _ = stdcopy.NewStdWriter(&dockerLogs, stdcopy.Stderr).Write([]byte("console-fatal\n"))

	logDir := t.TempDir()
	errorDir := filepath.Join(logDir, "log", "none-20260605.215723.824")
	if err := os.MkdirAll(errorDir, 0o700); err != nil {
		t.Fatalf("mkdir error log dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(errorDir, "errors.log"), []byte("Run node failure\nstorage.auth.secret=LOCALSECRET\njava.io.IOException: Permission denied\n"), 0o600); err != nil {
		t.Fatalf("write errors.log: %v", err)
	}

	f := &fakeDockerClient{logBody: dockerLogs.Bytes()}
	dm := &DockerManager{client: f, ctx: context.Background(), nodeLogDir: logDir}
	got, err := dm.ContainerDiagnosticTail("cid", 7)
	if err != nil {
		t.Fatalf("ContainerDiagnosticTail() error = %v", err)
	}
	for _, want := range []string{"Docker logs:", "console-start", "console-fatal", "Engine errors.log:", "Permission denied"} {
		if !strings.Contains(got, want) {
			t.Fatalf("diagnostics missing %q:\n%s", want, got)
		}
	}
	for _, leaked := range []string{"LOCALACCESS", "LOCALSECRET"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("diagnostics leaked %q:\n%s", leaked, got)
		}
	}
	if !strings.Contains(got, "--access-key ***") || !strings.Contains(got, "storage.auth.secret=***") {
		t.Fatalf("diagnostics missing masked credentials:\n%s", got)
	}
	if f.logOptions.Tail != "7" {
		t.Fatalf("docker log tail = %q, want 7", f.logOptions.Tail)
	}
}
