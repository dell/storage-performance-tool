package tui

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// fakeDockerClient implements dockerClient for tests
type fakeDockerClient struct {
	pulled           bool
	stopped, removed int
	ver              types.Version
	inspectErr       error
}

func (f *fakeDockerClient) ImageInspect(ctx context.Context, imageID string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
	return image.InspectResponse{}, f.inspectErr
}
func (f *fakeDockerClient) ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
	f.pulled = true
	return io.NopCloser(strings.NewReader("ok")), nil
}
func (f *fakeDockerClient) ContainerLogs(ctx context.Context, container string, options container.LogsOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("log1\nlog2\n")), nil
}
func (f *fakeDockerClient) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
	return container.CreateResponse{ID: "abc"}, nil
}
func (f *fakeDockerClient) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	return nil
}
func (f *fakeDockerClient) ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error {
	f.stopped++
	return nil
}
func (f *fakeDockerClient) ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error {
	f.removed++
	return nil
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
