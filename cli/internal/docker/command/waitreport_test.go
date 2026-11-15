package command

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestParseContainerStateFormats(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"running:0:false", "running"},
		{"exited:1:false\n", "exited with exit code 1"},
		{"exited:137:true", "exited with exit code 137 (killed for running out of memory)"},
		{"created:0:false", "created"},
	}
	for _, tt := range tests {
		state, err := parseContainerState(tt.in)
		if err != nil || state.String() != tt.want {
			t.Errorf("parseContainerState(%q) = %q, %v; want %q", tt.in, state, err, tt.want)
		}
	}
	for _, bad := range []string{"", "running", "exited:x:false"} {
		if _, err := parseContainerState(bad); err == nil {
			t.Errorf("parseContainerState(%q) succeeded, want an error", bad)
		}
	}
}

func TestInspectContainerStateMissingAndFailed(t *testing.T) {
	mock := NewMockCommandExecutor()
	host := CreateRemoteHost("worker-1")
	inspect := "docker inspect --format " + constants.DockerContainerStateFormat + " "
	mock.SetCommandFailure(inspect+"gone", "Error: No such object: gone", errors.New("exit status 1"))
	mock.SetCommandFailure(inspect+"cid", "permission denied", errors.New("exit status 1"))

	state, err := InspectContainerState(context.Background(), mock, host, "gone")
	if err != nil || !state.Missing {
		t.Fatalf("missing container = %+v, %v; want Missing", state, err)
	}
	if _, err := InspectContainerState(context.Background(), mock, host, "cid"); err == nil ||
		!strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("failed inspect error = %v, want its stderr", err)
	}
}

func TestAPIWaitReportEmptyLogsAndLongID(t *testing.T) {
	got := APIWaitReport("0123456789abcdef", ContainerState{Status: "running"}, nil, " \n", nil)
	want := "Container 0123456789ab: running\nContainer logs: (empty)"
	if got != want {
		t.Fatalf("APIWaitReport() = %q, want %q", got, want)
	}
}

// TestAPIWaitReportMasksOnlyLogLines keeps the container ID and the CLI's own text whole, even
// when a credential's value equals them.
func TestAPIWaitReportMasksOnlyLogLines(t *testing.T) {
	t.Setenv(constants.EnvS3AccessKey, "test")
	t.Setenv(constants.EnvS3SecretKey, "running")
	got := APIWaitReport("test", ContainerState{Status: "running"}, nil, "key test\nstate running\nlatest", nil)
	want := "Container test: running\nLast 3 container log lines:\n  key ***\n  state ***\n  latest"
	if got != want {
		t.Fatalf("APIWaitReport() =\n%s\nwant\n%s", got, want)
	}
}
