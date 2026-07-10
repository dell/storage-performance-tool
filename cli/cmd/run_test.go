package cmd

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestDeriveBaseURLSingleHost(t *testing.T) {
	got := deriveBaseURL("8080", nil)
	want := "http://localhost:8080"
	if got != want {
		t.Fatalf("deriveBaseURL() = %q, want %q", got, want)
	}
}

func TestDeriveBaseURLMultiHost(t *testing.T) {
	hosts := []*hostparse.HostInfo{
		{Host: "entry.example", Original: "entry.example"},
		{Host: "worker", Original: "worker"},
	}
	got := deriveBaseURL("9000", hosts)
	want := "http://entry.example:9000"
	if got != want {
		t.Fatalf("deriveBaseURL() = %q, want %q", got, want)
	}
}

func TestDeriveBaseURLSingleRemoteHost(t *testing.T) {
	hosts := []*hostparse.HostInfo{
		{Host: "worker.example", IsLocal: false, Original: "root@worker.example"},
	}
	got := deriveBaseURL("9000", hosts)
	want := "http://worker.example:9000"
	if got != want {
		t.Fatalf("deriveBaseURL() = %q, want %q", got, want)
	}
}

func TestShouldUseMultiHostOrchestrator(t *testing.T) {
	tests := []struct {
		name  string
		hosts []*hostparse.HostInfo
		want  bool
	}{
		{
			name: "no hosts",
		},
		{
			name: "single local host",
			hosts: []*hostparse.HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
			},
		},
		{
			name: "single remote host",
			hosts: []*hostparse.HostInfo{
				{Host: "worker.example", IsLocal: false, Original: "root@worker.example"},
			},
			want: true,
		},
		{
			name: "multiple hosts",
			hosts: []*hostparse.HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
				{Host: "worker.example", IsLocal: false, Original: "root@worker.example"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldUseMultiHostOrchestrator(tt.hosts); got != tt.want {
				t.Fatalf("shouldUseMultiHostOrchestrator() = %t, want %t", got, tt.want)
			}
		})
	}
}
