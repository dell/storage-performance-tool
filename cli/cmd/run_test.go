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
