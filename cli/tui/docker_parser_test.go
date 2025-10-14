package tui

import (
	"testing"
)

// build a Docker multiplexed frame
func muxFrame(stream byte, payload string) []byte {
	p := []byte(payload)
	n := len(p)
	hdr := []byte{
		stream, 0, 0, 0,
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
	return append(hdr, p...)
}

func TestProcessDockerOutput_MixedStreams(t *testing.T) {
	dm := &DockerManager{}
	data := append(muxFrame(1, "line1\nline2\n"), muxFrame(2, "err1\nerr2\n")...)

	res := dm.processDockerOutput(data)
	if len(res.StdoutLines) != 2 || res.StdoutLines[0] != "line1" || res.StdoutLines[1] != "line2" {
		t.Fatalf("unexpected stdout: %#v", res.StdoutLines)
	}
	if len(res.StderrLines) != 2 || res.StderrLines[0] != "err1" || res.StderrLines[1] != "err2" {
		t.Fatalf("unexpected stderr: %#v", res.StderrLines)
	}
}

func TestProcessDockerOutput_RawText_NoHeader(t *testing.T) {
	dm := &DockerManager{}
	raw := []byte("just a line without header\nsecond line\n")
	res := dm.processDockerOutput(raw)
	if len(res.StdoutLines) != 2 || res.StdoutLines[0] != "just a line without header" || res.StdoutLines[1] != "second line" {
		t.Fatalf("unexpected stdout for raw text: %#v", res.StdoutLines)
	}
	if len(res.StderrLines) != 0 {
		t.Fatalf("expected no stderr, got: %#v", res.StderrLines)
	}
}

func TestProcessDockerOutput_IncompletePayload_TreatedAsRaw(t *testing.T) {
	dm := &DockerManager{}
	// Build a frame header claiming 10 bytes, but provide only 5
	p := []byte("abcde")
	n := 10
	hdr := []byte{1, 0, 0, 0, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	data := append(hdr, p...)
	res := dm.processDockerOutput(data)
	// We can't assert exact line contents due to header bytes being included, but ensure some stdout is emitted
	if len(res.StdoutLines) == 0 {
		t.Fatalf("expected some stdout lines for incomplete payload, got none")
	}
}

func TestProcessDockerOutput_UnknownStreamDefaultsToStdout(t *testing.T) {
	dm := &DockerManager{}
	data := muxFrame(0, "mystery\n")
	res := dm.processDockerOutput(data)
	if len(res.StdoutLines) != 1 || res.StdoutLines[0] != "mystery" {
		t.Fatalf("expected stdout to contain payload for unknown stream, got: %#v", res.StdoutLines)
	}
	if len(res.StderrLines) != 0 {
		t.Fatalf("expected no stderr, got: %#v", res.StderrLines)
	}
}
