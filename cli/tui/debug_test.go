package tui

import (
	"strings"
	"testing"
)

func TestTruncateString(t *testing.T) {
	if got := truncateString("short", 10); got != "short" {
		t.Fatalf("unexpected: %q", got)
	}
	if got := truncateString("this is a very long line", 10); !strings.HasSuffix(got, "...") || len(got) != 10 {
		t.Fatalf("expected ellipsis and max len, got %q (len=%d)", got, len(got))
	}
}

func TestRenderLayoutToString_NoData(t *testing.T) {
	r := NewDebugLayoutRenderer(100, 40)
	out := r.RenderLayoutToString(false)
	if !strings.Contains(out, "TUI LAYOUT DEBUG") || !strings.Contains(out, "RENDERED TUI") {
		t.Fatalf("unexpected debug render output")
	}
}
