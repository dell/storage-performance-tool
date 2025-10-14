package scenario

import (
	"regexp"
	"testing"
)

func TestFormatStepID_ZeroPadAndOrder(t *testing.T) {
	ts := "20250909.154231.045"

	got1 := formatStepID(1, ts, "Create")
	want1 := "mt-001-20250909.154231.045-create"
	if got1 != want1 {
		t.Fatalf("formatStepID(1) = %q, want %q", got1, want1)
	}

	got2 := formatStepID(12, ts, "delete")
	want2 := "mt-012-20250909.154231.045-delete"
	if got2 != want2 {
		t.Fatalf("formatStepID(12) = %q, want %q", got2, want2)
	}

	got3 := formatStepID(3, ts, "")
	want3 := "mt-003-20250909.154231.045-step"
	if got3 != want3 {
		t.Fatalf("formatStepID(3, blank op) = %q, want %q", got3, want3)
	}

	re := regexp.MustCompile(`^mt-[0-9]{3}-[0-9]{8}\.[0-9]{6}\.[0-9]{3}-[a-z0-9_-]+$`)
	for _, s := range []string{got1, got2, got3} {
		if !re.MatchString(s) {
			t.Fatalf("step id %q does not match expected format", s)
		}
	}
}
