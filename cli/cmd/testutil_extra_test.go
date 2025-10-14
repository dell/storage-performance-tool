package cmd

import "testing"

func TestCreateTestCommand_HasExpectedFlags(t *testing.T) {
	h := NewTestHelper(t)
	c := h.CreateTestCommand()
	flags := []string{"endpoint", "access-key", "secret-key", "bucket", "threads", "object-size", "object-count", "duration"}
	for _, f := range flags {
		if c.Flags().Lookup(f) == nil {
			t.Fatalf("expected flag %q to exist", f)
		}
	}
}
