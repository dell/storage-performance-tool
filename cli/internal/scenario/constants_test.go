package scenario

import "testing"

func TestSeededDeleteCleanupStepIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		stepID string
		want   bool
	}{
		{stepID: "mt-003-delete-cleanup", want: true},
		{stepID: " MT-003-DELETE-CLEANUP ", want: true},
		{stepID: "mt-003-delete", want: false},
		{stepID: "mt-003-cleanup-put", want: false},
	} {
		if got := IsSeededDeleteCleanupStepID(test.stepID); got != test.want {
			t.Errorf("IsSeededDeleteCleanupStepID(%q) = %t, want %t", test.stepID, got, test.want)
		}
	}
}
