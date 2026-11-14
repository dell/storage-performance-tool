/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"regexp"
	"strconv"
	"testing"
	"time"
)

// Two runs that start the same role on the same host in the same second used to get the same
// container name, and the second docker run failed. The name now carries the time to the
// nanosecond, so names generated once the clock has moved differ.
func TestRemoteContainerNamesGeneratedBackToBackDiffer(t *testing.T) {
	for _, role := range []string{"node", "worker", "entry"} {
		t.Run(role, func(t *testing.T) {
			name := regexp.MustCompile(`^spt-` + role + `-([0-9]+)-worker1-example-com$`)
			stampOf := func(got string) int64 {
				t.Helper()
				match := name.FindStringSubmatch(got)
				if match == nil {
					t.Fatalf("name %q, want spt-%s-<time>-worker1-example-com", got, role)
				}
				stamp, err := strconv.ParseInt(match[1], 10, 64)
				if err != nil {
					t.Fatalf("name %q carries time %s: %v", got, match[1], err)
				}
				return stamp
			}
			before := time.Now().UnixNano()
			first := generateRemoteContainerName(role, "worker1.example.com")
			// A wall clock coarser than a nanosecond (macOS, Windows) can give two calls in a
			// row the same reading, so wait until it has moved past the first name's time.
			for waitStart := time.Now(); time.Now().UnixNano() <= stampOf(first); {
				if time.Since(waitStart) > time.Second {
					t.Fatalf("the clock did not pass %q's time within a second", first)
				}
			}
			second := generateRemoteContainerName(role, "worker1.example.com")
			after := time.Now().UnixNano()
			if first == second {
				t.Fatalf("names generated back to back are both %q", first)
			}
			for _, got := range []string{first, second} {
				if stamp := stampOf(got); stamp < before || stamp > after {
					t.Fatalf("name %q carries time %d, want nanoseconds between %d and %d", got, stamp, before, after)
				}
			}
		})
	}
}
