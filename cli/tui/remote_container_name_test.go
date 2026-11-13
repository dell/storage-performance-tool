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
// nanosecond, so names generated back to back differ.
func TestRemoteContainerNamesGeneratedBackToBackDiffer(t *testing.T) {
	for _, role := range []string{"node", "worker", "entry"} {
		t.Run(role, func(t *testing.T) {
			name := regexp.MustCompile(`^spt-` + role + `-([0-9]+)-worker1-example-com$`)
			before := time.Now().UnixNano()
			first := generateRemoteContainerName(role, "worker1.example.com")
			second := generateRemoteContainerName(role, "worker1.example.com")
			after := time.Now().UnixNano()
			if first == second {
				t.Fatalf("names generated back to back are both %q", first)
			}
			for _, got := range []string{first, second} {
				match := name.FindStringSubmatch(got)
				if match == nil {
					t.Fatalf("name %q, want spt-%s-<time>-worker1-example-com", got, role)
				}
				stamp, err := strconv.ParseInt(match[1], 10, 64)
				if err != nil || stamp < before || stamp > after {
					t.Fatalf("name %q carries time %s, want nanoseconds between %d and %d", got, match[1], before, after)
				}
			}
		})
	}
}
