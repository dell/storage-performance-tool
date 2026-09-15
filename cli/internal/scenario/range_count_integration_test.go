package scenario

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// This gate exercises the unchanged generated script through the actual packaged engine.
// Set SPT_RANGE_TEST_IMAGE to a locally built, commit-specific image (never latest).
func TestPackagedRangeCountExceedsInventory(t *testing.T) {
	image := os.Getenv("SPT_RANGE_TEST_IMAGE")
	if image == "" {
		t.Skip("requires a locally built engine image: SPT_RANGE_TEST_IMAGE")
	}
	for _, tc := range []struct {
		name                        string
		distributed, seeded, random bool
	}{
		{"seeded_fixed", false, true, false},
		{"existing_random", false, false, true},
		{"distributed_fixed", true, false, false},
		{"distributed_random", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Params{WorkloadType: "read", Bucket: "bucket", Threads: 1, ObjectSize: "8", SeedCount: 2,
				ObjectCount: 12, RangeSize: "3", RangeOffset: "2", Cleanup: true, BaseTimestamp: "20260914.210000.000"}
			if !tc.seeded {
				p.ItemsFile = "/work/items.csv"
			}
			if tc.random {
				p.RangeOffset = ""
			}
			script, err := GenerateReadScenario(p)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "scenario.js")
			if err = os.WriteFile(path, []byte(script), 0600); err != nil {
				t.Fatal(err)
			}
			mode := "local"
			if tc.distributed {
				mode = "distributed"
			}
			seed := "existing"
			if tc.seeded {
				seed = "seeded"
			}
			offset := "2"
			if tc.random {
				offset = "random"
			}
			cmd := exec.Command("python3", "testdata/range_count_probe.py", image, path, mode, seed, offset)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("packaged count failed: %v\n%s", err, output)
			}
			t.Log(string(output))
		})
	}
}
