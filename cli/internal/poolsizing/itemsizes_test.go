package poolsizing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const fixtures = "../../../test-fixtures/pool-demand"

func TestEverySharedFixtureGivesTheEnginesSummary(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(fixtures, "expected-summaries.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected struct {
		Files map[string]struct {
			Complete bool       `json:"complete"`
			Reason   string     `json:"reason"`
			Items    int64      `json:"items"`
			Bounds   [][2]int64 `json:"bounds"`
		} `json:"files"`
	}
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if len(expected.Files) == 0 {
		t.Fatal("no fixtures")
	}
	for name, want := range expected.Files {
		got := ReadItemSizes(filepath.Join(fixtures, "item-files", name), time.Minute)
		if got.Complete != want.Complete {
			t.Errorf("%s: complete = %v: %+v", name, got.Complete, got)
			continue
		}
		if want.Reason != "" && got.Reason != want.Reason {
			t.Errorf("%s: reason = %q, want %q", name, got.Reason, want.Reason)
		}
		if !got.Complete {
			if len(got.Bounds) != 0 {
				t.Errorf("%s: an incomplete file keeps no bounds: %v", name, got.Bounds)
			}
			continue
		}
		if got.Items != want.Items {
			t.Errorf("%s: items = %d, want %d", name, got.Items, want.Items)
		}
		if len(got.Bounds) != len(want.Bounds) {
			t.Errorf("%s: bounds = %v, want %v", name, got.Bounds, want.Bounds)
			continue
		}
		for i := range want.Bounds {
			if got.Bounds[i] != want.Bounds[i] {
				t.Errorf("%s: bounds = %v, want %v", name, got.Bounds, want.Bounds)
			}
		}
	}
}

func TestAMissingFileIsAReadFailure(t *testing.T) {
	got := ReadItemSizes(filepath.Join(t.TempDir(), "gone.csv"), time.Minute)
	if got.Complete || got.Reason != reasonRead {
		t.Errorf("got %+v", got)
	}
}

func TestAnExpiredBudgetLeavesItIncomplete(t *testing.T) {
	got := ReadItemSizes(filepath.Join(fixtures, "item-files", "one-class.csv"), -time.Second)
	if got.Complete || got.Reason != reasonBudget {
		t.Errorf("got %+v", got)
	}
}

func TestALastLineWithoutANewlineCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(path, []byte("a,0,4194304,0/0"), 0o600); err != nil {
		t.Fatal(err)
	}
	got := ReadItemSizes(path, time.Minute)
	if !got.Complete || got.Items != 1 {
		t.Errorf("got %+v", got)
	}
}
