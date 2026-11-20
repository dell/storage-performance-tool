package scenario

import (
	"strings"
	"testing"
)

// The summary no longer receives a size for these reads, but the engine must see the same scenario.
func TestGenerateReadScenarioFromFileWithoutSizeMatchesDefaultSize(t *testing.T) {
	for _, test := range []struct {
		name     string
		count    int
		duration string
		cleanup  bool
	}{
		{name: "count", count: 5000},
		{name: "duration", duration: "2m"},
		{name: "default duration"},
		{name: "count cleanup", count: 5000, cleanup: true},
		{name: "duration cleanup", duration: "2m", cleanup: true},
		{name: "default duration cleanup", cleanup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := Params{
				WorkloadType: WorkloadTypeRead, Bucket: "bench", Threads: 4, ItemsFile: "/data/items.csv",
				ObjectCount: test.count, Duration: test.duration, Cleanup: test.cleanup,
				BaseTimestamp: "20261118.120000.000",
			}
			got, err := GenerateReadScenario(params)
			if err != nil {
				t.Fatal(err)
			}
			params.ObjectSize = "1MB"
			want, err := GenerateReadScenario(params)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("scenario differs from the 1MB one\ngot:\n%s\nwant:\n%s", got, want)
			}
			if !strings.Contains(got, `"1MB"`) {
				t.Fatalf("scenario does not send the default size:\n%s", got)
			}
		})
	}
}

func TestGenerateReadScenarioFromFileKeepsExplicitSize(t *testing.T) {
	got, err := GenerateReadScenario(Params{
		WorkloadType: WorkloadTypeRead, Bucket: "bench", Threads: 4, ItemsFile: "/data/items.csv",
		ObjectSize: "64KiB", Duration: "2m",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `"64KiB"`) || strings.Contains(got, `"1MB"`) {
		t.Fatalf("scenario does not carry the explicit size:\n%s", got)
	}
}
