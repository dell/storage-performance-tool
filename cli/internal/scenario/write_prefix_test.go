package scenario

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestWritePrefixAllVariants(t *testing.T) {
	prefixPattern := regexp.MustCompile(`"naming":\s*\{"prefix":\s*("(?:[^"\\]|\\.)*")\}`)
	for _, mode := range []string{"count", "duration", "default"} {
		for _, cleanup := range []bool{false, true} {
			for _, save := range []bool{false, true} {
				for _, prefix := range []string{"", "quickstart/", "quickstart-", "quotes\"/back\\slash/\n"} {
					t.Run(fmt.Sprintf("%s/cleanup=%t/save=%t/prefix=%q", mode, cleanup, save, prefix), func(t *testing.T) {
						p := Params{WorkloadType: "write", Bucket: "bucket", Threads: 1, ObjectSize: "1KiB", Prefix: prefix,
							Cleanup: cleanup, SaveItems: save, BaseTimestamp: "20261001.120000.000"}
						if mode == "count" {
							p.ObjectCount = 3
						}
						if mode == "duration" {
							p.Duration = "1s"
						}
						text, err := GenerateWriteScenario(p)
						if err != nil {
							t.Fatal(err)
						}
						matches := prefixPattern.FindAllStringSubmatch(text, -1)
						if prefix == "" {
							if len(matches) != 0 {
								t.Fatal("omitted prefix changed naming defaults")
							}
						} else {
							if len(matches) != 1 {
								t.Fatalf("want exactly one CREATE prefix, got %d", len(matches))
							}
							var got string
							if err := json.Unmarshal([]byte(matches[0][1]), &got); err != nil {
								t.Fatal(err)
							}
							if got != prefix {
								t.Fatalf("prefix changed: %q, want %q", got, prefix)
							}
							if cleanup {
								createAt, deleteAt := strings.Index(text, "CreateLoad"), strings.Index(text, "DeleteLoad")
								prefixAt := strings.Index(text, matches[0][0])
								if prefixAt < createAt || prefixAt > deleteAt {
									t.Fatal("prefix must apply only to CREATE")
								}
							}
						}
						plan, err := BuildStepPlanFromScenario(text)
						if err != nil {
							t.Fatal(err)
						}
						if save {
							if !strings.Contains(text, `"`+plan.Steps[0].ID+`" + "/items.csv"`) {
								t.Fatal("saved manifest not under labeled CREATE step")
							}
							if strings.Contains(text, "cleanup(itemsFile);") {
								t.Fatal("saved manifest removed before retrieval")
							}
						}
						if cleanup && !strings.Contains(text, `"file": itemsFile`) {
							t.Fatal("cleanup lost saved identities")
						}
					})
				}
			}
		}
	}
}
