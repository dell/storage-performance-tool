package scenario

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRangePolicyBoundsAndPresence(t *testing.T) {
	for _, tc := range []struct {
		size, offset, align string
		valid               bool
	}{
		{"", "", "", true}, {"", "0", "", false}, {"", "", "0", false}, {"0", "", "", false},
		{"3", "0", "0", true}, {"3", "6", "3", true}, {"3", "5", "3", false}, {"64KiB", "1MiB", "4KiB", true},
		{"9223372036854775807", "1", "", true}, {"9223372036854775807", "2", "", false},
		{"1", "9223372036854775807", "", true}, {"8EiB", "", "", false}, {"-1", "", "", false},
		{"1", "-1", "", false}, {"1", "", "-1", false}, {"1.5KiB", "", "", false},
	} {
		p, err := ParseRangePolicy(Params{WorkloadType: "read", RangeSize: tc.size, RangeOffset: tc.offset, RangeAlign: tc.align})
		if (err == nil) != tc.valid {
			t.Fatalf("%+v: policy=%+v err=%v", tc, p, err)
		}
		if err == nil && p != nil && (p.Offset != nil) != (tc.offset != "") {
			t.Fatal("offset presence lost")
		}
	}
	p, err := ParseRangePolicy(Params{WorkloadType: "read", RangeSize: "1", RangeOffset: "9223372036854775807"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"offset":"9223372036854775807"`) {
		t.Fatalf("lossy policy: %s", data)
	}
	for _, w := range []string{"write", "read-verify", "mixed", "delete", "list"} {
		if _, err := GenerateScenario(Params{WorkloadType: w, RangeSize: "3"}); err == nil {
			t.Fatalf("accepted workload %s", w)
		}
	}
	for _, d := range []string{"aws", "rdma", "s3-aws", "unknown"} {
		if _, err := ParseRangePolicy(Params{WorkloadType: "read", S3Driver: d, RangeSize: "3"}); err == nil {
			t.Fatalf("accepted driver %s", d)
		}
	}
}

func TestRangePolicyOnlyInReadPhase(t *testing.T) {
	for _, items := range []string{"", "/work/items.csv"} {
		for _, cleanup := range []bool{false, true} {
			for _, count := range []int{0, 2} {
				p := Params{WorkloadType: "read", Bucket: "test", Threads: 1, ObjectSize: "8", ItemsFile: items, Cleanup: cleanup, ObjectCount: count, Duration: "1s", RangeSize: "3", RangeOffset: "0", BaseTimestamp: "20260913.120000.000"}
				s, err := GenerateScenario(p)
				if err != nil {
					t.Fatal(err)
				}
				found := 0
				for _, c := range parseGeneratedScenarioConfigs(t, strings.NewReplacer(".config(sharedConfig)", "", ": itemsFile", ": \"items.csv\"", ": seedCount", ": 1", ": duration", ": \"1s\"", ": itemCount", ": 2", ": readCount", ": 2").Replace(s)) {
					r, has := configPath(c, "load", "op", "read", "range")
					op, _ := configPath(c, "load", "op", "type")
					if has {
						found++
						mode, _ := configPath(c, "load", "op", "recycle", "mode")
						if mode != true {
							t.Fatalf("count=%d recycle=%v", count, mode)
						}
						if op != "read" {
							t.Fatalf("range leaked into %v", op)
						}
						m := r.(map[string]interface{})
						if m["size"] != "3" || m["offset"] != "0" || m["align"] != "1" {
							t.Fatalf("policy=%v", r)
						}
					}
				}
				if found != 1 {
					t.Fatalf("found %d read policies", found)
				}
				p.RangeSize = ""
				p.RangeOffset = ""
				ordinary, err := GenerateScenario(p)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(ordinary, `"range":`) {
					t.Fatal("disabled policy leaked")
				}
			}
		}
	}
}

func TestRangeReadRejectsActiveMultipartThresholdBeforeGeneratingSeed(t *testing.T) {
	for _, part := range []string{"1", "5MiB", "invalid"} {
		for _, items := range []string{"", "/work/items.csv"} {
			p := Params{WorkloadType: "read", RangeSize: "3", PartSize: part, ItemsFile: items}
			if s, err := GenerateScenario(p); err == nil || s != "" {
				t.Fatalf("part=%q items=%q accepted", part, items)
			}
		}
	}
	for _, part := range []string{"", "0", "0KiB"} {
		if _, err := ParseRangePolicy(Params{WorkloadType: "read", RangeSize: "3", PartSize: part}); err != nil {
			t.Fatal(err)
		}
	}
	if p, err := ParseRangePolicy(Params{WorkloadType: "write", PartSize: "5MiB"}); err != nil || p != nil {
		t.Fatal("ordinary multipart changed")
	}
}
