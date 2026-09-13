package scenario

import (
	"strings"
	"testing"
)

func TestRangeDefaultsRejectMergedConflicts(t *testing.T) {
	for _, override := range []string{
		"load.op.read.range.size=3", "load.op.read.range={offset: 0}",
		"item.data.ranges.threshold=1KiB", "item-data-ranges-random=1", "item.data.ranges.fixed=[0-2]",
		"item.data={ranges: {threshold: 1}}", "item.data.verify=true",
		"load.op.recycle.content.update=true", "storage.integrity.mode=metadata",
		"storage.net.http.read.metadata.only=true", "storage.object.tagging.enabled=true",
		"storage.net.timeoutMilliSec=0", "storage.net.timeoutMilliSec=-1", "storage.net.timeoutMilliSec=1KiB", "item.data.ranges.random=0KiB", "item.type=path",
		"storage.driver.type=s3-aws", "item.data.ranges=invalid",
	} {
		t.Run(override, func(t *testing.T) {
			p := Params{WorkloadType: "read", Endpoint: "http://localhost:9000", RangeSize: "3", EngineOverrides: []string{override}}
			data, err := GenerateDefaults(p)
			if err == nil || data != nil {
				t.Fatalf("accepted conflict %q", override)
			}
		})
	}
}

func TestRangeDefaultsAllowInactiveFinalOverridesAndOrdinaryCompatibility(t *testing.T) {
	p := Params{WorkloadType: "read", Endpoint: "http://localhost:9000", RangeSize: "3", EngineOverrides: []string{
		"item.data.ranges.threshold=1KiB", "item-data-ranges-threshold=0",
		"item.data.ranges.random=0", "item.data.ranges.fixed=[]", "item.data.ranges.concat=true",
		"item.data.verify=false", "storage.net.timeoutMilliSec=2000",
	}}
	if _, err := GenerateDefaults(p); err != nil {
		t.Fatal(err)
	}
	p.RangeSize = ""
	p.WorkloadType = "write"
	p.EngineOverrides = []string{"item.data.ranges.threshold=1KiB"}
	if _, err := GenerateDefaults(p); err != nil {
		t.Fatalf("ordinary defaults changed: %v", err)
	}
	p.WorkloadType = "read"
	p.RangeSize = "3"
	p.EngineOverrides = []string{"storage.integrity.mode=private-value"}
	_, err := GenerateDefaults(p)
	if err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatalf("error disclosed value: %v", err)
	}
}
