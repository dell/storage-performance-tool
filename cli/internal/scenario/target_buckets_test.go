package scenario

import (
	"reflect"
	"strings"
	"testing"
)

func TestTargetBucketMemberNamesAreUnpaddedAndNeverDependOnTheCount(t *testing.T) {
	ten, err := TargetBucketMemberNames("ps", 10)
	if err != nil {
		t.Fatal(err)
	}
	eleven, err := TargetBucketMemberNames("/ps/", 11)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ten, eleven[:10]) || eleven[10] != "ps-10" || ten[1] != "ps-1" {
		t.Fatalf("ten = %v, eleven = %v", ten, eleven)
	}
}

func TestTargetBucketMemberNamesKeepOneBucketUnvalidated(t *testing.T) {
	for _, bucket := range []string{"Bucket_With.Old..Rules", "", "/b"} {
		got, err := TargetBucketMemberNames(bucket, 1)
		if err != nil || !reflect.DeepEqual(got, []string{bucket}) {
			t.Fatalf("TargetBucketMemberNames(%q, 1) = %v, %v", bucket, got, err)
		}
	}
}

func TestTargetBucketMemberNamesBoundsTheCount(t *testing.T) {
	for _, count := range []int{0, -1, 1001} {
		if _, err := TargetBucketMemberNames("bench", count); err == nil ||
			!strings.Contains(err.Error(), "between 1 and 1000") {
			t.Fatalf("count %d: err = %v", count, err)
		}
	}
	for _, count := range []int{2, 1000} {
		if got, err := TargetBucketMemberNames("bench", count); err != nil || len(got) != count {
			t.Fatalf("count %d: %d names, err = %v", count, len(got), err)
		}
	}
}

func TestTargetBucketMemberNamesApplyTheRulesToTheExpandedName(t *testing.T) {
	if got, err := TargetBucketMemberNames("ab", 2); err != nil || got[0] != "ab-0" {
		t.Fatalf("short base: %v, %v", got, err)
	}
	if got, err := TargetBucketMemberNames("bench/prefix", 2); err != nil || got[1] != "bench-1" {
		t.Fatalf("base with a prefix: %v, %v", got, err)
	}
	base := strings.Repeat("b", 61)
	if _, err := TargetBucketMemberNames(base, 10); err != nil {
		t.Fatalf("63-character member rejected: %v", err)
	}
	cases := map[string]string{
		base:                "3 to 63 characters",
		"a..b":              "two adjacent periods",
		"xn--bench":         `reserved prefix "xn--"`,
		"sthree-bench":      `reserved prefix "sthree-"`,
		"amzn-s3-demo-b":    `reserved prefix "amzn-s3-demo-"`,
		"Bench":             "only lowercase letters",
		"-bench":            "start and end",
		"/":                 "requires --bucket to name a base bucket",
		"bench_underscored": "only lowercase letters",
	}
	for bucket, want := range cases {
		count := 2
		if bucket == base {
			count = 11
		}
		if _, err := TargetBucketMemberNames(bucket, count); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("TargetBucketMemberNames(%q) err = %v, want %q", bucket, err, want)
		}
	}
	if got := bucketNameProblem("192.168.5.4"); !strings.Contains(got, "IP address") {
		t.Fatalf("IP form: %q", got)
	}
	for _, suffix := range reservedBucketSuffixes {
		if got := bucketNameProblem("bench" + suffix); !strings.Contains(got, suffix) {
			t.Fatalf("suffix %s: %q", suffix, got)
		}
	}
}

func TestGeneratedDefaultsCarryABucketSetOnlyWhenThereIsOne(t *testing.T) {
	base := Params{WorkloadType: "write", Endpoint: "http://127.0.0.1:9000", Bucket: "bench", AccessKey: "a", SecretKey: "s"}
	single, err := GenerateDefaults(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(single), "targetBucket") {
		t.Fatalf("one bucket must leave the defaults unchanged:\n%s", single)
	}
	set := base
	set.BucketCount = 4
	got, err := GenerateDefaults(set)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "targetBucket:\n        count: 4\n") && !strings.Contains(string(got), "targetBucket:\n    count: 4\n") {
		t.Fatalf("bucket set missing from defaults:\n%s", got)
	}
	if strings.Contains(string(got), "provision") {
		t.Fatalf("provisioning stays at its shipped default:\n%s", got)
	}
	optOut := set
	optOut.TargetBucketProvisionDisabled = true
	both, err := GenerateDefaults(optOut)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(both), "count: 4") || !strings.Contains(string(both), "provision: false") {
		t.Fatalf("bucket set and opt-out must both be present:\n%s", both)
	}
}
