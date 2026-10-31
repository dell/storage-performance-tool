package scenario

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
	"gopkg.in/yaml.v3"
)

func TestValidateBucketListAcceptsNamesAsWritten(t *testing.T) {
	for _, names := range [][]string{
		{"ps"},
		{"ps-eu", "PS_US"},
		{"ab", "a..b", "-lead", "UPPER", "under_score", "x.y", "192.168.1.1", "xn--name", "name-s3alias"},
		{"Case", "case"},
		{"yes", "on", "null", "0123", "~", "*star", "&anchor", "!tag", "a:b", "a#b", "{x}", "[y]", `"q"`, "'q'"},
	} {
		if err := ValidateBucketList(names); err != nil {
			t.Fatalf("%q: %v", names, err)
		}
	}
	thousand := make([]string, MaxBucketCount)
	for i := range thousand {
		thousand[i] = "b" + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + strings.Repeat("0", i/26)
	}
	if err := ValidateBucketList(thousand); err != nil {
		t.Fatalf("%d names: %v", MaxBucketCount, err)
	}
}

func TestValidateBucketListRejectsWhatTheEngineRejects(t *testing.T) {
	tooMany := make([]string, MaxBucketCount+1)
	for i := range tooMany {
		tooMany[i] = "b" + strings.Repeat("0", i)
	}
	for _, c := range []struct {
		names []string
		want  string
	}{
		{nil, "no bucket names"},
		{[]string{}, "no bucket names"},
		{tooMany, "1001 bucket names are listed, at most 1000"},
		{[]string{"a", ""}, `bucket name 2 "" must not be empty`},
		{[]string{"a/b"}, "must not contain '/'"},
		{[]string{"a,b"}, "must not contain ','"},
		{[]string{"a b"}, "must not contain whitespace"},
		{[]string{"a\tb"}, "must not contain whitespace"},
		{[]string{"a b"}, "must not contain whitespace"},
		{[]string{"a\x1fb"}, "must not contain a control character"},
		{[]string{"a${b}"}, "expression marker ${"},
		{[]string{"a#{b}"}, "expression marker #{"},
		{[]string{"a%{b}"}, "expression marker %{"},
		{[]string{"ps", "eu", "ps"}, `bucket name 3 "ps" repeats bucket name 1`},
	} {
		err := ValidateBucketList(c.names)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%q: err = %v, want %q", c.names, err, c.want)
		}
	}
}

func TestBucketSetSizeCountsTheListTheCountOrOneBucket(t *testing.T) {
	for _, c := range []struct {
		params Params
		want   int
	}{
		{Params{Bucket: "b"}, 1},
		{Params{Bucket: "b", BucketCount: 1}, 1},
		{Params{Bucket: "b", BucketCount: 4}, 4},
		{Params{Bucket: "a", BucketNames: []string{"a", "b", "c"}}, 3},
	} {
		if got := c.params.BucketSetSize(); got != c.want {
			t.Fatalf("%+v: BucketSetSize() = %d, want %d", c.params, got, c.want)
		}
	}
}

func generatedTargetBucketNode(t *testing.T, params Params) map[string]any {
	t.Helper()
	data, err := GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Storage struct {
			TargetBucket map[string]any `yaml:"targetBucket"`
		} `yaml:"storage"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	return root.Storage.TargetBucket
}

func TestGeneratedDefaultsCarryTheListedNamesInOrder(t *testing.T) {
	params := Params{WorkloadType: workload.Write, Endpoint: "http://127.0.0.1:9000", AccessKey: "a", SecretKey: "s",
		Bucket: "ps-eu", BucketNames: []string{"ps-eu", "PS_US", "ab"}}
	node := generatedTargetBucketNode(t, params)
	names, ok := node["names"].([]any)
	if !ok || len(names) != 3 || names[0] != "ps-eu" || names[1] != "PS_US" || names[2] != "ab" {
		t.Fatalf("storage.targetBucket = %v, want the names in order", node)
	}
	if _, ok := node["count"]; ok {
		t.Fatalf("a list sets no count: %v", node)
	}
	params.TargetBucketProvisionDisabled = true
	if node := generatedTargetBucketNode(t, params); node["provision"] != false || len(node["names"].([]any)) != 3 {
		t.Fatalf("opt-out with a list: %v", node)
	}
}

// Names are used as written, so a name YAML would read as another type must stay a string.
func TestGeneratedDefaultsKeepEveryListedNameAString(t *testing.T) {
	names := []string{"yes", "on", "off", "no", "y", "n", "true", "null", "~", "0123", "1e3", "0x1F", "*star", "&anchor", "!tag", "a:b", "a#b", "{x}", "[y]", `"q"`, "'q'", "@at", "%pct", "|pipe", ">gt", "-"}
	for _, overrides := range [][]string{nil, {"storage.targetBucket={provision: false}"}} {
		params := Params{WorkloadType: workload.Write, Endpoint: "http://127.0.0.1:9000", AccessKey: "a", SecretKey: "s",
			Bucket: names[0], BucketNames: names, EngineOverrides: overrides}
		data, err := GenerateDefaults(params)
		if err != nil {
			t.Fatal(err)
		}
		var root map[string]any
		if err := yaml.Unmarshal(data, &root); err != nil {
			t.Fatal(err)
		}
		got := root["storage"].(map[string]any)["targetBucket"].(map[string]any)["names"].([]any)
		if len(got) != len(names) {
			t.Fatalf("overrides %q: names = %v", overrides, got)
		}
		for i, name := range names {
			if s, ok := got[i].(string); !ok || s != name {
				t.Fatalf("overrides %q: name %d = %#v, want the string %q\n%s", overrides, i, got[i], name, data)
			}
		}
		// The engine reads YAML 1.1, where yes, on, off, no, y and n are booleans unless quoted.
		for _, name := range []string{"yes", "on", "off", "no", "y", "n"} {
			if !strings.Contains(string(data), `- "`+name+`"`) {
				t.Fatalf("overrides %q: %q must be quoted for a YAML 1.1 reader:\n%s", overrides, name, data)
			}
		}
	}
}

func TestGeneratedDefaultsRestoreTheNamesAfterOverridesReplaceTheirMap(t *testing.T) {
	for _, c := range []struct {
		override  string
		provision any
	}{
		{"storage.targetBucket.provision=false", false},
		{"storage.targetBucket={provision: false}", false},
		{"storage-targetBucket={}", nil},
		{"storage.targetBucket=null", nil},
		{"storage={region: eu-west-1}", nil},
	} {
		params := Params{WorkloadType: workload.Write, Endpoint: "http://127.0.0.1:9000", AccessKey: "a", SecretKey: "s",
			Bucket: "ps-eu", BucketNames: []string{"ps-eu", "PS_US"}, EngineOverrides: []string{c.override}}
		node := generatedTargetBucketNode(t, params)
		names, ok := node["names"].([]any)
		if !ok || len(names) != 2 || names[0] != "ps-eu" || names[1] != "PS_US" {
			t.Fatalf("override %q: storage.targetBucket = %v, want both names", c.override, node)
		}
		if node["provision"] != c.provision {
			t.Fatalf("override %q: provision = %v, want %v", c.override, node["provision"], c.provision)
		}
	}
	params := Params{WorkloadType: workload.Write, Endpoint: "http://127.0.0.1:9000", AccessKey: "a", SecretKey: "s",
		Bucket: "ps-eu", BucketNames: []string{"ps-eu", "PS_US"}, EngineOverrides: []string{"storage.targetBucket=5"}}
	if _, err := GenerateDefaults(params); err == nil || !strings.Contains(err.Error(), "scalar") {
		t.Fatalf("a scalar storage.targetBucket cannot hold the names: err = %v", err)
	}
}

func TestOneBucketGeneratesNoTargetBucketSetting(t *testing.T) {
	for _, params := range []Params{
		{WorkloadType: workload.Write, Endpoint: "http://127.0.0.1:9000", AccessKey: "a", SecretKey: "s", Bucket: "ps"},
		{WorkloadType: workload.Write, Endpoint: "http://127.0.0.1:9000", AccessKey: "a", SecretKey: "s", Bucket: "ps", BucketNames: []string{"ps"}},
	} {
		if node := generatedTargetBucketNode(t, params); node != nil {
			t.Fatalf("%+v: storage.targetBucket = %v, want none", params.BucketNames, node)
		}
	}
}

// The output path carries the first listed name as its placeholder; the scenario is otherwise the
// one a single bucket of that name produces, because the list lives in the defaults.
func TestAListLeavesTheScenarioToItsPlaceholder(t *testing.T) {
	base := Params{WorkloadType: workload.Write, Bucket: "ps-eu", ObjectSize: "1KiB", ObjectCount: 5, Threads: 1, BaseTimestamp: "20261031.120000.000"}
	single, err := GenerateScenario(base)
	if err != nil {
		t.Fatal(err)
	}
	listed := base
	listed.BucketNames = []string{"ps-eu", "PS_US"}
	got, err := GenerateScenario(listed)
	if err != nil {
		t.Fatal(err)
	}
	if got != single {
		t.Fatal("a write scenario over a list must equal the single-bucket scenario of its placeholder")
	}
	if !strings.Contains(got, `"/ps-eu`) || strings.Contains(got, "PS_US") {
		t.Fatalf("the output path carries only the placeholder:\n%s", got)
	}
}
