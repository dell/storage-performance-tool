package scenario

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// engineReadsTrue reads a setting the way the engine's configuration reads a boolean: a YAML
// boolean as itself, and a string with Java's Boolean.parseBoolean.
func engineReadsTrue(value any) bool {
	switch value := value.(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(value, "true")
	default:
		return false
	}
}

// EngineOverridesPersistOpTraces agrees with the defaults GenerateDefaults sends to the engine,
// including the order the overrides are applied in, and finds nothing in overrides it rejects.
func TestEngineOverridesPersistOpTracesMatchesGeneratedDefaults(t *testing.T) {
	for _, test := range []struct {
		name      string
		overrides []string
		want      bool
	}{
		{name: "no override"},
		{name: "dot path", overrides: []string{"output.metrics.trace.persist=true"}, want: true},
		{name: "dash path", overrides: []string{"output-metrics-trace-persist=true"}, want: true},
		{name: "map value of trace", overrides: []string{"output.metrics.trace={persist: true}"}, want: true},
		{name: "map value of metrics", overrides: []string{"output.metrics={trace: {persist: true}}"}, want: true},
		{name: "YAML boolean in capitals", overrides: []string{"output.metrics.trace.persist=TRUE"}, want: true},
		{name: "string in mixed case", overrides: []string{`output.metrics.trace.persist="True"`}, want: true},
		{name: "string with a space", overrides: []string{`output.metrics.trace.persist=" true"`}},
		{name: "yes is a string the engine reads as false", overrides: []string{"output.metrics.trace.persist=yes"}},
		{name: "number", overrides: []string{"output.metrics.trace.persist=1"}},
		{name: "off", overrides: []string{"output.metrics.trace.persist=false"}},
		{name: "a later override turns it on", overrides: []string{
			"output.metrics.trace.persist=false", "output.metrics.trace.persist=true",
		}, want: true},
		{name: "a later override turns it off", overrides: []string{
			"output.metrics.trace.persist=true", "output.metrics.trace.persist=false",
		}},
		{name: "a later map value replaces it", overrides: []string{
			"output.metrics.trace.persist=true", "output.metrics.trace={}",
		}},
		{name: "an unrelated override keeps it", overrides: []string{
			"output.metrics.trace.persist=true", "storage.driver.threads=6",
		}, want: true},
		{name: "an override without a value", overrides: []string{"output.metrics.trace.persist"}},
		{name: "an override under a scalar", overrides: []string{
			"output.metrics.trace=on", "output.metrics.trace.persist=true",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := EngineOverridesPersistOpTraces(test.overrides); got != test.want {
				t.Fatalf("EngineOverridesPersistOpTraces(%q) = %t, want %t", test.overrides, got, test.want)
			}
			data, err := GenerateDefaults(Params{
				WorkloadType: "write", Endpoint: "http://minio:9000", AccessKey: "testkey",
				SecretKey: "testsecret", Bucket: "testbucket", Threads: 4, EngineOverrides: test.overrides,
			})
			if err != nil {
				if test.want {
					t.Fatalf("GenerateDefaults rejected overrides that turn tracing on: %v", err)
				}
				return
			}
			var root struct {
				Output struct {
					Metrics struct {
						Trace map[string]any `yaml:"trace"`
					} `yaml:"metrics"`
				} `yaml:"output"`
			}
			if err := yaml.Unmarshal(data, &root); err != nil {
				t.Fatal(err)
			}
			if got := engineReadsTrue(root.Output.Metrics.Trace["persist"]); got != test.want {
				t.Fatalf("generated output.metrics.trace.persist = %#v, engine reads %t, want %t",
					root.Output.Metrics.Trace["persist"], got, test.want)
			}
		})
	}
}
