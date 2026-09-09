package results

import (
	"encoding/json"
	"testing"
)

func TestManifestJSONClearsKnownOptionalFieldsWithoutLosingIndependentFields(t *testing.T) {
	var manifest Manifest
	if err := json.Unmarshal([]byte(`{"steps":[],"runFiles":[{"name":"old"}],"integrity":{"complete":true},"qualification":{"samples":9007199254740993}}`), &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.RunFiles = nil
	manifest.Integrity = nil
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"runFiles", "integrity"} {
		if _, ok := fields[name]; ok {
			t.Fatalf("cleared field %s resurrected: %s", name, data)
		}
	}
	if string(fields["qualification"]) != `{"samples":9007199254740993}` {
		t.Fatalf("independent value changed: %s", data)
	}
	// Reusing the decoder target must not retain fields from an earlier bundle.
	if err := json.Unmarshal([]byte(`{"steps":[]}`), &manifest); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	fields = nil
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["qualification"]; ok {
		t.Fatalf("stale independent field survived: %s", data)
	}
}
