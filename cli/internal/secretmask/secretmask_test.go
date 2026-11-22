package secretmask

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestTextMasksCredentialAssignments(t *testing.T) {
	input := "run --access-key LOCALACCESS --secret-key=LOCALSECRET storage.auth.secret=CFGSECRET"
	got := Text(input)
	for _, leaked := range []string{"LOCALACCESS", "LOCALSECRET", "CFGSECRET"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("Text leaked %q in %q", leaked, got)
		}
	}
	for _, want := range []string{"--access-key ***", "--secret-key=***", "storage.auth.secret=***"} {
		if !strings.Contains(got, want) {
			t.Fatalf("Text missing %q in %q", want, got)
		}
	}
}

func TestYAMLMasksCredentialFields(t *testing.T) {
	input := []byte(`storage:
  auth:
    uid: LOCALACCESS
    secret: LOCALSECRET
  driver:
    type: s3
aws:
  accessKeyId: AWSACCESS
  secretAccessKey: AWSSECRET
`)
	got := string(YAML(input))
	for _, leaked := range []string{"LOCALACCESS", "LOCALSECRET", "AWSACCESS", "AWSSECRET"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("YAML leaked %q:\n%s", leaked, got)
		}
	}
	if strings.Count(got, masked) < 4 {
		t.Fatalf("YAML did not mask expected fields:\n%s", got)
	}
	if !strings.Contains(got, "type: s3") {
		t.Fatalf("YAML should preserve non-sensitive fields:\n%s", got)
	}
}

// A weighted step's config.yaml in the shape the engine writes it: the base config, then one
// document per sub-load, each starting with "---".
const weightedConfig = `---
storage:
  auth:
    uid: BASEUID
    secret: BASESECRET
  driver:
    type: s3
load:
  op:
    limit:
      fail:
        count: 20
---
storage:
  auth:
    uid: SUB1UID
    secret: SUB1SECRET
  net:
    http:
      headers:
        x-amz-security-token: SUB1TOKEN
load:
  op:
    weight: 50
    limit:
      count: 100
item:
  output:
    path: /bkt/wl
---
aws:
  secretAccessKey: SUB2AWSSECRET
storage:
  auth:
    uid: SUB2UID
    secret: SUB2SECRET
load:
  op:
    type: read
    weight: 50
item:
  input:
    file: items.csv
`

func decodeAll(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []map[string]any
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return docs
		}
		if err != nil {
			t.Fatalf("masked output is not valid YAML: %v\n%s", err, data)
		}
		docs = append(docs, doc)
	}
}

func lookup(doc map[string]any, path ...string) any {
	var cur any = doc
	for _, key := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[key]
	}
	return cur
}

func TestYAMLKeepsEveryDocumentAndMasksEach(t *testing.T) {
	got := YAML([]byte(weightedConfig))
	for _, leaked := range []string{"BASEUID", "BASESECRET", "SUB1UID", "SUB1SECRET", "SUB1TOKEN", "SUB2UID", "SUB2SECRET", "SUB2AWSSECRET"} {
		if bytes.Contains(got, []byte(leaked)) {
			t.Fatalf("YAML leaked %q:\n%s", leaked, got)
		}
	}
	docs := decodeAll(t, got)
	if len(docs) != 3 {
		t.Fatalf("documents = %d, want 3:\n%s", len(docs), got)
	}
	checks := []struct {
		doc  int
		path []string
		want any
	}{
		{0, []string{"load", "op", "limit", "fail", "count"}, 20},
		{1, []string{"storage", "auth", "uid"}, masked},
		{1, []string{"storage", "auth", "secret"}, masked},
		{1, []string{"storage", "net", "http", "headers", "x-amz-security-token"}, masked},
		{1, []string{"load", "op", "weight"}, 50},
		{1, []string{"load", "op", "limit", "count"}, 100},
		{1, []string{"item", "output", "path"}, "/bkt/wl"},
		{2, []string{"storage", "auth", "uid"}, masked},
		{2, []string{"storage", "auth", "secret"}, masked},
		{2, []string{"aws", "secretAccessKey"}, masked},
		{2, []string{"load", "op", "type"}, "read"},
		{2, []string{"item", "input", "file"}, "items.csv"},
	}
	for _, c := range checks {
		if v := lookup(docs[c.doc], c.path...); v != c.want {
			t.Errorf("document %d %s = %v, want %v", c.doc+1, strings.Join(c.path, "."), v, c.want)
		}
	}
}

func TestYAMLKeepsManyDocuments(t *testing.T) {
	const n = 125
	var in strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&in, "---\nstorage:\n  auth:\n    uid: UID%03d\n    secret: SECRET%03d\nload:\n  op:\n    weight: %d\n", i, i, i)
	}
	got := YAML([]byte(in.String()))
	if bytes.Contains(got, []byte("UID")) || bytes.Contains(got, []byte("SECRET")) {
		t.Fatalf("YAML leaked a credential:\n%s", got)
	}
	docs := decodeAll(t, got)
	if len(docs) != n {
		t.Fatalf("documents = %d, want %d", len(docs), n)
	}
	for i, doc := range docs {
		if v := lookup(doc, "load", "op", "weight"); v != i {
			t.Fatalf("document %d weight = %v, want %d (order lost)", i+1, v, i)
		}
		if lookup(doc, "storage", "auth", "uid") != masked || lookup(doc, "storage", "auth", "secret") != masked {
			t.Fatalf("document %d not masked: %v", i+1, doc)
		}
	}
}

// singleDocumentYAML is the implementation before multi-document support; a single document must
// still give exactly its bytes.
func singleDocumentYAML(data []byte) []byte {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return []byte(Text(string(data)))
	}
	maskYAMLNode(&node, nil)
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&node); err != nil {
		return []byte(Text(string(data)))
	}
	_ = enc.Close()
	return out.Bytes()
}

func TestYAMLSingleDocumentBytesAreUnchanged(t *testing.T) {
	// The engine's leading "---" is dropped and its quotes kept, as before.
	const golden = `# comment
storage:
  auth:
    uid: '***'
    secret: '***'
  driver:
    type: "s3"
load:
  op:
    limit:
      fail:
        count: 20
      rate: 0.0
  step:
    limit:
      time: "12s"
`
	engineShaped := "---\n# comment\nstorage:\n  auth:\n    uid: \"LOCALUID\"\n    secret: \"LOCALSECRET\"\n  driver:\n    type: \"s3\"\nload:\n  op:\n    limit:\n      fail:\n        count: 20\n      rate: 0.0\n  step:\n    limit:\n      time: \"12s\"\n"
	if got := string(YAML([]byte(engineShaped))); got != string(singleDocumentYAML([]byte(engineShaped))) || got != golden {
		t.Fatalf("single document changed:\n%s", got)
	}
	inputs := map[string]string{
		"plain":           "a: 1\nb:\n  - x\n  - y\n",
		"leading marker":  "---\na: 1\n",
		"flow and quotes": "a: {b: 'c', d: \"e\"}\nsecret: [1, 2]\n",
		"block scalar":    "text: |\n  line one\n  line two\nkey: value # trailing\n",
		"invalid":         "load: [unclosed\nsecret: LEAK\n",
	}
	for name, in := range inputs {
		if got, want := string(YAML([]byte(in))), string(singleDocumentYAML([]byte(in))); got != want {
			t.Errorf("%s: got %q, want %q", name, got, want)
		}
	}
}

func TestYAMLMalformedLaterDocumentMasksTheWholeInputAsText(t *testing.T) {
	in := "---\nstorage:\n  auth:\n    secret: DOC1SECRET\n    password: DOC1PASSWORD\n---\nstorage: [unclosed\n  auth:\n    secret: DOC2SECRET\n    token: DOC2TOKEN\n    access_key: DOC2ACCESSKEY\n"
	got := string(YAML([]byte(in)))
	if got != Text(in) {
		t.Fatalf("got %q, want the Text fallback %q", got, Text(in))
	}
	for _, leaked := range []string{"DOC1SECRET", "DOC1PASSWORD", "DOC2SECRET", "DOC2TOKEN", "DOC2ACCESSKEY"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("fallback leaked %q:\n%s", leaked, got)
		}
	}
	if !strings.Contains(got, "storage: [unclosed") {
		t.Fatalf("fallback should keep the whole input:\n%s", got)
	}
}

func TestYAMLEmptyInputIsNull(t *testing.T) {
	for _, in := range []string{"", "   \n", "\n\n"} {
		if got := string(YAML([]byte(in))); got != "null\n" {
			t.Errorf("YAML(%q) = %q, want %q", in, got, "null\n")
		}
	}
}
