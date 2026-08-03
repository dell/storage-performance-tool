package integrity

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCanonicalCSVSharedWriterAndParserCorpus(t *testing.T) {
	type writerCase struct {
		Name   string   `json:"name"`
		Fields []string `json:"fields"`
		Base64 string   `json:"base64"`
	}
	fixture := sharedIntegrityFixtureFile(t, "canonical-v1", "writer-cases.json")
	encoded, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var cases []writerCase
	if err = json.Unmarshal(encoded, &cases); err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			expected, decodeErr := base64.StdEncoding.DecodeString(test.Base64)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if test.Fields == nil {
				path := filepath.Join(t.TempDir(), "manifest.csv")
				if err := os.WriteFile(path, expected, 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := validateCanonicalManifestEvidence(path); err == nil {
					t.Fatal("validator accepted invalid shared corpus case")
				}
				return
			}

			var output bytes.Buffer
			writer := newCanonicalCSVWriter(&output)
			if err := writer.Write(canonicalHeader); err != nil {
				t.Fatal(err)
			}
			if err := writer.Write(test.Fields); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(output.Bytes(), expected) {
				t.Fatalf("writer bytes = %q, want %q", output.Bytes(), expected)
			}

			path := filepath.Join(t.TempDir(), "manifest.csv")
			if err := os.WriteFile(path, expected, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := validateCanonicalManifestEvidence(path); err != nil {
				t.Fatalf("validator rejected writer golden: %v", err)
			}

			reader := newCanonicalCSVReader(bytes.NewReader(expected))
			header, err := reader.Read()
			if err != nil || !reflect.DeepEqual(header, canonicalHeader) {
				t.Fatalf("parsed header = %#v, %v", header, err)
			}
			fields, err := reader.Read()
			if err != nil || !reflect.DeepEqual(fields, test.Fields) {
				t.Fatalf("parsed fields = %#v, %v; want %#v", fields, err, test.Fields)
			}
			if _, err := reader.Read(); !errors.Is(err, io.EOF) {
				t.Fatalf("trailing parser error = %v, want EOF", err)
			}
		})
	}
}
