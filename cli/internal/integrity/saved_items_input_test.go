package integrity

import (
	"fmt"
	"strings"
	"testing"
)

// savedItemsLineMessage is the refusal of one line of a DELETE items file read as saved items.
func savedItemsLineMessage(line int, reason string) string {
	return `items file is neither a canonical manifest with the exact header ["bucket" "key" "size" "version_id"] ` +
		"nor a saved S3 items file (<name>,<hex offset>,<size>,<layer>/<mask>, as write --save-items saves it): " +
		fmt.Sprintf("line %d %s", line, reason)
}

// The reasons for a "~" or a "%" in a saved row, spelled out so that the tests pin the text.
const (
	tildeReason = `has a "~" in its key, as a versioned write names a version (key~versionId, and ` +
		`the version ID can hold "/"); a saved items file deletes current keys only, so convert such ` +
		"a file by hand to a canonical bucket,key,size,version_id manifest of the stored keys and versions"
	percentReason = `has a "%" in its object name, and a write can store a percent escape decoded, so ` +
		"the row may not name the stored key; convert such a file by hand to a canonical " +
		"bucket,key,size,version_id manifest of the stored keys"
)

func notAnObjectPath(line int) string {
	return savedItemsLineMessage(line, `has an object name that does not start with "/" (not an S3 object path)`)
}

func TestParseSavedItemLineConvertsToACurrentKeyRecord(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		bucket string
		key    string
		size   int64
	}{
		{"plain write", "/b/s0000000/9dpngcferyc9,cb00c2fdd62852c,4096,0/0", "b", "s0000000/9dpngcferyc9", 4096},
		{"prefix", "/b/pre/s0000003/k1,2a0f,10,0/0", "b", "pre/s0000003/k1", 10},
		{"flat prefix", "/b/pre/k1,2a0f,10,0/0", "b", "pre/k1", 10},
		{"bucket with a path", "/b/sub/s0/k1,2a0f,10,0/0", "b", "sub/s0/k1", 10},
		{"inner empty segment", "/b/a//b/k1,2a0f,10,0/0", "b", "a//b/k1", 10},
		{"trailing slash kept in the key", "/b/dir/,2a0f,0,0/0", "b", "dir/", 0},
		{"negative offset", "/b/k,-ff,10,0/0", "b", "k", 10},
		{"offset zero", "/b/k,0,10,0/0", "b", "k", 10},
		{"updated item", "/b/k,ff,10,1/0f", "b", "k", 10},
		{"large layer and mask", "/b/k,ff,10,7fffffff/0102030405060708", "b", "k", 10},
		{"upper-case digits", "/b/k,FF,10,A/0B", "b", "k", 10},
		{"empty mask", "/b/k,ff,10,0/", "b", "k", 10},
		{"zero size", "/b/k,ff,0,0/0", "b", "k", 0},
		{"spaces and symbols in the key", "/b/ k#1 ,ff,10,0/0", "b", " k#1 ", 10},
		// An absolute fs path can't be told from an S3 path either: the docs say the file must
		// come from an S3 write, and --bucket checks it.
		{"absolute fs path", "/var/tmp/d/k,ff,10,0/0", "var", "tmp/d/k", 10},
		// A canonical row without its header that happens to be a valid engine row: it can't be
		// told apart, so it is read as the engine row it is.
		{"headerless canonical-looking engine row", "/b/k,ff,10,0/0", "b", "k", 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, reason := parseSavedItemLine(test.line)
			if reason != "" {
				t.Fatalf("parseSavedItemLine(%q) refused: %s", test.line, reason)
			}
			want := ManifestRecord{bucket: test.bucket, key: test.key, size: test.size}
			if record != want {
				t.Fatalf("parseSavedItemLine(%q) = %+v, want %+v", test.line, record, want)
			}
		})
	}
}

func TestParseSavedItemLineRefusesRowsThatNameNoSingleObject(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		reason string
	}{
		{"blank", "", "is blank"},
		{"invalid UTF-8", "/b/k\xff,ff,10,0/0", "is not valid UTF-8"},
		{"bare CR", "/b/k\rx,ff,10,0/0", "holds a CR that does not end the line"},
		{"three fields", "/b/k,ff,10", "has 3 fields, want 4"},
		{"one field", "obj-1", "has 1 fields, want 4"},
		{"comma in the key", "/b/a,b/k,ff,10,0/0", "has more than 4 fields: a comma in the object key cannot be told from a field separator"},
		{"quote in the name", `/b/k"1,ff,10,0/0`, "has a quote in the object name"},
		{"relative fs path", "data/k,ff,10,0/0", `has an object name that does not start with "/" (not an S3 object path)`},
		{"mock driver name", "k,ff,10,0/0", `has an object name that does not start with "/" (not an S3 object path)`},
		{"byte order mark", "\ufeff/b/k,ff,10,0/0", `has an object name that does not start with "/" (not an S3 object path)`},
		{"empty bucket", "//k,ff,10,0/0", "has an empty bucket"},
		{"bucket only", "/b,ff,10,0/0", "has no object key after the bucket"},
		{"bucket and slash only", "/b/,ff,10,0/0", "has no object key after the bucket"},
		{"key starting with a slash", "/b//k,ff,10,0/0", `has an object key that starts with "/"`},
		{"version suffix", "/b/s0/k~66984584-e910-4588-9d70-2fa1fbce3908,ff,10,0/0", tildeReason},
		// A version ID can hold "/", so the suffix can sit before the last segment; and a "~"
		// there can be part of the key: neither is guessed.
		{"version ID with a slash", "/b/s0/k~3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY,ff,10,0/0", tildeReason},
		{"tilde before the last segment", "/b/a~x/k,ff,10,0/0", tildeReason},
		{"tilde in the first key segment", "/b/~k,ff,10,0/0", tildeReason},
		// A write can send a %XX as a URI escape and store the decoded key.
		{"percent escape in the key", "/b/r%41/k,ff,10,0/0", percentReason},
		{"bare percent in the key", "/b/s0/50%,ff,10,0/0", percentReason},
		{"percent in the bucket", "/b%41/k,ff,10,0/0", percentReason},
		{"bad hex offset", "/b/k,zz,10,0/0", "has an offset that is not a hexadecimal 64-bit integer"},
		{"empty offset", "/b/k,,10,0/0", "has an offset that is not a hexadecimal 64-bit integer"},
		{"offset too large", "/b/k,ffffffffffffffffff,10,0/0", "has an offset that is not a hexadecimal 64-bit integer"},
		{"negative size", "/b/k,ff,-1,0/0", "has a size that is not a nonnegative decimal integer"},
		{"hex size", "/b/k,ff,a,0/0", "has a size that is not a nonnegative decimal integer"},
		{"layer/mask without a slash", "/b/k,ff,10,00", "has a layer/mask that is not <hex layer>/<hex mask>"},
		{"bad layer", "/b/k,ff,10,g/0", "has a layer/mask that is not <hex layer>/<hex mask>"},
		{"bad mask", "/b/k,ff,10,0/g", "has a layer/mask that is not <hex layer>/<hex mask>"},
		{"canonical version as layer/mask", "/b/k,ff,10,", "has a layer/mask that is not <hex layer>/<hex mask>"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, reason := parseSavedItemLine(test.line)
			if reason != test.reason {
				t.Fatalf("parseSavedItemLine(%q) = %+v, reason %q, want reason %q", test.line, record, reason, test.reason)
			}
		})
	}
}

func TestSavedItemsLineErrorNamesBothFormatsAndNotTheRow(t *testing.T) {
	err := savedItemsLineError(7, "has an empty bucket")
	if err.Error() != savedItemsLineMessage(7, "has an empty bucket") {
		t.Fatalf("savedItemsLineError() = %q", err)
	}
	for _, want := range []string{"canonical manifest", `"bucket" "key" "size" "version_id"`, "saved S3 items file", "line 7 "} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("savedItemsLineError() = %q, want it to name %q", err, want)
		}
	}
}
