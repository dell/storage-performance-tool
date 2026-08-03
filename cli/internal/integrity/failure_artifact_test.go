/*
Copyright © 2025 Dell Technologies
*/

package integrity

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadFailureArtifactPreservesIdentityFields(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "ordinary", value: "ordinary"},
		{name: "line feed", value: "line\nfeed"},
		{name: "carriage return", value: "carriage\rreturn"},
		{name: "CRLF", value: "carriage\r\nline-feed"},
		{name: "comma", value: "comma,value"},
		{name: "quote", value: `quote"value`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := "key-" + test.value
			requestedVersion := "requested-" + test.value
			returnedVersion := "returned-" + test.value
			path := writeFailureArtifactTestFile(t, [][]string{
				failureHeader,
				validFailureArtifactRow(key, requestedVersion, returnedVersion),
			})

			count, samples, err := readFailureArtifact(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 || len(samples) != 1 {
				t.Fatalf("count = %d, samples = %d, want 1 and 1", count, len(samples))
			}
			if samples[0].Key != key ||
				samples[0].RequestedVersion != requestedVersion ||
				samples[0].ReturnedVersion != returnedVersion {
				t.Fatalf("identity fields changed: %+v", samples[0])
			}
		})
	}
}

func TestReadFailureArtifactEnforcesValidationAndSampleBound(t *testing.T) {
	path := writeFailureArtifactTestFile(t, [][]string{
		failureHeader,
		validFailureArtifactRow("first", "requested-1", "returned-1"),
		validFailureArtifactRow("second", "requested-2", "returned-2"),
	})
	count, samples, err := readFailureArtifact(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || len(samples) != 1 || samples[0].Key != "first" {
		t.Fatalf("bounded result = count %d, samples %+v", count, samples)
	}

	badHeader := append([]string(nil), failureHeader...)
	badHeader[0] = "bad_timestamp"
	shortRow := validFailureArtifactRow("short", "", "")
	shortRow = shortRow[:len(shortRow)-1]
	badReason := validFailureArtifactRow("reason", "", "")
	badReason[8] = "not_supported"
	tests := []struct {
		name    string
		records [][]string
	}{
		{name: "invalid header", records: [][]string{badHeader}},
		{name: "invalid field count", records: [][]string{failureHeader, shortRow}},
		{name: "unsupported reason", records: [][]string{failureHeader, badReason}},
		{name: "record size bound", records: [][]string{
			failureHeader,
			validFailureArtifactRow(strings.Repeat("x", maxCanonicalCSVRecordBytes+1), "", ""),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, readErr := readFailureArtifact(
				writeFailureArtifactTestFile(t, test.records), 1); readErr == nil {
				t.Fatal("readFailureArtifact() succeeded, want validation error")
			}
		})
	}
}

func validFailureArtifactRow(key, requestedVersion, returnedVersion string) []string {
	return []string{
		"2026-08-03T00:00:00Z", "node-0", "verify", "s3-aws", key,
		requestedVersion, returnedVersion, "request-1", "digest_mismatch", "sha256",
		"expected", "actual", "1", "1", "0", "1",
	}
}

func writeFailureArtifactTestFile(t *testing.T, records [][]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), IntegrityFailuresName)
	file, err := os.Create(path) // #nosec G304 -- private test path
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	if err = writer.WriteAll(records); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
