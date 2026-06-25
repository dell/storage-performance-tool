package update

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"testing"
)

func TestExtractGzipBinary(t *testing.T) {
	data := []byte("new binary")
	archive := gzipBytes(t, data)
	got, err := ExtractBinary("spt-5.10.4-linux-amd64.gz", archive)
	if err != nil {
		t.Fatalf("ExtractBinary returned error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ExtractBinary = %q, want %q", got, data)
	}
}

func TestExtractGzipBinaryRejectsDecompressedLimit(t *testing.T) {
	archive := gzipBytes(t, []byte("0123456789"))
	if _, err := extractBinaryWithLimit("spt-5.10.4-linux-amd64.gz", archive, 5); err == nil {
		t.Fatal("extractBinaryWithLimit accepted oversized decompressed data")
	}
}

func TestExtractZipBinary(t *testing.T) {
	data := []byte("windows binary")
	archive := zipBytes(t, map[string][]byte{"spt-5.10.4-windows-amd64.exe": data})
	got, err := ExtractBinary("spt-5.10.4-windows-amd64.zip", archive)
	if err != nil {
		t.Fatalf("ExtractBinary returned error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ExtractBinary = %q, want %q", got, data)
	}
}

func TestExtractZipBinaryRejectsUnexpectedNames(t *testing.T) {
	tests := map[string]map[string][]byte{
		"path traversal": {"../spt-5.10.4-windows-amd64.exe": []byte("x")},
		"nested path":    {"dir/spt-5.10.4-windows-amd64.exe": []byte("x")},
		"drive letter":   {"C:spt-5.10.4-windows-amd64.exe": []byte("x")},
		"wrong name":     {"other.exe": []byte("x")},
		"multiple files": {"spt-5.10.4-windows-amd64.exe": []byte("x"), "extra.exe": []byte("x")},
	}

	for name, files := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ExtractBinary("spt-5.10.4-windows-amd64.zip", zipBytes(t, files)); err == nil {
				t.Fatal("ExtractBinary accepted invalid zip")
			}
		})
	}
}

func TestExtractZipBinaryRejectsDecompressedLimit(t *testing.T) {
	archive := zipBytes(t, map[string][]byte{"spt-5.10.4-windows-amd64.exe": []byte("0123456789")})
	if _, err := extractBinaryWithLimit("spt-5.10.4-windows-amd64.zip", archive, 5); err == nil {
		t.Fatal("extractBinaryWithLimit accepted oversized zip entry")
	}
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func zipBytes(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create: %v", err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("zip write: %v", err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}
