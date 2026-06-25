package update

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const MaxDecompressedBinaryBytes = 250 * 1024 * 1024

func ExtractBinary(assetName string, archiveBytes []byte) ([]byte, error) {
	return extractBinaryWithLimit(assetName, archiveBytes, MaxDecompressedBinaryBytes)
}

func extractBinaryWithLimit(assetName string, archiveBytes []byte, maxBytes int64) ([]byte, error) {
	switch {
	case strings.HasSuffix(assetName, ".gz"):
		return extractGzipBinary(archiveBytes, maxBytes)
	case strings.HasSuffix(assetName, ".zip"):
		return extractZipBinary(assetName, archiveBytes, maxBytes)
	default:
		return nil, fmt.Errorf("unsupported archive format for %q", assetName)
	}
}

func extractGzipBinary(archiveBytes []byte, maxBytes int64) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(archiveBytes))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return readBounded(zr, maxBytes)
}

func extractZipBinary(assetName string, archiveBytes []byte, maxBytes int64) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	if err != nil {
		return nil, err
	}
	expectedName, err := expectedZipMemberName(assetName)
	if err != nil {
		return nil, err
	}
	if len(zr.File) != 1 {
		return nil, fmt.Errorf("zip archive must contain exactly one file")
	}
	entry := zr.File[0]
	if !safeZipEntryName(entry.Name) || entry.Name != expectedName {
		return nil, fmt.Errorf("zip archive contains unexpected entry %q", entry.Name)
	}
	r, err := entry.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return readBounded(r, maxBytes)
}

func expectedZipMemberName(assetName string) (string, error) {
	base := strings.TrimSuffix(assetName, ".zip")
	if base == assetName {
		return "", fmt.Errorf("asset %q is not a zip archive", assetName)
	}
	return base + ".exe", nil
}

func safeZipEntryName(name string) bool {
	if name == "" || name != filepath.Base(name) {
		return false
	}
	if filepath.IsAbs(name) || strings.HasPrefix(name, "/") || strings.HasPrefix(name, `\`) {
		return false
	}
	if strings.Contains(name, "..") || strings.ContainsAny(name, `/:\`) {
		return false
	}
	return true
}

func readBounded(r io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("decompressed binary exceeds size limit")
	}
	return data, nil
}
