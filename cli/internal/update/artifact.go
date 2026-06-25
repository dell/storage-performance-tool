package update

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	ChecksumAssetName       = "SHA256SUMS"
	MaxCompressedAssetBytes = 100 * 1024 * 1024
)

func AssetNameForPlatform(version, goos, goarch string) (string, error) {
	suffix, ok := platformAssetSuffix(goos, goarch)
	if !ok {
		return "", fmt.Errorf("unsupported platform %s/%s", goos, goarch)
	}
	return "spt-" + version + suffix, nil
}

func FindAsset(assets []Asset, name string) (Asset, error) {
	for _, asset := range assets {
		if asset.Name == name {
			return asset, nil
		}
	}
	return Asset{}, fmt.Errorf("release asset %q not found", name)
}

func FindChecksum(contents []byte, assetName string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(contents)))
	var found string
	matches := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		sum := fields[0]
		name := strings.TrimPrefix(fields[1], "*")
		if name != assetName {
			continue
		}
		matches++
		found = sum
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if matches == 0 {
		return "", fmt.Errorf("checksum for %q not found", assetName)
	}
	if matches > 1 {
		return "", fmt.Errorf("checksum for %q appears %d times", assetName, matches)
	}
	if _, err := hex.DecodeString(found); err != nil || len(found) != sha256.Size*2 {
		return "", fmt.Errorf("checksum for %q is not a SHA-256 hex digest", assetName)
	}
	return strings.ToLower(found), nil
}

func VerifyChecksum(data []byte, wantHex string) error {
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if !strings.EqualFold(got, wantHex) {
		return fmt.Errorf("checksum mismatch: got %s, want %s", got, wantHex)
	}
	return nil
}

func platformAssetSuffix(goos, goarch string) (string, bool) {
	switch goos + "/" + goarch {
	case "linux/amd64":
		return "-linux-amd64.gz", true
	case "linux/arm64":
		return "-linux-arm64.gz", true
	case "darwin/amd64":
		return "-darwin-amd64.gz", true
	case "darwin/arm64":
		return "-darwin-arm64.gz", true
	case "windows/amd64":
		return "-windows-amd64.zip", true
	default:
		return "", false
	}
}
