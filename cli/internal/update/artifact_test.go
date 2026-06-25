package update

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestAssetNameForPlatform(t *testing.T) {
	tests := []struct {
		goos   string
		goarch string
		want   string
	}{
		{"linux", "amd64", "spt-5.10.4-linux-amd64.gz"},
		{"linux", "arm64", "spt-5.10.4-linux-arm64.gz"},
		{"darwin", "amd64", "spt-5.10.4-darwin-amd64.gz"},
		{"darwin", "arm64", "spt-5.10.4-darwin-arm64.gz"},
		{"windows", "amd64", "spt-5.10.4-windows-amd64.zip"},
	}

	for _, tt := range tests {
		t.Run(tt.goos+"_"+tt.goarch, func(t *testing.T) {
			got, err := AssetNameForPlatform("5.10.4", tt.goos, tt.goarch)
			if err != nil {
				t.Fatalf("AssetNameForPlatform returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("asset = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAssetNameForPlatformRejectsUnsupported(t *testing.T) {
	if _, err := AssetNameForPlatform("5.10.4", "linux", "386"); err == nil {
		t.Fatal("AssetNameForPlatform accepted unsupported platform")
	}
}

func TestFindChecksumExactBasename(t *testing.T) {
	sums := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  spt-5.10.4-linux-amd64.gz.extra\n" +
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb *spt-5.10.4-linux-amd64.gz\n")
	got, err := FindChecksum(sums, "spt-5.10.4-linux-amd64.gz")
	if err != nil {
		t.Fatalf("FindChecksum returned error: %v", err)
	}
	if got != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("checksum = %q", got)
	}
}

func TestFindChecksumRejectsMissingAndDuplicate(t *testing.T) {
	if _, err := FindChecksum([]byte(""), "asset.gz"); err == nil {
		t.Fatal("FindChecksum accepted missing checksum")
	}

	line := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  asset.gz\n"
	if _, err := FindChecksum([]byte(line+line), "asset.gz"); err == nil {
		t.Fatal("FindChecksum accepted duplicate checksum")
	}
}

func TestVerifyChecksum(t *testing.T) {
	data := []byte("release asset bytes")
	sum := sha256.Sum256(data)
	if err := VerifyChecksum(data, fmt.Sprintf("%x", sum)); err != nil {
		t.Fatalf("VerifyChecksum returned error: %v", err)
	}
	if err := VerifyChecksum(data, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("VerifyChecksum accepted wrong checksum")
	}
}
