package sizeparse

import (
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		// Plain integers (bytes)
		{"0", 0, false},
		{"1", 1, false},
		{"1048576", 1048576, false},

		// Kilobytes
		{"1K", Kilobyte, false},
		{"1KB", Kilobyte, false},
		{"256KB", 256 * Kilobyte, false},
		{"1kb", Kilobyte, false},

		// Megabytes
		{"1M", Megabyte, false},
		{"1MB", Megabyte, false},
		{"4MB", 4 * Megabyte, false},
		{"1mb", Megabyte, false},

		// Gigabytes
		{"1G", Gigabyte, false},
		{"1GB", Gigabyte, false},
		{"4gb", 4 * Gigabyte, false},

		// Terabytes
		{"1T", Terabyte, false},
		{"1TB", Terabyte, false},

		// IEC suffixes (KiB, MiB, GiB, TiB)
		{"1KiB", Kilobyte, false},
		{"256KiB", 256 * Kilobyte, false},
		{"1MiB", Megabyte, false},
		{"4MiB", 4 * Megabyte, false},
		{"1GiB", Gigabyte, false},
		{"1TiB", Terabyte, false},
		{"1kib", Kilobyte, false}, // case-insensitive

		// Bytes suffix
		{"512B", 512, false},

		// Whitespace
		{"  1MB  ", Megabyte, false},

		// Errors
		{"", 0, true},
		{"MB", 0, true},
		{"abc", 0, true},
		{"-1", 0, true},
		{"1XB", 0, true},
		{"1.5MB", 0, true}, // no fractions
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Parse(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	if Kilobyte != 1024 {
		t.Errorf("Kilobyte = %d, want 1024", Kilobyte)
	}
	if Megabyte != 1024*1024 {
		t.Errorf("Megabyte = %d, want %d", Megabyte, 1024*1024)
	}
	if Gigabyte != 1024*1024*1024 {
		t.Errorf("Gigabyte = %d, want %d", Gigabyte, 1024*1024*1024)
	}
	if Terabyte != 1024*1024*1024*1024 {
		t.Errorf("Terabyte = %d, want %d", Terabyte, 1024*1024*1024*1024)
	}
}
