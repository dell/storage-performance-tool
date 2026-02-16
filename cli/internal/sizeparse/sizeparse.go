// Package sizeparse converts human-readable byte-size strings (e.g. "1MB",
// "256KB", "0") into int64 byte counts. It is intentionally kept small and
// dependency-free so it can be used from both the CLI flag layer and the
// scenario generation layer.
package sizeparse

import (
	"fmt"
	"strconv"
	"strings"
)

// Binary size unit multipliers (IEC: 1 KB = 1024 bytes).
const (
	Byte     int64 = 1
	Kilobyte       = 1024
	Megabyte       = 1024 * Kilobyte
	Gigabyte       = 1024 * Megabyte
	Terabyte       = 1024 * Gigabyte
)

// suffixMultipliers maps recognised upper-cased suffixes to their multiplier.
// Both SI-style (KB, MB) and IEC-style (KiB, MiB) suffixes are accepted as
// synonyms since this package always uses binary (1024-based) multipliers.
var suffixMultipliers = map[string]int64{
	"B":   Byte,
	"K":   Kilobyte,
	"KB":  Kilobyte,
	"KIB": Kilobyte,
	"M":   Megabyte,
	"MB":  Megabyte,
	"MIB": Megabyte,
	"G":   Gigabyte,
	"GB":  Gigabyte,
	"GIB": Gigabyte,
	"T":   Terabyte,
	"TB":  Terabyte,
	"TIB": Terabyte,
}

// Parse converts a human-readable size string into a byte count.
//
// Accepted formats:
//
//	"0"          – plain integer (bytes)
//	"1048576"    – plain integer (bytes)
//	"256KB"      – number followed by unit suffix
//	"1M"         – short suffix (K, M, G, T)
//	"4mb"        – case-insensitive
//
// The numeric portion may be a decimal integer only (no fractions).
// Negative values are rejected.
func Parse(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Find where the numeric part ends and the suffix begins.
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid size %q: must start with a digit", s)
	}

	numStr := s[:i]
	suffix := strings.ToUpper(strings.TrimSpace(s[i:]))

	n, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid size %q: negative values not allowed", s)
	}

	if suffix == "" {
		return n, nil
	}

	mult, ok := suffixMultipliers[suffix]
	if !ok {
		return 0, fmt.Errorf("invalid size %q: unrecognised unit %q (valid: KB/KiB, MB/MiB, GB/GiB, TB/TiB)", s, s[i:])
	}

	return n * mult, nil
}
