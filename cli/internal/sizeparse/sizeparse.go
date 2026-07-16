// Package sizeparse converts human-readable byte-size strings (e.g. "1MB",
// "256KB", "0") into int64 byte counts. It is intentionally kept small and
// dependency-free so it can be used from both the CLI flag layer and the
// scenario generation layer.
package sizeparse

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// Binary size unit multipliers. The decimal-style names are retained for API
// compatibility; display code should use IEC labels such as KiB and MiB.
const (
	Byte     int64 = 1
	Kilobyte       = constants.BytesPerKiB
	Megabyte       = constants.BytesPerMiB
	Gigabyte       = constants.BytesPerGiB
	Terabyte       = constants.BytesPerTiB
	Petabyte       = constants.BytesPerPiB
	Exabyte        = constants.BytesPerEiB
)

// suffixMultipliers maps recognised upper-cased suffixes to their multiplier.
// Both legacy (KB, MB) and IEC (KiB, MiB) suffixes are accepted as synonyms
// since this package always uses binary (1024-based) multipliers.
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
	"P":   Petabyte,
	"PB":  Petabyte,
	"PIB": Petabyte,
	"E":   Exabyte,
	"EB":  Exabyte,
	"EIB": Exabyte,
}

// Parse converts a human-readable size string into a byte count.
//
// Accepted formats:
//
//	"0"          – plain integer (bytes)
//	"1048576"    – plain integer (bytes)
//	"256KB"      – number followed by unit suffix
//	"1M"         – short suffix (K, M, G, T, P, E)
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
		return 0, fmt.Errorf("invalid size %q: unrecognised unit %q (valid: KB/KiB through EB/EiB)", s, s[i:])
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("invalid size %q: value overflows int64 bytes", s)
	}

	return n * mult, nil
}
