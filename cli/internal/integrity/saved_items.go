package integrity

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// IsSavedItemFields reports whether offset and layerMask have the form of the second and fourth
// fields of a saved engine items row, <name>,<hex offset>,<decimal size>,<hex layer>/<hex mask>.
// The rules are the ones the engine's DataItemImpl reads: a signed 64-bit offset, a signed 32-bit
// layer, and a mask of hex digits in either case that may be empty.
func IsSavedItemFields(offset, layerMask string) bool {
	if _, err := strconv.ParseInt(offset, 16, 64); err != nil {
		return false
	}
	layer, mask, found := strings.Cut(layerMask, "/")
	if !found {
		return false
	}
	if _, err := strconv.ParseInt(layer, 16, 32); err != nil {
		return false
	}
	for _, digit := range mask {
		if !strings.ContainsRune("0123456789abcdefABCDEF", digit) {
			return false
		}
	}
	return true
}

// isSavedItemRow reports whether a record read as a canonical row is really a saved engine items
// row. Such a row parses as bucket=<object path>, key=<hex offset>, version_id=<layer>/<mask>,
// which names no object. The bucket is not tested: canonical buckets and engine paths overlap.
func isSavedItemRow(fields []string, recordNumber int) bool {
	record, err := parseManifestRecord(fields, recordNumber)
	return err == nil && IsSavedItemFields(record.key, record.version)
}

func savedItemRowError(recordNumber int) error {
	return fmt.Errorf(
		"record %d is a saved engine items row (as --save-items writes it: object path, hex offset, "+
			"size, layer/mask), not a canonical manifest row; the %q header does not convert it. "+
			"Give each object as <bucket>,<key>,<size>,<version_id or empty>",
		recordNumber, strings.Join(canonicalHeader, ","))
}

// RefuseSavedItemsUnderHeader returns an error when path starts with the canonical header, which
// makes the engine read it as a manifest, and its first data row is a saved engine items row. It
// reads no further than that row. A file it cannot read or parse is left for the engine to judge.
func RefuseSavedItemsUnderHeader(path string) error {
	in, err := os.Open(path) // #nosec G304 -- explicit user-selected local input file
	if err != nil {
		return nil
	}
	defer func() { _ = in.Close() }()
	reader := newIdentityCSVReader(in)
	header, err := reader.Read()
	if err != nil || !equalFields(header, canonicalHeader) {
		return nil
	}
	const firstDataRecord = 2
	fields, err := reader.Read()
	if err != nil || !isSavedItemRow(fields, firstDataRecord) {
		return nil
	}
	return savedItemRowError(firstDataRecord)
}
