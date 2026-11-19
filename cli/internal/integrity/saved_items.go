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

// RefuseSavedItemsUnderHeader returns an error when the engine would read path as a canonical
// manifest whose first data row is a saved engine items row. info is the caller's StatItemsFile of
// path; the check reads only that regular file, opened without waiting for a writer and confirmed
// to be the same file, and leaves anything else to the engine. It reads the file by the engine's own CSV
// rules (engineCSVReader), no further than that row, so a row the canonical reader refuses, such
// as one with a quote inside its object path or with bare-CR line ends, is still judged. A file
// from which the engine itself takes no object is left to the engine (see scanAsEngineReads).
func RefuseSavedItemsUnderHeader(path string, info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() {
		return nil
	}
	in, err := openWithoutWaiting(path)
	if err != nil {
		return nil
	}
	defer func() { _ = in.Close() }()
	opened, err := in.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil
	}
	verdict := scanAsEngineReads(in, engineScan{firstRecord: 1, fileStart: true, maxDataRecords: 1})
	if !verdict.found || !verdict.underHeader {
		return nil
	}
	return savedItemRowError(verdict.record)
}
