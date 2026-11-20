package integrity

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ItemsFileFormat names the format a DELETE items file was read in.
type ItemsFileFormat string

// The formats StageDeleteInputManifest accepts.
const (
	// ItemsFileFormatCanonical is a bucket,key,size,version_id manifest with its header.
	ItemsFileFormatCanonical ItemsFileFormat = "canonical"
	// ItemsFileFormatSavedItems is the headerless items file an S3 write --save-items saves.
	ItemsFileFormatSavedItems ItemsFileFormat = "saved-items"
)

// maxReportedBuckets bounds the bucket names a staged saved items file reports; savedBucketsTracked
// bounds the distinct buckets it counts, so a file of many buckets keeps its memory bounded.
const (
	maxReportedBuckets  = 3
	savedBucketsTracked = 1024
)

// savedItemsBuckets collects the distinct buckets of a saved items file, in file order.
type savedItemsBuckets struct {
	seen  map[string]struct{}
	names []string
	more  bool // more than savedBucketsTracked distinct buckets
}

func (buckets *savedItemsBuckets) add(bucket string) {
	if _, found := buckets.seen[bucket]; found || buckets.more {
		return
	}
	if len(buckets.seen) == savedBucketsTracked {
		buckets.more = true
		return
	}
	if buckets.seen == nil {
		buckets.seen = make(map[string]struct{})
	}
	buckets.seen[bucket] = struct{}{}
	if len(buckets.names) < maxReportedBuckets {
		buckets.names = append(buckets.names, bucket)
	}
}

// savedItemsLineError is a refusal of one line of a saved items file. It names the line and the
// reason, never the line's content, and the two formats DELETE accepts.
func savedItemsLineError(line int, reason string) error {
	return fmt.Errorf(
		"items file is neither a canonical manifest with the exact header %q nor a saved S3 items "+
			"file (<name>,<hex offset>,<size>,<layer>/<mask>, as write --save-items saves it): line %d %s",
		canonicalHeader, line, reason)
}

// parseSavedItemLine converts one saved engine items row, without its line end, to a current-key
// canonical record: the bucket is the name's first segment, the key all the rest, verbatim, and
// the version empty. The fields are read as the engine's DataItemImpl reads them, from the left:
// the name ends at the first comma. On refusal it returns the reason for savedItemsLineError.
func parseSavedItemLine(line string) (ManifestRecord, string) {
	if line == "" {
		return ManifestRecord{}, "is blank"
	}
	if !utf8.ValidString(line) {
		return ManifestRecord{}, "is not valid UTF-8"
	}
	if strings.ContainsRune(line, '\r') {
		return ManifestRecord{}, "holds a CR that does not end the line"
	}
	switch commas := strings.Count(line, ","); {
	case commas < 3:
		return ManifestRecord{}, fmt.Sprintf("has %d fields, want 4", commas+1)
	case commas > 3:
		return ManifestRecord{}, "has more than 4 fields: a comma in the object key cannot be told from a field separator"
	}
	fields := strings.SplitN(line, ",", 4)
	name, offset, sizeField, layerMask := fields[0], fields[1], fields[2], fields[3]
	if strings.ContainsRune(name, '"') {
		return ManifestRecord{}, "has a quote in the object name"
	}
	if !strings.HasPrefix(name, "/") {
		return ManifestRecord{}, `has an object name that does not start with "/" (not an S3 object path)`
	}
	bucket, key, found := strings.Cut(name[1:], "/")
	switch {
	case bucket == "":
		return ManifestRecord{}, "has an empty bucket"
	case !found || key == "":
		return ManifestRecord{}, "has no object key after the bucket"
	case strings.HasPrefix(key, "/"):
		return ManifestRecord{}, `has an object key that starts with "/"`
	}
	if strings.ContainsRune(key[strings.LastIndexByte(key, '/')+1:], '~') {
		return ManifestRecord{}, `has a "~" in the last segment of its key, as a versioned write ` +
			"names a version (key~versionId); a saved items file deletes current keys only, and " +
			"exact versions need a canonical manifest"
	}
	if !isSavedItemOffset(offset) {
		return ManifestRecord{}, "has an offset that is not a hexadecimal 64-bit integer"
	}
	size, err := strconv.ParseInt(sizeField, 10, 64)
	if err != nil || size < 0 {
		return ManifestRecord{}, "has a size that is not a nonnegative decimal integer"
	}
	if !isSavedItemLayerMask(layerMask) {
		return ManifestRecord{}, "has a layer/mask that is not <hex layer>/<hex mask>"
	}
	return ManifestRecord{bucket: bucket, key: key, size: size}, ""
}

// readSavedItems reads input, from the start of the file, as a saved engine items file and passes
// each row to publish as a canonical record with the row's line number. A line ends at LF or CRLF;
// the last line may have no line end. One line that cannot be converted refuses the whole file.
// It returns the number of rows read.
func readSavedItems(input io.Reader, publish func(ManifestRecord, int) error) (int, error) {
	// One line, its line end and one byte more fit the buffer, so a longer line is seen as such.
	lines := bufio.NewReaderSize(input, maxCanonicalCSVRecordBytes+2)
	for number := 1; ; number++ {
		line, err := lines.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return number - 1, savedItemsLineError(number, fmt.Sprintf("exceeds %d bytes", maxCanonicalCSVRecordBytes))
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return number - 1, fmt.Errorf("read items file line %d: %w", number, err)
		}
		if len(line) == 0 && err != nil {
			return number - 1, nil
		}
		line = bytes.TrimSuffix(line, []byte("\n"))
		if err == nil {
			line = bytes.TrimSuffix(line, []byte("\r"))
		}
		record, reason := parseSavedItemLine(string(line))
		if reason != "" {
			return number - 1, savedItemsLineError(number, reason)
		}
		if publishErr := publish(record, number); publishErr != nil {
			return number - 1, publishErr
		}
		if err != nil {
			return number, nil
		}
	}
}

// savedRowUnderBareCRHeader returns the saved-row refusal when the engine, which also ends a
// record at a bare CR, reads first the canonical header and then a saved engine items row from
// the first record the canonical reader read. Such a file has the header for the engine, so it
// gets the refusal a header file gets, not the saved items conversion.
func savedRowUnderBareCRHeader(firstRecord []byte) error {
	verdict := scanAsEngineReads(bytes.NewReader(firstRecord), engineScan{firstRecord: 1, fileStart: true})
	if verdict.found && verdict.underHeader {
		return savedItemRowError(verdict.record)
	}
	return nil
}
