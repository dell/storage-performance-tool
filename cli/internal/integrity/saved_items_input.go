package integrity

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
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

// The reasons for a row whose key may not be the stored key. Such a file is converted by hand.
const (
	savedItemsTildeReason = `has a "~" in its key, as a versioned write names a version ` +
		`(key~versionId, and the version ID can hold "/"); a saved items file deletes current keys ` +
		"only, so convert such a file by hand to a canonical bucket,key,size,version_id manifest " +
		"of the stored keys and versions"
	savedItemsPercentReason = `has a "%" in its object name, and a write can store a percent ` +
		"escape decoded, so the row may not name the stored key; convert such a file by hand to a " +
		"canonical bucket,key,size,version_id manifest of the stored keys"
)

// savedItemsLineTooLong is the reason for a line whose content, without its line end, is longer
// than the canonical record limit.
var savedItemsLineTooLong = fmt.Sprintf("exceeds %d bytes without its line end", maxCanonicalCSVRecordBytes)

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
	// A versioned Netty write names a version as key~versionId, and a version ID can hold "/", so
	// the suffix is not always in the last segment: a "~" anywhere in the key is refused.
	if strings.ContainsRune(key, '~') {
		return ManifestRecord{}, savedItemsTildeReason
	}
	// A Netty S3 write can send a %XX in a name as a URI escape, so the target stores the decoded
	// key, while a DELETE addresses the literal "%": the row may name another object.
	if strings.ContainsRune(name, '%') {
		return ManifestRecord{}, savedItemsPercentReason
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
	// A line of the longest content and a CRLF fit the buffer; the content's length is checked
	// after the line end is stripped, so the limit is the same for LF, CRLF and no line end.
	lines := bufio.NewReaderSize(input, maxCanonicalCSVRecordBytes+2)
	tooLong := savedItemsLineTooLong
	for number := 1; ; number++ {
		line, err := lines.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return number - 1, savedItemsLineError(number, tooLong)
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
		if len(line) > maxCanonicalCSVRecordBytes {
			return number - 1, savedItemsLineError(number, tooLong)
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
		return deleteSavedItemRowError(verdict.record)
	}
	return nil
}

// savedItemsConflictError refuses a saved items file that gives one object two sizes. It names
// the two source lines, not the object: rawPath is the stager's unsorted copy, whose data record n
// is the saved file's line n, and conflict is the sort's record of the object.
func savedItemsConflictError(rawPath string, conflict ManifestRecord) error {
	first, second, err := findSavedItemsConflict(rawPath, conflict)
	if err != nil || second == 0 {
		return errors.New("items file gives one object two sizes on two of its lines")
	}
	return fmt.Errorf("items file lines %d and %d give the same object two sizes", first, second)
}

// findSavedItemsConflict returns the first line naming conflict's identity and the first line
// after it that names the identity with another size.
func findSavedItemsConflict(rawPath string, conflict ManifestRecord) (first, second int, err error) {
	raw, err := os.Open(rawPath) // #nosec G304 -- private staging path created by the stager
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = raw.Close() }()
	reader := newCanonicalCSVReader(raw)
	if _, err = reader.Read(); err != nil {
		return 0, 0, err
	}
	var firstSize int64
	for line := 1; ; line++ {
		fields, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			return first, 0, nil
		}
		if readErr != nil {
			return 0, 0, readErr
		}
		record, parseErr := parseManifestRecord(fields, line)
		if parseErr != nil {
			return 0, 0, parseErr
		}
		if record.identity() != conflict.identity() {
			continue
		}
		switch {
		case first == 0:
			first, firstSize = line, record.size
		case record.size != firstSize:
			return first, line, nil
		}
	}
}
