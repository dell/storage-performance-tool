package integrity

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// engineCSVReader reads records as the engine reads an items file it may take as a manifest:
// Apache Commons CSV 1.14.1 with CSVFormat.RFC4180 over a UTF-8 reader
// (IntegrityManifestItemInput.java:25-28, :44-53 and :149-153). It is deliberately looser than
// the canonical reader, and is used only to judge what the engine would see:
//   - a record ends at LF, CRLF or a bare CR outside quotes;
//   - a quote is special only as a field's first character; elsewhere it is literal text;
//   - inside quotes, a doubled quote is one quote and line breaks are content;
//   - after a closing quote, white space (Java's Character.isWhitespace) is skipped up to the
//     comma or line end, and anything else is an error;
//   - an empty line is a record of one empty field, and the last record needs no line end;
//   - a byte sequence that is not UTF-8, or end of file inside quotes, is an error.
//
// Memory stays bounded: a record keeps its first engineKeptFields fields, counting the rest, and a
// kept field keeps at most maxCanonicalCSVRecordBytes bytes, marking the field as cut.
type engineCSVReader struct {
	input    *bufio.Reader
	lastChar rune
}

const (
	engineNoChar     rune = -2 // nothing read yet
	engineEndOfInput rune = -1
	engineKeptFields      = 5 // the four canonical fields and one more, to tell four from more
)

type engineCSVRecord struct {
	fields []string // the first engineKeptFields fields
	count  int      // every field of the record
	cut    [engineKeptFields]bool
}

type engineTokenType int

const (
	engineToken engineTokenType = iota
	engineEndOfRecord
	engineEOF
)

type engineCSVToken struct {
	kind    engineTokenType
	ready   bool // an EOF token that still carries a field
	content []byte
	cut     bool
}

func newEngineCSVReader(input io.Reader) *engineCSVReader {
	return &engineCSVReader{input: bufio.NewReader(input), lastChar: engineNoChar}
}

func (r *engineCSVReader) read() (rune, error) {
	value, size, err := r.input.ReadRune()
	if errors.Is(err, io.EOF) {
		r.lastChar = engineEndOfInput
		return engineEndOfInput, nil
	}
	if err != nil {
		return 0, err
	}
	if value == utf8.RuneError && size == 1 {
		return 0, fmt.Errorf("input is not valid UTF-8")
	}
	r.lastChar = value
	return value, nil
}

// peekIs reports whether the next byte is the ASCII character want; no UTF-8 lead or
// continuation byte equals an ASCII character.
func (r *engineCSVReader) peekIs(want byte) bool {
	next, err := r.input.Peek(1)
	return err == nil && next[0] == want
}

// readEndOfLine consumes the LF of a CRLF and reports whether value ends a line.
func (r *engineCSVReader) readEndOfLine(value rune) (bool, error) {
	if value == '\r' && r.peekIs('\n') {
		if _, err := r.read(); err != nil {
			return false, err
		}
	}
	return value == '\n' || value == '\r', nil
}

func (t *engineCSVToken) appendRune(value rune) {
	if len(t.content)+utf8.RuneLen(value) > maxCanonicalCSVRecordBytes {
		t.cut = true
		return
	}
	t.content = utf8.AppendRune(t.content, value)
}

func (r *engineCSVReader) nextToken() (engineCSVToken, error) {
	var token engineCSVToken
	lastChar := r.lastChar
	value, err := r.read()
	if err != nil {
		return token, err
	}
	endOfLine, err := r.readEndOfLine(value)
	if err != nil {
		return token, err
	}
	if lastChar == engineEndOfInput || (lastChar != ',' && value == engineEndOfInput) {
		token.kind = engineEOF
		return token, nil
	}
	switch {
	case value == ',':
		token.kind = engineToken
	case endOfLine:
		token.kind = engineEndOfRecord
	case value == '"':
		err = r.quotedToken(&token)
	case value == engineEndOfInput:
		token.kind, token.ready = engineEOF, true
	default:
		err = r.simpleToken(&token, value)
	}
	return token, err
}

func (r *engineCSVReader) simpleToken(token *engineCSVToken, value rune) error {
	for {
		endOfLine, err := r.readEndOfLine(value)
		if err != nil {
			return err
		}
		switch {
		case endOfLine:
			token.kind = engineEndOfRecord
			return nil
		case value == engineEndOfInput:
			token.kind, token.ready = engineEOF, true
			return nil
		case value == ',':
			token.kind = engineToken
			return nil
		}
		token.appendRune(value)
		if value, err = r.read(); err != nil {
			return err
		}
	}
}

func (r *engineCSVReader) quotedToken(token *engineCSVToken) error {
	for {
		value, err := r.read()
		if err != nil {
			return err
		}
		switch {
		case value == '"' && r.peekIs('"'):
			if _, err = r.read(); err != nil {
				return err
			}
			token.appendRune('"')
		case value == '"':
			return r.afterClosingQuote(token)
		case value == engineEndOfInput:
			return fmt.Errorf("end of file inside a quoted field")
		default:
			token.appendRune(value)
		}
	}
}

func (r *engineCSVReader) afterClosingQuote(token *engineCSVToken) error {
	for {
		value, err := r.read()
		if err != nil {
			return err
		}
		if value == ',' {
			token.kind = engineToken
			return nil
		}
		if value == engineEndOfInput {
			token.kind, token.ready = engineEOF, true
			return nil
		}
		endOfLine, err := r.readEndOfLine(value)
		if err != nil {
			return err
		}
		if endOfLine {
			token.kind = engineEndOfRecord
			return nil
		}
		if !javaIsWhitespace(value) {
			return fmt.Errorf("invalid character between a closing quote and the delimiter")
		}
	}
}

// Read returns the next record, or io.EOF after the last one.
func (r *engineCSVReader) Read() (engineCSVRecord, error) {
	var record engineCSVRecord
	for {
		token, err := r.nextToken()
		if err != nil {
			return engineCSVRecord{}, err
		}
		if token.kind != engineEOF || token.ready {
			if record.count < engineKeptFields {
				record.fields = append(record.fields, string(token.content))
				record.cut[record.count] = token.cut
			}
			record.count++
		}
		if token.kind != engineToken {
			break
		}
	}
	if record.count == 0 {
		return engineCSVRecord{}, io.EOF
	}
	return record, nil
}

// javaIsWhitespace is Java's Character.isWhitespace for one UTF-16 unit; a rune outside the
// Basic Multilingual Plane is two units, neither of them white space.
func javaIsWhitespace(value rune) bool {
	switch value {
	case '\t', '\n', '\v', '\f', '\r', 0x1c, 0x1d, 0x1e, 0x1f:
		return true
	case 0xa0, 0x2007, 0x202f:
		return false
	}
	return value <= 0xffff && unicode.In(value, unicode.Zs, unicode.Zl, unicode.Zp)
}

func (record engineCSVRecord) isCanonicalHeader() bool {
	if record.count != len(canonicalHeader) {
		return false
	}
	for index := range canonicalHeader {
		if record.cut[index] {
			return false
		}
	}
	return equalFields(record.fields, canonicalHeader)
}

// isSavedItemRow applies the saved-row test to a record. A cut key, size or version is never one:
// a saved row's offset, size and layer/mask are a few dozen bytes. The bucket may be cut.
func (record engineCSVRecord) isSavedItemRow(recordNumber int) bool {
	if record.count != len(canonicalHeader) || record.cut[1] || record.cut[2] || record.cut[3] {
		return false
	}
	return isSavedItemRow(record.fields, recordNumber)
}

// passesEngineRowCheck is the engine's own row check (IntegrityManifestItemInput.java:176-205),
// which it applies to every row when it opens a manifest (:149-161).
func (record engineCSVRecord) passesEngineRowCheck(recordNumber int) bool {
	if record.count != len(canonicalHeader) {
		return false
	}
	_, err := parseManifestRecord(record.fields, recordNumber)
	return err == nil
}

// engineScan says where a scan's input sits in the file it judges.
type engineScan struct {
	firstRecord int  // the engine's number for the input's first record
	fileStart   bool // the input starts the file, so its first record is the header or record 1
	// maxDataRecords stops after that many data records (0: no limit).
	maxDataRecords int
	// cut reports whether the input was cut short of the file's end: a record that runs to the end
	// of a cut input may be incomplete, so it is not judged.
	cut func() bool
	// everyRecord tests every record for a saved row and counts them all, without the engine's
	// row check: for bytes the stager will publish whatever the engine makes of its source.
	everyRecord bool
}

// engineVerdict is a scan's result. records is the number of whole records read. found is true
// with record the number of the first saved engine items row, underHeader true when it lies under
// the canonical header. found is false when the scan ends first: at the end of the input, at a
// limit in the scan, or (except with everyRecord) where the engine would take no object from the
// file: a byte sequence that is not UTF-8, a record Commons CSV refuses, or a data record the
// engine's row check refuses.
type engineVerdict struct {
	records     int
	record      int
	underHeader bool
	found       bool
}

// scanAsEngineReads reads input as the engine does and finds a saved engine items row. At the
// file's start, a first record that is not the canonical header is tested as record 1 and ends
// the scan; otherwise data records are tested in order until one is a saved row, one fails the
// engine's row check (unless scan.everyRecord), the input ends, or a limit in scan is reached.
func scanAsEngineReads(input io.Reader, scan engineScan) engineVerdict {
	reader := newEngineCSVReader(input)
	verdict := engineVerdict{underHeader: !scan.fileStart}
	for recordNumber := scan.firstRecord; ; recordNumber++ {
		next, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return verdict
		}
		if reader.lastChar == engineEndOfInput && scan.cut != nil && scan.cut() {
			return verdict // the record, or the error, may come from the cut: undecided
		}
		if err != nil {
			return verdict
		}
		verdict.records++
		switch {
		case scan.fileStart && recordNumber == 1 && !next.isCanonicalHeader():
			verdict.record, verdict.found = 1, next.isSavedItemRow(1)
			return verdict
		case scan.fileStart && recordNumber == 1:
			verdict.underHeader = true
		case !scan.everyRecord && !next.passesEngineRowCheck(recordNumber):
			return verdict
		case next.isSavedItemRow(recordNumber):
			verdict.record, verdict.found = recordNumber, true
			return verdict
		}
		dataRecords := recordNumber - 1
		if scan.maxDataRecords > 0 && dataRecords >= scan.maxDataRecords {
			return verdict
		}
	}
}

// cappedReader reads at most left bytes from input and records whether it stopped at that cap.
type cappedReader struct {
	input  io.Reader
	left   int64
	capped bool
}

func (r *cappedReader) Read(buffer []byte) (int, error) {
	if r.left <= 0 {
		r.capped = true
		return 0, io.EOF
	}
	if int64(len(buffer)) > r.left {
		buffer = buffer[:r.left]
	}
	n, err := r.input.Read(buffer)
	r.left -= int64(n)
	return n, err
}
