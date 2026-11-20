package integrity

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

const maxCanonicalCSVRecordBytes = 1 << 20

type csvRecordReader interface {
	Read() ([]string, error)
}

// canonicalCSVReader parses canonical manifest records without encoding/csv's
// CRLF normalization. It retains exact decoded field identities while keeping
// memory bounded to one canonical record.
type canonicalCSVReader struct {
	input               *bufio.Reader
	done                bool
	allowRecordCRLF     bool
	allowMissingFinalLF bool
	// keepRaw keeps the bytes of the last record read, as read, in raw (see Raw), and marks in
	// bareCR whether one of its unquoted fields kept a CR that does not start a CRLF.
	keepRaw bool
	raw     []byte
	bareCR  bool
}

func newCanonicalCSVReader(input io.Reader) *canonicalCSVReader {
	return &canonicalCSVReader{input: bufio.NewReader(input)}
}

func newIdentityCSVReader(input io.Reader) *canonicalCSVReader {
	return &canonicalCSVReader{
		input: bufio.NewReader(input), allowRecordCRLF: true, allowMissingFinalLF: true,
	}
}

// newStagingCSVReader is newIdentityCSVReader keeping each record's bytes, for the stager's
// saved-row checks (see Raw).
func newStagingCSVReader(input io.Reader) *canonicalCSVReader {
	reader := newIdentityCSVReader(input)
	reader.keepRaw = true
	return reader
}

// Raw returns the bytes of the last record read, as read: a whole record with its line end after a
// successful Read, or the bytes up to and including the byte the reader refused after an error.
// The slice is reused by the next Read. Rest returns the input after them.
func (reader *canonicalCSVReader) Raw() []byte { return reader.raw }

// Rest returns the rest of the input, from the first byte after Raw.
func (reader *canonicalCSVReader) Rest() io.Reader { return reader.input }

// Buffered returns the input already read past Raw and held in the buffer, without reading more.
// The slice is valid until the next read.
func (reader *canonicalCSVReader) Buffered() []byte {
	buffered, _ := reader.input.Peek(reader.input.Buffered())
	return buffered
}

// RecordHasBareCR reports whether an unquoted field of the last record read kept a CR that does
// not start a CRLF. The engine's Commons CSV parser ends a record there.
func (reader *canonicalCSVReader) RecordHasBareCR() bool { return reader.bareCR }

func (reader *canonicalCSVReader) readByte() (byte, error) {
	value, err := reader.input.ReadByte()
	if err == nil && reader.keepRaw {
		reader.raw = append(reader.raw, value)
	}
	return value, err
}

func (reader *canonicalCSVReader) Read() ([]string, error) {
	reader.raw = reader.raw[:0]
	reader.bareCR = false
	if reader.done {
		return nil, io.EOF
	}
	fields := make([]string, 0, len(canonicalHeader))
	field := make([]byte, 0, 128)
	inQuotes := false
	afterQuote := false
	fieldStarted := false
	recordStarted := false
	recordBytes := 0

	finishField := func() {
		fields = append(fields, string(field))
		field = field[:0]
		fieldStarted = false
		afterQuote = false
	}

	for {
		value, err := reader.readByte()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			reader.done = true
			if !recordStarted {
				return nil, io.EOF
			}
			if inQuotes {
				return nil, fmt.Errorf("unterminated quoted field")
			}
			if reader.allowMissingFinalLF {
				finishField()
				return fields, nil
			}
			return nil, fmt.Errorf("canonical CSV record is missing final LF")
		}
		recordStarted = true
		recordBytes++
		if recordBytes > maxCanonicalCSVRecordBytes {
			return nil, fmt.Errorf("canonical CSV record exceeds %d bytes", maxCanonicalCSVRecordBytes)
		}

		if inQuotes {
			if value != '"' {
				field = append(field, value)
				continue
			}
			next, peekErr := reader.input.Peek(1)
			if peekErr == nil && next[0] == '"' {
				_, _ = reader.readByte()
				recordBytes++
				if recordBytes > maxCanonicalCSVRecordBytes {
					return nil, fmt.Errorf("canonical CSV record exceeds %d bytes", maxCanonicalCSVRecordBytes)
				}
				field = append(field, '"')
				continue
			}
			inQuotes = false
			afterQuote = true
			continue
		}

		if afterQuote {
			switch value {
			case ',':
				finishField()
			case '\r':
				if reader.allowRecordCRLF {
					next, peekErr := reader.input.Peek(1)
					if peekErr == nil && next[0] == '\n' {
						_, _ = reader.readByte()
						finishField()
						return fields, nil
					}
				}
				return nil, fmt.Errorf("unexpected byte %q after closing quote", value)
			case '\n':
				finishField()
				return fields, nil
			default:
				return nil, fmt.Errorf("unexpected byte %q after closing quote", value)
			}
			continue
		}

		switch value {
		case ',':
			finishField()
		case '\r':
			if reader.allowRecordCRLF {
				next, peekErr := reader.input.Peek(1)
				if peekErr == nil && next[0] == '\n' {
					_, _ = reader.readByte()
					finishField()
					return fields, nil
				}
			}
			fieldStarted = true
			reader.bareCR = true
			field = append(field, value)
		case '\n':
			finishField()
			return fields, nil
		case '"':
			if fieldStarted {
				return nil, fmt.Errorf("unexpected quote in unquoted field")
			}
			fieldStarted = true
			inQuotes = true
		default:
			fieldStarted = true
			field = append(field, value)
		}
	}
}
