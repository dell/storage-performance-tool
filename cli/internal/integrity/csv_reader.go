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
}

func newCanonicalCSVReader(input io.Reader) *canonicalCSVReader {
	return &canonicalCSVReader{input: bufio.NewReader(input)}
}

func newIdentityCSVReader(input io.Reader) *canonicalCSVReader {
	return &canonicalCSVReader{
		input: bufio.NewReader(input), allowRecordCRLF: true, allowMissingFinalLF: true,
	}
}

func (reader *canonicalCSVReader) Read() ([]string, error) {
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
		value, err := reader.input.ReadByte()
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
				_, _ = reader.input.ReadByte()
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
						_, _ = reader.input.ReadByte()
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
					_, _ = reader.input.ReadByte()
					finishField()
					return fields, nil
				}
			}
			fieldStarted = true
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
