package integrity

import (
	"io"
	"strings"
)

// canonicalCSVWriter emits the same minimal-quoting dialect as
// IntegrityCsvFormat.RFC4180_LF in the engine. encoding/csv deliberately has
// different edge-case quoting rules, so using it for committed evidence would
// make physical canonicalization depend on which process wrote the manifest.
type canonicalCSVWriter struct {
	output io.Writer
	err    error
}

func newCanonicalCSVWriter(output io.Writer) *canonicalCSVWriter {
	return &canonicalCSVWriter{output: output}
}

func (writer *canonicalCSVWriter) Write(fields []string) error {
	if writer.err != nil {
		return writer.err
	}
	var record strings.Builder
	for index, field := range fields {
		if index > 0 {
			record.WriteByte(',')
		}
		if canonicalCSVFieldNeedsQuotes(field) {
			record.WriteByte('"')
			record.WriteString(strings.ReplaceAll(field, "\"", "\"\""))
			record.WriteByte('"')
		} else {
			record.WriteString(field)
		}
	}
	record.WriteByte('\n')
	_, writer.err = io.WriteString(writer.output, record.String())
	return writer.err
}

func (writer *canonicalCSVWriter) Flush() {}

func (writer *canonicalCSVWriter) Error() error {
	return writer.err
}

func canonicalCSVFieldNeedsQuotes(field string) bool {
	if field == "" {
		return false
	}
	// Commons CSV quotes a leading byte through '#' and a trailing ASCII
	// control or space character.
	if field[0] <= '#' || field[len(field)-1] <= ' ' {
		return true
	}
	return strings.ContainsAny(field, ",\"\r\n")
}
