package integrity

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// readEngineRecords renders every record as count:field|field..., then "error" if reading failed.
func readEngineRecords(input string) []string {
	reader := newEngineCSVReader(strings.NewReader(input))
	var records []string
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return records
		}
		if err != nil {
			return append(records, "error")
		}
		records = append(records, fmt.Sprintf("%d:%s", record.count, strings.Join(record.fields, "|")))
	}
}

// The expected records are what Commons CSV 1.14.1 with CSVFormat.RFC4180 returns for the same
// bytes, checked against the library when this reader was written.
func TestEngineCSVReaderFollowsCommonsCSVRFC4180(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty input", "", nil},
		{"LF", "a,b\nc,d\n", []string{"2:a|b", "2:c|d"}},
		{"CRLF", "a,b\r\nc,d\r\n", []string{"2:a|b", "2:c|d"}},
		{"bare CR", "a,b\rc,d\r", []string{"2:a|b", "2:c|d"}},
		{"CR then CRLF", "a\r\r\nb", []string{"1:a", "1:", "1:b"}},
		{"no final line end", "a,b", []string{"2:a|b"}},
		{"trailing comma", "a,\n", []string{"2:a|"}},
		{"trailing comma at the end", "a,", []string{"2:a|"}},
		{"empty line", "a\n\nb\n", []string{"1:a", "1:", "1:b"}},
		{"quote inside an unquoted field", "a\"b,c\n", []string{"2:a\"b|c"}},
		{"quote after a leading space", " \"a\",b\n", []string{"2: \"a\"|b"}},
		{"quoted field", "\"a,b\",c\n", []string{"2:a,b|c"}},
		{"doubled quote", "\"a\"\"b\"\n", []string{"1:a\"b"}},
		{"line breaks inside quotes", "\"a\r\nb\rc\"\n", []string{"1:a\r\nb\rc"}},
		{"white space after a closing quote", "\"a\" \t,b\n", []string{"2:a|b"}},
		{"ideographic space after a closing quote", "\"a\"　,b\n", []string{"2:a|b"}},
		{"no-break space after a closing quote", "\"a\" ,b\n", []string{"error"}},
		{"text after a closing quote", "\"a\"x,b\n", []string{"error"}},
		{"closing quote at the end", "\"a\"", []string{"1:a"}},
		{"unterminated quote", "\"a,b\n", []string{"error"}},
		{"byte order mark kept", "\xef\xbb\xbfa\n", []string{"1:\xef\xbb\xbfa"}},
		// Commons CSV fails here before its first record, because its UTF-8 reader decodes ahead.
		// Either way the engine takes no object from the file.
		{"invalid UTF-8", "a\n\xff\n", []string{"1:a", "error"}},
		{"fields past the fifth counted", "1,2,3,4,5,6,7\n", []string{"7:1|2|3|4|5"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := readEngineRecords(test.input)
			if strings.Join(got, "\n") != strings.Join(test.want, "\n") {
				t.Fatalf("records of %q = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestEngineCSVReaderCutsLongFieldsAndMarksThem(t *testing.T) {
	long := strings.Repeat("x", maxCanonicalCSVRecordBytes+10)
	reader := newEngineCSVReader(strings.NewReader(long + ",k\n"))
	record, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if record.count != 2 || len(record.fields[0]) != maxCanonicalCSVRecordBytes || !record.cut[0] || record.cut[1] ||
		record.fields[1] != "k" {
		t.Fatalf("record = count %d, field lengths %d and %q, cut %v",
			record.count, len(record.fields[0]), record.fields[1], record.cut)
	}
}

func TestJavaIsWhitespace(t *testing.T) {
	for _, value := range []rune{' ', '\t', '\n', '\v', '\f', '\r', 0x1c, 0x1f, 0x2003, 0x2028, 0x2029, 0x3000} {
		if !javaIsWhitespace(value) {
			t.Errorf("javaIsWhitespace(%U) = false", value)
		}
	}
	for _, value := range []rune{'a', 0, 0x85, 0xa0, 0x2007, 0x202f, 0x200b, 0xfeff, 0x1f600} {
		if javaIsWhitespace(value) {
			t.Errorf("javaIsWhitespace(%U) = true", value)
		}
	}
}
