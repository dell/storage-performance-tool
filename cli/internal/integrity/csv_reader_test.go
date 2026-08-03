package integrity

import (
	"reflect"
	"strings"
	"testing"
)

func TestCanonicalCSVReaderPreservesFieldIdentity(t *testing.T) {
	input := "bucket,key,size,version_id\n" +
		"b,\"line\r\nkey\",3,\"version,one\"\n"
	reader := newCanonicalCSVReader(strings.NewReader(input))
	header, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(header, canonicalHeader) {
		t.Fatalf("header = %#v, want %#v", header, canonicalHeader)
	}
	record, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b", "line\r\nkey", "3", "version,one"}
	if !reflect.DeepEqual(record, want) {
		t.Fatalf("record = %#v, want %#v", record, want)
	}
}
