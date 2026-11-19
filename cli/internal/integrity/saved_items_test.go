package integrity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func savedItemRowMessage(record string) string {
	return "record " + record + " is a saved engine items row (as --save-items writes it: object path, " +
		"hex offset, size, layer/mask), not a canonical manifest row; the " +
		`"bucket,key,size,version_id" header does not convert it. ` +
		"Give each object as <bucket>,<key>,<size>,<version_id or empty>"
}

func TestIsSavedItemFields(t *testing.T) {
	tests := []struct {
		name      string
		offset    string
		layerMask string
		want      bool
	}{
		{"as the engine writes it", "2a0f5ed8eab22f7e", "0/0", true},
		{"offset zero", "0", "0/0", true},
		{"negative offset", "-7f0f5ed8eab22f7e", "0/0", true},
		{"layer above zero", "2a0f", "ff/0", true},
		{"long mask", "2a0f", "0/0102030405060708", true},
		{"largest offset and layer", "7fffffffffffffff", "7fffffff/0", true},
		// The engine reads these, so item sizing accepts them too.
		{"upper-case digits", "2A0F", "FF/0A", true},
		{"leading plus on offset", "+2a0f", "0/0", true},
		{"leading plus on layer", "2a0f", "+1/0", true},
		{"negative layer", "2a0f", "-1/0", true},
		{"empty mask", "2a0f", "0/", true},

		{"empty offset", "", "0/0", false},
		{"offset not hex", "s0000000/abc", "0/0", false},
		{"offset above 64 bits", "8000000000000000", "0/0", false},
		{"empty version", "2a0f", "", false},
		{"no separator", "2a0f", "00", false},
		{"null version", "2a0f", "null", false},
		{"empty layer", "2a0f", "/0", false},
		{"layer not hex", "2a0f", "x/0", false},
		{"layer above 32 bits", "2a0f", "80000000/0", false},
		{"mask not hex", "2a0f", "0/xy", false},
		{"signed mask", "2a0f", "0/-1", false},
		{"second separator", "2a0f", "0/0/0", false},
		{"AWS-style version with a slash", "2a0f", "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo", false},
		{"mask with invalid UTF-8", "2a0f", "0/\xff", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsSavedItemFields(test.offset, test.layerMask); got != test.want {
				t.Fatalf("IsSavedItemFields(%q, %q) = %v, want %v", test.offset, test.layerMask, got, test.want)
			}
		})
	}
}

func TestRefuseSavedItemsUnderHeader(t *testing.T) {
	const header = "bucket,key,size,version_id\n"
	const engineRow = "/b/s0/name,2a0f,1024,0/0\n"
	tests := []struct {
		name    string
		content string
		refused bool
	}{
		{"engine row first", header + engineRow, true},
		{"engine row first, CRLF", "bucket,key,size,version_id\r\n/b/s0/name,2a0f,1024,0/0\r\n", true},
		{"engine row without final newline", header + "/b/s0/name,2a0f,1024,0/0", true},
		{"quoted header, as the engine reads it", `"bucket","key","size","version_id"` + "\n" + engineRow, true},
		{"white space after a quoted header field", `"bucket" ,key,size,version_id` + "\n" + engineRow, true},
		{"quote inside the object path", header + `/sample/pre"fix/s0000000/item,2a0f,1024,0/0` + "\n", true},
		{"bare-CR line ends", "bucket,key,size,version_id\r/b/s0/name,2a0f,1024,0/0\r/b/s0/two,2a10,1024,0/0\r", true},
		{"bare-CR line ends after the header only", "bucket,key,size,version_id\r" + engineRow, true},
		{"quoted object path", header + `"/b/s0/a,b",2a0f,1024,0/0` + "\n", true},
		{"object path longer than a canonical record", header + "/b/" + strings.Repeat("n", 1<<20) + ",2a0f,1024,0/0\n", true},

		{"canonical row first", header + "b,k,1,\n", false},
		{"only the first data row is read", header + "b,k,1,\n" + engineRow, false},
		{"header only", header, false},
		{"empty file", "", false},
		{"saved items file without the header", engineRow, false},
		{"plain item list", "obj-1\n", false},
		{"header in another order", "key,bucket,size,version_id\n" + engineRow, false},
		{"blank first line", "\n" + header + engineRow, false},
		{"first data row does not parse as CSV", header + "\"unterminated\n", false},
		{"first data row has too few fields", header + "b,2a0f,0/0\n", false},
		{"first data row has an invalid size", header + "b,2a0f,big,0/0\n", false},
		{"canonical row with a quoted field", header + `b,"dir/k,""1""",1024,` + "\n", false},
		{"canonical row with a quote inside the key", header + `b,dir/k"1,1024,` + "\n", false},
		{"hex key with an AWS-style version", header + "b,deadbeef,1024,3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo\n", false},
		{"bare-CR canonical rows", "bucket,key,size,version_id\rb,k,1024,\r/b/s0/name,2a0f,1024,0/0\r", false},
		// Files from which the engine itself takes no object.
		{"text after a closing quote", header + `"/b/s0/name"x,2a0f,1024,0/0` + "\n", false},
		{"unterminated quote in the first data row", header + `"/b/s0/name,2a0f,1024,0/0` + "\n", false},
		{"first data row with five fields", header + "/b/s0/name,2a0f,1024,0/0,x\n", false},
		{"first data row with an empty bucket", header + ",2a0f,1024,0/0\n", false},
		{"invalid UTF-8 before the first data row ends", header + "/b/s0/n\xffame,2a0f,1024,0/0\n", false},
		{"mask longer than a canonical record", header + "/b/s0/name,2a0f,1024,0/" + strings.Repeat("0", 1<<20) + "\n", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "items.csv")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			err := RefuseSavedItemsUnderHeader(path)
			if !test.refused {
				if err != nil {
					t.Fatalf("RefuseSavedItemsUnderHeader() error = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != savedItemRowMessage("2") {
				t.Fatalf("RefuseSavedItemsUnderHeader() error = %v, want %q", err, savedItemRowMessage("2"))
			}
		})
	}
}

func TestRefuseSavedItemsUnderHeaderLeavesAnUnreadableFileToTheEngine(t *testing.T) {
	if err := RefuseSavedItemsUnderHeader(filepath.Join(t.TempDir(), "missing.csv")); err != nil {
		t.Fatalf("RefuseSavedItemsUnderHeader() error = %v, want nil", err)
	}
}
