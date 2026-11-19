package cmd

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestBuildScenarioParamsObjectSizeOfReads(t *testing.T) {
	for _, test := range []struct {
		name, itemsFile, objectSize, want string
	}{
		{name: "items file without size stays unset", itemsFile: "items.csv"},
		{name: "items file keeps explicit size", itemsFile: "items.csv", objectSize: "64KiB", want: "64KiB"},
		{name: "items file keeps explicit wrong size", itemsFile: "items.csv", objectSize: "5MiB", want: "5MiB"},
		{name: "seeded read keeps default", want: "1MB"},
		{name: "seeded read keeps explicit size", objectSize: "4KiB", want: "4KiB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := deleteValidationCommand(deleteValidationCase{bucket: "bench", itemsFile: test.itemsFile, batchSize: 1})
			if test.objectSize != "" {
				if err := cmd.Flags().Set("object-size", test.objectSize); err != nil {
					t.Fatal(err)
				}
			}
			params, err := buildScenarioParams(WorkloadTypeRead, cmd)
			if err != nil {
				t.Fatal(err)
			}
			if params.ObjectSize != test.want {
				t.Fatalf("ObjectSize = %q, want %q", params.ObjectSize, test.want)
			}
		})
	}
}

func TestBuildScenarioParamsItemsFileKeepsDefaultOutsideRead(t *testing.T) {
	cmd := deleteValidationCommand(deleteValidationCase{bucket: "bench", itemsFile: "items.csv", batchSize: 1})
	params, err := buildScenarioParams(WorkloadTypeWrite, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if params.ObjectSize != scenario.DefaultObjectSize {
		t.Fatalf("write ObjectSize = %q, want %q", params.ObjectSize, scenario.DefaultObjectSize)
	}
}

func TestFormatScenarioParamsObjectSizeOfReads(t *testing.T) {
	for _, test := range []struct {
		name   string
		params scenario.Params
		want   string
	}{
		{
			name:   "items file without size",
			params: scenario.Params{WorkloadType: WorkloadTypeRead, ItemsFile: "items.csv"},
			want:   "Object Size: (from items file)",
		},
		{
			name:   "items file with size",
			params: scenario.Params{WorkloadType: WorkloadTypeRead, ItemsFile: "items.csv", ObjectSize: "64KiB"},
			want:   "Object Size: 64KiB",
		},
		{
			name:   "items file with another size",
			params: scenario.Params{WorkloadType: WorkloadTypeRead, ItemsFile: "items.csv", ObjectSize: "5MiB"},
			want:   "Object Size: 5MiB",
		},
		{
			name:   "seeded read",
			params: scenario.Params{WorkloadType: WorkloadTypeRead, ObjectSize: "1MB"},
			want:   "Object Size: 1MB",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lines := strings.Split(formatScenarioParams(test.params), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "Object Size:") {
					if line != test.want {
						t.Fatalf("header line = %q, want %q", line, test.want)
					}
					return
				}
			}
			t.Fatalf("no Object Size line in %q", lines)
		})
	}
}
