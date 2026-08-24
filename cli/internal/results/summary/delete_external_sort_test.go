package summary

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDeleteExternalSortBoundsChunksAndRemovesTemporaryFiles(t *testing.T) {
	temp := t.TempDir()
	source := filepath.Join(temp, "source.csv")
	file, err := os.Create(source) // #nosec G304 -- test-owned temporary path
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"id", "value"}); err != nil {
		t.Fatal(err)
	}
	rowCount := deleteSortChunkRows*2 + 17
	for index := rowCount - 1; index >= 0; index-- {
		if err := writer.Write([]string{fmt.Sprintf("%08d", index), fmt.Sprintf("value-%d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(temp, "target.csv")
	rows, err := sortDeleteCSV(
		context.Background(), source, []string{"id", "value"}, target, temp, "bounded",
		func(left, right []string) int { return compareString(left[0], right[0]) },
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows != int64(rowCount) {
		t.Fatalf("rows = %d, want %d", rows, rowCount)
	}

	sorted, err := os.Open(target) // #nosec G304 -- test-owned temporary path
	if err != nil {
		t.Fatal(err)
	}
	reader := csv.NewReader(sorted)
	if _, err := reader.Read(); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < rowCount; index++ {
		row, readErr := reader.Read()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if row[0] != fmt.Sprintf("%08d", index) {
			t.Fatalf("row %d id = %q", index, row[0])
		}
	}
	if err := sorted.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(temp)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("temporary sort directory was not removed: %s", entry.Name())
		}
	}
}
