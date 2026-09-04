package summary

import (
	"container/heap"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"unicode/utf8"
)

const (
	deleteSortChunkRows  = 1024
	deleteSortMergeFanIn = 64
)

type deleteRowCompare func(left, right []string) int

type deleteMergeCursor struct {
	file   *os.File
	reader *csv.Reader
	row    []string
	source int
}

func openDeleteMergeCursor(path string, source int) (*deleteMergeCursor, error) {
	file, err := os.Open(path) // #nosec G304 -- path is an agent-created validation artifact
	if err != nil {
		return nil, err
	}
	return &deleteMergeCursor{file: file, reader: csv.NewReader(file), source: source}, nil
}

func (cursor *deleteMergeCursor) advance() (bool, error) {
	row, err := cursor.reader.Read()
	if errors.Is(err, io.EOF) {
		cursor.row = nil
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cursor.row = row
	return true, nil
}

type deleteMergeHeap struct {
	rows    []*deleteMergeCursor
	compare deleteRowCompare
}

func (h deleteMergeHeap) Len() int { return len(h.rows) }
func (h deleteMergeHeap) Less(left, right int) bool {
	compared := h.compare(h.rows[left].row, h.rows[right].row)
	if compared != 0 {
		return compared < 0
	}
	return h.rows[left].source < h.rows[right].source
}
func (h deleteMergeHeap) Swap(left, right int) {
	h.rows[left], h.rows[right] = h.rows[right], h.rows[left]
}
func (h *deleteMergeHeap) Push(value any) { h.rows = append(h.rows, value.(*deleteMergeCursor)) }
func (h *deleteMergeHeap) Pop() any {
	last := len(h.rows) - 1
	value := h.rows[last]
	h.rows = h.rows[:last]
	return value
}

func sortDeleteCSV(
	ctx context.Context,
	source string,
	expectedHeader []string,
	target string,
	tempRoot string,
	prefix string,
	compare deleteRowCompare,
) (rows int64, returnedErr error) {
	tempDir, err := os.MkdirTemp(tempRoot, prefix+"-")
	if err != nil {
		return 0, err
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tempDir); cleanupErr != nil {
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()

	file, err := os.Open(source) // #nosec G304 -- path is a fetched result artifact
	if err != nil {
		return 0, err
	}
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil || !equalStringSlices(header, expectedHeader) {
		_ = file.Close()
		return 0, fmt.Errorf("DELETE artifact %s has a noncanonical header", filepath.Base(source))
	}
	batch := make([][]string, 0, deleteSortChunkRows)
	var chunks int64
	for {
		if err := checkDeleteContext(ctx); err != nil {
			_ = file.Close()
			return 0, err
		}
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = file.Close()
			return 0, fmt.Errorf("parse DELETE artifact %s: %w", filepath.Base(source), readErr)
		}
		if len(row) != len(expectedHeader) {
			_ = file.Close()
			return 0, fmt.Errorf("DELETE artifact %s row %d has %d fields", filepath.Base(source), rows+2, len(row))
		}
		for _, field := range row {
			if !utf8.ValidString(field) {
				_ = file.Close()
				return 0, fmt.Errorf("DELETE artifact %s row %d is not valid UTF-8", filepath.Base(source), rows+2)
			}
		}
		batch = append(batch, append([]string(nil), row...))
		rows, err = checkedDeleteAdd(rows, 1)
		if err != nil {
			_ = file.Close()
			return 0, err
		}
		if len(batch) == deleteSortChunkRows {
			if err := writeDeleteSortChunk(batch, compare, deleteChunkPath(tempDir, 0, chunks)); err != nil {
				_ = file.Close()
				return 0, err
			}
			chunks++
			batch = batch[:0]
		}
	}
	if err := file.Close(); err != nil {
		return 0, err
	}
	if len(batch) > 0 {
		if err := writeDeleteSortChunk(batch, compare, deleteChunkPath(tempDir, 0, chunks)); err != nil {
			return 0, err
		}
		chunks++
	}
	if err := mergeDeleteSortChunks(ctx, tempDir, chunks, target, expectedHeader, compare); err != nil {
		return 0, err
	}
	return rows, nil
}

func writeDeleteSortChunk(batch [][]string, compare deleteRowCompare, path string) error {
	sort.SliceStable(batch, func(left, right int) bool { return compare(batch[left], batch[right]) < 0 })
	return writeDeleteCSVFile(path, nil, batch)
}

func mergeDeleteSortChunks(
	ctx context.Context,
	tempDir string,
	initialCount int64,
	target string,
	header []string,
	compare deleteRowCompare,
) error {
	count := initialCount
	round := 0
	for count > deleteSortMergeFanIn {
		var nextCount int64
		for start := int64(0); start < count; start += deleteSortMergeFanIn {
			group := deleteChunkGroup(tempDir, round, start, count)
			if err := mergeDeleteSortGroup(
				ctx, group, deleteChunkPath(tempDir, round+1, nextCount), nil, compare,
			); err != nil {
				return err
			}
			if err := removeDeleteChunks(group); err != nil {
				return err
			}
			nextCount++
		}
		count = nextCount
		round++
	}
	group := deleteChunkGroup(tempDir, round, 0, count)
	if err := mergeDeleteSortGroup(ctx, group, target, header, compare); err != nil {
		return err
	}
	return removeDeleteChunks(group)
}

func mergeDeleteSortGroup(
	ctx context.Context,
	chunks []string,
	target string,
	header []string,
	compare deleteRowCompare,
) (returnedErr error) {
	file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- bounded validation output
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	defer func() {
		writer.Flush()
		returnedErr = errors.Join(returnedErr, writer.Error(), file.Close())
	}()
	if header != nil {
		if err := writer.Write(header); err != nil {
			return err
		}
	}
	cursors := make([]*deleteMergeCursor, 0, len(chunks))
	defer func() {
		for _, cursor := range cursors {
			returnedErr = errors.Join(returnedErr, cursor.file.Close())
		}
	}()
	queue := &deleteMergeHeap{compare: compare}
	for index, chunk := range chunks {
		cursor, err := openDeleteMergeCursor(chunk, index)
		if err != nil {
			return err
		}
		cursors = append(cursors, cursor)
		hasRow, err := cursor.advance()
		if err != nil {
			return err
		}
		if hasRow {
			heap.Push(queue, cursor)
		}
	}
	for queue.Len() > 0 {
		if err := checkDeleteContext(ctx); err != nil {
			return err
		}
		cursor := heap.Pop(queue).(*deleteMergeCursor)
		if err := writer.Write(cursor.row); err != nil {
			return err
		}
		hasRow, err := cursor.advance()
		if err != nil {
			return err
		}
		if hasRow {
			heap.Push(queue, cursor)
		}
	}
	return nil
}

func writeDeleteCSVFile(path string, header []string, rows [][]string) (returnedErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- bounded validation output
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	defer func() {
		writer.Flush()
		returnedErr = errors.Join(returnedErr, writer.Error(), file.Close())
	}()
	if header != nil {
		if err := writer.Write(header); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func deleteChunkGroup(tempDir string, round int, start, count int64) []string {
	end := start + deleteSortMergeFanIn
	if end > count {
		end = count
	}
	group := make([]string, 0, end-start)
	for index := start; index < end; index++ {
		group = append(group, deleteChunkPath(tempDir, round, index))
	}
	return group
}

func deleteChunkPath(tempDir string, round int, index int64) string {
	return filepath.Join(tempDir, fmt.Sprintf("r%d-%020d.csv", round, index))
}

func removeDeleteChunks(chunks []string) error {
	var errs []error
	for _, chunk := range chunks {
		if err := os.Remove(chunk); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func checkDeleteContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("DELETE artifact validation canceled: %w", ctx.Err())
	default:
		return nil
	}
}

func compareString(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
