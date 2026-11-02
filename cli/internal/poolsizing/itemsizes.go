// Package poolsizing is the CLI's side of RDMA Pool Demand and of sizing -XX:MaxDirectMemorySize
// (spec items 14a and 14b). The arithmetic is the engine's: this package builds the request the
// image's spt-nic-plan command answers on each host, composes the answers across a run's steps and
// decides the value each engine gets.
package poolsizing

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Completion reasons, as the engine's ItemSizeSummary names them.
const (
	reasonMalformed = "MALFORMED_ITEM"
	reasonRead      = "READ_FAILED"
	reasonBudget    = "BUDGET_EXPIRED"
)

// ItemFileBudget is how long the CLI spends reading one item file before launch: a quarter of the
// engine's default preparation deadline, the same budget as the engine's own read.
const ItemFileBudget = 150 * time.Second

// checkEvery is how many items pass between checks of the budget.
const checkEvery = 4096

// ItemSizes is the JSON form of the engine's ItemSizeSummary: only the smallest and largest size in
// each power-of-two range. An incomplete file's bounds are never used.
type ItemSizes struct {
	Source   string     `json:"source"`
	Complete bool       `json:"complete"`
	Reason   string     `json:"reason,omitempty"`
	Detail   string     `json:"detail,omitempty"`
	Items    int64      `json:"items"`
	Millis   int64      `json:"millis"`
	Bounds   [][2]int64 `json:"bounds"`
}

type sizeRecorder struct {
	bounds map[int][2]int64
	items  int64
}

// exponent is the smallest e with size <= 2^e.
func exponent(size int64) int {
	if size <= 1 {
		return 0
	}
	return bits.Len64(uint64(size - 1))
}

func (r *sizeRecorder) record(size int64) {
	r.items++
	if r.bounds == nil {
		r.bounds = make(map[int][2]int64)
	}
	e := exponent(size)
	known, ok := r.bounds[e]
	if !ok {
		r.bounds[e] = [2]int64{size, size}
		return
	}
	if size < known[0] {
		known[0] = size
	}
	if size > known[1] {
		known[1] = size
	}
	r.bounds[e] = known
}

func (r *sizeRecorder) sorted() [][2]int64 {
	keys := make([]int, 0, len(r.bounds))
	for key := range r.bounds {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	bounds := make([][2]int64, 0, len(keys))
	for _, key := range keys {
		bounds = append(bounds, r.bounds[key])
	}
	return bounds
}

// ReadItemSizes reads a user's item file on the CLI's host, within budget, and says whether it saw
// all of it (D54-R1). It accepts what the engine's parsers accept for a file the CLI mounts: every
// mount is named .csv in the container, so a file is an item-list CSV or, with the exact header, a
// canonical integrity manifest. Shared fixtures under test-fixtures/pool-demand hold it to the
// engine's own scan; any difference is only a logged discrepancy, and the engine's figure decides.
func ReadItemSizes(path string, budget time.Duration) ItemSizes {
	start := time.Now()
	deadline := start.Add(budget)
	result := ItemSizes{Source: path}
	finish := func(recorder *sizeRecorder, reason, detail string) ItemSizes {
		result.Millis = time.Since(start).Milliseconds()
		if recorder != nil {
			result.Items = recorder.items
		}
		if reason != "" {
			result.Reason, result.Detail, result.Bounds = reason, detail, [][2]int64{}
			return result
		}
		result.Complete = true
		result.Bounds = recorder.sorted()
		return result
	}
	file, err := os.Open(path) // #nosec G304 -- the user's own --items-file, read on the user's own host.
	if err != nil {
		return finish(nil, reasonRead, err.Error())
	}
	defer func() { _ = file.Close() }()

	manifest, err := hasManifestHeader(file)
	if err != nil {
		return finish(nil, reasonRead, err.Error())
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return finish(nil, reasonRead, err.Error())
	}
	recorder := &sizeRecorder{}
	if manifest {
		reason, detail := readManifest(file, recorder, deadline)
		return finish(recorder, reason, detail)
	}
	reason, detail := readItemList(file, recorder, deadline)
	return finish(recorder, reason, detail)
}

var manifestHeader = []string{"bucket", "key", "size", "version_id"}

func hasManifestHeader(file *os.File) (bool, error) {
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	first, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	var parseError *csv.ParseError
	if errors.As(err, &parseError) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(first) != len(manifestHeader) {
		return false, nil
	}
	for i, field := range manifestHeader {
		if first[i] != field {
			return false, nil
		}
	}
	return true, nil
}

func readManifest(file io.Reader, recorder *sizeRecorder, deadline time.Time) (string, string) {
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	if _, err := reader.Read(); err != nil {
		return reasonRead, err.Error()
	}
	for {
		if recorder.items%checkEvery == 0 && time.Now().After(deadline) {
			return reasonBudget, ""
		}
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return "", ""
		}
		if err != nil {
			return manifestReadFailure(err)
		}
		size, err := manifestSize(record)
		if err != nil {
			return reasonMalformed, strings.Join(record, ",")
		}
		recorder.record(size)
	}
}

func manifestReadFailure(err error) (string, string) {
	var parseError *csv.ParseError
	if errors.As(err, &parseError) {
		return reasonMalformed, err.Error()
	}
	return reasonRead, err.Error()
}

// manifestSize reads one manifest record as the engine's IntegrityManifestItemInput does.
func manifestSize(record []string) (int64, error) {
	if len(record) != len(manifestHeader) || record[0] == "" || record[1] == "" {
		return 0, fmt.Errorf("a manifest record has four fields and a bucket and key")
	}
	size, err := strconv.ParseInt(record[2], 10, 64)
	if err != nil {
		return 0, err
	}
	if size < 0 {
		return 0, fmt.Errorf("a manifest record's size is not negative")
	}
	return size, nil
}

func readItemList(file io.Reader, recorder *sizeRecorder, deadline time.Time) (string, string) {
	reader := bufio.NewReader(file)
	for {
		if recorder.items%checkEvery == 0 && time.Now().After(deadline) {
			return reasonBudget, ""
		}
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return reasonRead, err.Error()
		}
		atEnd := errors.Is(err, io.EOF)
		if atEnd && line == "" {
			return "", ""
		}
		line = strings.TrimRight(line, "\r\n")
		size, parseErr := itemLineSize(line)
		if parseErr != nil {
			return reasonMalformed, truncate(line)
		}
		recorder.record(size)
		if atEnd {
			return "", ""
		}
	}
}

// itemLineSize reads one item-list line as the engine's DataItemImpl does:
// <name>,<offset in hex>,<size in decimal>,<layer in hex>/<mask in hex>.
func itemLineSize(line string) (int64, error) {
	fields := strings.SplitN(line, ",", 4)
	if len(fields) != 4 {
		return 0, fmt.Errorf("an item line has four fields: %q", line)
	}
	if _, err := strconv.ParseInt(fields[1], 16, 64); err != nil {
		return 0, err
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return 0, err
	}
	layer, mask, found := strings.Cut(fields[3], "/")
	if !found {
		return 0, fmt.Errorf("an item line has a layer and a mask: %q", line)
	}
	if _, err := strconv.ParseInt(layer, 16, 32); err != nil {
		return 0, err
	}
	for _, digit := range mask {
		if !strings.ContainsRune("0123456789abcdefABCDEF", digit) {
			return 0, fmt.Errorf("an item line's mask is hexadecimal: %q", line)
		}
	}
	return size, nil
}

func truncate(line string) string {
	const longest = 120
	if len(line) > longest {
		return line[:longest] + "..."
	}
	return line
}
