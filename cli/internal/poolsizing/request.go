package poolsizing

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// The operation kinds of a request.
const (
	kindCreate = "create"
	kindRead   = "read"
	kindOther  = "other"
)

// defaultThresholdBytes is storage.rdma.thresholdBytes's shipped default.
const defaultThresholdBytes = 1048576

// Request is the workload spt-nic-plan estimates: a run's steps, each with the drivers that run at
// once. spt run generates one driver per step, a mixed step included; concurrent drivers come only
// from a hand-written pipeline or weighted scenario, which the engine's own check covers.
type Request struct {
	Steps []StepRequest `json:"steps"`
}

// StepRequest is one step.
type StepRequest struct {
	ID      string          `json:"id"`
	Drivers []DriverRequest `json:"drivers"`
}

// DriverRequest is one driver's workload.
type DriverRequest struct {
	Concurrency      int         `json:"concurrency"`
	ThresholdBytes   int64       `json:"thresholdBytes"`
	MultipartObjects int         `json:"multipartObjects"`
	MultipartParts   int         `json:"multipartParts"`
	Operations       []Operation `json:"operations"`
}

// Operation is one operation type of a driver.
type Operation struct {
	Kind      string `json:"kind"`
	PartBytes int64  `json:"partBytes,omitempty"`
	Ranged    bool   `json:"ranged,omitempty"`
	Sizes     *Sizes `json:"sizes,omitempty"`
}

// Sizes says where an operation's sizes come from: a range, with the setting or step it came from;
// a read of an item list; or why they are unknown.
type Sizes struct {
	Range   *[2]int64  `json:"range,omitempty"`
	Source  string     `json:"source,omitempty"`
	Listed  *ItemSizes `json:"listed,omitempty"`
	Unknown string     `json:"unknown,omitempty"`
}

// Encode gives the form --pool-request takes, which passes unchanged through a remote shell.
func (r Request) Encode() (string, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("encode the pool request: %w", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// effectiveWorkload is what the generated scenario gives the engine, engine overrides included.
type effectiveWorkload struct {
	concurrency      int
	objectSize       *[2]int64
	objectSizeSource string
	sizeError        string
	partBytes        int64
	thresholdBytes   int64
	multipartObjects int
	multipartParts   int
	ranged           bool
	enabled          bool
}

// Applies reports whether sizing applies to a run: a NIC set on some host, s3-rdma and the pool on.
func Applies(params scenario.Params) bool {
	if params.S3Driver != scenario.S3DriverRdma || params.RdmaDisableBufferPool {
		return false
	}
	if len(params.Interfaces) == 0 && len(anyNicSet(params)) == 0 {
		return false
	}
	return effective(params).enabled
}

func anyNicSet(params scenario.Params) []string {
	hosts := []string{}
	for _, override := range params.HostInterfaces {
		if len(override.Entries) > 0 {
			hosts = append(hosts, override.Host)
		}
	}
	return hosts
}

func effective(params scenario.Params) effectiveWorkload {
	w := effectiveWorkload{
		concurrency:      params.Threads,
		objectSizeSource: "--object-size",
		thresholdBytes:   params.RdmaThresholdBytes,
		multipartObjects: params.MpuObjects,
		multipartParts:   params.MpuParts,
		ranged:           params.RangeSize != "",
		enabled:          true,
	}
	if w.thresholdBytes == 0 {
		w.thresholdBytes = defaultThresholdBytes
	}
	size := params.ObjectSize
	part := params.PartSize
	for _, override := range params.EngineOverrides {
		path, value, found := strings.Cut(override, "=")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		switch path = strings.TrimSpace(path); path {
		case "item.data.size":
			size = value
			w.objectSizeSource = path
		case "item.data.ranges.threshold":
			part = value
		default:
			w.applyOverride(path, value)
		}
	}
	if parsed, err := sizeparse.Parse(part); part != "" && err == nil && parsed > 0 {
		w.partBytes = parsed
	}
	sizes, err := parseSizeRange(size)
	if err != nil {
		w.sizeError = err.Error()
	} else {
		w.objectSize = &sizes
	}
	return w
}

// applyOverride reads an engine override of a setting the estimate takes as a number or a switch.
func (w *effectiveWorkload) applyOverride(path, value string) {
	if path == "storage.rdma.enabled" || path == "storage.rdma.bufferPool" {
		w.enabled = w.enabled && !strings.EqualFold(value, "false")
		return
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return
	}
	switch path {
	case "storage.driver.limit.concurrency":
		w.concurrency = int(parsed)
	case "storage.driver.limit.multipart.objects":
		w.multipartObjects = int(parsed)
	case "storage.driver.limit.multipart.parts":
		w.multipartParts = int(parsed)
	case "storage.rdma.thresholdBytes":
		w.thresholdBytes = parsed
	}
}

// parseSizeRange reads a fixed size or a min-max range, as item.data.size holds them.
func parseSizeRange(value string) ([2]int64, error) {
	value = strings.TrimSpace(value)
	if head, _, found := strings.Cut(value, ","); found {
		// A range's bias does not change its bounds.
		value = head
	}
	if low, high, found := strings.Cut(value, "-"); found {
		minimum, err := sizeparse.Parse(low)
		if err != nil {
			return [2]int64{}, err
		}
		maximum, err := sizeparse.Parse(high)
		if err != nil {
			return [2]int64{}, err
		}
		return [2]int64{minimum, maximum}, nil
	}
	size, err := sizeparse.Parse(value)
	if err != nil {
		return [2]int64{}, err
	}
	return [2]int64{size, size}, nil
}

func (w effectiveWorkload) sizes(source string) *Sizes {
	if w.objectSize == nil {
		return &Sizes{Unknown: "its object size could not be read: " + w.sizeError}
	}
	if source == "" {
		source = w.objectSizeSource
	}
	return &Sizes{Range: w.objectSize, Source: source}
}

func (w effectiveWorkload) driver(operations ...Operation) DriverRequest {
	return DriverRequest{
		Concurrency:      w.concurrency,
		ThresholdBytes:   w.thresholdBytes,
		MultipartObjects: w.multipartObjects,
		MultipartParts:   w.multipartParts,
		Operations:       operations,
	}
}

// BuildRequest turns a generated run's steps into its request. Steps not named here, such as a
// delete or a list, use no RDMA. A read of a file a generated step writes takes that step's object
// size; a read of a user's file takes the listed sizes that hostFiles found.
//
// A generated read step carries the seed's part size when --part-size is set, so the engine splits
// an object above it into range requests, which go over HTTP: such a read has no demand.
func BuildRequest(params scenario.Params, steps []scenario.StepInfo, listed func(path string) ItemSizes) Request {
	w := effective(params)
	request := Request{Steps: make([]StepRequest, 0, len(steps))}
	writer := ""
	for _, step := range steps {
		driver := w.driver(Operation{Kind: kindOther})
		switch step.Op {
		case "create", "seed":
			part := w.partBytes
			if params.WorkloadType == workload.Mixed {
				part = 0 // The mixed scenario's shared configuration names no part size.
			}
			writer = step.ID
			driver = w.driver(Operation{Kind: kindCreate, PartBytes: part, Sizes: w.sizes("")})
		case "read", "verify":
			driver = w.driver(readOperation(params, w, writer, listed))
		case "mixed":
			operations := make([]Operation, 0, 2)
			if params.GetDistrib > 0 {
				operations = append(operations, mixedRead(params, w, writer, listed))
			}
			if params.PutDistrib > 0 {
				operations = append(operations, Operation{Kind: kindCreate, Sizes: w.sizes("")})
			}
			if len(operations) > 0 {
				driver = w.driver(operations...)
			}
		}
		request.Steps = append(request.Steps, StepRequest{ID: step.ID, Drivers: []DriverRequest{driver}})
	}
	return request
}

func readOperation(params scenario.Params, w effectiveWorkload, writer string, listed func(string) ItemSizes) Operation {
	operation := Operation{Kind: kindRead, Ranged: w.ranged}
	switch {
	case params.ItemsFile != "":
		sizes := listed(params.ItemsFile)
		operation.Sizes = &Sizes{Listed: &sizes}
	case writer != "":
		operation.PartBytes = w.partBytes
		operation.Sizes = w.sizes(writer)
	default:
		operation.Sizes = &Sizes{Unknown: "its items come from a bucket listing"}
	}
	return operation
}

func mixedRead(params scenario.Params, w effectiveWorkload, writer string, listed func(string) ItemSizes) Operation {
	operation := Operation{Kind: kindRead}
	switch {
	case params.ReadItemsFile != "":
		sizes := listed(params.ReadItemsFile)
		operation.Sizes = &Sizes{Listed: &sizes}
	case writer != "":
		operation.Sizes = w.sizes(writer)
	default:
		operation.Sizes = &Sizes{Unknown: "its items come from a bucket listing"}
	}
	return operation
}

// HostFile returns the host file the prepared mount maps a container path to, or the path itself
// before PrepareExternalItemFiles has rewritten it. The scenario names the container path, which
// does not exist on the CLI's host.
func HostFile(params scenario.Params, path string) string {
	for _, mount := range params.ItemFileMounts {
		if mount.ContainerPath == path {
			return mount.HostPath
		}
	}
	return path
}

// CachedItemReader reads each of the run's item files once, on the CLI's host.
func CachedItemReader(params scenario.Params) func(string) ItemSizes {
	read := map[string]ItemSizes{}
	return func(path string) ItemSizes {
		host := HostFile(params, path)
		if sizes, ok := read[host]; ok {
			return sizes
		}
		sizes := ReadItemSizes(host, ItemFileBudget)
		read[host] = sizes
		return sizes
	}
}
