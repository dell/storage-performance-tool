package scenario

import (
	"fmt"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
	"math"
)

// RangePolicy is the checked, effective partial-read policy. JSON strings preserve
// signed-64-bit byte values through the JavaScript scenario engine.
type RangePolicy struct {
	Size   int64  `json:"size,string"`
	Offset *int64 `json:"offset,omitempty,string"`
	Align  int64  `json:"align,string"`
}

// ParseRangePolicy validates optional public range parameters before resource setup.
func ParseRangePolicy(params Params) (*RangePolicy, error) {
	if params.RangeSize == "" {
		if params.RangeOffset != "" || params.RangeAlign != "" {
			return nil, fmt.Errorf("--range-offset and --range-align require --range-size")
		}
		return nil, nil
	}
	if params.WorkloadType != workload.Read {
		return nil, fmt.Errorf("--range-size supports only the read workload")
	}
	switch params.S3Driver {
	case "", S3DriverDefault, S3DriverNetty:
	default:
		return nil, fmt.Errorf("partial reads require the Netty S3 driver; AWS and native S3-RDMA are deferred")
	}
	if params.PartSize != "" {
		partSize, err := sizeparse.Parse(params.PartSize)
		if err != nil {
			return nil, fmt.Errorf("--part-size: %w", err)
		}
		if partSize > 0 {
			return nil, fmt.Errorf("--range-size conflicts with positive --part-size (legacy range splitting)")
		}
	}
	length, err := sizeparse.Parse(params.RangeSize)
	if err != nil {
		return nil, fmt.Errorf("--range-size: %w", err)
	}
	if length == 0 {
		return nil, fmt.Errorf("--range-size must be positive")
	}
	policy := &RangePolicy{Size: length, Align: 1}
	if params.RangeAlign != "" {
		policy.Align, err = sizeparse.Parse(params.RangeAlign)
		if err != nil {
			return nil, fmt.Errorf("--range-align: %w", err)
		}
		if policy.Align == 0 {
			policy.Align = 1
		}
	}
	if params.RangeOffset != "" {
		offset, parseErr := sizeparse.Parse(params.RangeOffset)
		if parseErr != nil {
			return nil, fmt.Errorf("--range-offset: %w", parseErr)
		}
		if offset%policy.Align != 0 {
			return nil, fmt.Errorf("--range-offset must be divisible by the effective --range-align")
		}
		if offset > math.MaxInt64-(length-1) {
			return nil, fmt.Errorf("--range-offset plus --range-size minus one exceeds signed 64-bit range")
		}
		policy.Offset = &offset
	}
	return policy, nil
}
