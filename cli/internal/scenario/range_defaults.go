package scenario

import (
	"fmt"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"gopkg.in/yaml.v3"
	"strconv"
	"strings"
)

// Validate the merged tree, including parent-map overrides and last-write-wins values.
// Error messages name configuration paths without echoing potentially sensitive values.
func validateRangeDefaults(params Params, data []byte) error {
	policy, err := ParseRangePolicy(params)
	if err != nil {
		return err
	}
	if policy == nil {
		return nil
	}
	var root map[string]any
	if err = yaml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("invalid partial READ defaults: %w", err)
	}
	get := func(path string) (any, error) {
		var value any = root
		for _, key := range strings.Split(path, ".") {
			if value == nil {
				return nil, nil
			}
			node, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("partial READ requires an object along %s", path)
			}
			value = node[key]
		}
		return value, nil
	}
	for _, leaf := range []string{"size", "offset", "align"} {
		value, e := get("load.op.read.range." + leaf)
		if e != nil {
			return e
		}
		if value != nil {
			return fmt.Errorf("use --range-size/offset/align instead of global load.op.read.range overrides, which also affect seed and cleanup")
		}
	}
	for _, path := range []string{"item.data.verify", "load.op.recycle.content.update", "storage.net.http.read.metadata.only", "storage.object.tagging.enabled"} {
		value, e := get(path)
		if e != nil {
			return e
		}
		if value == nil {
			continue
		}
		enabled, ok := value.(bool)
		if !ok || enabled {
			return fmt.Errorf("partial READ requires %s=false", path)
		}
	}
	for path, want := range map[string]string{"item.type": "data", "storage.integrity.mode": "none", "storage.driver.type": "s3"} {
		value, e := get(path)
		if e != nil {
			return e
		}
		if value != nil && !strings.EqualFold(fmt.Sprint(value), want) {
			return fmt.Errorf("partial READ requires %s=%s", path, want)
		}
	}
	for _, path := range []string{"item.data.ranges.threshold", "item.data.ranges.random"} {
		value, e := get(path)
		if e != nil {
			return e
		}
		if value == nil {
			continue
		}
		n, e := sizeparse.Parse(fmt.Sprint(value))
		if path == "item.data.ranges.random" {
			n, e = strconv.ParseInt(fmt.Sprint(value), 10, 64)
		}
		if e != nil || n != 0 {
			return fmt.Errorf("partial READ conflicts with active or invalid %s", path)
		}
	}
	fixed, e := get("item.data.ranges.fixed")
	if e != nil {
		return e
	}
	if fixed != nil {
		values, ok := fixed.([]any)
		if !ok || len(values) != 0 {
			return fmt.Errorf("partial READ conflicts with active or invalid item.data.ranges.fixed")
		}
	}
	timeout, e := get("storage.net.timeoutMilliSec")
	if e != nil {
		return e
	}
	if timeout != nil {
		n, e := strconv.ParseInt(fmt.Sprint(timeout), 10, 64)
		if e != nil || n <= 0 {
			return fmt.Errorf("partial READ requires positive storage.net.timeoutMilliSec")
		}
	}
	return nil
}
