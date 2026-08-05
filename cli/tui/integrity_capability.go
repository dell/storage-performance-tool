/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// ErrEngineIncompatible indicates the engine cannot run a persisted-data verification workload.
var ErrEngineIncompatible = errors.New("engine is not integrity-capable")

// IncompatibleEngineError explains why a verification run was refused before any object I/O.
// It always names the entry node and what could not be proven, and includes the configured engine
// image when the caller knows it.
type IncompatibleEngineError struct {
	EntryNode    string
	EngineImage  string
	MissingPaths []string
	Reason       string
	Err          error
}

func (e *IncompatibleEngineError) Error() string {
	var b strings.Builder
	b.WriteString("engine at ")
	b.WriteString(e.EntryNode)
	b.WriteString(" is not integrity-capable: ")
	b.WriteString(e.Reason)
	if len(e.MissingPaths) > 0 {
		b.WriteString(" (missing: ")
		b.WriteString(strings.Join(e.MissingPaths, ", "))
		b.WriteString(")")
	}
	if e.EngineImage != "" {
		b.WriteString("; engine image: ")
		b.WriteString(e.EngineImage)
	}
	b.WriteString("; a verification workload requires an engine that declares the storage.integrity configuration")
	return b.String()
}

func (e *IncompatibleEngineError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrEngineIncompatible
}

// Is lets callers match this error with errors.Is(err, ErrEngineIncompatible) regardless of the
// specific reason the engine was rejected.
func (e *IncompatibleEngineError) Is(target error) bool { return target == ErrEngineIncompatible }

// VerifyIntegrityCapability proves, before scenario submission, that the entry node declares every
// configuration path the verification contract depends on.
//
// The engine serves its merged confuse schema at /config/schema. Schema leaves are type descriptors
// rather than runtime values, so this checks path presence only and never interprets leaf contents.
//
// Engine startup rejection and the scenario POST are both unreliable substitutes: startup argument
// handling can continue past an invalid path, and an integrity-unaware engine rejects the generated
// scenario later with an opaque HTTP 400 that cannot be distinguished from any other bad config.
func (c *SptAPIClient) VerifyIntegrityCapability(engineImage string) error {
	return c.VerifyIntegrityCapabilityContext(context.Background(), engineImage)
}

// VerifyIntegrityCapabilityContext binds the schema capability probe to the
// caller's launch lifecycle.
func (c *SptAPIClient) VerifyIntegrityCapabilityContext(ctx context.Context, engineImage string) error {
	url := c.baseURL + constants.SptConfigSchemaEndpoint

	req, err := http.NewRequestWithContext(normalizeContext(ctx), http.MethodGet, url, nil)
	if err != nil {
		return &IncompatibleEngineError{
			EntryNode:   c.baseURL,
			EngineImage: engineImage,
			Reason:      fmt.Sprintf("could not build a request for %s", constants.SptConfigSchemaEndpoint),
			Err:         err,
		}
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &IncompatibleEngineError{
			EntryNode:   c.baseURL,
			EngineImage: engineImage,
			Reason:      fmt.Sprintf("the configuration schema endpoint %s is unreachable", constants.SptConfigSchemaEndpoint),
			Err:         err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return &IncompatibleEngineError{
			EntryNode:   c.baseURL,
			EngineImage: engineImage,
			Reason:      "the configuration schema response could not be read",
			Err:         err,
		}
	}

	if resp.StatusCode != http.StatusOK {
		return &IncompatibleEngineError{
			EntryNode:   c.baseURL,
			EngineImage: engineImage,
			Reason: fmt.Sprintf("the configuration schema endpoint returned HTTP %d (an engine without %s predates this capability)",
				resp.StatusCode, constants.SptConfigSchemaEndpoint),
		}
	}

	var schema map[string]any
	if err := json.Unmarshal(bodyData, &schema); err != nil {
		return &IncompatibleEngineError{
			EntryNode:   c.baseURL,
			EngineImage: engineImage,
			Reason:      "the configuration schema response was not valid JSON",
			Err:         err,
		}
	}

	missing := missingSchemaPaths(schema, constants.RequiredIntegritySchemaPaths)
	if len(missing) > 0 {
		return &IncompatibleEngineError{
			EntryNode:    c.baseURL,
			EngineImage:  engineImage,
			MissingPaths: missing,
			Reason:       "the configuration schema does not declare the required integrity paths",
		}
	}

	logging.LogDebug("spt-api", "engine integrity capability confirmed",
		"url", url,
		"paths", len(constants.RequiredIntegritySchemaPaths))
	return nil
}

// missingSchemaPaths reports which dotted paths are absent from the nested schema document, in the
// order they were requested. A path is present when every segment resolves; the leaf may hold any
// value, including a type-descriptor string or null.
func missingSchemaPaths(schema map[string]any, paths []string) []string {
	var missing []string
	for _, path := range paths {
		if !schemaPathPresent(schema, path) {
			missing = append(missing, path)
		}
	}
	return missing
}

func schemaPathPresent(schema map[string]any, path string) bool {
	segments := strings.Split(path, ".")
	node := schema
	for i, segment := range segments {
		value, ok := node[segment]
		if !ok {
			return false
		}
		if i == len(segments)-1 {
			return true
		}
		child, ok := value.(map[string]any)
		if !ok {
			// An intermediate segment resolved to a leaf, so the remaining path cannot exist.
			return false
		}
		node = child
	}
	return false
}
