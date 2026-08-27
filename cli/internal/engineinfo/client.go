// Package engineinfo consumes engine-owned build information without deriving
// identity from configuration, images, or the CLI build.
package engineinfo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// CollectionStatus is the result of querying one engine participant.
type CollectionStatus string

// Stable participant collection states shared with fleet aggregation.
const (
	StatusCollected                 CollectionStatus = "collected"
	StatusLegacyEndpointUnavailable CollectionStatus = "legacy_endpoint_unavailable"
	StatusUnsupportedSchema         CollectionStatus = "unsupported_schema"
	StatusIncompleteBuildInfo       CollectionStatus = "incomplete_build_info"
	StatusCollectionFailed          CollectionStatus = "collection_failed"
)

// BuildInformation is the stable schema-1 engine-owned build record.
type BuildInformation struct {
	SchemaVersion int
	Product       string
	Version       string
	Revision      string
	BuildTime     string
	Development   bool
	SourceDirty   *bool
}

// CollectionResult contains only approved, safe participant evidence.
type CollectionResult struct {
	Status                CollectionStatus
	ReportedSchemaVersion int
	Build                 *BuildInformation
	Complete              bool
	Reason                string
	Attempts              int
}

// Client fetches Engine Build Information with bounded I/O and retries.
type Client struct {
	httpClient       *http.Client
	requestAttempts  int
	requestTimeout   time.Duration
	retryDelay       time.Duration
	responseMaxBytes int64
}

// ClientOptions supplies deterministic bounds and an optional HTTP transport.
// Zero values select the production defaults.
type ClientOptions struct {
	HTTPClient       *http.Client
	RequestAttempts  int
	RequestTimeout   time.Duration
	RetryDelay       time.Duration
	ResponseMaxBytes int64
}

// NewClient returns a client with production request bounds.
func NewClient() *Client {
	return NewClientWithOptions(ClientOptions{})
}

// NewClientWithOptions returns a client with explicit test or adapter bounds.
func NewClientWithOptions(options ClientOptions) *Client {
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	httpClientCopy := *httpClient
	httpClientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	attempts := options.RequestAttempts
	if attempts <= 0 {
		attempts = constants.EngineVersionRequestAttempts
	}
	requestTimeout := options.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = constants.EngineVersionRequestTimeout
	}
	retryDelay := options.RetryDelay
	if retryDelay <= 0 {
		retryDelay = constants.EngineVersionRetryDelay
	}
	maxBytes := options.ResponseMaxBytes
	if maxBytes <= 0 {
		maxBytes = constants.EngineVersionResponseMaxBytes
	}
	return &Client{
		httpClient:       &httpClientCopy,
		requestAttempts:  attempts,
		requestTimeout:   requestTimeout,
		retryDelay:       retryDelay,
		responseMaxBytes: maxBytes,
	}
}

// Fetch requests one participant's immutable Engine Build Information.
func (c *Client) Fetch(ctx context.Context, baseURL string) (CollectionResult, error) {
	endpoint := strings.TrimRight(baseURL, "/") + constants.EngineVersionEndpoint
	for attempt := 1; attempt <= c.requestAttempts; attempt++ {
		result, retry, err := c.fetchOnce(ctx, endpoint)
		if err == nil || !retry {
			result.Attempts = attempt
			return result, err
		}
		if ctx.Err() != nil {
			result, cancelErr := canceled(ctx.Err())
			result.Attempts = attempt
			return result, cancelErr
		}
		if attempt == c.requestAttempts {
			result, fetchErr := failed(fmt.Sprintf("engine version request failed after %d attempts", c.requestAttempts))
			result.Attempts = attempt
			return result, fetchErr
		}
		timer := time.NewTimer(c.retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			result, cancelErr := canceled(ctx.Err())
			result.Attempts = attempt
			return result, cancelErr
		case <-timer.C:
		}
	}
	result, err := failed("engine version request failed")
	result.Attempts = c.requestAttempts
	return result, err
}

func (c *Client) fetchOnce(ctx context.Context, endpoint string) (CollectionResult, bool, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		result, fetchErr := failed("engine version request could not be created")
		return result, false, fetchErr
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		if ctx.Err() != nil {
			result, fetchErr := canceled(ctx.Err())
			return result, false, fetchErr
		}
		result, fetchErr := failed("engine version request failed")
		return result, true, fetchErr
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed {
		return CollectionResult{
			Status: StatusLegacyEndpointUnavailable,
			Reason: "engine version endpoint is unavailable on this engine",
		}, false, nil
	}
	if response.StatusCode == http.StatusTooManyRequests ||
		(response.StatusCode >= http.StatusInternalServerError && response.StatusCode <= 599) {
		result, fetchErr := failed("engine version endpoint returned a transient HTTP status")
		return result, true, fetchErr
	}
	if response.StatusCode != http.StatusOK {
		result, fetchErr := failed("engine version endpoint returned an unexpected HTTP status")
		return result, false, fetchErr
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		result, fetchErr := failed("engine version endpoint returned an invalid content type")
		return result, false, fetchErr
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, c.responseMaxBytes+1))
	if err != nil {
		result, fetchErr := failed("engine version response could not be read")
		return result, true, fetchErr
	}
	if int64(len(body)) > c.responseMaxBytes {
		result, fetchErr := failed("engine version response exceeds the size limit")
		return result, false, fetchErr
	}

	fields, err := decodeJSONObject(body)
	if err != nil {
		result, fetchErr := failed("engine version response is malformed JSON")
		return result, false, fetchErr
	}
	schemaJSON, ok := fields["schema_version"]
	if !ok {
		result, fetchErr := failed("engine version response is missing schema_version")
		return result, false, fetchErr
	}
	var schemaVersion int
	if err := json.Unmarshal(schemaJSON, &schemaVersion); err != nil {
		result, fetchErr := failed("engine version response has an invalid schema_version")
		return result, false, fetchErr
	}
	if schemaVersion > constants.EngineBuildInfoSchemaVersion {
		return CollectionResult{
			Status:                StatusUnsupportedSchema,
			ReportedSchemaVersion: schemaVersion,
			Reason:                "engine version schema is newer than this CLI supports",
		}, false, nil
	}
	if schemaVersion != constants.EngineBuildInfoSchemaVersion {
		result, fetchErr := failed("engine version response has an invalid schema_version")
		return result, false, fetchErr
	}

	var wire schemaOneResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		result, fetchErr := failedSchemaOne("engine version response violates schema 1")
		return result, false, fetchErr
	}
	sourceDirty, err := decodeSourceDirty(wire.SourceDirty)
	if err != nil {
		result, fetchErr := failedSchemaOne("engine version response has invalid source_dirty")
		return result, false, fetchErr
	}
	if reason := validateSchemaOne(wire, sourceDirty); reason != "" {
		result, fetchErr := failedSchemaOne(reason)
		return result, false, fetchErr
	}
	build := BuildInformation{
		SchemaVersion: *wire.SchemaVersion,
		Product:       *wire.Product,
		Version:       *wire.Version,
		Revision:      *wire.Revision,
		BuildTime:     *wire.BuildTime,
		Development:   *wire.Development,
		SourceDirty:   sourceDirty,
	}
	status := StatusCollected
	complete := true
	reason := ""
	if build.Version == constants.EngineBuildInfoUnknown || build.Revision == constants.EngineBuildInfoUnknown || build.SourceDirty == nil {
		status = StatusIncompleteBuildInfo
		complete = false
		reason = "engine build comparison fields are incomplete"
	}
	return CollectionResult{
		Status:                status,
		ReportedSchemaVersion: build.SchemaVersion,
		Build:                 &build,
		Complete:              complete,
		Reason:                reason,
	}, false, nil
}

type schemaOneResponse struct {
	SchemaVersion *int            `json:"schema_version"`
	Product       *string         `json:"product"`
	Version       *string         `json:"version"`
	Revision      *string         `json:"revision"`
	BuildTime     *string         `json:"build_time"`
	Development   *bool           `json:"development"`
	SourceDirty   json.RawMessage `json:"source_dirty"`
}

var (
	revisionPattern = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)
	semverPattern   = regexp.MustCompile(`^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)
)

func failed(reason string) (CollectionResult, error) {
	return CollectionResult{Status: StatusCollectionFailed, Reason: reason}, fmt.Errorf("%s", reason)
}

func failedSchemaOne(reason string) (CollectionResult, error) {
	return CollectionResult{
		Status:                StatusCollectionFailed,
		ReportedSchemaVersion: constants.EngineBuildInfoSchemaVersion,
		Reason:                reason,
	}, fmt.Errorf("%s", reason)
}

func canceled(cause error) (CollectionResult, error) {
	const reason = "engine version collection canceled"
	return CollectionResult{Status: StatusCollectionFailed, Reason: reason}, fmt.Errorf("%s: %w", reason, cause)
}

func validateSchemaOne(wire schemaOneResponse, sourceDirty *bool) string {
	if wire.SchemaVersion == nil || *wire.SchemaVersion != constants.EngineBuildInfoSchemaVersion {
		return "engine version response has invalid schema_version"
	}
	if wire.Product == nil || *wire.Product != constants.EngineBuildInfoProduct {
		return "engine version response has invalid product"
	}
	if wire.Version == nil || !validVersion(*wire.Version) {
		return "engine version response has invalid version"
	}
	if wire.Revision == nil || (*wire.Revision != constants.EngineBuildInfoUnknown && !revisionPattern.MatchString(*wire.Revision)) {
		return "engine version response has invalid revision"
	}
	if wire.BuildTime == nil || !validBuildTime(*wire.BuildTime) {
		return "engine version response has invalid build_time"
	}
	if wire.Development == nil {
		return "engine version response has invalid development"
	}
	if !*wire.Development &&
		(*wire.Version == constants.EngineBuildInfoUnknown ||
			*wire.Revision == constants.EngineBuildInfoUnknown ||
			*wire.BuildTime == constants.EngineBuildInfoUnknown ||
			sourceDirty == nil || *sourceDirty) {
		return "engine version response has incomplete release information"
	}
	return ""
}

func decodeSourceDirty(raw json.RawMessage) (*bool, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("source_dirty is missing")
	}
	if bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func decodeJSONObject(body []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, fmt.Errorf("JSON value is not an object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := nameToken.(string)
		if !ok {
			return nil, fmt.Errorf("JSON object field name is invalid")
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("JSON object contains a duplicate field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("JSON object has trailing content")
	}
	return fields, nil
}

func validBuildTime(value string) bool {
	if value == constants.EngineBuildInfoUnknown {
		return true
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return false
	}
	_, offset := parsed.Zone()
	return offset == 0
}

func validVersion(value string) bool {
	if value == constants.EngineBuildInfoUnknown {
		return true
	}
	if !semverPattern.MatchString(value) {
		return false
	}
	withoutBuild, _, _ := strings.Cut(value, "+")
	_, prerelease, hasPrerelease := strings.Cut(withoutBuild, "-")
	if !hasPrerelease {
		return true
	}
	for _, identifier := range strings.Split(prerelease, ".") {
		if len(identifier) > 1 && identifier[0] == '0' && onlyDigits(identifier) {
			return false
		}
	}
	return true
}

func onlyDigits(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
