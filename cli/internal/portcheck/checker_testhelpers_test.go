package portcheck

import "net/http"

// isSptResponse is a test-only helper to validate endpoint semantics.
func (p *PortChecker) isSptResponse(resp *http.Response, endpoint string) bool {
	switch endpoint {
	case endpointMetrics:
		return resp.StatusCode == http.StatusOK
	case endpointRun:
		return resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound
	case "/":
		return false
	}
	return false
}
