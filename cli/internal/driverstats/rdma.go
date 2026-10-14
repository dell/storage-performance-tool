package driverstats

// DriverS3RDMA is the driver name the S3-RDMA storage driver reports.
const DriverS3RDMA = "s3-rdma"

// Counters of the S3-RDMA driver that the run summary reads. Its other statistics are stored
// as reported and not interpreted by the CLI.
const (
	rdmaRequestsSent           = "rdma_requests_sent"
	rdmaTransferred            = "rdma_transferred"
	rdmaDeclinedServedOverHTTP = "rdma_declined_served_over_http"
	rdmaDeclinedFailed         = "rdma_declined_failed"
	rdmaRequestFailed          = "rdma_request_failed"
	rdmaProtocolError          = "rdma_protocol_error"
	rdmaTimedOut               = "rdma_timed_out"
	httpRDMAUnavailable        = "http_rdma_unavailable"
	httpBelowThreshold         = "http_below_threshold"
	httpIneligible             = "http_ineligible"
	httpOversize               = "http_oversize"
	httpFallback               = "http_fallback"
	prepareFailed              = "prepare_failed"
	registrationFailedPooled   = "registration_failed_pooled"
	registrationFailedOnDemand = "registration_failed_on_demand"
	allocationFailedPooled     = "allocation_failed_pooled"
	allocationFailedOnDemand   = "allocation_failed_on_demand"
	tokenGenerationFailed      = "token_generation_failed"
)

// rdmaOutcomes are the counters of which every RDMA request sent ends in at most one.
var rdmaOutcomes = []string{
	rdmaTransferred, rdmaDeclinedServedOverHTTP, rdmaDeclinedFailed,
	rdmaRequestFailed, rdmaProtocolError, rdmaTimedOut,
}

// rdmaOverHTTP are the counters of payload requests whose payload went over HTTP.
var rdmaOverHTTP = []string{
	httpRDMAUnavailable, httpBelowThreshold, httpIneligible, httpOversize, httpFallback,
	rdmaDeclinedServedOverHTTP,
}

// rdmaProblems are the counters the summary reports when they are not zero, in report order.
var rdmaProblems = []struct{ counter, what string }{
	{rdmaDeclinedFailed, "RDMA requests declined by the server and failed"},
	{rdmaRequestFailed, "RDMA requests ended in failure"},
	{rdmaProtocolError, "RDMA replies broke the reply contract"},
	{rdmaTimedOut, "RDMA requests timed out"},
	{prepareFailed, "attempts failed in RDMA preparation"},
	{registrationFailedPooled, "pooled buffer registrations failed"},
	{registrationFailedOnDemand, "per-operation buffer registrations failed"},
	{allocationFailedPooled, "pooled buffer allocations failed"},
	{allocationFailedOnDemand, "per-operation buffer allocations failed"},
	{tokenGenerationFailed, "RDMA token generations failed"},
}

// rdmaWithoutOutcome describes RDMA requests sent that none of the outcome counters covers.
const rdmaWithoutOutcome = "RDMA requests had no outcome when the step ended"

// RDMADataPath is what the run summary shows of the S3-RDMA driver's statistics. Every number
// counts requests or events, not operations and not data: a retry and each part of a multipart
// upload count separately.
type RDMADataPath struct {
	// Transferred counts the requests whose payload the server reported as moved over RDMA.
	Transferred int64
	// OverHTTP counts the payload requests whose payload went over HTTP, by selection, by
	// fallback, or because the server declined RDMA and sent the payload in the HTTP body.
	OverHTTP int64
	// Problems holds the problem counts that are not zero, in a fixed order.
	Problems []RDMAProblem
}

// RDMAProblem is one problem count and the words that follow it in a sentence.
type RDMAProblem struct {
	Count int64
	What  string
}

// Reported reports whether the step sent payload requests or met a problem. A step that only
// deleted or listed objects has nothing to report.
func (p RDMADataPath) Reported() bool {
	return p.Transferred > 0 || p.OverHTTP > 0 || len(p.Problems) > 0
}

// RDMADataPath returns the summary's view of S3-RDMA statistics. ok is false when the
// statistics are absent or belong to another driver.
func (s *Stats) RDMADataPath() (path RDMADataPath, ok bool) {
	if s == nil || s.Driver != DriverS3RDMA {
		return RDMADataPath{}, false
	}
	path.Transferred = s.Counters[rdmaTransferred]
	path.OverHTTP = s.sum(rdmaOverHTTP)
	for _, problem := range rdmaProblems {
		if count := s.Counters[problem.counter]; count > 0 {
			path.Problems = append(path.Problems, RDMAProblem{Count: count, What: problem.what})
		}
	}
	// A request is counted as sent once it was accepted, and its outcome can be counted first:
	// outcomes ahead of the requests sent are not a negative number of requests.
	if withoutOutcome := s.Counters[rdmaRequestsSent] - s.sum(rdmaOutcomes); withoutOutcome > 0 {
		path.Problems = append(path.Problems, RDMAProblem{Count: withoutOutcome, What: rdmaWithoutOutcome})
	}
	return path, true
}

func (s *Stats) sum(counters []string) int64 {
	var total int64
	for _, name := range counters {
		total += s.Counters[name]
	}
	return total
}
