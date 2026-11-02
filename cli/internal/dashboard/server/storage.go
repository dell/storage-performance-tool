package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

const (
	maxHosts        = 8
	endpointTimeout = 5 * time.Second
	localHost       = "127.0.0.1"
)

var (
	userPattern      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,31}$`)
	hostLabelPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
)

// storageConfig is the part of the target the browser may set. The bucket and
// credentials stay in the host's .env files.
type storageConfig struct {
	Hosts    []string `json:"hosts"`
	Endpoint string   `json:"endpoint"`
}

// verificationRunner keeps the dashboard's report handling independent of how
// node checks are executed. A nil app runner uses the real CLI verifier.
type verificationRunner func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report

func runHostVerification(hosts []*hostparse.HostInfo, cfg verifyapi.Config) *verifyapi.Report {
	return verifyapi.NewVerifier(hosts, cfg).VerifyAll()
}

type verifyCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type verifyNode struct {
	Host    string        `json:"host"`
	Status  string        `json:"status"`
	Message string        `json:"message"`
	Checks  []verifyCheck `json:"checks"`
}

type verifyEndpoint struct {
	Label   string `json:"label"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type verification struct {
	Status     string         `json:"status"`
	Signature  string         `json:"config_signature,omitempty"`
	StartedAt  string         `json:"started_at,omitempty"`
	FinishedAt string         `json:"finished_at,omitempty"`
	Summary    string         `json:"summary"`
	Nodes      []verifyNode   `json:"nodes"`
	Endpoint   verifyEndpoint `json:"endpoint"`
}

func (n verifyNode) clone() verifyNode {
	n.Checks = append([]verifyCheck{}, n.Checks...)
	return n
}

func (v verification) clone() verification {
	nodes := make([]verifyNode, len(v.Nodes))
	for i, n := range v.Nodes {
		nodes[i] = n.clone()
	}
	v.Nodes = nodes
	return v
}

// hostAddress returns the address part of a [user@]host entry, as SPT reports it.
func hostAddress(entry string) string {
	host := entry
	if i := strings.LastIndex(entry, "@"); i >= 0 {
		host = entry[i+1:]
	}
	if host == "localhost" {
		return localHost
	}
	return host
}

// validateHostEntry mirrors SPT's own --test-hosts rules. They also guarantee an
// entry can never start with "-" and be read as a command-line option.
func validateHostEntry(entry string) error {
	host := entry
	if i := strings.LastIndex(entry, "@"); i >= 0 {
		if !userPattern.MatchString(entry[:i]) {
			return errors.New("the user name is not valid")
		}
		host = entry[i+1:]
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if len(host) == 0 || len(host) > 253 {
		return errors.New("the host name is not valid")
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if !hostLabelPattern.MatchString(label) {
			return errors.New("the host name is not valid")
		}
	}
	return nil
}

func validateEndpoint(candidate string) (endpoint, label string, ok bool) {
	u, err := url.Parse(candidate)
	if err != nil || (strings.ToLower(u.Scheme) != "http" && strings.ToLower(u.Scheme) != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(candidate, " \t\r\n") {
		return "", "", false
	}
	return candidate, u.Host, true
}

func validateStorage(c storageConfig) (storageConfig, error) {
	out := storageConfig{Hosts: []string{}, Endpoint: strings.TrimSpace(c.Endpoint)}
	seen := map[string]bool{}
	for _, raw := range c.Hosts {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if len(out.Hosts) == maxHosts {
			return out, userErrorf("List at most %d client machines.", maxHosts)
		}
		if err := validateHostEntry(entry); err != nil {
			return out, userErrorf("Client machine %q cannot be used: %v. Enter a host or user@host.", entry, err)
		}
		key := strings.ToLower(hostAddress(entry))
		if seen[key] {
			return out, userErrorf("Client machine %q is listed more than once.", hostAddress(entry))
		}
		seen[key] = true
		out.Hosts = append(out.Hosts, entry)
	}
	if len(out.Hosts) == 0 {
		return out, userError("Add at least one client machine.")
	}
	if out.Endpoint != "" {
		if _, _, ok := validateEndpoint(out.Endpoint); !ok {
			return out, userError("The S3 endpoint must be a valid HTTP or HTTPS URL without embedded credentials.")
		}
	}
	return out, nil
}

func configSignature(hosts []string, endpoint string) string {
	sum := sha256.Sum256([]byte(strings.Join(hosts, ",") + "\n" + endpoint))
	return hex.EncodeToString(sum[:])
}

func writeFileAtomic(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, b, 0600); e != nil {
		return e
	}
	if e = os.Chmod(tmp, 0600); e != nil {
		return e
	}
	return os.Rename(tmp, path)
}

func envList(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for k, v := range values {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// configValues merges the host's .env files with the process environment, which wins.
func (a *app) configValues() map[string]string {
	values := map[string]string{}
	for _, path := range []string{filepath.Join(a.home, ".env"), filepath.Join(a.root, ".env")} {
		for k, v := range readDotenv(path, values) {
			values[k] = v
		}
	}
	for _, item := range os.Environ() {
		if k, v, ok := strings.Cut(item, "="); ok {
			values[k] = v
		}
	}
	return values
}

func (a *app) storageSnapshot() storageConfig {
	a.storageMu.Lock()
	defer a.storageMu.Unlock()
	hosts := append([]string{}, a.storage.Hosts...)
	if len(hosts) == 0 {
		hosts = []string{localHost}
	}
	return storageConfig{Hosts: hosts, Endpoint: a.storage.Endpoint}
}

// loadStorage restores the saved client machines, endpoint, and last verification.
func (a *app) loadStorage() {
	a.storageMu.Lock()
	defer a.storageMu.Unlock()
	if b, e := os.ReadFile(a.storageFile); e == nil {
		var saved storageConfig
		if json.Unmarshal(b, &saved) == nil {
			if cfg, err := validateStorage(saved); err == nil {
				a.storage = cfg
			}
		}
	}
	if b, e := os.ReadFile(a.verifyFile); e == nil {
		var saved verification
		if json.Unmarshal(b, &saved) == nil {
			if saved.Status == verifyRunning {
				saved.Status = verifyFailed
				saved.Summary = "The dashboard restarted before verification finished."
			}
			a.verification = saved
		}
	}
}

func (a *app) updateVerification(change func(*verification)) {
	a.storageMu.Lock()
	defer a.storageMu.Unlock()
	change(&a.verification)
}

// currentVerification returns the last verification, or an idle one when it no
// longer describes the configuration a run would use.
func (a *app) currentVerification(target map[string]any) verification {
	hosts, _ := target["hosts"].([]string)
	a.storageMu.Lock()
	v := a.verification.clone()
	a.storageMu.Unlock()
	if v.Status == "" || v.Signature != configSignature(hosts, str(target["endpoint"])) {
		return verification{Status: "idle", Nodes: []verifyNode{}}
	}
	return v
}

func (a *app) verified(target map[string]any) bool {
	return a.currentVerification(target).Status == verifyPassed
}

// nodesVerified reports whether every client machine passed the last verification
// of the current configuration, ignoring the S3 endpoint check (mock runs need no S3).
func (a *app) nodesVerified(target map[string]any) bool {
	v := a.currentVerification(target)
	if v.Status == "idle" || v.Status == verifyRunning || len(v.Nodes) == 0 {
		return false
	}
	for _, n := range v.Nodes {
		if n.Status != verifyPassed {
			return false
		}
	}
	return true
}

func (a *app) storageState(target map[string]any) map[string]any {
	cfg := a.storageSnapshot()
	return map[string]any{"hosts": cfg.Hosts, "endpoint": cfg.Endpoint, "max_hosts": maxHosts, "verification": a.currentVerification(target)}
}

func targetHosts(target map[string]any) string {
	hosts, _ := target["hosts"].([]string)
	if len(hosts) == 0 {
		return localHost
	}
	return strings.Join(hosts, ",")
}

// metricsEndpoint is SPT's live metrics API, served by the entry node (the first client machine).
func (a *app) metricsEndpoint() string {
	entry := hostAddress(a.storageSnapshot().Hosts[0])
	return "http://" + net.JoinHostPort(entry, strconv.Itoa(constants.SptAPIPortInt)) + "/metrics/json"
}

func (a *app) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		writeJSON(w, 404, map[string]any{keyError: msgEndpointNotFound})
		return
	}
	if !a.sameOrigin(r) {
		writeJSON(w, 403, map[string]any{keyError: "Verification requests must come from this dashboard through its SSH tunnel."})
		return
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		writeJSON(w, 415, map[string]any{keyError: "Send JSON storage settings."})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var requested storageConfig
	if err := dec.Decode(&requested); err != nil {
		writeJSON(w, 400, map[string]any{keyError: "Send the client machines and an optional S3 endpoint."})
		return
	}
	if err := ensureEOF(dec); err != nil {
		writeJSON(w, 400, map[string]any{keyError: "Send one JSON storage settings object."})
		return
	}
	cfg, err := validateStorage(requested)
	if err != nil {
		writeJSON(w, 400, map[string]any{keyError: err.Error()})
		return
	}
	if !executable(a.sptBin) {
		writeJSON(w, 503, map[string]any{keyError: "SPT is not executable on this VM."})
		return
	}
	// Verification starts SPT containers on the ports a run uses, so the two never overlap.
	a.mu.Lock()
	busy := a.stopping || a.verifying
	if !busy {
		lock, lockErr := a.lockRun()
		busy = lockErr != nil
		unlockClose(lock)
	}
	if busy {
		a.mu.Unlock()
		writeJSON(w, 409, map[string]any{keyError: "A run or verification is already in progress. Wait for it to finish first."})
		return
	}
	a.verifying = true
	a.processes.Add(1)
	a.mu.Unlock()

	if err := writeFileAtomic(a.storageFile, cfg); err != nil {
		a.processes.Done()
		a.mu.Lock()
		a.verifying = false
		a.mu.Unlock()
		writeJSON(w, 500, map[string]any{keyError: "Could not save the storage configuration."})
		return
	}
	a.storageMu.Lock()
	a.storage = cfg
	a.storageMu.Unlock()

	target := a.targetConfiguration()
	endpoint := str(target["endpoint"])
	nodes := make([]verifyNode, len(cfg.Hosts))
	for i, host := range cfg.Hosts {
		nodes[i] = verifyNode{Host: host, Status: verifyPending, Checks: []verifyCheck{}}
	}
	a.updateVerification(func(v *verification) {
		*v = verification{Status: verifyRunning, Signature: configSignature(cfg.Hosts, endpoint), StartedAt: utcNow(), Nodes: nodes,
			Endpoint: verifyEndpoint{Label: str(target["endpoint_label"]), Status: verifyPending}}
	})
	go a.runVerification(cfg.Hosts, endpoint)
	writeJSON(w, 202, map[string]any{"storage": a.storageState(target)})
}

func (a *app) runVerification(hosts []string, endpoint string) {
	defer a.processes.Done()
	defer func() {
		a.mu.Lock()
		a.verifying = false
		a.mu.Unlock()
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		status, message := verifyFailed, "No valid S3 endpoint is configured."
		if endpoint != "" {
			status, message = checkEndpoint(a.endpointClient, endpoint)
		}
		a.updateVerification(func(v *verification) {
			v.Endpoint.Status = status
			v.Endpoint.Message = message
		})
	}()
	summary, err := a.runVerificationAPI(hosts)
	wg.Wait()

	a.updateVerification(func(v *verification) {
		passed := err == nil && v.Endpoint.Status == verifyPassed
		for i := range v.Nodes {
			if v.Nodes[i].Status == verifyPending {
				v.Nodes[i].Status = verifyFailed
				v.Nodes[i].Message = "SPT did not report a result for this machine."
			}
			if v.Nodes[i].Status != verifyPassed {
				passed = false
			}
		}
		v.Status = verifyFailed
		v.Summary = firstNonempty(summary, "Verification did not pass.")
		if err != nil && summary == "" {
			v.Summary = "SPT verification could not complete: " + err.Error()
		}
		if passed {
			v.Status = verifyPassed
			v.Summary = "All client machines passed SPT verification and the S3 endpoint is reachable."
		}
		v.FinishedAt = utcNow()
		_ = writeFileAtomic(a.verifyFile, v)
	})
}

// runVerificationAPI consumes the CLI's structured verifier results directly.
func (a *app) runVerificationAPI(hosts []string) (string, error) {
	parsed, err := hostparse.ParseTestHosts(strings.Join(hosts, ","))
	if err != nil {
		return "", err
	}
	a.mu.Lock()
	stopping := a.stopping
	a.mu.Unlock()
	if stopping {
		return "", context.Canceled
	}
	run := a.verifyNodes
	if run == nil {
		run = runHostVerification
	}
	report := run(parsed, verifyapi.Config{
		NetworkMode:  constants.DefaultNetworkMode,
		APIPort:      constants.SptAPIPortInt,
		RMIPortStart: constants.DefaultRMIPortStart,
		RMIPortCount: constants.DefaultRMIPortCount,
		MinHosts:     len(parsed),
		ShowProgress: false,
	})
	index := make(map[string]int, len(hosts))
	for i, host := range hosts {
		index[strings.ToLower(hostAddress(host))] = i
	}
	for host, result := range report.Results {
		i, ok := index[strings.ToLower(host)]
		if !ok || i >= len(hosts) {
			continue
		}
		node := verifyNode{Host: hosts[i], Status: verifyFailed, Message: result.Overall.Message, Checks: []verifyCheck{}}
		if result.Overall.Passed {
			node.Status = verifyPassed
		}
		checks := []struct {
			name  string
			check verifyapi.Check
		}{
			{"SSH connectivity", result.SSHConnectivity}, {"Clock skew", result.ClockSkew},
			{"Docker available", result.DockerAvailable}, {"Docker image", result.DockerImagePull},
			{"Container start", result.ContainerStart}, {"RDMA device", result.RdmaDevice},
			{"RDMA driver", result.RdmaDriver}, {"RDMA readiness", result.RdmaReadiness},
			{"Ports accessible", result.PortsAccessible}, {"Metrics endpoint", result.MetricsEndpoint},
			{"Control endpoint", result.ControlEndpoint}, {"Container cleanup", result.ContainerCleanup},
		}
		for _, item := range checks {
			if item.check.Message == "" {
				continue
			}
			status := "fail"
			if item.check.Passed {
				status = "pass"
			}
			if item.check.Warning {
				status = "warn"
			}
			node.Checks = append(node.Checks, verifyCheck{Name: item.name, Status: status, Message: item.check.Message})
		}
		a.updateVerification(func(v *verification) { v.Nodes[i] = node })
	}
	if report.Ready {
		return report.Summary, nil
	}
	return report.Summary, errors.New("one or more client machines did not pass verification")
}

// checkEndpoint reports whether the S3 endpoint answers HTTP(S) from the dashboard
// host. Any HTTP response counts, as does a TLS handshake that fails only on
// certificate trust; it does not test credentials or the bucket.
func checkEndpoint(client *http.Client, endpoint string) (status, message string) {
	c := http.Client{}
	if client != nil {
		c = *client
	}
	c.Timeout = endpointTimeout
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return verifyFailed, "The S3 endpoint is not a valid URL."
	}
	resp, err := c.Do(req)
	if err != nil {
		// The server answered and presented a certificate, so it is reachable.
		// Lab storage commonly uses a self-signed or privately issued one.
		var certErr *tls.CertificateVerificationError
		if errors.As(err, &certErr) {
			return verifyPassed, "Reachable from the dashboard host. Its certificate is not from an authority this host trusts."
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return verifyFailed, "Not reachable from the dashboard host: " + err.Error()
	}
	_ = resp.Body.Close()
	return verifyPassed, fmt.Sprintf("Reachable from the dashboard host (HTTP %d).", resp.StatusCode)
}
