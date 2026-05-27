package remoteip

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	cmdpkg "github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// Legacy shell command candidates kept for test compatibility and as a final fallback.
// These mirror the logic proven in tools/nodes-up.sh.
const (
	shellLoginFlag = "-lc"
	routeCmd       = "ip -o route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if ($i==\"src\"){print $(i+1); exit}}'"
	ifaceCmd       = "ip -br -4 addr show scope global 2>/dev/null | awk '$1!~/(docker|br|veth|lo)/{print $3}' | cut -d/ -f1 | head -n1"
	hostIPCmd      = "hostname -I 2>/dev/null | awk '{print $1}'"
	getentCmd      = "getent hosts \"$(hostname -f)\" 2>/dev/null | awk '{print $1}' | head -n1"
)

// DetectAdvertisedIP determines a stable, routable IPv4 address on the target host
// suitable for advertising via java.rmi.server.hostname.
//
// It executes simple, shell-free commands via the executor and parses their output
// locally to avoid quoting/compatibility pitfalls across distros.
func DetectAdvertisedIP(ctx context.Context, exec cmdpkg.CommandExecutor, host *hostparse.HostInfo) (string, error) {
	if exec == nil {
		return "", fmt.Errorf("executor cannot be nil")
	}
	if host == nil {
		return "", fmt.Errorf("host cannot be nil")
	}

	// 0) Per-host override via env ADVERTISED_IPS="hostA=10.0.0.11,hostB=10.0.0.12"
	if ip := overrideForHost(host); ip != "" {
		logging.LogInfo("remoteip", "using override for advertised IP", "host", host.Original, "ip", ip)
		return ip, nil
	}

	// 1) Default-route source IP: ip -o route get 1.1.1.1
	if out, errStderr, err := tryIP(ctx, exec, host, []string{"-o", "route", "get", "1.1.1.1"}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output",
			"host", host.Original,
			"step", "ip route get",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		// e.g., "1.1.1.1 via 10.0.0.1 dev eth0 src 10.0.0.2 ..."
		fields := strings.Fields(out)
		for i := 0; i < len(fields)-1; i++ {
			if fields[i] == "src" {
				if ip := net.ParseIP(fields[i+1]); ip != nil && ip.To4() != nil {
					logging.LogInfo("remoteip", "advertised IP selected (route)", "host", host.Original, "ip", ip.String())
					return ip.String(), nil
				}
			}
		}
	}

	// 2) First global IPv4 on a non-docker/non-bridge interface
	if out, errStderr, err := tryIP(ctx, exec, host, []string{"-br", "-4", "addr", "show", "scope", "global"}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output",
			"host", host.Original,
			"step", "ip -br -4 addr show scope global",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		// Lines like: "eth0    UP   10.0.0.2/24"
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			f := strings.Fields(line)
			if len(f) >= 3 {
				name := f[0]
				if name == "lo" || strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "br") || strings.HasPrefix(name, "veth") {
					continue
				}
				ipCIDR := f[2]
				if i := strings.IndexByte(ipCIDR, '/'); i > 0 {
					ipCIDR = ipCIDR[:i]
				}
				if ip := net.ParseIP(ipCIDR); ip != nil && ip.To4() != nil && isRoutableIPv4(ip) {
					logging.LogInfo("remoteip", "advertised IP selected (iface)", "host", host.Original, "ip", ip.String(), "iface", name)
					return ip.String(), nil
				}
			}
		}
	}

	// 3) hostname -I (space-separated IPs); pick the first parsable IPv4
	if out, errStderr, err := exec.ExecuteCommand(ctx, host, []string{"hostname", "-I"}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output",
			"host", host.Original,
			"step", "hostname -I",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		if ip := firstRoutableIPv4(out); ip != "" {
			logging.LogInfo("remoteip", "advertised IP selected (hostname -I)", "host", host.Original, "ip", ip)
			return ip, nil
		}
	}

	// 4) getent hosts $(hostname -f) → use first column
	if fqdn, fqdnStderr, fqdnErr := exec.ExecuteCommand(ctx, host, []string{"hostname", "-f"}); fqdn != "" || fqdnErr != nil || fqdnStderr != "" {
		logging.LogDebug("remoteip", "probe output",
			"host", host.Original,
			"step", "hostname -f",
			"stdout", fqdn,
			"stderr", fqdnStderr,
			"error", fqdnErr)
		if out, errStderr, err := exec.ExecuteCommand(ctx, host, []string{"getent", "hosts", strings.TrimSpace(fqdn)}); out != "" || err != nil || errStderr != "" {
			logging.LogDebug("remoteip", "probe output",
				"host", host.Original,
				"step", "getent hosts <fqdn>",
				"stdout", out,
				"stderr", errStderr,
				"error", err)
			if ip := firstRoutableIPv4(out); ip != "" {
				logging.LogInfo("remoteip", "advertised IP selected (getent)", "host", host.Original, "ip", ip)
				return ip, nil
			}
		}
	}

	// Final fallback: execute legacy single-string shell pipelines via sh -lc
	if out, errStderr, err := exec.ExecuteCommand(ctx, host, []string{"sh", shellLoginFlag, routeCmd}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output (fallback)",
			"host", host.Original,
			"step", "routeCmd",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		if ip := firstRoutableIPv4(out); ip != "" {
			logging.LogInfo("remoteip", "advertised IP selected (fallback route)", "host", host.Original, "ip", ip)
			return ip, nil
		}
	}
	if out, errStderr, err := exec.ExecuteCommand(ctx, host, []string{"sh", shellLoginFlag, ifaceCmd}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output (fallback)",
			"host", host.Original,
			"step", "ifaceCmd",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		if ip := firstRoutableIPv4(out); ip != "" {
			logging.LogInfo("remoteip", "advertised IP selected (fallback iface)", "host", host.Original, "ip", ip)
			return ip, nil
		}
	}
	if out, errStderr, err := exec.ExecuteCommand(ctx, host, []string{"sh", shellLoginFlag, hostIPCmd}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output (fallback)",
			"host", host.Original,
			"step", "hostIPCmd",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		if ip := firstRoutableIPv4(out); ip != "" {
			logging.LogInfo("remoteip", "advertised IP selected (fallback hostIP)", "host", host.Original, "ip", ip)
			return ip, nil
		}
	}
	if out, errStderr, err := exec.ExecuteCommand(ctx, host, []string{"sh", shellLoginFlag, getentCmd}); out != "" || err != nil || errStderr != "" {
		logging.LogDebug("remoteip", "probe output (fallback)",
			"host", host.Original,
			"step", "getentCmd",
			"stdout", out,
			"stderr", errStderr,
			"error", err)
		if ip := firstRoutableIPv4(out); ip != "" {
			logging.LogInfo("remoteip", "advertised IP selected (fallback getent)", "host", host.Original, "ip", ip)
			return ip, nil
		}
	}

	logging.LogError("remoteip", "failed to determine advertised IP", fmt.Errorf("no candidates matched"), "host", host.Original)
	return "", fmt.Errorf("could not determine advertised IP on %s", host.Original)
}

// firstParsableIP scans tokens and returns the first value that parses as an IP.
// (firstParsableIP) removed in favor of firstRoutableIPv4

// firstRoutableIPv4 returns the first IPv4 address in the input that is not loopback or link-local.
func firstRoutableIPv4(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	fields := strings.FieldsFunc(s, func(r rune) bool {
		switch r {
		case '\n', '\t', ' ', ',', ';':
			return true
		default:
			return false
		}
	})
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if i := strings.IndexByte(f, '/'); i > 0 {
			f = f[:i]
		}
		if ip := net.ParseIP(f); ip != nil && ip.To4() != nil && isRoutableIPv4(ip) {
			return ip.String()
		}
	}
	return ""
}

// isRoutableIPv4 returns true if the IP is a non-loopback, non-link-local IPv4.
func isRoutableIPv4(ip net.IP) bool {
	if ip == nil {
		return false
	}
	v4 := ip.To4()
	if v4 == nil {
		return false
	}
	if ip.IsLoopback() {
		return false
	}
	// 169.254.0.0/16 link-local and 0.0.0.0
	if v4[0] == 169 && v4[1] == 254 {
		return false
	}
	if ip.IsUnspecified() {
		return false
	}
	return true
}

// tryIP runs the ip command trying common absolute paths when PATH lacks sbin on remote shells.
func tryIP(ctx context.Context, exec cmdpkg.CommandExecutor, host *hostparse.HostInfo, args []string) (stdout, stderr string, err error) {
	candidates := [][]string{
		append([]string{"ip"}, args...),
		append([]string{"/sbin/ip"}, args...),
		append([]string{"/usr/sbin/ip"}, args...),
	}
	var lastErr error
	for _, cmd := range candidates {
		out, errStderr, err := exec.ExecuteCommand(ctx, host, cmd)
		// Log each attempt at debug level
		logging.LogDebug("remoteip", "ip probe attempt", "host", host.Original, "cmd", strings.Join(cmd, " "), "stdout", out, "stderr", errStderr, "error", err)
		// If we have meaningful output, return it
		if strings.TrimSpace(out) != "" {
			return out, errStderr, err
		}
		// If the command executed cleanly (no error/stderr), return even if stdout empty (preserve behavior)
		if err == nil && strings.TrimSpace(errStderr) == "" {
			return out, errStderr, err
		}
		// Otherwise, remember error and try next candidate path
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("ip command not available on remote host")
	}
	return "", "", lastErr
}

// overrideForHost parses ADVERTISED_IPS and returns an override match for the host, if any.
func overrideForHost(host *hostparse.HostInfo) string {
	raw := strings.TrimSpace(os.Getenv("ADVERTISED_IPS"))
	if raw == "" {
		return ""
	}
	// Format: key=ip[,key=ip]; keys may be user@host or host
	entries := strings.Split(raw, ",")
	hostKeys := []string{host.Original, host.Host}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		kv := strings.SplitN(e, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		for _, hk := range hostKeys {
			if k == hk {
				if ip := net.ParseIP(v); ip != nil && ip.To4() != nil {
					return ip.String()
				}
			}
		}
	}
	return ""
}
