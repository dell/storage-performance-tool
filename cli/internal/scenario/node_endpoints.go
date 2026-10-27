package scenario

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// NodeEndpoints is the engine's storage.net.node view of the S3 endpoints.
type NodeEndpoints struct {
	// Scheme is http or https, shared by every endpoint.
	Scheme string
	// Addrs is storage.net.node.addrs: the hosts when every port is the same, otherwise host:port, in configured order without duplicates.
	Addrs []string
	// Port is storage.net.node.port: the common port, or 0 when the ports differ.
	Port int
}

// ParseNodeEndpoints parses endpoint URLs into the engine's node addresses.
// A missing port defaults to the scheme's; duplicate host:port pairs are dropped, keeping the first.
func ParseNodeEndpoints(endpoints []string) (NodeEndpoints, error) {
	type hostPort struct {
		host string
		port int
	}
	parsed := make([]hostPort, 0, len(endpoints))
	scheme := ""

	for _, ep := range endpoints {
		u, err := url.Parse(ep)
		if err != nil {
			return NodeEndpoints{}, fmt.Errorf("invalid endpoint URL: %w", err)
		}
		if u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS {
			return NodeEndpoints{}, fmt.Errorf("invalid endpoint URL: unsupported scheme %q (must be http or https)", u.Scheme)
		}
		if scheme == "" {
			scheme = u.Scheme
		} else if scheme != u.Scheme {
			return NodeEndpoints{}, fmt.Errorf("all endpoints must use the same scheme; found %q and %q", scheme, u.Scheme)
		}

		h := u.Hostname()
		p := u.Port()
		if p == "" {
			if u.Scheme == schemeHTTPS {
				p = constants.DefaultHTTPSPort
			} else {
				p = constants.DefaultHTTPPort
			}
		}
		var pi int
		if _, err := fmt.Sscanf(p, "%d", &pi); err != nil {
			return NodeEndpoints{}, fmt.Errorf("invalid port: %s", p)
		}
		parsed = append(parsed, hostPort{host: h, port: pi})
	}
	if len(parsed) == 0 {
		return NodeEndpoints{}, fmt.Errorf("no endpoints")
	}

	// Deduplicate while preserving order
	seen := make(map[string]struct{}, len(parsed))
	uniq := make([]hostPort, 0, len(parsed))
	for _, hp := range parsed {
		key := fmt.Sprintf("%s:%d", hp.host, hp.port)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniq = append(uniq, hp)
	}

	// Determine if ports are uniform
	uniformPort := true
	refPort := uniq[0].port
	for _, hp := range uniq[1:] {
		if hp.port != refPort {
			uniformPort = false
			break
		}
	}

	// Build node.addrs and optional node.port
	nodes := NodeEndpoints{Scheme: scheme, Addrs: make([]string, 0, len(uniq))}
	if uniformPort {
		nodes.Port = refPort
		for _, hp := range uniq {
			nodes.Addrs = append(nodes.Addrs, hp.host)
		}
	} else {
		for _, hp := range uniq {
			nodes.Addrs = append(nodes.Addrs, fmt.Sprintf("%s:%d", hp.host, hp.port))
		}
	}
	return nodes, nil
}

// DriverEndpoints returns the endpoint strings a storage driver names its connections by:
// each of addrs, with Port appended to an address that has no colon, as the Netty drivers do.
func (n NodeEndpoints) DriverEndpoints(addrs []string) []string {
	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, ":") {
			endpoints = append(endpoints, addr)
		} else {
			endpoints = append(endpoints, fmt.Sprintf("%s:%d", addr, n.Port))
		}
	}
	return endpoints
}
