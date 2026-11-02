package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			w.Header().Set("Cache-Control", "no-store")
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

// msgLoopbackOnly is returned when a request does not name the dashboard's
// own loopback address.
const msgLoopbackOnly = "Open the dashboard through its SSH tunnel on 127.0.0.1."

// loopbackHost reports whether the request names the dashboard's own loopback
// address, which guards the API against DNS rebinding.
func (a *app) loopbackHost(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	return host == "127.0.0.1:"+a.port || host == "localhost:"+a.port
}

// apiGuard applies loopbackHost to every /api route, so a page served
// from another origin (for example after DNS rebinding) cannot read run state,
// history, run logs or the results catalog. A browser sends Origin on
// cross-origin reads and on every state-changing request; when it is present it
// must name this dashboard.
// The SSH tunnel case works because the browser's Host is 127.0.0.1:<port> or
// localhost:<port>, the same port the dashboard listens on.
func (a *app) apiGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") {
			if !a.loopbackHost(r) || (r.Header.Get("Origin") != "" && !a.sameOrigin(r)) {
				writeJSON(w, http.StatusForbidden, map[string]any{keyError: msgLoopbackOnly})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (a *app) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	body, err := assets.ReadFile("index.html")
	if err != nil {
		http.Error(w, "Dashboard page not found", 404)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		status = 500
		body = []byte(`{"error":"Could not encode dashboard response."}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (a *app) handleState(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		writeJSON(w, 404, map[string]any{keyError: msgEndpointNotFound})
		return
	}
	a.mu.Lock()
	attached := a.attachedHost != ""
	a.mu.Unlock()
	if attached {
		state, err := a.attachedState()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{keyError: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, state)
		return
	}
	state, err := a.currentState()
	if err != nil {
		writeJSON(w, 500, map[string]any{keyError: "Could not read SPT state: " + err.Error()})
		return
	}
	writeJSON(w, 200, state)
}
