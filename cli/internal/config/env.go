// Package config contains lightweight configuration helpers,
// including a .env loader used to seed environment-based defaults.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"sync/atomic"
)

// DotEnvResult captures the outcome of loading .env files.
type DotEnvResult struct {
	LoadedFiles []string
	// Map of variables set/updated by the loader (does not include pre-existing env vars)
	Vars map[string]string
	// Non-fatal parse errors encountered; loading continues where possible.
	Errors []error
}

// loadedCounter tracks how many times LoadDotEnv has been called in this process.
// Used in tests to assert that subcommands invoke the loader.
var loadedCounter atomic.Uint32

// LoadDotEnv loads environment variables from $HOME/.env and ./.env.
// Precedence:
//  1. Existing OS environment variables (highest) – never overwritten here
//  2. Local ./.env (overrides values loaded from $HOME/.env by this loader)
//  3. $HOME/.env (lowest)
//
// Missing files are ignored. Values are simple KEY=VALUE pairs with optional
// surrounding quotes; lines starting with '#' are comments. 'export ' prefix is allowed.
func LoadDotEnv() DotEnvResult {
	loadedCounter.Add(1)
	res := DotEnvResult{Vars: map[string]string{}}

	// Discover candidate files
	var candidates []string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates = append(candidates, filepath.Join(home, ".env"))
	}
	// Local last for higher precedence
	candidates = append(candidates, ".env")

	// Parse both into in-memory maps to handle precedence without clobbering OS env
	parsed := make([]map[string]string, len(candidates))
	for i, p := range candidates {
		m, loaded, perr := readDotEnvMap(p)
		if perr != nil {
			res.Errors = append(res.Errors, fmt.Errorf("%s: %w", p, perr))
		}
		if loaded {
			res.LoadedFiles = append(res.LoadedFiles, p)
			parsed[i] = m
		} else {
			parsed[i] = map[string]string{}
		}
	}

	// Apply in precedence: first home map, then local map.
	// - Never override pre-existing OS env
	// - Allow local to override values that were set by this loader from $HOME/.env
	loaderSet := map[string]struct{}{}
	setIfAllowed := func(k, v string) {
		if k == "" {
			return
		}
		if _, exists := loaderSet[k]; exists {
			// Override previously set value from our own loader
			_ = os.Setenv(k, v)
			res.Vars[k] = v
			return
		}
		if _, exists := os.LookupEnv(k); exists {
			// Respect already-set OS env
			return
		}
		// Set fresh and record as loader-managed
		_ = os.Setenv(k, v)
		res.Vars[k] = v
		loaderSet[k] = struct{}{}
	}

	if len(parsed) >= 1 {
		for k, v := range parsed[0] { // $HOME/.env
			setIfAllowed(k, v)
		}
	}
	if len(parsed) >= 2 {
		for k, v := range parsed[1] { // ./.env overrides home (within loader-managed vars only)
			setIfAllowed(k, v)
		}
	}

	return res
}

// ResetDotEnvLoadedForTests resets the internal loader counter (testing only).
func ResetDotEnvLoadedForTests() { loadedCounter.Store(0) }

// WasDotEnvLoaded reports whether LoadDotEnv has been called at least once (testing only).
func WasDotEnvLoaded() bool { return loadedCounter.Load() > 0 }

// readDotEnvMap reads and parses a .env file to a map using godotenv.
// Returns (map, loaded, error). If the file does not exist, loaded=false and error=nil.
func readDotEnvMap(path string) (map[string]string, bool, error) {
	m, err := godotenv.Read(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "no such file or directory") {
			return nil, false, nil
		}
		return nil, false, err
	}
	// Perform a second-pass expansion against OS env + file vars to support $PWD and friends
	envSnap := envSnapshot()
	// Merge file vars over OS for intra-file references
	merged := merge(envSnap, m)
	for k, v := range m {
		m[k] = os.Expand(v, func(key string) string { return merged[key] })
	}
	// One more pass to converge simple chains (A=$B, B=$C)
	merged = merge(envSnap, m)
	for k, v := range m {
		m[k] = os.Expand(v, func(key string) string { return merged[key] })
	}
	return m, true, nil
}

// envSnapshot returns a map view of the current environment.
func envSnapshot() map[string]string {
	out := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			out[kv[:i]] = kv[i+1:]
		}
	}
	return out
}

// merge returns a shallow copy of a with b overlayed.
func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
