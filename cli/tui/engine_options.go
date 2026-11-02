package tui

import (
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/poolsizing"
)

// withEngineOptions adds what the CLI decided for the engine on its own host to a container's
// environment, as KEY=value entries. The value goes in JAVA_OPTS, which the entrypoint places before
// SPT_JAVA_OPTS, so a user's -XX:MaxDirectMemorySize wins unchanged. An engine it did not size gets
// nothing.
func withEngineOptions(env []string) []string {
	options := poolsizing.LaunchOptionsFor(poolsizing.LocalHostKey)
	if options.JavaOpts != "" {
		prefix := constants.JavaOptsEnvVar + "="
		merged := false
		for i, entry := range env {
			if strings.HasPrefix(entry, prefix) {
				env[i] = entry + " " + options.JavaOpts
				merged = true
			}
		}
		if !merged {
			env = append(env, prefix+options.JavaOpts)
		}
	}
	for key, value := range options.Environment() {
		env = append(env, key+"="+value)
	}
	return env
}

// remoteEngineEnvironment is withEngineOptions for a remote container's environment map.
func remoteEngineEnvironment(env map[string]string, host string) map[string]string {
	options := poolsizing.LaunchOptionsFor(host)
	if options.JavaOpts != "" {
		if existing := env[constants.JavaOptsEnvVar]; existing != "" {
			env[constants.JavaOptsEnvVar] = existing + " " + options.JavaOpts
		} else {
			env[constants.JavaOptsEnvVar] = options.JavaOpts
		}
	}
	for key, value := range options.Environment() {
		env[key] = value
	}
	return env
}
