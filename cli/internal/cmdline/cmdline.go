package cmdline

import (
	"strconv"
	"strings"
)

const redactedValue = "***"

// SanitizeArgs masks sensitive credential arguments in argv-style input.
func SanitizeArgs(args []string) []string {
	out := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			flag, hasValue := splitLongArg(arg)
			if isSensitiveLongFlag(flag) {
				if hasValue {
					out[i] = flag + "=" + redactedValue
				} else {
					out[i] = flag
					if i+1 < len(args) {
						out[i+1] = redactedValue
						i++
					}
				}
				continue
			}
			out[i] = arg
			continue
		}
		if isSensitiveShortFlag(arg) {
			out[i] = arg
			if strings.Contains(arg, "=") {
				out[i] = sensitiveShortPrefix(arg)
			} else if i+1 < len(args) {
				out[i+1] = redactedValue
				i++
			}
			continue
		}
		out[i] = arg
	}
	for i := range out {
		if out[i] == "" {
			out[i] = args[i]
		}
	}
	return out
}

// FormatForArtifact renders sanitized args as a command line for logs/artifacts.
func FormatForArtifact(args []string) string {
	sanitized := SanitizeArgs(args)
	if len(sanitized) == 0 {
		return ""
	}
	parts := make([]string, len(sanitized))
	for i, arg := range sanitized {
		if arg == "" || strings.ContainsAny(arg, " \t\n\"'\\") {
			parts[i] = strconv.Quote(arg)
			continue
		}
		parts[i] = arg
	}
	return strings.Join(parts, " ")
}

func splitLongArg(arg string) (flag string, hasValue bool) {
	if !strings.HasPrefix(arg, "--") {
		return "", false
	}
	if idx := strings.Index(arg, "="); idx != -1 {
		return arg[:idx], true
	}
	return arg, false
}

func isSensitiveLongFlag(flag string) bool {
	switch flag {
	case "--secret-key", "--access-key":
		return true
	default:
		return false
	}
}

func isSensitiveShortFlag(arg string) bool {
	if !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "--") {
		return false
	}
	base := arg
	if strings.Contains(arg, "=") {
		base = arg[:strings.Index(arg, "=")]
	}
	switch base {
	case "-s", "-a":
		return true
	default:
		return false
	}
}

func sensitiveShortPrefix(arg string) string {
	if idx := strings.Index(arg, "="); idx != -1 {
		return arg[:idx+1] + redactedValue
	}
	return arg
}
