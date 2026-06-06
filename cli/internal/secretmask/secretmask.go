// Package secretmask centralizes best-effort masking for user credentials in
// logs and fetched artifacts.
package secretmask

import (
	"bytes"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const masked = "***"

var sensitiveAssignmentPatterns = []struct {
	re          *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`(?i)(--(?:access-key|secret-key|storage-auth-(?:uid|secret))(?:=|\s+))("[^"]*"|'[^']*'|[^\s,;]+)`), "${1}" + masked},
	{regexp.MustCompile(`(?i)(-(?:a|s)(?:=|\s+))("[^"]*"|'[^']*'|[^\s,;]+)`), "${1}" + masked},
	{regexp.MustCompile(`(?im)^(\s*(?:access[-_]?key|secret[-_]?key|accessKeyId|secretAccessKey|password|token|secret)\s*:\s*)(.*)$`), "${1}" + masked},
	{regexp.MustCompile(`(?i)\b((?:storage\.)?auth\.(?:uid|secret)|access[-_]?key|secret[-_]?key|accessKeyId|secretAccessKey|password|token|secret)(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s,;]+)`), "${1}${2}" + masked},
}

// Text masks common credential-looking values in free-form diagnostics.
func Text(s string) string {
	out := s
	for _, pattern := range sensitiveAssignmentPatterns {
		out = pattern.re.ReplaceAllString(out, pattern.replacement)
	}
	return out
}

// YAML masks sensitive fields in YAML documents, falling back to text masking if
// the input is not valid YAML.
func YAML(data []byte) []byte {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return []byte(Text(string(data)))
	}
	maskYAMLNode(&node, nil)
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&node); err != nil {
		return []byte(Text(string(data)))
	}
	_ = enc.Close()
	return out.Bytes()
}

func maskYAMLNode(node *yaml.Node, path []string) {
	if node == nil {
		return
	}
	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			maskYAMLNode(child, path)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			keyNode := node.Content[i]
			valueNode := node.Content[i+1]
			key := ""
			if keyNode != nil {
				key = keyNode.Value
			}
			nextPath := append(path, key)
			if isSensitiveYAMLPath(nextPath) {
				replaceWithMask(valueNode)
				continue
			}
			maskYAMLNode(valueNode, nextPath)
		}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			maskYAMLNode(child, path)
		}
	}
}

func replaceWithMask(node *yaml.Node) {
	if node == nil {
		return
	}
	node.Kind = yaml.ScalarNode
	node.Tag = "!!str"
	node.Value = masked
	node.Content = nil
	node.Style = 0
}

func isSensitiveYAMLPath(path []string) bool {
	if len(path) == 0 {
		return false
	}
	key := normalizeKey(path[len(path)-1])
	switch {
	case strings.Contains(key, "secret"):
		return true
	case strings.Contains(key, "password"):
		return true
	case strings.Contains(key, "token"):
		return true
	case strings.Contains(key, "accesskey"):
		return true
	case strings.Contains(key, "keyid"):
		return true
	case key == "uid" && pathContains(path[:len(path)-1], "auth"):
		return true
	case key == "key" && pathContains(path[:len(path)-1], "auth", "credential"):
		return true
	default:
		return false
	}
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.ReplaceAll(key, "-", "")
	key = strings.ReplaceAll(key, "_", "")
	key = strings.ReplaceAll(key, ".", "")
	return key
}

func pathContains(path []string, needles ...string) bool {
	for _, part := range path {
		part = normalizeKey(part)
		for _, needle := range needles {
			if strings.Contains(part, normalizeKey(needle)) {
				return true
			}
		}
	}
	return false
}
