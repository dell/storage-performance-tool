package replay

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// Replay keeps an archived bucket list (storage.targetBucket.names) exactly as the archived run
// used it, or refuses the replay before anything runs. It never turns a value it cannot read, such
// as a variable reference, into bucket names.

// archivedJSONBucketList returns the first non-empty bucket list of an archived JSON scenario's
// configurations, or an error when a list cannot be kept exactly or names a bucket the engine
// would refuse.
func archivedJSONBucketList(legacy legacyScenario, vars map[string]string) ([]string, error) {
	lists := jsonBucketLists{vars: vars}
	if err := lists.check("the scenario configuration", legacy.Config); err != nil {
		return nil, err
	}
	for _, steps := range [][]legacyStep{legacy.Steps, legacy.Jobs} {
		if err := lists.walk(steps); err != nil {
			return nil, err
		}
	}
	return lists.first, nil
}

// jsonBucketLists checks every bucket list of an archived JSON scenario and keeps the first.
type jsonBucketLists struct {
	vars  map[string]string
	first []string
}

func (l *jsonBucketLists) walk(steps []legacyStep) error {
	for i, step := range steps {
		if err := l.check(fmt.Sprintf("step %d", i+1), step.Config); err != nil {
			return err
		}
		for _, nested := range [][]legacyStep{step.Steps, step.Jobs} {
			if err := l.walk(nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *jsonBucketLists) check(where string, config map[string]any) error {
	raw := getPath(config, legacyKeyStorage, legacyKeyTargetBucket, legacyKeyNames)
	if raw == nil {
		return nil
	}
	names, err := jsonBucketNames(raw, l.vars)
	if err == nil {
		err = validArchivedBucketList(names)
	}
	if err != nil {
		return fmt.Errorf("archived storage.targetBucket.names in %s: %w", where, err)
	}
	if l.first == nil && len(names) > 0 {
		l.first = names
	}
	return nil
}

// jsonBucketNames reads a JSON bucket list: an array of strings, or a comma-separated string as
// the engine splits it. Elements are kept as written, empty ones included, so validation sees them.
func jsonBucketNames(raw any, vars map[string]string) ([]string, error) {
	switch value := raw.(type) {
	case nil:
		return nil, nil
	case string:
		return splitBucketNames(expandWithExports(value, vars)), nil
	case []any:
		names := make([]string, 0, len(value))
		for i, element := range value {
			name, ok := element.(string)
			if !ok {
				return nil, fmt.Errorf("element %d is not a string", i+1)
			}
			names = append(names, expandWithExports(name, vars))
		}
		return names, nil
	default:
		return nil, fmt.Errorf("the value is not a list of names")
	}
}

// archivedJSBucketList returns the first non-empty bucket list of a JavaScript scenario, found in
// every targetBucket object whether its keys are quoted or not, or an error when a list is not a
// literal that replay can keep exactly, such as a variable reference, or names a bucket the engine
// would refuse.
func archivedJSBucketList(source string, vars map[string]string) ([]string, error) {
	var first []string
	for _, match := range jsTargetBucketKeyRe.FindAllStringIndex(source, -1) {
		open := match[1] - 1
		closeIdx := findMatchingJSBrace(source, open)
		if closeIdx < 0 {
			return nil, fmt.Errorf("archived storage.targetBucket object at offset %d is not terminated", open)
		}
		object := source[open : closeIdx+1]
		key := jsNamesKeyRe.FindStringIndex(object)
		if key == nil {
			continue
		}
		valueStart := skipJSWhitespace(object, key[1])
		valueEnd := findJSValueEnd(object, valueStart, len(object)-1)
		raw := strings.TrimSpace(object[valueStart:max(valueStart, valueEnd)])
		names, err := jsLiteralBucketNames(raw, vars)
		if err == nil {
			err = validArchivedBucketList(names)
		}
		if err != nil {
			return nil, fmt.Errorf("archived storage.targetBucket.names %s: %w", abbreviate(raw), err)
		}
		if first == nil && len(names) > 0 {
			first = names
		}
	}
	return first, nil
}

// jsLiteralBucketNames reads a JavaScript bucket list that replay can keep exactly: an array of
// string literals, or one string literal of comma-separated names as the engine splits it.
func jsLiteralBucketNames(raw string, vars map[string]string) ([]string, error) {
	if name, ok := jsStringLiteral(raw, vars); ok {
		return splitBucketNames(name), nil
	}
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return nil, fmt.Errorf("replay keeps only a literal array of quoted names, not a reference or expression")
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	if inner == "" {
		return []string{}, nil
	}
	parts := splitJSArrayValues(inner)
	names := make([]string, 0, len(parts))
	for i, part := range parts {
		name, ok := jsStringLiteral(strings.TrimSpace(part), vars)
		if !ok {
			return nil, fmt.Errorf("element %d is not a quoted name; replay keeps only a literal array of quoted names", i+1)
		}
		names = append(names, name)
	}
	return names, nil
}

func jsStringLiteral(raw string, vars map[string]string) (string, bool) {
	if len(raw) >= 2 && strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) {
		return expandWithExports(unquoteJSString(raw[1:len(raw)-1]), vars), true
	}
	if len(raw) >= 2 && strings.HasPrefix(raw, `'`) && strings.HasSuffix(raw, `'`) {
		return raw[1 : len(raw)-1], true
	}
	return "", false
}

// splitBucketNames splits a comma-separated list as the engine does, keeping empty names so that
// validation refuses them.
func splitBucketNames(value string) []string {
	if value == "" {
		return []string{}
	}
	return strings.Split(value, ",")
}

// validArchivedBucketList checks a non-empty archived list by the rules the engine applies.
func validArchivedBucketList(names []string) error {
	if len(names) == 0 {
		return nil
	}
	return scenario.ValidateBucketList(names)
}

func abbreviate(raw string) string {
	const limit = 60
	if len(raw) > limit {
		return raw[:limit] + "..."
	}
	return raw
}
