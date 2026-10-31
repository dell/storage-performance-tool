package replay

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// Replay keeps an archived bucket set exactly as the archived run used it, or refuses the replay
// before anything runs. It reads a set only from a JSON archive's data; a JavaScript archive that
// sets one is refused.

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
	if err := checkJSONTargetBucketKeys(config, nil); err != nil {
		return fmt.Errorf("%s: %w", where, err)
	}
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

// jsonBucketNames reads a JSON bucket list: an array of strings, kept element by element, or a
// comma-separated string, split as the engine splits it.
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

// jsLooseEscapeRe matches every JavaScript escape: \\uHHHH, \\u{H...}, \\xHH, an octal escape, and
// any other escaped character, which stands for itself.
var jsLooseEscapeRe = regexp.MustCompile(`\\(?:u\{[0-9A-Fa-f]{1,6}\}|u[0-9A-Fa-f]{4}|x[0-9A-Fa-f]{2}|[0-7]{1,3}|.)`)

// jsLooseDecodeRounds bounds the decoding of escapes inside escapes, such as a JSON document held
// in a JavaScript string.
const jsLooseDecodeRounds = 8

// refuseJSTargetBucket refuses a JavaScript archive that sets a target bucket set. A JavaScript
// scenario is a program, so replay cannot read a bucket count or list from it exactly; it refuses
// the archive rather than replay into other buckets. The archive counts as setting one when
// targetBucket appears anywhere, in comments and strings too, as a dashed path such as
// storage-targetBucket-names and with escapes decoded, nested ones included, so the check errs
// toward refusing. A name assembled at run time, such as "target" + "Bucket", is outside replay's
// model.
func refuseJSTargetBucket(source string) error {
	if !jsMentionsTargetBucket(source) {
		return nil
	}
	return fmt.Errorf("archived JavaScript scenario sets storage.targetBucket (a bucket count or list), which replay " +
		"cannot keep exactly from a program; replay a JSON archive, or replay the workload without its bucket set")
}

// jsMentionsTargetBucket reports whether targetBucket appears anywhere in the source once every
// escape is decoded, repeatedly, so that escapes inside escaped strings are decoded too.
func jsMentionsTargetBucket(source string) bool {
	text := source
	for round := 0; round < jsLooseDecodeRounds; round++ {
		if strings.Contains(text, legacyKeyTargetBucket) {
			return true
		}
		decoded := jsLooseEscapeRe.ReplaceAllStringFunc(text, decodeLooseJSEscape)
		if decoded == text {
			return false
		}
		text = decoded
	}
	return strings.Contains(text, legacyKeyTargetBucket)
}

// decodeLooseJSEscape decodes one escape that jsLooseEscapeRe matched.
func decodeLooseJSEscape(escape string) string {
	body := escape[1:]
	digits, base := "", 16
	switch {
	case strings.HasPrefix(body, "u{"):
		digits = strings.Trim(body[1:], "{}")
	case len(body) > 1 && (body[0] == 'u' || body[0] == 'x'):
		digits = body[1:]
	case body[0] >= '0' && body[0] <= '7':
		digits, base = body, 8
	default:
		return body
	}
	value, err := strconv.ParseUint(digits, base, 32)
	if err != nil || value > utf8.MaxRune {
		return body
	}
	return string(rune(value))
}

// checkJSONTargetBucketKeys refuses a targetBucket that a JSON configuration sets anywhere but as
// storage.targetBucket with count, names and provision, such as a dashed path like
// storage-targetBucket-names, which replay would otherwise drop.
func checkJSONTargetBucketKeys(node any, path []string) error {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			childPath := append(append([]string(nil), path...), key)
			if strings.Contains(key, legacyKeyTargetBucket) {
				if err := checkJSONTargetBucketObject(childPath, child); err != nil {
					return err
				}
				continue
			}
			if err := checkJSONTargetBucketKeys(child, childPath); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := checkJSONTargetBucketKeys(child, path); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkJSONTargetBucketObject(path []string, value any) error {
	where := strings.Join(path, ".")
	object, ok := value.(map[string]any)
	if strings.Join(path, ".") != legacyKeyStorage+"."+legacyKeyTargetBucket || !ok {
		return fmt.Errorf("archived configuration sets %s; replay reads a bucket set only as storage.targetBucket "+
			"with count, names and provision", where)
	}
	for key := range object {
		if key != legacyKeyCount && key != legacyKeyNames && key != "provision" {
			return fmt.Errorf("archived configuration sets %s.%s, which replay does not know", where, key)
		}
	}
	return nil
}

// splitBucketNames splits a comma-separated list as the engine does (Java's String.split(",")):
// an empty string is one empty name, and trailing empty names are dropped.
func splitBucketNames(value string) []string {
	if value == "" {
		return []string{""}
	}
	names := strings.Split(value, ",")
	for len(names) > 0 && names[len(names)-1] == "" {
		names = names[:len(names)-1]
	}
	return names
}

// validArchivedBucketList checks a non-empty archived list by the rules the engine applies.
func validArchivedBucketList(names []string) error {
	if len(names) == 0 {
		return nil
	}
	return scenario.ValidateBucketList(names)
}
