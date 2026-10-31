package replay

import (
	"encoding/json"
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

// archivedJSONBucketList returns the bucket list replay keeps from an archived JSON scenario: the
// first non-empty list among its load steps' effective configurations, each step merged over the
// scenario configuration as conversion merges it. It returns an error when a configuration sets
// targetBucket anywhere but storage.targetBucket or a list names a bucket the engine would refuse.
func archivedJSONBucketList(legacy legacyScenario, vars map[string]string) ([]string, error) {
	if err := checkArchivedJSONTargetBucketKeys(legacy); err != nil {
		return nil, err
	}
	steps := legacy.Steps
	if len(steps) == 0 {
		steps = legacy.Jobs
	}
	var first []string
	for i, step := range steps {
		names, err := effectiveJSONBucketNames(legacy.Config, step, vars)
		if err != nil {
			return nil, fmt.Errorf("archived storage.targetBucket.names of step %d: %w", i+1, err)
		}
		if first == nil && len(names) > 0 {
			first = names
		}
	}
	return first, nil
}

// checkArchivedJSONTargetBucketKeys checks the scenario configuration and every step's own.
func checkArchivedJSONTargetBucketKeys(legacy legacyScenario) error {
	if err := checkJSONTargetBucketKeys(legacy.Config, nil); err != nil {
		return fmt.Errorf("the scenario configuration: %w", err)
	}
	if err := checkJSONStepTargetBuckets(legacy.Steps, "step"); err != nil {
		return err
	}
	return checkJSONStepTargetBuckets(legacy.Jobs, "job")
}

// effectiveJSONBucketNames returns the bucket list a load step uses, its configuration merged over
// the scenario's as conversion merges it, or nil for a step that is not converted as a load.
func effectiveJSONBucketNames(base map[string]any, step legacyStep, vars map[string]string) ([]string, error) {
	stepType := strings.ToLower(strings.TrimSpace(step.Type))
	if stepType != legacyStepTypeLoad && stepType != legacyStepTypePrecondition {
		return nil, nil
	}
	effective := mergeConfig(base, step.Config)
	names, err := jsonBucketNames(getPath(effective, legacyKeyStorage, legacyKeyTargetBucket, legacyKeyNames), vars)
	if err != nil {
		return nil, err
	}
	return names, validArchivedBucketList(names)
}

// checkJSONStepTargetBuckets checks every step's own configuration, nested steps included.
func checkJSONStepTargetBuckets(steps []legacyStep, kind string) error {
	for i, step := range steps {
		if err := checkJSONTargetBucketKeys(step.Config, nil); err != nil {
			return fmt.Errorf("%s %d: %w", kind, i+1, err)
		}
		for _, nested := range [][]legacyStep{step.Steps, step.Jobs} {
			if err := checkJSONStepTargetBuckets(nested, kind+" "+strconv.Itoa(i+1)+"."); err != nil {
				return err
			}
		}
	}
	return nil
}

// jsonBucketNames reads a JSON bucket list: an array of strings, kept element by element, or a
// comma-separated string, split as the engine splits it. A list of one empty name is no list, as
// the engine reads it.
func jsonBucketNames(raw any, vars map[string]string) ([]string, error) {
	var names []string
	switch value := raw.(type) {
	case nil:
		return nil, nil
	case string:
		names = splitBucketNames(expandWithExports(value, vars))
	case []any:
		names = make([]string, 0, len(value))
		for i, element := range value {
			name, ok := element.(string)
			if !ok {
				return nil, fmt.Errorf("element %d is not a string", i+1)
			}
			names = append(names, expandWithExports(name, vars))
		}
	default:
		return nil, fmt.Errorf("the value is not a list of names")
	}
	if len(names) == 1 && names[0] == "" {
		return nil, nil
	}
	return names, nil
}

// jsLooseEscapeRe matches every JavaScript escape: a line continuation (a backslash before a line
// terminator, CR LF counting as one), \\uHHHH, \\u{H...}, \\xHH, an octal escape, and any other
// escaped character, which stands for itself.
var jsLooseEscapeRe = regexp.MustCompile("\\\\(?:\r\n|[\r\n\u2028\u2029]|u\\{[0-9A-Fa-f]{1,6}\\}|u[0-9A-Fa-f]{4}|x[0-9A-Fa-f]{2}|[0-7]{1,3}|.)")

// jsLooseDecodeRounds bounds the decoding of escapes inside escapes, such as a JSON document held
// in a JavaScript string. Each round that changes the text shortens it, so decoding settles; text
// that has not settled within the bound counts as a mention.
const jsLooseDecodeRounds = 64

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
	return true
}

// jsLineTerminators are the JavaScript line terminators, CR LF counted as one.
var jsLineTerminators = map[string]bool{"\r\n": true, "\r": true, "\n": true, "\u2028": true, "\u2029": true}

// decodeLooseJSEscape decodes one escape that jsLooseEscapeRe matched.
func decodeLooseJSEscape(escape string) string {
	body := escape[1:]
	if jsLineTerminators[body] {
		return "" // a line continuation stands for nothing
	}
	digits, base := looseEscapeDigits(body)
	if digits == "" {
		return body // any other escaped character stands for itself
	}
	value, err := strconv.ParseUint(digits, base, 32)
	if err != nil || value > utf8.MaxRune {
		return body
	}
	return string(rune(value))
}

// looseEscapeDigits returns the digits and base of a numeric escape's body, or no digits.
func looseEscapeDigits(body string) (string, int) {
	switch {
	case strings.HasPrefix(body, "u{"):
		return strings.Trim(body[1:], "{}"), 16
	case len(body) > 1 && (body[0] == 'u' || body[0] == 'x'):
		return body[1:], 16
	case body[0] >= '0' && body[0] <= '7':
		return body, 8
	}
	return "", 0
}

// checkJSONTargetBucketKeys refuses a targetBucket that a JSON configuration sets anywhere but as
// the member targetBucket of the top-level object storage, with count, names and provision, such
// as a dashed path like storage-targetBucket-names or a dotted key, which replay would otherwise
// drop. Paths are compared member by member; an array element is never storage.targetBucket.
func checkJSONTargetBucketKeys(node any, path []string) error {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			childPath := append(append([]string(nil), path...), key)
			var err error
			if strings.Contains(key, legacyKeyTargetBucket) {
				err = checkJSONTargetBucketObject(childPath, child)
			} else {
				err = checkJSONTargetBucketKeys(child, childPath)
			}
			if err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := checkJSONTargetBucketKeys(child, append(append([]string(nil), path...), "[]")); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkJSONTargetBucketObject(path []string, value any) error {
	where := strings.Join(path, ".")
	object, ok := value.(map[string]any)
	if len(path) != 2 || path[0] != legacyKeyStorage || path[1] != legacyKeyTargetBucket || !ok {
		return fmt.Errorf("archived configuration sets %s; replay reads a bucket set only as storage.targetBucket "+
			"with count, names and provision", where)
	}
	for key, member := range object {
		if !jsonTargetBucketMemberValid(key, member) {
			return fmt.Errorf("archived configuration sets %s.%s to a value replay does not know", where, key)
		}
	}
	return nil
}

// jsonTargetBucketMemberValid reports whether a storage.targetBucket member is one replay knows,
// with a value of its type: a number or string count, a string or array names, a boolean or string
// provision.
func jsonTargetBucketMemberValid(key string, member any) bool {
	switch key {
	case legacyKeyCount:
		switch member.(type) {
		case json.Number, float64, string:
			return true
		}
	case legacyKeyNames:
		switch member.(type) {
		case string, []any:
			return true
		}
	case "provision":
		switch member.(type) {
		case bool, string:
			return true
		}
	}
	return false
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
