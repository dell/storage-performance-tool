package replay

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Replay keeps an archived bucket count, with the replay bucket as its base, and refuses an archive
// whose bucket set it cannot keep exactly before anything runs: any bucket list, any bucket set in
// a JavaScript archive, and a JSON count it cannot read with the archived exports.

// checkArchivedJSONBucketSets refuses a JSON archive that sets a bucket list, sets targetBucket
// anywhere but storage.targetBucket, or gives a load step a count replay cannot read with the
// archived exports. A list cannot be kept: replay targets every step at one bucket and reads the run
// script's exports as text, so a listed set could reach other buckets.
func checkArchivedJSONBucketSets(legacy legacyScenario, exports map[string]string) error {
	if err := checkArchivedJSONTargetBucketKeys(legacy); err != nil {
		return err
	}
	steps := legacy.Steps
	if len(steps) == 0 {
		steps = legacy.Jobs
	}
	for i, step := range steps {
		stepType := strings.ToLower(strings.TrimSpace(step.Type))
		if stepType != legacyStepTypeLoad && stepType != legacyStepTypePrecondition {
			continue
		}
		effective := mergeConfig(legacy.Config, step.Config)
		if _, err := jsonBucketCount(getPath(effective, legacyKeyStorage, legacyKeyTargetBucket, legacyKeyCount), exports); err != nil {
			return fmt.Errorf("archived storage.targetBucket.count of step %d: %w", i+1, err)
		}
	}
	return nil
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

// jsonSetsBucketList reports whether a JSON storage.targetBucket.names value names any bucket. The
// forms the engine reads as no list (an empty array, an empty string, one empty name) do not.
func jsonSetsBucketList(raw any) bool {
	switch value := raw.(type) {
	case nil:
		return false
	case string:
		return value != ""
	case []any:
		return len(value) > 1 || (len(value) == 1 && value[0] != "")
	}
	return true
}

// resolveArchivedString expands the exports in an archived value, and refuses a value that uses a
// variable the archive does not define, which replay would otherwise expand to nothing.
func resolveArchivedString(value string, exports map[string]string) (string, error) {
	var missing []string
	resolved := os.Expand(value, func(key string) string {
		if v, ok := exports[key]; ok {
			return v
		}
		missing = append(missing, key)
		return ""
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("%q uses $%s, which the archived run script does not export, so replay cannot tell its value",
			value, missing[0])
	}
	return resolved, nil
}

// jsonBucketCount reads a JSON storage.targetBucket.count with the archived exports: a whole
// number of at least 1, or 0 when it is not set. A JSON number counts by its value, so 4.0 and
// 1e1 are whole; a string must be a whole number as written, as the engine parses it.
func jsonBucketCount(raw any, exports map[string]string) (int64, error) {
	if raw == nil {
		return 0, nil
	}
	count, err := jsonCountValue(raw, exports)
	if err == nil && count < 1 {
		err = fmt.Errorf("%d is not a whole number of at least 1", count)
	}
	return count, err
}

// jsonCountValue reads a count's value: a JSON number by its value, a string as written.
func jsonCountValue(raw any, exports map[string]string) (int64, error) {
	switch value := raw.(type) {
	case json.Number:
		return wholeNumber(value.String())
	case float64:
		return wholeNumber(strconv.FormatFloat(value, 'g', -1, 64))
	case string:
		resolved, err := resolveArchivedString(value, exports)
		if err != nil {
			return 0, err
		}
		count, err := strconv.ParseInt(resolved, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%q is not a whole number of at least 1", resolved)
		}
		return count, nil
	}
	return 0, fmt.Errorf("the value is not a number")
}

// wholeNumber returns the integer a JSON number denotes, refusing a fraction or one too large.
func wholeNumber(text string) (int64, error) {
	value, ok := new(big.Rat).SetString(text)
	if !ok || !value.IsInt() || !value.Num().IsInt64() {
		return 0, fmt.Errorf("%s is not a whole number of at least 1", text)
	}
	return value.Num().Int64(), nil
}

// jsLooseEscapeRe matches every JavaScript escape: a line continuation (a backslash before a line
// terminator, CR LF counting as one), \\uHHHH, \\u{H...}, \\xHH, an octal escape, and any other
// escaped character, which stands for itself.
var jsLooseEscapeRe = regexp.MustCompile("\\\\(?:\r\n|[\r\n\u2028\u2029]|u\\{[0-9A-Fa-f]{1,6}\\}|u[0-9A-Fa-f]{4}|x[0-9A-Fa-f]{2}|[0-7]{1,3}|.)")

// jsLooseDecodeWorkLimit bounds the bytes decoding may scan in total. Each pass that changes the
// text shortens it, so decoding always settles; the limit only stops an input built to make it
// settle slowly, which is then refused rather than read.
const jsLooseDecodeWorkLimit = 128 << 20

// errJSDecodeLimit reports an archive whose escapes did not settle within the decoding work limit.
var errJSDecodeLimit = fmt.Errorf("archived JavaScript scenario has escapes that replay could not decode within its work "+
	"limit (%d MiB scanned), so it cannot tell whether the scenario sets storage.targetBucket", jsLooseDecodeWorkLimit>>20)

// refuseJSTargetBucket refuses a JavaScript archive that sets a target bucket set. A JavaScript
// scenario is a program, so replay cannot read a bucket count or list from it exactly; it refuses
// the archive rather than replay into other buckets. The archive counts as setting one when
// targetBucket appears anywhere, in comments and strings too, as a dashed path such as
// storage-targetBucket-names and with escapes decoded, nested ones included, so the check errs
// toward refusing. A name assembled at run time, such as "target" + "Bucket", is outside replay's
// model.
func refuseJSTargetBucket(source string) error {
	mentioned, err := jsMentionsTargetBucket(source)
	if err != nil {
		return err
	}
	if !mentioned {
		return nil
	}
	return fmt.Errorf("archived JavaScript scenario sets storage.targetBucket (a bucket count or list), which replay " +
		"cannot keep exactly from a program; replay a JSON archive, or replay the workload without its bucket set")
}

// jsMentionsTargetBucket reports whether targetBucket appears anywhere in the source once every
// escape is decoded, repeatedly until the text settles, so that escapes inside escaped strings are
// decoded too. It returns errJSDecodeLimit when the text has not settled within the work limit.
func jsMentionsTargetBucket(source string) (bool, error) {
	text := source
	for scanned := 0; ; {
		if strings.Contains(text, legacyKeyTargetBucket) {
			return true, nil
		}
		if scanned += len(text); scanned > jsLooseDecodeWorkLimit {
			return false, errJSDecodeLimit
		}
		decoded := jsLooseEscapeRe.ReplaceAllStringFunc(text, decodeLooseJSEscape)
		if decoded == text {
			return false, nil
		}
		text = decoded
	}
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
		if key == legacyKeyNames && jsonSetsBucketList(member) {
			return fmt.Errorf("archived configuration sets a bucket list (%s.names), which replay cannot keep: it targets "+
				"every step at one bucket and reads the run script's exports as text, so a listed set could reach other "+
				"buckets; replay the workload without its list", where)
		}
	}
	return nil
}

// jsonTargetBucketMemberValid reports whether a storage.targetBucket member is one replay knows,
// with a value of its type: a number or string count, a string or an array of strings names, a
// boolean or string provision.
func jsonTargetBucketMemberValid(key string, member any) bool {
	switch key {
	case legacyKeyCount:
		switch member.(type) {
		case json.Number, float64, string:
			return true
		}
	case legacyKeyNames:
		switch names := member.(type) {
		case string:
			return true
		case []any:
			for _, name := range names {
				if _, ok := name.(string); !ok {
					return false
				}
			}
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
