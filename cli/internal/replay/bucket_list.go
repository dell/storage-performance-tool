package replay

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

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

// archivedJSBucketList returns the first non-empty bucket list of a JavaScript scenario, or an
// error when replay cannot keep a list exactly or the list names a bucket the engine would refuse.
// It reads the code as tokens with comments masked, so neither comments nor string contents count.
// A list is read only as the direct "names" member of a "targetBucket" object literal, with a value
// that is one array literal of string literals or one string literal. Any other appearance of
// targetBucket, such as a member access or a variable as its value, could hide a list, so it is
// refused as well.
func archivedJSBucketList(source string, vars map[string]string) ([]string, error) {
	code := jsCodeMask(source)
	if err := checkJSTargetBucketKeys(code); err != nil {
		return nil, err
	}
	names, _ := jsKeyOccurrences(code, legacyKeyNames)
	var first []string
	for _, occurrence := range names {
		if occurrence.parentKey != legacyKeyTargetBucket {
			continue
		}
		list, err := jsLiteralBucketNames(code, occurrence.valueStart, vars)
		if err == nil {
			err = validArchivedBucketList(list)
		}
		if err != nil {
			return nil, fmt.Errorf("archived storage.targetBucket.names at offset %d: %w", occurrence.keyStart, err)
		}
		if first == nil && len(list) > 0 {
			first = list
		}
	}
	return first, nil
}

// checkJSTargetBucketKeys refuses any targetBucket that replay cannot read: a use outside an
// object literal key, such as a member access, or a key whose value is not an object literal.
func checkJSTargetBucketKeys(code string) error {
	targetBuckets, otherUses := jsKeyOccurrences(code, legacyKeyTargetBucket)
	if len(otherUses) > 0 {
		return fmt.Errorf("archived scenario uses targetBucket outside an object literal key at offset %d; "+
			"replay can keep storage.targetBucket only as an object literal", otherUses[0])
	}
	for _, occurrence := range targetBuckets {
		if occurrence.valueStart >= len(code) || code[occurrence.valueStart] != '{' {
			return fmt.Errorf("archived storage.targetBucket at offset %d is not an object literal, "+
				"so replay cannot tell which buckets it names", occurrence.keyStart)
		}
	}
	return nil
}

// jsTargetBucketCount reads storage.targetBucket.count from a JavaScript configuration object:
// the direct "count" member of a "targetBucket" object literal, a number or an exported variable,
// read as tokens with comments masked. It returns 0 when no count is set.
func jsTargetBucketCount(source string, vars map[string]string) (int, error) {
	code := jsCodeMask(source)
	counts, _ := jsKeyOccurrences(code, legacyKeyCount)
	count := 0
	for _, occurrence := range counts {
		if occurrence.parentKey != legacyKeyTargetBucket {
			continue
		}
		token, end, err := parseJSLiteralToken(code, occurrence.valueStart)
		if err == nil {
			if next := skipJSWhitespace(code, end); next >= len(code) || (code[next] != ',' && code[next] != '}') {
				err = fmt.Errorf("an expression")
			}
		}
		if err == nil {
			count, err = jsResolveInt(token, "targetBucket count", vars)
		}
		if err != nil {
			return 0, fmt.Errorf("sets storage.targetBucket.count in a form replay cannot keep exactly: %w", err)
		}
	}
	return count, nil
}

// jsLiteralBucketNames reads the value at start: an array literal whose elements are each one
// string literal, kept exactly, or one string literal, split as the engine splits a string. The
// value must end where the member ends, so an expression such as a concatenation or a call is
// refused.
func jsLiteralBucketNames(code string, start int, vars map[string]string) ([]string, error) {
	var names []string
	var end int
	var err error
	switch c := jsByteAt(code, start); {
	case isJSQuote(c):
		var name string
		if name, end, err = jsStringLiteralAt(code, start, vars); err == nil {
			names = splitBucketNames(name)
		}
	case c == '[':
		names, end, err = jsStringLiteralArray(code, start, vars)
	default:
		err = fmt.Errorf("replay keeps only an array of string literals or one string literal, not a reference or expression")
	}
	if err == nil && !jsMemberValueEnds(code, end) {
		err = fmt.Errorf("the value is an expression, not a literal list; replay keeps only literal names")
	}
	return names, err
}

// jsMemberValueEnds reports whether an object member's value ends at end: only a comma or the
// object's closing brace may follow it.
func jsMemberValueEnds(code string, end int) bool {
	next := jsByteAt(code, skipJSWhitespace(code, end))
	return next == ',' || next == '}'
}

// jsByteAt returns the byte at i, or 0 past the end.
func jsByteAt(code string, i int) byte {
	if i < len(code) {
		return code[i]
	}
	return 0
}

func isJSQuote(c byte) bool {
	return c == '"' || c == '\''
}

// jsStringLiteralArray reads the array literal opening at open, each element one string literal,
// and returns its elements and the offset after the closing bracket.
func jsStringLiteralArray(code string, open int, vars map[string]string) ([]string, int, error) {
	names := []string{}
	i := skipJSWhitespace(code, open+1)
	for c := jsByteAt(code, i); c != ']'; c = jsByteAt(code, i) {
		if !isJSQuote(c) {
			return nil, i, fmt.Errorf("element %d is not a string literal; replay keeps only literal names", len(names)+1)
		}
		name, end, err := jsStringLiteralAt(code, i, vars)
		if err != nil {
			return nil, i, fmt.Errorf("element %d: %w", len(names)+1, err)
		}
		names = append(names, name)
		i = skipJSWhitespace(code, end)
		switch jsByteAt(code, i) {
		case ',':
			i = skipJSWhitespace(code, i+1)
		case ']':
		default:
			return nil, i, fmt.Errorf("element %d is followed by an expression; replay keeps only literal names", len(names))
		}
	}
	return names, i + 1, nil
}

// jsStringLiteralAt decodes the string literal opening at open and returns it with exported
// placeholders expanded and the offset after its closing quote. An escape replay cannot decode
// exactly is refused rather than copied.
func jsStringLiteralAt(code string, open int, vars map[string]string) (string, int, error) {
	end := jsStringEnd(code, open)
	if end <= open || code[end] != code[open] {
		return "", open, fmt.Errorf("an unterminated string")
	}
	decoded, err := decodeJSString(code[open+1 : end])
	if err != nil {
		return "", open, err
	}
	return expandWithExports(decoded, vars), end + 1, nil
}

// jsSimpleEscapes maps a single-character JavaScript escape to the character it stands for.
var jsSimpleEscapes = map[byte]byte{
	'\'': '\'', '"': '"', '\\': '\\', 'n': '\n', 't': '\t', 'r': '\r', 'b': '\b', 'f': '\f', 'v': '\v',
}

// decodeJSString decodes a JavaScript string literal's body: the single-character escapes, \\xHH,
// \\uHHHH and \\u{H...}. A line continuation, an octal escape or a raw line break is refused.
func decodeJSString(body string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(body); i++ {
		switch c := body[i]; {
		case c == '\n' || c == '\r':
			return "", fmt.Errorf("a string with a raw line break")
		case c != '\\':
			out.WriteByte(c)
		default:
			decoded, last, err := decodeJSEscape(body, i+1)
			if err != nil {
				return "", err
			}
			out.WriteString(decoded)
			i = last
		}
	}
	return out.String(), nil
}

// decodeJSEscape decodes the escape whose letter is at i and returns its text and the index of
// its last byte.
func decodeJSEscape(body string, i int) (string, int, error) {
	if i >= len(body) {
		return "", i, fmt.Errorf("a string that ends in a backslash")
	}
	if simple, ok := jsSimpleEscapes[body[i]]; ok {
		return string(simple), i, nil
	}
	if body[i] != 'x' && body[i] != 'u' {
		return "", i, fmt.Errorf("an escape replay does not decode (\\%c)", body[i])
	}
	digits, last := jsEscapeDigits(body, i)
	value, err := strconv.ParseUint(digits, 16, 32)
	if digits == "" || err != nil || value > utf8.MaxRune {
		return "", i, fmt.Errorf("a malformed \\%c escape", body[i])
	}
	if value >= 0xD800 && value <= 0xDFFF {
		// JavaScript pairs surrogates into one character; decoding them one by one would not.
		return "", i, fmt.Errorf("a surrogate \\u escape replay does not decode")
	}
	return string(rune(value)), last, nil
}

// jsEscapeDigits returns the hexadecimal digits of the \\x or \\u escape whose letter is at i, and
// the index of the escape's last byte; no digits when the escape is cut short.
func jsEscapeDigits(body string, i int) (string, int) {
	switch {
	case body[i] == 'x' && i+2 < len(body):
		return body[i+1 : i+3], i + 2
	case body[i] == 'u' && jsByteAt(body, i+1) == '{':
		if closing := strings.IndexByte(body[i+1:], '}'); closing > 0 {
			return body[i+2 : i+1+closing], i + 1 + closing
		}
	case body[i] == 'u' && i+4 < len(body):
		return body[i+1 : i+5], i + 4
	}
	return "", i
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
