package replay

import (
	"fmt"
	"regexp"
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
// error when replay cannot keep the scenario's bucket set exactly or a list names a bucket the
// engine would refuse.
func archivedJSBucketList(source string, vars map[string]string) ([]string, error) {
	sets, err := jsTargetBuckets(source, vars)
	if err != nil {
		return nil, err
	}
	for _, set := range sets {
		if len(set.names) > 0 {
			return set.names, nil
		}
	}
	return nil, nil
}

// jsTargetBucket is one storage.targetBucket object literal of a JavaScript scenario.
type jsTargetBucket struct {
	count int      // 0 when the object sets no count
	names []string // nil when the object sets no names
}

// jsTargetBuckets reads every storage.targetBucket of a JavaScript scenario, or refuses the
// scenario when it cannot read them exactly. JavaScript is a program, so replay reads a bucket set
// only where its form is plain data:
//   - A scenario that never names targetBucket, even in a comment or string and with escapes
//     decoded, has no bucket set. Any other scenario must be lexically simple outside its strings
//     and comments: no slash (a regular expression or a division), no backslash (an escaped
//     identifier), no template literal, no HTML-like comment and no non-ASCII character, such as a
//     Unicode line terminator or space, so its tokens read as the engine's script engine reads them.
//   - targetBucket must appear only as an object literal key, once per object, whose value is an
//     object literal of count, names and provision with literal values; names is an array of string
//     literals or one string literal, split as the engine splits a string.
//
// A configuration built programmatically, such as a property name assembled from strings, is
// outside replay's model.
func jsTargetBuckets(source string, vars map[string]string) ([]jsTargetBucket, error) {
	if !jsMentionsTargetBucket(source) {
		return nil, nil
	}
	code := jsCodeMask(source)
	if err := checkJSLexicallySimple(code); err != nil {
		return nil, err
	}
	occurrences, otherUses := jsKeyOccurrences(code, legacyKeyTargetBucket)
	if len(otherUses) > 0 {
		return nil, fmt.Errorf("archived scenario uses targetBucket outside an object literal key at offset %d; "+
			"replay can keep storage.targetBucket only as an object literal", otherUses[0])
	}
	sets := make([]jsTargetBucket, 0, len(occurrences))
	parents := map[int]bool{}
	for _, occurrence := range occurrences {
		if parents[occurrence.parentOpen] {
			return nil, fmt.Errorf("archived scenario sets targetBucket twice in one object at offset %d", occurrence.keyStart)
		}
		parents[occurrence.parentOpen] = true
		set, err := parseJSTargetBucket(code, occurrence.valueStart, vars)
		if err == nil {
			err = validArchivedBucketList(set.names)
		}
		if err != nil {
			return nil, fmt.Errorf("archived storage.targetBucket at offset %d: %w", occurrence.keyStart, err)
		}
		sets = append(sets, set)
	}
	return sets, nil
}

// parseJSTargetBucket parses the targetBucket value at open: an object literal whose keys are
// count, names and provision, each at most once, with literal values, that ends where the member
// ends.
func parseJSTargetBucket(code string, open int, vars map[string]string) (jsTargetBucket, error) {
	var set jsTargetBucket
	if jsByteAt(code, open) != '{' {
		return set, fmt.Errorf("the value is not an object literal, so replay cannot tell which buckets it names")
	}
	seen := map[string]bool{}
	i := skipJSWhitespace(code, open+1)
	for jsByteAt(code, i) != '}' {
		key, valueStart, err := parseJSTargetBucketKey(code, i)
		if err == nil && seen[key] {
			err = fmt.Errorf("%s is set twice", key)
		}
		if err != nil {
			return set, err
		}
		seen[key] = true
		end, err := parseJSTargetBucketValue(&set, key, code, valueStart, vars)
		if err != nil {
			return set, fmt.Errorf("%s: %w", key, err)
		}
		if i, err = jsNextMember(code, end, key); err != nil {
			return set, err
		}
	}
	if !jsMemberValueEnds(code, i+1) {
		return set, fmt.Errorf("the value is an expression, not an object literal")
	}
	return set, nil
}

// jsNextMember returns the offset of the next member after a value that ends at end, or of the
// closing brace, and refuses anything else after the value, such as an operator.
func jsNextMember(code string, end int, key string) (int, error) {
	i := skipJSWhitespace(code, end)
	switch jsByteAt(code, i) {
	case ',':
		return skipJSWhitespace(code, i+1), nil
	case '}':
		return i, nil
	}
	return i, fmt.Errorf("%s is an expression, not a literal", key)
}

// parseJSTargetBucketKey reads one key of a targetBucket object literal, decoded, and returns it
// with the offset of its value.
func parseJSTargetBucketKey(code string, i int) (string, int, error) {
	var key string
	var end int
	switch c := jsByteAt(code, i); {
	case isJSQuote(c):
		closing := jsStringEnd(code, i)
		decoded, err := decodeJSString(code[i+1 : closing])
		if closing <= i || err != nil {
			return "", i, fmt.Errorf("a key replay cannot read")
		}
		key, end = decoded, closing+1
	case isJSIdentifierStart(c):
		end = i + 1
		for end < len(code) && isJSIdentifierPart(code[end]) {
			end++
		}
		key = code[i:end]
	default:
		return "", i, fmt.Errorf("a key that is not a literal name, such as a spread or computed key")
	}
	colon := skipJSWhitespace(code, end)
	if jsByteAt(code, colon) != ':' {
		return "", i, fmt.Errorf("an entry that is not a key and value")
	}
	return key, skipJSWhitespace(code, colon+1), nil
}

// parseJSTargetBucketValue reads the literal value of one targetBucket key into set and returns
// the offset after it.
func parseJSTargetBucketValue(set *jsTargetBucket, key, code string, start int, vars map[string]string) (int, error) {
	switch key {
	case legacyKeyNames:
		if isJSQuote(jsByteAt(code, start)) {
			name, end, err := jsStringLiteralAt(code, start, vars)
			set.names = splitBucketNames(name)
			return end, err
		}
		if jsByteAt(code, start) != '[' {
			return start, fmt.Errorf("replay keeps only an array of string literals or one string literal, not a reference or expression")
		}
		names, end, err := jsStringLiteralArray(code, start, vars)
		set.names = names
		return end, err
	case legacyKeyCount, "provision":
		token, end, err := parseJSLiteralToken(code, start)
		if err != nil {
			return start, err
		}
		if key == legacyKeyCount {
			set.count, err = jsResolveInt(token, "targetBucket count", vars)
		} else if token != jsTrue && token != jsFalse {
			err = fmt.Errorf("the value is not true or false")
		}
		return end, err
	default:
		return start, fmt.Errorf("replay does not know this targetBucket setting")
	}
}

// jsLooseEscapeRe matches the escapes that can spell a name in a string or an identifier.
var jsLooseEscapeRe = regexp.MustCompile(`\\u\{[0-9A-Fa-f]{1,6}\}|\\u[0-9A-Fa-f]{4}|\\x[0-9A-Fa-f]{2}`)

// jsMentionsTargetBucket reports whether the source names targetBucket anywhere, in comments and
// strings too and with escapes decoded wherever they appear, so detection errs toward reading.
func jsMentionsTargetBucket(source string) bool {
	decoded := jsLooseEscapeRe.ReplaceAllStringFunc(source, func(escape string) string {
		digits := strings.Trim(escape[2:], "{}")
		value, err := strconv.ParseUint(digits, 16, 32)
		if err != nil || value > utf8.MaxRune {
			return escape
		}
		return string(rune(value))
	})
	return strings.Contains(decoded, legacyKeyTargetBucket)
}

// checkJSLexicallySimple refuses code, with comments masked, whose tokens replay might read
// differently from the engine's script engine: a slash outside strings (a regular expression or a
// division), a backslash (an escaped identifier), a template literal, an HTML-like comment, or a
// non-ASCII character such as a Unicode line terminator or space.
func checkJSLexicallySimple(code string) error {
	for i := 0; i < len(code); i++ {
		c := code[i]
		switch {
		case isJSQuote(c):
			i = jsStringEnd(code, i)
		case c == '/' || c == '\\' || c == '`' || c >= 0x80 || strings.HasPrefix(code[i:], "<!--") || strings.HasPrefix(code[i:], "-->"):
			return fmt.Errorf("archived scenario sets storage.targetBucket and has %q outside strings and comments at offset %d; "+
				"replay reads a bucket set only from a scenario without regular expressions, divisions, escaped names, "+
				"template literals, HTML-like comments or non-ASCII characters outside strings", code[i:min(i+4, len(code))], i)
		}
	}
	return nil
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

// decodeJSString decodes a JavaScript string literal's body: the single-character escapes, \\xHH and
// \\uHHHH. A braced \\u{...} escape, a line continuation, an octal escape or a raw line break is
// refused.
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
	if err := checkJSNumericEscape(body, i); err != nil {
		return "", i, err
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

// checkJSNumericEscape refuses an escape other than \\x and \\u, and the braced \\u{...} form: the
// engine's script engine reads ECMAScript 5, which has none.
func checkJSNumericEscape(body string, i int) error {
	switch {
	case body[i] != 'x' && body[i] != 'u':
		return fmt.Errorf("an escape replay does not decode (\\%c)", body[i])
	case body[i] == 'u' && jsByteAt(body, i+1) == '{':
		return fmt.Errorf("a braced \\u{...} escape, which the engine's script engine does not accept")
	}
	return nil
}

// jsEscapeDigits returns the hexadecimal digits of the \\x or \\u escape whose letter is at i, and
// the index of the escape's last byte; no digits when the escape is cut short.
func jsEscapeDigits(body string, i int) (string, int) {
	switch {
	case body[i] == 'x' && i+2 < len(body):
		return body[i+1 : i+3], i + 2
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
