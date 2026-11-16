/*
Copyright © 2026 Dell Technologies
*/

// Package engineformat checks the syntax of size and duration values before they reach the
// engine. Each check mirrors an engine parser and may be wider than it, never narrower: a value
// the engine accepts must always pass, because refusing it would break a command line that runs
// today. The shared fixture under engine/core/spt-base/src/test/resources/engine-format/ holds
// the engine's accepted values; a JUnit test keeps it equal to the engine and a Go test proves
// these checks accept every line of it.
package engineformat

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// ErrSize and ErrDuration are the reasons CheckSize and CheckDuration refuse a value.
var (
	ErrSize     = errors.New("not a size the engine can parse")
	ErrDuration = errors.New("not a time the engine can parse")
)

// sizeBound is SizeInBytes.toFixedSize's pattern, ([\d\.]+)([kmgtpe]?)b?, applied to a bound
// that is already ASCII lower case. Java's \d is ASCII-only, as here.
var sizeBound = regexp.MustCompile(`^([0-9.]+)[kmgtpe]?b?$`)

// The two TimeUtil patterns, ([0-9]*)([smhdSMHD]?) and ([0-9]*)\.([a-zA-Z]{4,7}).
var (
	timeShort  = regexp.MustCompile(`^[0-9]*[smhdSMHD]?$`)
	timeCompat = regexp.MustCompile(`^[0-9]*\.([a-zA-Z]{4,7})$`)
)

// timeUnitNames are the java.util.concurrent.TimeUnit names that fit the compat pattern's 4 to 7
// letters. TimeUtil upper-cases the letters and calls TimeUnit.valueOf.
var timeUnitNames = map[string]struct{}{
	"DAYS": {}, "HOURS": {}, "MINUTES": {}, "SECONDS": {},
}

// CheckSize returns ErrSize when the engine's BinarySizeFormat.parseSize would refuse value for
// its syntax. It checks the trimmed value; an empty one, and one with any non-ASCII byte, is not
// checked. It never tests the order of a range or an overflow, and never reads a bias.
func CheckSize(value string) error {
	value = strings.TrimSpace(value)
	if value == "" || !isASCII(value) {
		return nil
	}
	value = dropIECInfix(value)
	dash := strings.IndexByte(value, '-')
	if dash < 0 {
		// SizeInBytes reads the whole value as one bound, so a bias without a range is refused.
		return checkSizeBound(value)
	}
	comma := strings.IndexByte(value, ',')
	if comma >= 0 && comma < dash {
		// A ',' before the first '-' is not a form SizeInBytes reads; leave it to the engine.
		return nil
	}
	if err := checkSizeBound(value[:dash]); err != nil {
		return err
	}
	upper := value[dash+1:]
	if comma >= 0 {
		// The text after the first ',' is the bias, read by Double.parseDouble: not checked.
		upper = value[dash+1 : comma]
	}
	return checkSizeBound(upper)
}

// CheckDuration returns ErrDuration when the engine's TimeUtil.getTimeInSeconds would refuse
// value. It checks the trimmed value; an empty one, and one with any non-ASCII byte, is not
// checked. An empty or overflowing number is 0 in the engine, so it is never refused here.
func CheckDuration(value string) error {
	value = strings.TrimSpace(value)
	if value == "" || !isASCII(value) {
		return nil
	}
	if timeShort.MatchString(value) {
		return nil
	}
	if m := timeCompat.FindStringSubmatch(value); m != nil {
		if _, ok := timeUnitNames[strings.ToUpper(m[1])]; ok {
			return nil
		}
	}
	return ErrDuration
}

// checkSizeBound mirrors SizeInBytes.toFixedSize for one bound: lower-case it, fully match the
// pattern, and parse the number with Double.parseDouble. An out-of-range number saturates in the
// engine, so ErrRange is accepted.
func checkSizeBound(bound string) error {
	m := sizeBound.FindStringSubmatch(strings.ToLower(bound))
	if m == nil {
		return ErrSize
	}
	if _, err := strconv.ParseFloat(m[1], 64); err != nil && !errors.Is(err, strconv.ErrRange) {
		return ErrSize
	}
	return nil
}

// dropIECInfix is BinarySizeFormat's (?i)([kmgtpe])i(?=b) to $1: an ASCII case-insensitive unit
// letter followed by i and then b loses the i. Go's regexp has no lookahead, so it is by hand.
func dropIECInfix(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		b.WriteByte(value[i])
		if isUnitLetter(value[i]) && i+2 < len(value) && lower(value[i+1]) == 'i' &&
			lower(value[i+2]) == 'b' {
			i++ // skip the i; the b is written next round
		}
	}
	return b.String()
}

func isUnitLetter(c byte) bool {
	return strings.IndexByte("kmgtpe", lower(c)) >= 0
}

func lower(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

func isASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] >= 0x80 {
			return false
		}
	}
	return true
}
