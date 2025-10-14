// Package textutil provides helpers for rune-aware text alignment used by multiple
// presentation layers (TUI and results reporting).
package textutil

import "strings"

// RuneLen returns the rune length of a string (approximate column width for ASCII/emoji).
func RuneLen(s string) int {
	r := []rune(s)
	return len(r)
}

// PadRight appends spaces to s until its rune length reaches width.
func PadRight(s string, width int) string {
	rl := RuneLen(s)
	if rl >= width {
		return s
	}
	return s + spaces(width-rl)
}

// PadLeft prepends spaces to s until its rune length reaches width.
func PadLeft(s string, width int) string {
	rl := RuneLen(s)
	if rl >= width {
		return s
	}
	return spaces(width-rl) + s
}

// TruncateWithEllipsis shortens s to maxWidth runes, adding ellipsis when truncated.
func TruncateWithEllipsis(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= maxWidth {
		return s
	}
	if maxWidth <= 3 {
		return repeat(".", maxWidth)
	}
	return string(r[:maxWidth-3]) + "..."
}

func spaces(n int) string {
	if n <= 0 {
		return ""
	}
	return repeat(" ", n)
}

func repeat(char string, n int) string {
	if n <= 0 {
		return ""
	}
	runes := []rune(char)
	if len(runes) == 0 {
		return ""
	}
	buf := make([]rune, n)
	for i := range buf {
		buf[i] = runes[i%len(runes)]
	}
	return string(buf)
}

// WrapWords wraps text to the specified width using rune length. Input newlines
// are preserved and treated as hard breaks.
func WrapWords(text string, width int) []string {
	if width <= 0 {
		return []string{text}
	}
	segments := strings.Split(text, "\n")
	out := make([]string, 0, len(segments)*2)
	for _, segment := range segments {
		words := strings.Fields(segment)
		if len(words) == 0 {
			out = append(out, "")
			continue
		}
		current := words[0]
		for _, word := range words[1:] {
			candidate := current + " " + word
			if RuneLen(candidate) > width {
				out = append(out, current)
				current = word
				continue
			}
			current = candidate
		}
		out = append(out, current)
	}
	return out
}
