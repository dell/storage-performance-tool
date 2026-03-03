package textutil

import (
	"testing"
)

func TestRuneLen(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want int
	}{
		{"empty", "", 0},
		{"ascii", "hello", 5},
		{"spaces", "  ", 2},
		{"emoji", "🚀🔥", 2},
		{"cjk", "日本語", 3},
		{"mixed", "ab🚀cd", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RuneLen(tt.s); got != tt.want {
				t.Errorf("RuneLen(%q) = %d, want %d", tt.s, got, tt.want)
			}
		})
	}
}

func TestPadRight(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		width int
		want  string
	}{
		{"normal", "hi", 5, "hi   "},
		{"exact", "hello", 5, "hello"},
		{"exceeds", "hello!", 5, "hello!"},
		{"empty_string", "", 3, "   "},
		{"zero_width", "hi", 0, "hi"},
		{"emoji", "🚀", 4, "🚀   "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PadRight(tt.s, tt.width); got != tt.want {
				t.Errorf("PadRight(%q, %d) = %q, want %q", tt.s, tt.width, got, tt.want)
			}
		})
	}
}

func TestPadLeft(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		width int
		want  string
	}{
		{"normal", "hi", 5, "   hi"},
		{"exact", "hello", 5, "hello"},
		{"exceeds", "hello!", 5, "hello!"},
		{"empty_string", "", 3, "   "},
		{"zero_width", "hi", 0, "hi"},
		{"emoji", "🚀", 4, "   🚀"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PadLeft(tt.s, tt.width); got != tt.want {
				t.Errorf("PadLeft(%q, %d) = %q, want %q", tt.s, tt.width, got, tt.want)
			}
		})
	}
}

func TestTruncateWithEllipsis(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		maxWidth int
		want     string
	}{
		{"no_truncation", "hi", 10, "hi"},
		{"exact_fit", "hello", 5, "hello"},
		{"truncated", "hello world", 8, "hello..."},
		{"max_3_dots", "hello", 3, "..."},
		{"max_2_dots", "hello", 2, ".."},
		{"max_1_dot", "hello", 1, "."},
		{"max_0", "hello", 0, ""},
		{"empty_string", "", 5, ""},
		{"negative_width", "hello", -1, ""},
		{"emoji_truncated", "🚀🔥💧🌊", 3, "..."},
		{"emoji_fit", "🚀🔥", 5, "🚀🔥"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TruncateWithEllipsis(tt.s, tt.maxWidth); got != tt.want {
				t.Errorf("TruncateWithEllipsis(%q, %d) = %q, want %q", tt.s, tt.maxWidth, got, tt.want)
			}
		})
	}
}

func TestWrapWords(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		width int
		want  []string
	}{
		{
			"fits_single_line",
			"hello world",
			20,
			[]string{"hello world"},
		},
		{
			"wraps_at_width",
			"hello world foo",
			11,
			[]string{"hello world", "foo"},
		},
		{
			"preserves_hard_newline",
			"line one\nline two",
			40,
			[]string{"line one", "line two"},
		},
		{
			"empty_line_preserved",
			"above\n\nbelow",
			40,
			[]string{"above", "", "below"},
		},
		{
			"zero_width",
			"hello world",
			0,
			[]string{"hello world"},
		},
		{
			"single_long_word",
			"superlongword",
			5,
			[]string{"superlongword"},
		},
		{
			"multiple_wraps",
			"a b c d e f",
			3,
			[]string{"a b", "c d", "e f"},
		},
		{
			"empty_string",
			"",
			10,
			[]string{""},
		},
		{
			"whitespace_only",
			"   ",
			10,
			[]string{""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WrapWords(tt.text, tt.width)
			if len(got) != len(tt.want) {
				t.Fatalf("WrapWords(%q, %d) returned %d lines, want %d\ngot:  %q\nwant: %q",
					tt.text, tt.width, len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("WrapWords(%q, %d)[%d] = %q, want %q",
						tt.text, tt.width, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSpaces(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want string
	}{
		{"positive", 3, "   "},
		{"zero", 0, ""},
		{"negative", -1, ""},
		{"one", 1, " "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := spaces(tt.n); got != tt.want {
				t.Errorf("spaces(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

func TestRepeat(t *testing.T) {
	tests := []struct {
		name string
		char string
		n    int
		want string
	}{
		{"dots", ".", 3, "..."},
		{"zero", "x", 0, ""},
		{"negative", "x", -1, ""},
		{"empty_char", "", 5, ""},
		{"single", "a", 1, "a"},
		{"multi_rune_char", "ab", 4, "abab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := repeat(tt.char, tt.n); got != tt.want {
				t.Errorf("repeat(%q, %d) = %q, want %q", tt.char, tt.n, got, tt.want)
			}
		})
	}
}
