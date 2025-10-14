package tui

import (
	"testing"
)

func TestANSIStripCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "No ANSI codes",
			input:    "150234.487|250714150234|CREATE|0|0.0|0|0|0.001|0.0|0.0|0|0",
			expected: "150234.487|250714150234|CREATE|0|0.0|0|0|0.001|0.0|0.0|0|0",
		},
		{
			name:     "Standard reset sequence",
			input:    "\x1b[0mHello\x1b[0m",
			expected: "Hello",
		},
		{
			name:     "256-color sequence",
			input:    "\x1b[38;5;67mCREATE\x1b[0m",
			expected: "CREATE",
		},
		{
			name:     "RGB color sequence",
			input:    "\x1b[38;2;0;200;0m     0\x1b[0m",
			expected: "     0",
		},
		{
			name:     "Short reset sequence \\x1b[m",
			input:    "\x1b[mText\x1b[m",
			expected: "Text",
		},
		{
			name:     "Multiple reset sequences",
			input:    "\x1b[m\x1b[0m150234.487",
			expected: "150234.487",
		},
		{
			name:     "Complex S3 line",
			input:    "\x1b[m\x1b[0m150234.487|250714150244|\x1b[38;5;67mCREATE\x1b[0m|         4|3.80612244|        1652|\x1b[38;2;0;200;0m     0\x1b[0m|10.001 |179.9236|179.923|     20589|      22839",
			expected: "150234.487|250714150244|CREATE|         4|3.80612244|        1652|     0|10.001 |179.9236|179.923|     20589|      22839",
		},
		{
			name:     "Bold text",
			input:    "\x1b[30;1mBold text\x1b[0m",
			expected: "Bold text",
		},
		{
			name:     "S3 final summary line",
			input:    "\x1b[m\x1b[0m---",
			expected: "---",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripANSIEscapeSequences(tt.input)
			if result != tt.expected {
				t.Errorf("stripANSIEscapeSequences(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
