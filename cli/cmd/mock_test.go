package cmd

import (
	"testing"
)

func TestRunCommandMockMode(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		shouldError bool
		errorMsg    string
	}{
		{
			name:        "Mock mode without S3 flags",
			args:        []string{"run", "mock", "--duration", "30s"},
			shouldError: false,
		},
		{
			name:        "Mock mode with threads",
			args:        []string{"run", "mock", "--threads", "8", "--object-count", "100"},
			shouldError: false,
		},
		{
			name:        "Mock mode with S3 flags (ignored)",
			args:        []string{"run", "mock", "--endpoint", "http://test", "--access-key", "test", "--secret-key", "test", "--bucket", "test", "--duration", "1m"},
			shouldError: false,
		},
		{
			name:        "Mock mode without duration or object-count",
			args:        []string{"run", "mock"},
			shouldError: true,
			errorMsg:    ErrDurationOrCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootCmd := getTestRootCmd()
			_, output, err := executeCommand(t, rootCmd, tt.args...)

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error for test '%s', got none\nOutput: %s", tt.name, output)
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("Expected error '%s', got '%s'\nOutput: %s", tt.errorMsg, err.Error(), output)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for test '%s', got: %v\nOutput: %s", tt.name, err, output)
				}
			}
		})
	}
}
