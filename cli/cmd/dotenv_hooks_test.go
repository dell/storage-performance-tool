package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/spf13/cobra"
)

// helper: create temp dir and chdir for the duration of the closure
func withTempDir(t *testing.T, fn func(dir string)) {
	t.Helper()
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(cwd)
	fn(dir)
}

func Test_TopLevelCommands_InvokeDotEnvLoader(t *testing.T) {
	// Disable root-level loader so we verify subcommands call it themselves.
	original := rootCmd.PersistentPreRunE
	rootCmd.PersistentPreRunE = func(*cobra.Command, []string) error { return nil }
	defer func() { rootCmd.PersistentPreRunE = original }()

	withTempDir(t, func(dir string) {
		// Prepare a .env with a known key
		envContent := "DOTENV_SENTINEL=from_env\n"
		if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(envContent), 0600); err != nil {
			t.Fatalf("write .env: %v", err)
		}

		// Ensure OS doesn't already have the key
		_ = os.Unsetenv("DOTENV_SENTINEL")

		// Iterate all top-level commands
		for _, c := range rootCmd.Commands() {
			// Reset loader counter and env var
			config.ResetDotEnvLoadedForTests()
			_ = os.Unsetenv("DOTENV_SENTINEL")

			// Command must define a PersistentPreRunE to load .env
			if c.PersistentPreRunE == nil {
				t.Fatalf("top-level command %q is missing PersistentPreRunE to load .env", c.Name())
			}

			// Invoke the command's persistent pre-run; it should load .env
			if err := c.PersistentPreRunE(c, nil); err != nil {
				t.Fatalf("command %q PersistentPreRunE error: %v", c.Name(), err)
			}

			if !config.WasDotEnvLoaded() {
				t.Fatalf("top-level command %q did not call LoadDotEnv", c.Name())
			}

			if got := os.Getenv("DOTENV_SENTINEL"); got != "from_env" {
				t.Fatalf("command %q did not apply .env vars, got DOTENV_SENTINEL=%q", c.Name(), got)
			}
		}
	})
}

func Test_DotEnvDoesNotOverrideOSEnv(t *testing.T) {
	original := rootCmd.PersistentPreRunE
	rootCmd.PersistentPreRunE = func(*cobra.Command, []string) error { return nil }
	defer func() { rootCmd.PersistentPreRunE = original }()

	withTempDir(t, func(dir string) {
		// .env attempts to set a key already present in OS env
		envContent := "EXISTING_KEY=from_env\n"
		if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(envContent), 0600); err != nil {
			t.Fatalf("write .env: %v", err)
		}
		t.Setenv("EXISTING_KEY", "from_os")

		for _, c := range rootCmd.Commands() {
			config.ResetDotEnvLoadedForTests()
			if c.PersistentPreRunE == nil {
				t.Fatalf("top-level command %q is missing PersistentPreRunE to load .env", c.Name())
			}
			if err := c.PersistentPreRunE(c, nil); err != nil {
				t.Fatalf("command %q PersistentPreRunE error: %v", c.Name(), err)
			}
			if got := os.Getenv("EXISTING_KEY"); got != "from_os" {
				t.Fatalf("command %q should not override OS env: got %q", c.Name(), got)
			}
		}
	})
}
