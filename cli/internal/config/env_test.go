package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// helper to create a temp dir and chdir into it for test isolation
func withTempDir(t *testing.T, fn func(dir string)) {
	t.Helper()
	cwd, _ := os.Getwd()
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(cwd)
	fn(dir)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func unsetEnv(t *testing.T, keys ...string) func() {
	t.Helper()
	prev := map[string]*string{}
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			vv := v
			prev[k] = &vv
		} else {
			prev[k] = nil
		}
		_ = os.Unsetenv(k)
	}
	return func() {
		for k, v := range prev {
			if v == nil {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, *v)
			}
		}
	}
}

func TestLoadDotEnv_HomeOnly(t *testing.T) {
	withTempDir(t, func(dir string) {
		// Simulate $HOME with a .env
		home := filepath.Join(dir, "home")
		if err := os.MkdirAll(home, 0700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		t.Setenv("HOME", home)
		writeFile(t, filepath.Join(home, ".env"), "HOSTS=home1,home2\nS3_ENDPOINT=http://home:9000\n")

		// Ensure variables are not already set
		restore := unsetEnv(t, "HOSTS", "S3_ENDPOINT")
		defer restore()

		res := LoadDotEnv()
		if len(res.LoadedFiles) != 1 {
			t.Fatalf("expected 1 loaded file, got %v", res.LoadedFiles)
		}
		if got := os.Getenv("HOSTS"); got != "home1,home2" {
			t.Fatalf("HOSTS not loaded from home .env, got %q", got)
		}
		if got := os.Getenv("S3_ENDPOINT"); got != "http://home:9000" {
			t.Fatalf("S3_ENDPOINT not loaded from home .env, got %q", got)
		}
	})
}

func TestLoadDotEnv_LocalOverridesHome_AndRespectOS(t *testing.T) {
	withTempDir(t, func(dir string) {
		// HOME/.env with initial values
		home := filepath.Join(dir, "home")
		if err := os.MkdirAll(home, 0700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		t.Setenv("HOME", home)
		writeFile(t, filepath.Join(home, ".env"), "HOSTS=home1\nS3_ENDPOINT=http://home:9000\nS3_BUCKET=homebucket\n")

		// Local .env overriding HOSTS and S3_ENDPOINT
		writeFile(t, filepath.Join(dir, ".env"), "HOSTS=local1,local2\nS3_ENDPOINT=\"http://local:9443\"\n")

		// Pre-set an OS env var that should NOT be overridden by .env
		t.Setenv("S3_ACCESS_KEY", "fromOS")
		// Clear others
		restore := unsetEnv(t, "HOSTS", "S3_ENDPOINT", "S3_BUCKET", "S3_SECRET_KEY")
		defer restore()

		res := LoadDotEnv()
		if len(res.LoadedFiles) != 2 {
			t.Fatalf("expected 2 loaded files, got %v", res.LoadedFiles)
		}
		if got := os.Getenv("HOSTS"); got != "local1,local2" {
			t.Fatalf("HOSTS should come from local .env, got %q", got)
		}
		if got := os.Getenv("S3_ENDPOINT"); got != "http://local:9443" {
			t.Fatalf("S3_ENDPOINT should come from local .env, got %q", got)
		}
		if got := os.Getenv("S3_BUCKET"); got != "homebucket" {
			t.Fatalf("S3_BUCKET should come from home .env, got %q", got)
		}
		if got := os.Getenv("S3_ACCESS_KEY"); got != "fromOS" {
			t.Fatalf("S3_ACCESS_KEY should respect existing OS env, got %q", got)
		}

		// Windows path sanity (no-op; test retains portability)
		_ = runtime.GOOS
		_ = res
	})
}

func TestLoadDotEnv_VariableExpansionWithinFile(t *testing.T) {
	withTempDir(t, func(dir string) {
		// Expansion should work for variables defined earlier in the same file
		content := "" +
			"PRIMARY_HOST=phost\n" +
			"WORKER_HOSTS=w1,w2\n" +
			"HOSTS=${PRIMARY_HOST},${WORKER_HOSTS}\n"
		writeFile(t, filepath.Join(dir, ".env"), content)

		// Clear possibly preset envs and set PWD for deterministic expansion
		restore := unsetEnv(t, "PRIMARY_HOST", "WORKER_HOSTS", "HOSTS")
		defer restore()

		_ = LoadDotEnv()
		if got := os.Getenv("HOSTS"); got != "phost,w1,w2" {
			t.Fatalf("HOSTS expansion failed, got %q", got)
		}
	})
}
