package cmd

import (
	"os"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/spf13/cobra"
)

func newRunLikeCmd() *cobra.Command {
	c := &cobra.Command{Use: "run"}
	c.Flags().String("endpoint", "", "")
	c.Flags().StringSlice("endpoints", []string{}, "")
	c.Flags().String("access-key", "", "")
	c.Flags().String("secret-key", "", "")
	c.Flags().String("bucket", "", "")
	c.Flags().Int("threads", 1, "")
	c.Flags().String("test-hosts", "127.0.0.1", "")
	c.Flags().Bool(flagSkipImagePull, false, "")
	// RDMA flags
	c.Flags().Bool("use-rdma", false, "")
	c.Flags().String("rdma-local-ip", "", "")
	c.Flags().Int64("rdma-threshold", 1048576, "")
	c.Flags().Bool("rdma-fallback", true, "")
	c.Flags().String("rdma-device", "auto", "")
	c.Flags().String("rdma-log-level", "WARN", "")
	c.Flags().Int64("rdma-timeout", 30000, "")
	return c
}

func newVerifyLikeCmd() *cobra.Command {
	c := &cobra.Command{Use: "verify"}
	c.Flags().String("test-hosts", "", "")
	c.Flags().Bool("use-rdma", false, "")
	return c
}

func TestApplyEnvDefaultsToRunFlags(t *testing.T) {
	cmd := newRunLikeCmd()

	// Seed env
	os.Setenv("S3_ENDPOINT", "http://env:9000")
	os.Setenv("S3_ACCESS_KEY", "AKIA")
	os.Setenv("S3_SECRET_KEY", "SECRET")
	os.Setenv("S3_BUCKET", "bucket1")
	os.Setenv("HOSTS", "h1,h2")
	os.Setenv(constants.EnvSkipImagePull, "1")
	t.Cleanup(func() {
		os.Unsetenv("S3_ENDPOINT")
		os.Unsetenv("S3_ACCESS_KEY")
		os.Unsetenv("S3_SECRET_KEY")
		os.Unsetenv("S3_BUCKET")
		os.Unsetenv("HOSTS")
		os.Unsetenv(constants.EnvSkipImagePull)
	})

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}

	eps, _ := cmd.Flags().GetStringSlice("endpoints")
	if len(eps) != 1 || eps[0] != "http://env:9000" {
		t.Fatalf("endpoints not applied from env, got %v", eps)
	}
	if v, _ := cmd.Flags().GetString("endpoint"); v != "" {
		t.Fatalf("legacy endpoint flag should remain unset, got %q", v)
	}
	if v, _ := cmd.Flags().GetString("access-key"); v != "AKIA" {
		t.Fatalf("access-key not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetString("secret-key"); v != "SECRET" {
		t.Fatalf("secret-key not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetString("bucket"); v != "bucket1" {
		t.Fatalf("bucket not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetString("test-hosts"); v != "h1,h2" {
		t.Fatalf("test-hosts not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetBool(flagSkipImagePull); !v {
		t.Fatalf("skip-image-pull flag not applied from env")
	}
}

func TestApplyEnvDefaultsToRunFlags_S3Endpoints(t *testing.T) {
	cmd := newRunLikeCmd()
	// Only S3_ENDPOINTS is set; ensure endpoints are used and single endpoint not set
	os.Setenv("S3_ENDPOINTS", "http://s3a:9000,http://s3b:9000")
	t.Cleanup(func() { os.Unsetenv("S3_ENDPOINTS") })

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}
	eps, _ := cmd.Flags().GetStringSlice("endpoints")
	if len(eps) != 2 || eps[0] != "http://s3a:9000" || eps[1] != "http://s3b:9000" {
		t.Fatalf("endpoints not applied from env, got %v", eps)
	}
	// endpoint should remain empty to avoid conflict
	if v, _ := cmd.Flags().GetString("endpoint"); v != "" {
		t.Fatalf("single endpoint should not be set when S3_ENDPOINTS present, got %q", v)
	}
}

func TestApplyEnvDefaultsToRunFlags_EndpointsFlagWins(t *testing.T) {
	cmd := newRunLikeCmd()
	// User provided endpoints flag; env should not override
	_ = cmd.Flags().Set("endpoints", "http://cli1:9000,http://cli2:9000")
	os.Setenv("S3_ENDPOINTS", "http://env1:9000,http://env2:9000")
	t.Cleanup(func() { os.Unsetenv("S3_ENDPOINTS") })

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}
	eps, _ := cmd.Flags().GetStringSlice("endpoints")
	if len(eps) != 2 || eps[0] != "http://cli1:9000" || eps[1] != "http://cli2:9000" {
		t.Fatalf("endpoints env should not override flag, got %v", eps)
	}
}

func TestApplyEnvDefaults_RespectUserFlags(t *testing.T) {
	cmd := newRunLikeCmd()
	// User provided an explicit flag
	cmd.Flags().Set("endpoint", "http://cli:9000")
	cmd.Flags().Set(flagSkipImagePull, "true")
	os.Setenv("S3_ENDPOINT", "http://env:9000")
	os.Setenv(constants.EnvSkipImagePull, "0")
	t.Cleanup(func() {
		os.Unsetenv("S3_ENDPOINT")
		os.Unsetenv(constants.EnvSkipImagePull)
	})

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}

	if v, _ := cmd.Flags().GetString("endpoint"); v != "http://cli:9000" {
		t.Fatalf("env should not override user flag, got %q", v)
	}
	eps, _ := cmd.Flags().GetStringSlice("endpoints")
	if len(eps) != 0 {
		t.Fatalf("endpoints slice should remain user-provided (empty), got %v", eps)
	}
	if v, _ := cmd.Flags().GetBool(flagSkipImagePull); !v {
		t.Fatalf("env should not override skip-image-pull flag")
	}
}

func TestApplyEnvDefaultsToRunFlags_ThreadsFromEnv(t *testing.T) {
	cmd := newRunLikeCmd()
	// Ensure user did not set --threads
	// Provide THREADS through env
	os.Setenv("THREADS", "16")
	t.Cleanup(func() { os.Unsetenv("THREADS") })

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}

	if v, _ := cmd.Flags().GetInt("threads"); v != 16 {
		t.Fatalf("threads not applied from THREADS env, got %d want 16", v)
	}
}

func TestApplyEnvDefaultsToVerifyFlags(t *testing.T) {
	cmd := newVerifyLikeCmd()
	os.Setenv("HOSTS", "v1,v2")
	t.Cleanup(func() { os.Unsetenv("HOSTS") })

	if err := applyEnvDefaultsToVerifyFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToVerifyFlags error: %v", err)
	}
	if v, _ := cmd.Flags().GetString("test-hosts"); v != "v1,v2" {
		t.Fatalf("verify test-hosts not applied from env, got %q", v)
	}

	// Now ensure explicit flag wins
	cmd = newVerifyLikeCmd()
	_ = cmd.Flags().Set("test-hosts", "cli1,cli2")
	if err := applyEnvDefaultsToVerifyFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToVerifyFlags error: %v", err)
	}
	if v, _ := cmd.Flags().GetString("test-hosts"); v != "cli1,cli2" {
		t.Fatalf("user flag should win, got %q", v)
	}
}

func TestApplyEnvDefaultsToRunFlags_RdmaFromEnv(t *testing.T) {
	cmd := newRunLikeCmd()

	os.Setenv(constants.EnvRdmaEnabled, "true")
	os.Setenv(constants.EnvRdmaLocalIP, "10.247.128.125")
	os.Setenv(constants.EnvRdmaDevice, "mlx5_0")
	os.Setenv(constants.EnvRdmaLogLevel, "DEBUG")
	os.Setenv(constants.EnvRdmaThreshold, "4194304")
	os.Setenv(constants.EnvRdmaTimeout, "60000")
	os.Setenv(constants.EnvRdmaFallback, "false")
	t.Cleanup(func() {
		os.Unsetenv(constants.EnvRdmaEnabled)
		os.Unsetenv(constants.EnvRdmaLocalIP)
		os.Unsetenv(constants.EnvRdmaDevice)
		os.Unsetenv(constants.EnvRdmaLogLevel)
		os.Unsetenv(constants.EnvRdmaThreshold)
		os.Unsetenv(constants.EnvRdmaTimeout)
		os.Unsetenv(constants.EnvRdmaFallback)
	})

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}

	if v, _ := cmd.Flags().GetBool("use-rdma"); !v {
		t.Fatal("use-rdma not applied from SPT_RDMA env")
	}
	if v, _ := cmd.Flags().GetString("rdma-local-ip"); v != "10.247.128.125" {
		t.Fatalf("rdma-local-ip not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetString("rdma-device"); v != "mlx5_0" {
		t.Fatalf("rdma-device not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetString("rdma-log-level"); v != "DEBUG" {
		t.Fatalf("rdma-log-level not applied from env, got %q", v)
	}
	if v, _ := cmd.Flags().GetInt64("rdma-threshold"); v != 4194304 {
		t.Fatalf("rdma-threshold not applied from env, got %d", v)
	}
	if v, _ := cmd.Flags().GetInt64("rdma-timeout"); v != 60000 {
		t.Fatalf("rdma-timeout not applied from env, got %d", v)
	}
	if v, _ := cmd.Flags().GetBool("rdma-fallback"); v {
		t.Fatal("rdma-fallback not applied from env (expected false)")
	}
}

func TestApplyEnvDefaultsToRunFlags_RdmaFlagWinsOverEnv(t *testing.T) {
	cmd := newRunLikeCmd()

	// User explicitly sets --use-rdma=false on CLI
	_ = cmd.Flags().Set("use-rdma", "false")

	// Env says true
	os.Setenv(constants.EnvRdmaEnabled, "true")
	t.Cleanup(func() { os.Unsetenv(constants.EnvRdmaEnabled) })

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}

	// Explicit CLI flag should win
	if v, _ := cmd.Flags().GetBool("use-rdma"); v {
		t.Fatal("CLI flag --use-rdma=false should win over SPT_RDMA=true env")
	}
}

func TestApplyEnvDefaultsToRunFlags_RdmaInvalidThreshold(t *testing.T) {
	cmd := newRunLikeCmd()

	os.Setenv(constants.EnvRdmaThreshold, "not-a-number")
	t.Cleanup(func() { os.Unsetenv(constants.EnvRdmaThreshold) })

	err := applyEnvDefaultsToRunFlags(cmd)
	if err == nil {
		t.Fatal("expected error for invalid RDMA_THRESHOLD_BYTES value")
	}
	if !strings.Contains(err.Error(), constants.EnvRdmaThreshold) {
		t.Errorf("error should mention %s, got: %v", constants.EnvRdmaThreshold, err)
	}
}

func TestApplyEnvDefaultsToRunFlags_RdmaInvalidTimeout(t *testing.T) {
	cmd := newRunLikeCmd()

	os.Setenv(constants.EnvRdmaTimeout, "abc")
	t.Cleanup(func() { os.Unsetenv(constants.EnvRdmaTimeout) })

	err := applyEnvDefaultsToRunFlags(cmd)
	if err == nil {
		t.Fatal("expected error for invalid RDMA_TIMEOUT_MS value")
	}
	if !strings.Contains(err.Error(), constants.EnvRdmaTimeout) {
		t.Errorf("error should mention %s, got: %v", constants.EnvRdmaTimeout, err)
	}
}

func TestApplyEnvDefaultsToRunFlags_RdmaInvalidBoolIgnored(t *testing.T) {
	cmd := newRunLikeCmd()

	// Invalid bool for SPT_RDMA should be silently ignored (not error)
	os.Setenv(constants.EnvRdmaEnabled, "maybe")
	t.Cleanup(func() { os.Unsetenv(constants.EnvRdmaEnabled) })

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("expected no error for invalid SPT_RDMA bool, got: %v", err)
	}

	// Flag should remain at default (false)
	if v, _ := cmd.Flags().GetBool("use-rdma"); v {
		t.Fatal("use-rdma should remain false when SPT_RDMA has invalid bool value")
	}
}

func TestApplyEnvDefaultsToRunFlags_RdmaInvalidFallbackIgnored(t *testing.T) {
	cmd := newRunLikeCmd()

	// Invalid bool for RDMA_FALLBACK_ENABLED should be silently ignored
	os.Setenv(constants.EnvRdmaFallback, "sometimes")
	t.Cleanup(func() { os.Unsetenv(constants.EnvRdmaFallback) })

	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("expected no error for invalid RDMA_FALLBACK_ENABLED, got: %v", err)
	}

	// Flag should remain at default (true)
	if v, _ := cmd.Flags().GetBool("rdma-fallback"); !v {
		t.Fatal("rdma-fallback should remain true (default) when env has invalid bool")
	}
}

func TestApplyEnvDefaultsToVerifyFlags_RdmaFromEnv(t *testing.T) {
	cmd := newVerifyLikeCmd()

	os.Setenv(constants.EnvRdmaEnabled, "1")
	t.Cleanup(func() { os.Unsetenv(constants.EnvRdmaEnabled) })

	if err := applyEnvDefaultsToVerifyFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToVerifyFlags error: %v", err)
	}

	if v, _ := cmd.Flags().GetBool("use-rdma"); !v {
		t.Fatal("use-rdma not applied from SPT_RDMA env for verify")
	}
}
