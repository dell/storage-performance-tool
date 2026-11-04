package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const environmentTestEndpoint = "http://storage.example.invalid:8333"

func writeS3EnvFixture(t *testing.T, a *app, overrides map[string]string) {
	t.Helper()
	values := map[string]string{
		"S3_ENDPOINTS": environmentTestEndpoint, "S3_ENDPOINT": "",
		"S3_BUCKET": "dashboard-env-test", "S3_ACCESS_KEY": "fixture-access", "S3_SECRET_KEY": "fixture-secret",
	}
	for key, value := range overrides {
		values[key] = value
	}
	var body strings.Builder
	for _, key := range []string{"S3_ENDPOINTS", "S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"} {
		body.WriteString(key + "=" + values[key] + "\n")
	}
	writeEnvFixture(t, a.root, body.String())
}

func newEnvironmentConfiguredApp(t *testing.T) (*app, string) {
	t.Helper()
	a, marker := newRunHandlerTestApp(t)
	a.home = t.TempDir()
	for _, key := range []string{"S3_ENDPOINTS", "S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"} {
		unsetEnvFixture(t, key)
	}
	writeS3EnvFixture(t, a, nil)
	return a, marker
}

func TestEnvironmentTargetSelectionAndReadiness(t *testing.T) {
	for _, tt := range []struct {
		name, savedEndpoint, endpoint, source, reason string
		overrides                                     map[string]string
		setup                                         func(*testing.T, *app)
	}{
		{name: "plain HTTP", endpoint: environmentTestEndpoint, source: "env"},
		{name: "HTTPS", overrides: map[string]string{"S3_ENDPOINTS": "https://storage.example.invalid:8333"}, endpoint: "https://storage.example.invalid:8333", source: "env"},
		{name: "plural setting wins", overrides: map[string]string{"S3_ENDPOINT": "https://unused.example.invalid"}, endpoint: environmentTestEndpoint, source: "env"},
		{name: "singular fallback", overrides: map[string]string{"S3_ENDPOINTS": "", "S3_ENDPOINT": "http://fallback.example.invalid:9000"}, endpoint: "http://fallback.example.invalid:9000", source: "env"},
		{name: "empty list entries ignored", overrides: map[string]string{"S3_ENDPOINTS": " , " + environmentTestEndpoint + " , "}, endpoint: environmentTestEndpoint, source: "env"},
		{name: "IPv6 endpoint", overrides: map[string]string{"S3_ENDPOINTS": "http://[2001:db8::1]:8333"}, endpoint: "http://[2001:db8::1]:8333", source: "env"},
		{name: "saved endpoint wins", savedEndpoint: "https://saved.example.invalid:8333", endpoint: "https://saved.example.invalid:8333", source: "dashboard"},
		{name: "saved endpoint replaces environment list", savedEndpoint: "http://saved.example.invalid:9000", overrides: map[string]string{"S3_ENDPOINTS": "http://one.invalid,http://two.invalid"}, endpoint: "http://saved.example.invalid:9000", source: "dashboard"},
		{name: "process endpoint wins over file", setup: func(t *testing.T, _ *app) { t.Setenv("S3_ENDPOINTS", "https://process.example.invalid") }, endpoint: "https://process.example.invalid", source: "env"},
		{name: "missing endpoint", overrides: map[string]string{"S3_ENDPOINTS": ""}, reason: "exactly one S3 endpoint"},
		{name: "multiple endpoints", overrides: map[string]string{"S3_ENDPOINTS": "http://one.invalid,http://two.invalid"}, reason: "exactly one S3 endpoint"},
		{name: "unsupported protocol", overrides: map[string]string{"S3_ENDPOINTS": "ftp://storage.example.invalid"}, reason: "valid HTTP or HTTPS"},
		{name: "credentials in URL", overrides: map[string]string{"S3_ENDPOINTS": "http://user:URL_PRIVATE_CANARY@storage.example.invalid"}, reason: "without embedded credentials"},
		{name: "query in URL", overrides: map[string]string{"S3_ENDPOINTS": "http://storage.example.invalid?token=URL_PRIVATE_CANARY"}, reason: "valid HTTP or HTTPS"},
		{name: "fragment in URL", overrides: map[string]string{"S3_ENDPOINTS": "http://storage.example.invalid#fragment"}, reason: "valid HTTP or HTTPS"},
		{name: "missing bucket", overrides: map[string]string{"S3_BUCKET": ""}, reason: "valid S3 bucket name"},
		{name: "invalid bucket", overrides: map[string]string{"S3_BUCKET": "bucket/name"}, reason: "valid S3 bucket name"},
		{name: "missing access key", overrides: map[string]string{"S3_ACCESS_KEY": ""}, reason: "S3_ACCESS_KEY and S3_SECRET_KEY"},
		{name: "missing secret key", overrides: map[string]string{"S3_SECRET_KEY": ""}, reason: "S3_ACCESS_KEY and S3_SECRET_KEY"},
		{name: "empty process key masks file", setup: func(t *testing.T, _ *app) { t.Setenv("S3_SECRET_KEY", "") }, reason: "S3_ACCESS_KEY and S3_SECRET_KEY"},
		{name: "missing SPT", setup: func(_ *testing.T, a *app) { a.sptBin = filepath.Join(a.root, "missing-spt") }, reason: "SPT is not executable"},
		{name: "SPT is a directory", setup: func(_ *testing.T, a *app) { a.sptBin = a.root }, reason: "SPT is not executable"},
		{name: "missing Docker", setup: func(t *testing.T, a *app) {
			if err := os.Remove(filepath.Join(a.root, "bin", "docker")); err != nil {
				t.Fatal(err)
			}
		}, reason: "Docker is not installed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker := newEnvironmentConfiguredApp(t)
			writeS3EnvFixture(t, a, tt.overrides)
			a.storage.Endpoint = tt.savedEndpoint
			if tt.setup != nil {
				tt.setup(t, a)
			}
			target := a.targetConfiguration()
			reasons, ok := target["reasons"].([]string)
			if !ok {
				t.Fatal("readiness reasons were not returned")
			}
			if got := target["ready"]; got != (tt.reason == "") {
				t.Errorf("ready = %v, reasons = %v", got, reasons)
			}
			if tt.reason != "" {
				if !strings.Contains(strings.Join(reasons, " "), tt.reason) {
					t.Errorf("reasons = %v, want explanation containing %q", reasons, tt.reason)
				}
			} else {
				if len(reasons) != 0 || target["endpoint"] != tt.endpoint || target["endpoint_source"] != tt.source {
					t.Errorf("wrong endpoint selection: endpoint=%v source=%v reasons=%v", target["endpoint"], target["endpoint_source"], reasons)
				}
				if target["credentials_configured"] != true {
					t.Error("configured credentials were not recognized")
				}
			}
			// Report only safe metadata; never expose credentials embedded in an invalid URL.
			if strings.Contains(str(target["endpoint"])+str(target["endpoint_label"])+strings.Join(reasons, " "), "URL_PRIVATE_CANARY") {
				t.Error("target readiness exposed a credential canary")
			}
			assertNotInvoked(t, marker)
		})
	}
}

func TestEnvironmentReadinessReportsAllMissingPrerequisites(t *testing.T) {
	a, marker := newEnvironmentConfiguredApp(t)
	writeS3EnvFixture(t, a, map[string]string{"S3_ENDPOINTS": "", "S3_BUCKET": "", "S3_ACCESS_KEY": "", "S3_SECRET_KEY": ""})
	a.sptBin = filepath.Join(a.root, "missing-spt")
	if err := os.Remove(filepath.Join(a.root, "bin", "docker")); err != nil {
		t.Fatal(err)
	}
	target := a.targetConfiguration()
	reasons, ok := target["reasons"].([]string)
	if !ok || len(reasons) != 5 || target["ready"] != false {
		t.Fatalf("readiness should report five independent missing prerequisites: %v", target["reasons"])
	}
	for _, want := range []string{"exactly one S3 endpoint", "valid S3 bucket name", "S3_ACCESS_KEY and S3_SECRET_KEY", "SPT is not executable", "Docker is not installed"} {
		if !strings.Contains(strings.Join(reasons, " "), want) {
			t.Errorf("missing prerequisite explanation %q", want)
		}
	}
	assertNotInvoked(t, marker)
}
