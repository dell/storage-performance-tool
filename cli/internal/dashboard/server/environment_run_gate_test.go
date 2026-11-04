package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func requestEnvironmentRun(a *app) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader(validRunRequest))
	request.Host = "localhost:8000"
	request.Header.Set("Origin", "http://localhost:8000")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	a.httpServer().Handler.ServeHTTP(recorder, request)
	return recorder
}

func TestEnvironmentRunGateRejectsUnsafeConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name, reason string
		overrides    map[string]string
	}{
		{"missing credentials", "S3_ACCESS_KEY and S3_SECRET_KEY", map[string]string{"S3_SECRET_KEY": ""}},
		{"invalid bucket", "valid S3 bucket name", map[string]string{"S3_BUCKET": "bucket/name"}},
		{"multiple endpoints", "exactly one S3 endpoint", map[string]string{"S3_ENDPOINTS": "http://one.invalid,http://two.invalid"}},
		{"credentials in endpoint", "without embedded credentials", map[string]string{"S3_ENDPOINTS": "http://user:RUN_PRIVATE_CANARY@storage.example.invalid"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker := newEnvironmentConfiguredApp(t)
			writeS3EnvFixture(t, a, tt.overrides)
			recorder := requestEnvironmentRun(a)
			assertRejectedRun(t, a, marker, recorder, http.StatusServiceUnavailable)
			if !strings.Contains(recorder.Body.String(), tt.reason) {
				t.Errorf("rejection did not explain %q", tt.reason)
			}
			if strings.Contains(recorder.Body.String(), "RUN_PRIVATE_CANARY") {
				t.Error("run rejection exposed credentials from the configured endpoint")
			}
		})
	}
}

func TestEnvironmentChangesUpdateReadinessAndRequireMatchingVerification(t *testing.T) {
	a, marker := newEnvironmentConfiguredApp(t)
	a.verification = verification{}
	writeS3EnvFixture(t, a, map[string]string{"S3_SECRET_KEY": ""})
	assertRejectedRun(t, a, marker, requestEnvironmentRun(a), http.StatusServiceUnavailable)

	// A corrected file is reread without restarting the dashboard. Readiness alone
	// must not bypass the separate verification requirement.
	writeS3EnvFixture(t, a, nil)
	if target := a.targetConfiguration(); target["ready"] != true {
		t.Fatal("corrected configuration did not restore target readiness")
	}
	assertRejectedRun(t, a, marker, requestEnvironmentRun(a), http.StatusConflict)

	a.verification = verification{Status: verifyPassed, Signature: configSignature(a.storageSnapshot().Hosts, environmentTestEndpoint)}
	if !a.verified(a.targetConfiguration()) {
		t.Fatal("verification did not match the original configuration")
	}
	writeS3EnvFixture(t, a, map[string]string{"S3_ENDPOINTS": "https://changed.example.invalid:8333"})
	recorder := requestEnvironmentRun(a)
	assertRejectedRun(t, a, marker, recorder, http.StatusConflict)
	if !strings.Contains(recorder.Body.String(), "Verify the storage configuration") {
		t.Error("changed endpoint did not require another verification")
	}

	// Process settings override the corrected file, including an explicitly empty
	// credential. Removing that override restores the file-backed readiness.
	writeS3EnvFixture(t, a, nil)
	t.Setenv("S3_ACCESS_KEY", "")
	assertRejectedRun(t, a, marker, requestEnvironmentRun(a), http.StatusServiceUnavailable)
	unsetEnvFixture(t, "S3_ACCESS_KEY")
	target := a.targetConfiguration()
	if target["ready"] != true || !a.verified(target) {
		t.Fatal("removing the empty override did not restore the verified target")
	}
	assertNotInvoked(t, marker)
}
