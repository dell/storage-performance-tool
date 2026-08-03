package secretmask

import (
	"reflect"
	"strings"
	"testing"
)

func TestEngineOverridesMaskCredentialPathsAndPreserveTuning(t *testing.T) {
	overrides := []string{
		"storage.auth.secret=alpha=bravo",
		"storage-auth-secret=charlie",
		"storage.auth.uid=access",
		"storage-auth-access-key=access-two",
		"storage.auth.password=password",
		"storage.auth.token=token",
		"storage.credentials.value=credential",
		"storage.auth.version=4",
		"storage.driver.threads=8",
	}
	want := []string{
		"storage.auth.secret=***",
		"storage-auth-secret=***",
		"storage.auth.uid=***",
		"storage-auth-access-key=***",
		"storage.auth.password=***",
		"storage.auth.token=***",
		"storage.credentials.value=***",
		"storage.auth.version=4",
		"storage.driver.threads=8",
	}
	if got := EngineOverrides(overrides); !reflect.DeepEqual(got, want) {
		t.Fatalf("EngineOverrides() = %#v, want %#v", got, want)
	}
	if overrides[0] != "storage.auth.secret=alpha=bravo" {
		t.Fatal("EngineOverrides mutated its input")
	}
}

func TestEngineOverrideListMasksEnvironmentSnapshot(t *testing.T) {
	got := EngineOverrideList(
		"storage.auth.secret=first; storage-auth-uid=second\nstorage.driver.threads=8")
	want := "storage.auth.secret=***; storage-auth-uid=***; storage.driver.threads=8"
	if got != want {
		t.Fatalf("EngineOverrideList() = %q, want %q", got, want)
	}
}

func TestEngineOverridePreservesNonSensitiveValues(t *testing.T) {
	for _, value := range []string{"storage.auth.version=4", "item.naming.prefix=a=b"} {
		if got := EngineOverride(value); got != value {
			t.Fatalf("EngineOverride(%q) = %q", value, got)
		}
	}
}

func TestEngineOverrideDoesNotEchoMalformedValues(t *testing.T) {
	for _, value := range []string{
		"MALFORMED_CREDENTIAL_7e31",
		"=EMPTY_PATH_CREDENTIAL_964a",
	} {
		if got := EngineOverride(value); got != invalidEngineOverride {
			t.Fatalf("EngineOverride(%q) = %q, want %q", value, got, invalidEngineOverride)
		}
		if strings.Contains(EngineOverride(value), value) {
			t.Fatalf("EngineOverride(%q) echoed unclassified input", value)
		}
	}
}
