package secretmask

import (
	"strings"
	"testing"
)

func TestTextMasksCredentialAssignments(t *testing.T) {
	input := "run --access-key LOCALACCESS --secret-key=LOCALSECRET storage.auth.secret=CFGSECRET"
	got := Text(input)
	for _, leaked := range []string{"LOCALACCESS", "LOCALSECRET", "CFGSECRET"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("Text leaked %q in %q", leaked, got)
		}
	}
	for _, want := range []string{"--access-key ***", "--secret-key=***", "storage.auth.secret=***"} {
		if !strings.Contains(got, want) {
			t.Fatalf("Text missing %q in %q", want, got)
		}
	}
}

func TestYAMLMasksCredentialFields(t *testing.T) {
	input := []byte(`storage:
  auth:
    uid: LOCALACCESS
    secret: LOCALSECRET
  driver:
    type: s3
aws:
  accessKeyId: AWSACCESS
  secretAccessKey: AWSSECRET
`)
	got := string(YAML(input))
	for _, leaked := range []string{"LOCALACCESS", "LOCALSECRET", "AWSACCESS", "AWSSECRET"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("YAML leaked %q:\n%s", leaked, got)
		}
	}
	if strings.Count(got, masked) < 4 {
		t.Fatalf("YAML did not mask expected fields:\n%s", got)
	}
	if !strings.Contains(got, "type: s3") {
		t.Fatalf("YAML should preserve non-sensitive fields:\n%s", got)
	}
}
