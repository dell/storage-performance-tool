package cmdline

import "testing"

func TestSanitizeArgsMasksSensitiveFlags(t *testing.T) {
	args := []string{
		"spt", "run", "write",
		"--secret-key=verysecret",
		"--access-key", "OPENKEY",
		"-s", "short",
		"-a=value",
	}
	got := SanitizeArgs(args)
	want := []string{
		"spt", "run", "write",
		"--secret-key=***",
		"--access-key", "***",
		"-s", "***",
		"-a=***",
	}
	if len(got) != len(want) {
		t.Fatalf("sanitized args length %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sanitized args[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestFormatForArtifactMasksAndQuotes(t *testing.T) {
	got := FormatForArtifact([]string{"spt", "run", "--secret-key", "top secret", "--name", "my run", "-a=AKIA"})
	want := `spt run --secret-key *** --name "my run" -a=***`
	if got != want {
		t.Fatalf("FormatForArtifact mismatch\nwant: %s\ngot:  %s", want, got)
	}
}
