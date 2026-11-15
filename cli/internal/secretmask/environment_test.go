package secretmask

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func setMaskEnvironment(t *testing.T, accessKey, secretKey, endpoint, endpoints string) {
	t.Helper()
	t.Setenv(constants.EnvS3AccessKey, accessKey)
	t.Setenv(constants.EnvS3SecretKey, secretKey)
	t.Setenv(constants.EnvS3Endpoint, endpoint)
	t.Setenv(constants.EnvS3Endpoints, endpoints)
}

// TestEnvironmentValuesReviewInput is the review's input: a 4-letter key that is also a word
// fragment, a 3-letter secret, and an endpoint list with a mixed-case host and a bare IP.
func TestEnvironmentValuesReviewInput(t *testing.T) {
	setMaskEnvironment(t, "test", "abc", "", "https://S3.Corp.Example:9021,10.0.0.1")
	tests := []struct {
		name, in, want string
	}{
		{"key inside a word is kept", "pulled latest image; attestation ok", "pulled latest image; attestation ok"},
		{"key as a token", "access key=test used", "access key=*** used"},
		{"short secret as a token", "secret abc loaded", "secret *** loaded"},
		{"short secret inside a word is kept", "abcd and xabc stay", "abcd and xabc stay"},
		{"IP inside a longer IP is kept", "peer 10.0.0.10 joined", "peer 10.0.0.10 joined"},
		{"IP as a token", "peer 10.0.0.1 joined", "peer *** joined"},
		{"IP with a port it was not given", "dial 10.0.0.1:9020", "dial ***:9020"},
		{"Java host/ip:port", "connecting to s3.corp.example/10.0.0.1:9021", "connecting to ***/***:9021"},
		{"host logged apart from its port", "host S3.CORP.EXAMPLE port 9021", "host *** port 9021"},
		{"URL keeps scheme and path", "GET https://s3.corp.example:9021/bucket", "GET https://***/bucket"},
		{"subdomain of the host is kept", "x.s3.corp.example resolved", "x.s3.corp.example resolved"},
		{"container ID with the secret's letters is kept", "container abc123def456 started", "container abc123def456 started"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EnvironmentValues(tt.in); got != tt.want {
				t.Fatalf("EnvironmentValues(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestEnvironmentValuesCredentialsAreNotSplit(t *testing.T) {
	setMaskEnvironment(t, "key,with,commas", "two words", "", "")
	in := "a=key,with,commas b=two words c=with d=two"
	want := "a=*** b=*** c=with d=two"
	if got := EnvironmentValues(in); got != want {
		t.Fatalf("EnvironmentValues() = %q, want %q", got, want)
	}
}

func TestEnvironmentValuesEndpointForms(t *testing.T) {
	setMaskEnvironment(t, "", "", "http://user@obj.example.com:9020/path", "10.0.0.2:9020 [fd00::5]:443")
	in := "obj.example.com:9020 OBJ.EXAMPLE.COM 10.0.0.2 10.0.0.2:9020 fd00::5 [fd00::5]:443 user"
	want := "*** *** *** *** *** *** user"
	if got := EnvironmentValues(in); got != want {
		t.Fatalf("EnvironmentValues() = %q, want %q", got, want)
	}
}

func TestEnvironmentValuesWithoutSettingsLeavesText(t *testing.T) {
	setMaskEnvironment(t, "", "", "", "")
	const in = "nothing to mask: http://10.0.0.1:9020"
	if got := EnvironmentValues(in); got != in {
		t.Fatalf("EnvironmentValues() = %q, want the text unchanged", got)
	}
}
