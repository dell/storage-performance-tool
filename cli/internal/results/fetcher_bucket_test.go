package results

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestBucketMetricsArtifactsAreOptional(t *testing.T) {
	want := map[string]string{
		"metrics.bucket.FileTotal":        constants.ResultsArtifactSuffixMetricsBucket,
		"metrics.bucket.steady.FileTotal": constants.ResultsArtifactSuffixMetricsBucketSteady,
	}
	for logger, suffix := range want {
		found := false
		for _, spec := range DefaultArtifacts {
			for _, name := range spec.Loggers {
				if name == logger {
					found = true
					if spec.Suffix != suffix {
						t.Errorf("%s: suffix %q, want %q", logger, spec.Suffix, suffix)
					}
					if spec.Required {
						t.Errorf("%s: an older engine writes no such file, so it must be optional", logger)
					}
				}
			}
		}
		if !found {
			t.Errorf("no artifact spec for %s", logger)
		}
	}
}
