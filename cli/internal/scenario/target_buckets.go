package scenario

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MaxBucketCount is the largest target bucket set, storage.targetBucket.count.
const MaxBucketCount = 1000

var (
	ipv4Form                 = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`)
	reservedBucketPrefixes   = []string{"xn--", "sthree-", "amzn-s3-demo-"}
	reservedBucketSuffixes   = []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"}
	errBucketCountNoBaseName = fmt.Errorf("a bucket count greater than 1 requires --bucket to name a base bucket")
)

// TargetBucketMemberNames returns the bucket names of a target bucket set: the bucket alone for a
// count of 1, otherwise <base>-0 to <base>-<count-1>, where <base> is the bucket's first path
// segment. The engine applies the same rules (TargetBucketSet). A count of 1 is never validated
// here, so single-bucket behavior is unchanged.
func TargetBucketMemberNames(bucket string, count int) ([]string, error) {
	if count < 1 || count > MaxBucketCount {
		return nil, fmt.Errorf("bucket count must be between 1 and %d, got %d", MaxBucketCount, count)
	}
	if count == 1 {
		return []string{bucket}, nil
	}
	base, _, _ := strings.Cut(strings.Trim(strings.TrimSpace(bucket), "/"), "/")
	if base == "" {
		return nil, errBucketCountNoBaseName
	}
	names := make([]string, count)
	for i := range names {
		name := base + "-" + strconv.Itoa(i)
		if problem := bucketNameProblem(name); problem != "" {
			return nil, fmt.Errorf("bucket count %d: the generated bucket name %q is not a valid bucket name: %s",
				count, name, problem)
		}
		names[i] = name
	}
	return names, nil
}

// bucketNameProblem reports why a generated name breaks the AWS general-purpose bucket naming
// rules, or "" when it follows them.
func bucketNameProblem(name string) string {
	if len(name) < 3 || len(name) > 63 {
		return "it must be 3 to 63 characters long"
	}
	for _, c := range name {
		if !lowercaseLetterOrDigit(c) && c != '-' && c != '.' {
			return "it may contain only lowercase letters, digits, hyphens and periods"
		}
	}
	if !lowercaseLetterOrDigit(rune(name[0])) || !lowercaseLetterOrDigit(rune(name[len(name)-1])) {
		return "it must start and end with a lowercase letter or digit"
	}
	if strings.Contains(name, "..") {
		return "it must not contain two adjacent periods"
	}
	if ipv4Form.MatchString(name) {
		return "it must not be formatted as an IP address"
	}
	for _, prefix := range reservedBucketPrefixes {
		if strings.HasPrefix(name, prefix) {
			return fmt.Sprintf("it must not start with the reserved prefix %q", prefix)
		}
	}
	for _, suffix := range reservedBucketSuffixes {
		if strings.HasSuffix(name, suffix) {
			return fmt.Sprintf("it must not end with the reserved suffix %q", suffix)
		}
	}
	return ""
}

func lowercaseLetterOrDigit(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}
