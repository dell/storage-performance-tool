package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	flagBuckets           = "buckets"
	flagBucketsFile       = "buckets-file"
	targetBucketNamesPath = "storage.targetBucket.names"
)

// registerBucketSetFlags registers the flags that choose a run's target buckets.
func registerBucketSetFlags(flags *pflag.FlagSet) {
	flags.StringP("bucket", "b", "", "Target bucket; for DELETE --items-file it is an optional assertion applied to every manifest row")
	flags.Int(flagBucketCount, 1, fmt.Sprintf("Spread new objects over N buckets <bucket>-0 ... <bucket>-<N-1> (1-%d); "+
		"existing objects keep the bucket their items file records; write, write-verify, seeded read and delete, mixed",
		scenario.MaxBucketCount))
	flags.StringSlice(flagBuckets, nil, fmt.Sprintf("Spread new objects over these existing or new buckets "+
		"(comma-separated or repeated, 1-%d; env %s), used as written and in this order; missing ones are created "+
		"unless --%s=false. SPT never lists, reads or deletes an object it did not write, except those an items file "+
		"you supply names, and one credential set covers every bucket. Excludes --bucket, --%s and --%s",
		scenario.MaxBucketCount, constants.EnvS3Buckets, flagProvisionTargetBucket, flagBucketCount, flagBucketsFile))
	flags.String(flagBucketsFile, "", fmt.Sprintf("Read the --%s names from a file, one per line; blank lines and "+
		"lines starting with # are ignored. Excludes --bucket, --%s and --%s", flagBuckets, flagBucketCount, flagBuckets))
}

// bucketSet is a run's target bucket set as the bucket flags, a buckets file and the environment
// describe it. Bucket is the single bucket, the base of a generated set, or the first listed name,
// which the output path carries as a placeholder.
type bucketSet struct {
	Bucket string
	Names  []string // two or more listed names; nil for a single bucket or a generated set
	Source string   // what listed the names: --buckets, --buckets-file or S3_BUCKETS
}

// resolveBucketSet owns the bucket-set rules of spt run. It applies the exclusions among the list
// flags, --bucket and --bucket-count, reads a buckets file, takes S3_BUCKETS from the --buckets
// value applyEnvDefaultsToRunFlags injected, and checks the names. It never changes a flag, so what
// the user typed stays distinguishable from what the environment supplied. A list of one name is
// returned as that bucket alone, exactly as --bucket NAME.
func resolveBucketSet(cmd *cobra.Command) (bucketSet, error) {
	source, err := bucketListSource(cmd)
	if err != nil || source == "" {
		var set bucketSet
		if err == nil && cmd.Flags().Lookup("bucket") != nil {
			set.Bucket, _ = cmd.Flags().GetString("bucket")
		}
		return set, err
	}
	var names []string
	if source == "--"+flagBucketsFile {
		path, _ := cmd.Flags().GetString(flagBucketsFile)
		if names, err = readBucketsFile(path); err != nil {
			return bucketSet{}, err
		}
	} else {
		raw, _ := cmd.Flags().GetStringSlice(flagBuckets)
		names = make([]string, len(raw))
		for i, name := range raw {
			names[i] = strings.TrimSpace(name)
		}
	}
	if err := scenario.ValidateBucketList(names); err != nil {
		return bucketSet{}, fmt.Errorf("%s: %w", source, err)
	}
	if len(names) == 1 {
		return bucketSet{Bucket: names[0]}, nil
	}
	return bucketSet{Bucket: names[0], Names: names, Source: source}, nil
}

// bucketListSource names what lists the run's buckets (--buckets, --buckets-file or S3_BUCKETS),
// or returns "" when nothing does, and applies the exclusions between a list and the other bucket
// flags. Each error names both flags.
func bucketListSource(cmd *cobra.Command) (string, error) {
	listed := cmd.Flags().Changed(flagBuckets)
	fromFile := cmd.Flags().Changed(flagBucketsFile)
	source := ""
	switch {
	case listed && fromFile:
		return "", fmt.Errorf("--%s and --%s cannot be used together; list the buckets once", flagBuckets, flagBucketsFile)
	case fromFile:
		source = "--" + flagBucketsFile
	case listed && flagFromEnv(cmd, flagBuckets):
		source = constants.EnvS3Buckets
	case listed:
		source = "--" + flagBuckets
	default:
		return "", nil
	}
	if flagGiven(cmd, flagBucketCount) {
		return "", fmt.Errorf("%s and --%s cannot be used together: a list names every bucket of the set "+
			"(use --bucket with --%s for generated names)", source, flagBucketCount, flagBucketCount)
	}
	if flagGiven(cmd, "bucket") {
		return "", fmt.Errorf("%s and --bucket cannot be used together: a list names every bucket of the set", source)
	}
	return source, nil
}

// readBucketsFile reads a --buckets-file: one name per line, surrounding whitespace removed, blank
// lines and lines starting with # ignored. It stops reading once the file lists more names than a
// set may hold.
func readBucketsFile(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("--%s needs a file path", flagBucketsFile)
	}
	file, err := os.Open(path) // #nosec G304 -- the user names the file to read
	if err != nil {
		return nil, fmt.Errorf("--%s %s cannot be read: %w", flagBucketsFile, path, err)
	}
	defer func() { _ = file.Close() }()
	var names []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(names) == scenario.MaxBucketCount {
			return nil, fmt.Errorf("--%s %s lists more than %d bucket names", flagBucketsFile, path, scenario.MaxBucketCount)
		}
		names = append(names, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("--%s %s cannot be read: %w", flagBucketsFile, path, err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("--%s %s lists no bucket names", flagBucketsFile, path)
	}
	return names, nil
}

// applyBucketList validates a listed set of two or more buckets for the workload and records it.
// The list flags are the only CLI source of the names: an engine override of
// storage.targetBucket.names is rejected, as the count's is, so validation, scenario policy and run
// metadata always see the names the engine uses.
func applyBucketList(cmd *cobra.Command, params *scenario.Params, set bucketSet) error {
	for _, override := range params.EngineOverrides {
		if scenario.EngineOverrideSets(override, strings.Split(targetBucketNamesPath, ".")...) {
			return fmt.Errorf("engine override %q sets %s, which is not supported; use --%s or --%s",
				secretmask.EngineOverride(override), targetBucketNamesPath, flagBuckets, flagBucketsFile)
		}
	}
	if len(set.Names) < 2 {
		return nil
	}
	if set.Source == constants.EnvS3Buckets &&
		(params.WorkloadType == WorkloadTypeMock || params.WorkloadType == WorkloadTypeTables) {
		// These workloads use no S3 target bucket, so a profile's list is not theirs to reject.
		return nil
	}
	if reason := bucketCountUnsupportedReason(*params); reason != "" {
		return fmt.Errorf("%s with %d buckets is not supported for %s", set.Source, len(set.Names), reason)
	}
	if params.WorkloadType == WorkloadTypeDelete && seededDeleteBatchSize(cmd) > scenario.MinDeleteBatchSize {
		return fmt.Errorf("%s with %d buckets and seeded delete requires --%s=1: each DELETE request covers one bucket, "+
			"and batching across buckets is not supported", set.Source, len(set.Names), flagDeleteBatchSize)
	}
	params.BucketNames = set.Names
	return nil
}

// seededDeleteBatchSize returns the DELETE batch size a seeded delete step uses.
func seededDeleteBatchSize(cmd *cobra.Command) int {
	if cmd.Flags().Lookup(flagDeleteBatchSize) == nil {
		return scenario.DefaultDeleteBatchSize
	}
	batchSize, _ := cmd.Flags().GetInt(flagDeleteBatchSize)
	return batchSize
}

// flagFromEnv reports whether applyEnvDefaultsToRunFlags set the flag from the environment.
func flagFromEnv(cmd *cobra.Command, flag string) bool {
	_, ok := cmd.Annotations[envAppliedAnnotationPrefix+flag]
	return ok
}

// flagGiven reports whether the user set the flag on the command line, not the environment.
func flagGiven(cmd *cobra.Command, flag string) bool {
	return cmd.Flags().Changed(flag) && !flagFromEnv(cmd, flag)
}
