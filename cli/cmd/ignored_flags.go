package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

// partSizeIgnored is the part-size rule of the runs that do not use one: mixed, and a read of an
// items file, whose scenarios set no item.data.ranges.threshold of their own.
func partSizeIgnored(workloadType, itemsFile string) bool {
	return workloadType == WorkloadTypeMixed || (workloadType == WorkloadTypeRead && itemsFile != "")
}

// ignoredPartSizeSource names where the part size a run ignores came from: --part-size, or
// SPT_PART_SIZE when the environment or .env set it. It is "" when the run has no part size or
// uses it.
func ignoredPartSizeSource(cmd *cobra.Command, workloadType string) string {
	partSize, _ := cmd.Flags().GetString("part-size")
	itemsFile, _ := cmd.Flags().GetString("items-file")
	switch {
	case partSize == "" || !partSizeIgnored(workloadType, itemsFile):
		return ""
	case flagFromEnv(cmd, "part-size"):
		return constants.EnvPartSize
	default:
		return "--part-size"
	}
}

// rangesThresholdOverridden reports whether an engine override gives the run's steps a ranges
// threshold. --mpu-concurrent-* then still bound its multipart uploads and split reads.
func rangesThresholdOverridden(overrides []string) bool {
	for _, override := range overrides {
		if scenario.EngineOverrideSets(override, "item", "data", "ranges", "threshold") {
			return true
		}
	}
	return false
}

// saveItemsIgnored reports whether a workload accepts --save-items and saves no items list.
func saveItemsIgnored(workloadType string) bool {
	return workloadType != WorkloadTypeWrite && workloadType != WorkloadTypeList
}

// dropIgnoredRunFlags clears the settings a run accepts and ignores, so the run parameters and
// --generate-only do not show them as used. The scenario does not change.
func dropIgnoredRunFlags(params *scenario.Params, partSizeSource string) {
	if partSizeSource != "" {
		params.PartSize = ""
		if !rangesThresholdOverridden(params.EngineOverrides) {
			params.MpuObjects, params.MpuParts = 0, 0
		}
	}
	if saveItemsIgnored(params.WorkloadType) {
		params.SaveItems = false
	}
}

// writeIgnoredFlagWarnings prints one warning for each flag the run accepts and ignores.
func writeIgnoredFlagWarnings(output io.Writer, cmd *cobra.Command, workloadType string) {
	if itemsFile, _ := cmd.Flags().GetString("items-file"); itemsFile != "" && scenario.IgnoresItemsFile(workloadType) {
		hint := ""
		if workloadType == WorkloadTypeMixed {
			hint = "; use --read-items-file for the READ pool or --delete-items-file for the DELETE queue"
		}
		_, _ = fmt.Fprintf(output, "Warning: %s ignores --items-file and does not read the file%s\n", workloadType, hint)
	}
	if saveItems, _ := cmd.Flags().GetBool("save-items"); saveItems && saveItemsIgnored(workloadType) {
		_, _ = fmt.Fprintf(output, "Warning: %s ignores --save-items and saves no items list; only write saves one\n", workloadType)
	}
	writeIgnoredVerifyFlagWarnings(output, cmd, workloadType)
}

// writeIgnoredVerifyFlagWarnings prints the warnings spt run and spt verify share: --read-items-file
// on read, and a part size the run ignores.
func writeIgnoredVerifyFlagWarnings(output io.Writer, cmd *cobra.Command, workloadType string) {
	if readItemsFile, _ := cmd.Flags().GetString("read-items-file"); readItemsFile != "" && workloadType == WorkloadTypeRead {
		_, _ = fmt.Fprintln(output, "Warning: read ignores --read-items-file and does not read the file; use --items-file for the items to read")
	}
	writeIgnoredPartSizeWarning(output, cmd, workloadType)
}

// writeIgnoredPartSizeWarning prints one warning when the run ignores a part size, naming its source
// and the --mpu-concurrent-* flags it ignores with it.
func writeIgnoredPartSizeWarning(output io.Writer, cmd *cobra.Command, workloadType string) {
	source := ignoredPartSizeSource(cmd, workloadType)
	if source == "" {
		return
	}
	partSize, _ := cmd.Flags().GetString("part-size")
	given := []string{"--part-size " + partSize}
	if source == constants.EnvPartSize {
		given[0] = constants.EnvPartSize + "=" + partSize
	}
	run, effect := "read --items-file", "each object is read whole"
	if workloadType == WorkloadTypeMixed {
		run, effect = workloadType, mixedPartSizeEffect(cmd)
	}
	if overrides, _ := cmd.Flags().GetStringArray(flagEngineOverride); rangesThresholdOverridden(overrides) {
		effect = "the engine override of item.data.ranges.threshold applies instead"
	} else {
		for _, name := range []string{"mpu-concurrent-objects", "mpu-concurrent-parts"} {
			if value, _ := cmd.Flags().GetInt(name); value != 0 {
				given = append(given, "--"+name)
			}
		}
	}
	names := given[0]
	if last := len(given) - 1; last > 0 {
		names = strings.Join(given[:last], ", ") + " and " + given[last]
	}
	_, _ = fmt.Fprintf(output, "Warning: %s ignores %s: %s\n", run, names, effect)
}

// mixedPartSizeEffect says what a mixed run sends in place of multipart uploads and range reads. It
// names only the operations the run has: no seed with --read-items-file, no CREATE with
// --put-distrib 0 and no READ with --get-distrib 0. spt verify has no distribution flags, so it
// names CREATE and READ.
func mixedPartSizeEffect(cmd *cobra.Command) string {
	distrib := func(name string) bool {
		if cmd.Flags().Lookup(name) == nil {
			return true
		}
		value, _ := cmd.Flags().GetInt(name)
		return value > 0
	}
	readItemsFile, _ := cmd.Flags().GetString("read-items-file")
	var writers string
	switch seed, create := readItemsFile == "", distrib("put-distrib"); {
	case seed && create:
		writers = "its seed and CREATE operations send single PUTs"
	case seed:
		writers = "its seed sends single PUTs"
	case create:
		writers = "its CREATE operations send single PUTs"
	}
	switch reads := distrib("get-distrib"); {
	case writers != "" && reads:
		return writers + " and its READs whole GETs"
	case writers != "":
		return writers
	case reads:
		return "its READs send whole GETs"
	default:
		return "it sends no PUT or GET that a part size applies to"
	}
}
