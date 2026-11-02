package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/poolsizing"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/dell/storage-performance-tool/cli/internal/verification"
	"github.com/spf13/cobra"
)

// buildPoolPlanFunc asks each host's image; tests replace it.
var buildPoolPlanFunc = poolsizing.BuildPlan

// poolPlanInput gathers what the sizing plan reads from the run.
func poolPlanInput(
	params scenario.Params, steps []scenario.StepInfo, hosts []*hostparse.HostInfo, image string, attached bool,
) poolsizing.Input {
	shortfall := params.RdmaPoolShortfall
	skipPull, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv(constants.EnvSkipImagePull)))
	skipPull = skipPull || constants.IsDevImage(image)
	return poolsizing.Input{
		Params:      params,
		Steps:       steps,
		Hosts:       hosts,
		Image:       image,
		SkipPull:    skipPull,
		Attached:    attached,
		Shortfall:   shortfall,
		SptJavaOpts: os.Getenv(constants.EnvSptJavaOpts),
		Executor:    command.NewCommandExecutor(),
	}
}

// planPoolSizing sizes -XX:MaxDirectMemorySize for each engine a run launches (spec item 14b), and
// prints what it set. A need above an engine's capacity under fail stops the run here, before
// anything starts. It returns nil when sizing does not apply, which leaves the JVM options as they
// are: no NIC set, another driver, or the pool off.
func planPoolSizing(
	ctx context.Context, params scenario.Params, steps []scenario.StepInfo, hosts []*hostparse.HostInfo, image string, attached bool,
) ([]poolsizing.Engine, error) {
	if !poolsizing.Applies(params) {
		poolsizing.ClearLaunchOptions()
		return nil, nil
	}
	plan, err := buildPoolPlanFunc(ctx, poolPlanInput(params, steps, hosts, image, attached))
	if err != nil {
		return nil, fmt.Errorf("size the RDMA pools: %w", err)
	}
	printPoolPlan(plan)
	if err := plan.Failure(); err != nil {
		return plan.Engines, err
	}
	poolsizing.SetLaunchOptions(plan, hosts)
	return plan.Engines, nil
}

func printPoolPlan(plan poolsizing.Plan) {
	for _, engine := range plan.Engines {
		if engine.Message == "" {
			continue
		}
		fmt.Println(engine.Message)
		if engine.Warning {
			logging.LogWarn("pool-sizing", "RDMA direct memory", "host", engine.Host, "message", engine.Message)
		} else {
			logging.LogInfo("pool-sizing", "RDMA direct memory", "host", engine.Host, "message", engine.Message)
		}
	}
}

// poolSizingSteps gives verify a seed-then-load run's steps by role, with no scenario to read IDs
// from: the same steps BuildRequest takes from a generated scenario.
func poolSizingSteps(params scenario.Params) []scenario.StepInfo {
	ops := []string{"create"}
	switch params.WorkloadType {
	case WorkloadTypeRead:
		ops = []string{"read"}
		if params.ItemsFile == "" {
			ops = []string{"seed", "read"}
		}
	case WorkloadTypeMixed:
		ops = []string{"mixed"}
		if params.ReadItemsFile == "" {
			ops = []string{"seed", "mixed"}
		}
	case WorkloadTypeWriteVerify:
		ops = []string{"create", "read"}
	case WorkloadTypeReadVerify:
		ops = []string{"read"}
	case WorkloadTypeDelete:
		ops = []string{"delete"}
		if params.ItemsFile == "" && !params.DeleteExisting {
			ops = []string{"seed", "delete"}
		}
	}
	steps := make([]scenario.StepInfo, 0, len(ops))
	for i, op := range ops {
		steps = append(steps, scenario.StepInfo{ID: strings.ToLower(op), Number: i + 1, Op: op})
	}
	return steps
}

// verifyPoolPreview adds the pool lines to verify's report (item 16), for s3-rdma with a NIC set and
// the workload's flags. A short pool fails the check under fail and warns under warn.
func verifyPoolPreview(ctx context.Context, cmd *cobra.Command, in verifyPoolInput, report *verification.Report) error {
	params, ok, err := verifyPoolParams(cmd, in)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	plan, err := buildPoolPlanFunc(ctx, poolPlanInput(params, poolSizingSteps(params), in.hosts, constants.EffectiveSptImage(), in.attached))
	if err != nil {
		return fmt.Errorf("preview the RDMA pools: %w", err)
	}
	var text strings.Builder
	plan.Render(&text)
	var failures, warnings []string
	for _, finding := range plan.Findings() {
		if finding.Failure {
			failures = append(failures, finding.Message)
		} else {
			warnings = append(warnings, finding.Message)
		}
	}
	report.AddPoolPreview(text.String(), failures, warnings)
	return nil
}

type verifyPoolInput struct {
	hosts          []*hostparse.HostInfo
	interfaces     []string
	hostInterfaces []nicplan.HostInterfaces
	endpoints      []string
	slice          bool
	threads        int
	attached       bool
}

// verifyPoolParams reads the run's workload flags. Without a size or an item file there is no
// workload to estimate, so verify shows no pool lines, as without these flags before.
func verifyPoolParams(cmd *cobra.Command, in verifyPoolInput) (scenario.Params, bool, error) {
	useRdma, _ := cmd.Flags().GetBool("use-rdma")
	objectSize, _ := cmd.Flags().GetString("object-size")
	itemsFile, _ := cmd.Flags().GetString("items-file")
	readItemsFile, _ := cmd.Flags().GetString("read-items-file")
	if !useRdma || (len(in.interfaces) == 0 && len(nicplan.AnyNicSet(in.hostInterfaces)) == 0) ||
		(objectSize == "" && itemsFile == "" && readItemsFile == "") {
		return scenario.Params{}, false, nil
	}
	shortfall, err := readRdmaPoolShortfall(cmd)
	if err != nil {
		return scenario.Params{}, false, err
	}
	workloadType, _ := cmd.Flags().GetString("workload")
	partSize, _ := cmd.Flags().GetString("part-size")
	mpuObjects, _ := cmd.Flags().GetInt("mpu-concurrent-objects")
	mpuParts, _ := cmd.Flags().GetInt("mpu-concurrent-parts")
	bufferPool, _ := cmd.Flags().GetBool("rdma-buffer-pool")
	thresholdText, _ := cmd.Flags().GetString("rdma-threshold")
	threshold, err := sizeparse.Parse(thresholdText)
	if err != nil {
		return scenario.Params{}, false, fmt.Errorf("invalid --rdma-threshold value %q: %w", thresholdText, err)
	}
	params := scenario.Params{
		WorkloadType:          workloadType,
		S3Driver:              scenario.S3DriverRdma,
		Interfaces:            in.interfaces,
		HostInterfaces:        in.hostInterfaces,
		Endpoints:             in.endpoints,
		SliceEndpoints:        in.slice,
		Threads:               in.threads,
		ObjectSize:            objectSize,
		PartSize:              partSize,
		MpuObjects:            mpuObjects,
		MpuParts:              mpuParts,
		ItemsFile:             itemsFile,
		ReadItemsFile:         readItemsFile,
		RdmaThresholdBytes:    threshold,
		RdmaDisableBufferPool: !bufferPool,
		RdmaPoolShortfall:     shortfall,
		GetDistrib:            1,
		PutDistrib:            1,
	}
	return params, poolsizing.Applies(params), nil
}
