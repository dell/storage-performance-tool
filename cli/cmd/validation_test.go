package cmd

import (
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func TestValidateWorkloadType(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid write workload",
			workloadType: "write",
			wantErr:      false,
		},
		{
			name:         "valid read workload",
			workloadType: "read",
			wantErr:      false,
		},
		{
			name:         "valid mixed workload",
			workloadType: "mixed",
			wantErr:      false,
		},
		{
			name:         "unimplemented delete workload",
			workloadType: "delete",
			wantErr:      true,
			errContains:  "not yet implemented",
		},
		{
			name:         "valid list workload",
			workloadType: "list",
			wantErr:      false,
		},
		{
			name:         "valid mock workload",
			workloadType: "mock",
			wantErr:      false,
		},
		{
			name:         "invalid workload type",
			workloadType: "invalid",
			wantErr:      true,
			errContains:  "invalid workload type: invalid",
		},
		{
			name:         "empty workload type",
			workloadType: "",
			wantErr:      true,
			errContains:  "invalid workload type:",
		},
		{
			name:         "case sensitive workload type",
			workloadType: "Write",
			wantErr:      true,
			errContains:  "invalid workload type: Write",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWorkloadType(tt.workloadType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateWorkloadType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateWorkloadType() error = %v, want error containing %v", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestValidateS3Flags(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		endpoint     string
		endpoints    []string
		accessKey    string
		secretKey    string
		bucket       string
		wantErr      bool
		errContains  string
	}{
		{
			name:         "all S3 flags present",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			wantErr:      false,
		},
		{
			name:         "missing endpoint",
			workloadType: "write",
			endpoint:     "",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			wantErr:      true,
			errContains:  "--endpoint flag is required for write workload",
		},
		{
			name:         "endpoints provided are accepted",
			workloadType: "write",
			endpoint:     "",
			endpoints:    []string{"http://s1:9000", "http://s2:9000"},
			accessKey:    "a",
			secretKey:    "s",
			bucket:       "b",
			wantErr:      false,
		},
		{
			name:         "endpoint alias merged with endpoints",
			workloadType: "write",
			endpoint:     "http://single:9000",
			endpoints:    []string{"http://s1:9000"},
			accessKey:    "a",
			secretKey:    "s",
			bucket:       "b",
			wantErr:      false,
		},
		{
			name:         "missing access key",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "",
			secretKey:    "secret456",
			bucket:       "mybucket",
			wantErr:      true,
			errContains:  "--access-key flag is required for write workload",
		},
		{
			name:         "missing secret key",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "",
			bucket:       "mybucket",
			wantErr:      true,
			errContains:  "--secret-key flag is required for write workload",
		},
		{
			name:         "missing bucket",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "",
			wantErr:      true,
			errContains:  "--bucket flag is required for write workload",
		},
		{
			name:         "list workload requires bucket",
			workloadType: "list",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "",
			wantErr:      true,
			errContains:  "--bucket flag is required for list workload",
		},
		{
			name:         "list workload with required flags",
			workloadType: "list",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			wantErr:      false,
		},
		{
			name:         "mock mode - no S3 flags required",
			workloadType: "mock",
			endpoint:     "",
			accessKey:    "",
			secretKey:    "",
			bucket:       "",
			wantErr:      false,
		},
		{
			name:         "mock mode - with S3 flags (should still pass)",
			workloadType: "mock",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().String("endpoint", tt.endpoint, "")
			cmd.Flags().StringSlice("endpoints", tt.endpoints, "")
			cmd.Flags().String("access-key", tt.accessKey, "")
			cmd.Flags().String("secret-key", tt.secretKey, "")
			cmd.Flags().String("bucket", tt.bucket, "")

			err := ValidateS3Flags(cmd, tt.workloadType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateS3Flags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateS3Flags() error = %v, want error containing %v", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestValidateDurationOrCount(t *testing.T) {
	tests := []struct {
		name        string
		objectCount int
		duration    string
		wantErr     bool
		errContains string
	}{
		{
			name:        "only object count specified",
			objectCount: 100,
			duration:    "",
			wantErr:     false,
		},
		{
			name:        "only duration specified",
			objectCount: 0,
			duration:    "5m",
			wantErr:     false,
		},
		{
			name:        "neither specified",
			objectCount: 0,
			duration:    "",
			wantErr:     false, // This is now allowed - defaults to 100 objects
		},
		{
			name:        "both specified",
			objectCount: 100,
			duration:    "5m",
			wantErr:     true,
			errContains: "cannot specify both --object-count and --duration",
		},
		{
			name:        "negative object count",
			objectCount: -1,
			wantErr:     true,
			errContains: "--object-count must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().Int("auth-version", 4, "")
			cmd.Flags().Int("object-count", tt.objectCount, "")
			cmd.Flags().String("duration", tt.duration, "")

			err := ValidateDurationOrCount(cmd)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDurationOrCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateDurationOrCount() error = %v, want error containing %v", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestValidateReadShuffleFlags(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		setup        func(*testing.T, *cobra.Command)
		wantErr      bool
		errContains  string
	}{
		{
			name:         "read workload allows shuffle only",
			workloadType: WorkloadTypeRead,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
		},
		{
			name:         "read workload allows shuffle batch override",
			workloadType: WorkloadTypeRead,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
				if err := cmd.Flags().Set(flagReadShuffleBatchSize, "512000"); err != nil {
					t.Fatalf("set shuffle-batch-size: %v", err)
				}
			},
		},
		{
			name:         "shuffle batch requires shuffle",
			workloadType: WorkloadTypeRead,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffleBatchSize, "512000"); err != nil {
					t.Fatalf("set shuffle-batch-size: %v", err)
				}
			},
			wantErr:     true,
			errContains: ErrReadShuffleBatchRequiresToggle,
		},
		{
			name:         "shuffle batch must be positive",
			workloadType: WorkloadTypeRead,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
				if err := cmd.Flags().Set(flagReadShuffleBatchSize, "0"); err != nil {
					t.Fatalf("set shuffle-batch-size: %v", err)
				}
			},
			wantErr:     true,
			errContains: ErrReadShuffleBatchPositive,
		},
		{
			name:         "shuffle batch must not exceed max",
			workloadType: WorkloadTypeRead,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
				if err := cmd.Flags().Set(flagReadShuffleBatchSize, strconv.Itoa(constants.ReadShuffleMaxBatchSize+1)); err != nil {
					t.Fatalf("set shuffle-batch-size: %v", err)
				}
			},
			wantErr:     true,
			errContains: strconv.Itoa(constants.ReadShuffleMaxBatchSize),
		},
		{
			name:         "write workload rejects shuffle flag",
			workloadType: WorkloadTypeWrite,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle flag is not supported for write workload",
		},
		{
			name:         "read-verify workload rejects shuffle flag",
			workloadType: WorkloadTypeReadVerify,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle flag is not supported for read-verify workload",
		},
		{
			name:         "delete workload rejects shuffle flag",
			workloadType: WorkloadTypeDelete,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle flag is not supported for delete workload",
		},
		{
			name:         "mixed workload rejects shuffle flag",
			workloadType: WorkloadTypeMixed,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle flag is not supported for mixed workload",
		},
		{
			name:         "list workload rejects shuffle flag",
			workloadType: WorkloadTypeList,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle flag is not supported for list workload",
		},
		{
			name:         "mock workload rejects shuffle flag",
			workloadType: WorkloadTypeMock,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffle, "true"); err != nil {
					t.Fatalf("set shuffle: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle flag is not supported for mock workload",
		},
		{
			name:         "write workload rejects shuffle batch flag",
			workloadType: WorkloadTypeWrite,
			setup: func(t *testing.T, cmd *cobra.Command) {
				if err := cmd.Flags().Set(flagReadShuffleBatchSize, "512000"); err != nil {
					t.Fatalf("set shuffle-batch-size: %v", err)
				}
			},
			wantErr:     true,
			errContains: "--shuffle-batch-size flag is not supported for write workload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().Bool(flagReadShuffle, false, "")
			cmd.Flags().Int(flagReadShuffleBatchSize, 0, "")
			if tt.setup != nil {
				tt.setup(t, cmd)
			}

			err := validateReadShuffleFlags(cmd, tt.workloadType)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateReadShuffleFlags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("validateReadShuffleFlags() error = %v, want containing %q", err, tt.errContains)
			}
		})
	}
}

func TestValidateReadPhasePauseFlag(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		seconds      string
		wantErr      bool
	}{
		{name: "read positive", workloadType: WorkloadTypeRead, seconds: "30"},
		{name: "read zero", workloadType: WorkloadTypeRead, seconds: "0", wantErr: true},
		{name: "write unsupported", workloadType: WorkloadTypeWrite, seconds: "30", wantErr: true},
		{name: "read-verify unsupported", workloadType: WorkloadTypeReadVerify, seconds: "30", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().Int(flagReadPhasePauseSeconds, scenario.DefaultReadPhasePauseSeconds, "")
			if err := cmd.Flags().Set(flagReadPhasePauseSeconds, tt.seconds); err != nil {
				t.Fatal(err)
			}
			err := validateReadPhasePauseFlag(cmd, tt.workloadType)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateReadPhasePauseFlag() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func newIntegrityValidationCommand(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Annotations: map[string]string{}}
	cmd.Flags().String("endpoint", "http://s3.example.com", "")
	cmd.Flags().StringSlice("endpoints", nil, "")
	cmd.Flags().String("access-key", "access", "")
	cmd.Flags().String("secret-key", "secret", "")
	cmd.Flags().String("bucket", "bucket", "")
	cmd.Flags().Int("auth-version", 4, "")
	cmd.Flags().Int("object-count", 0, "")
	cmd.Flags().Bool("allow-empty-selection", false, "")
	cmd.Flags().Int("integrity-max-console-failures", 20, "")
	cmd.Flags().Bool("auto-results", true, "")
	cmd.Flags().String("items-file", "", "")
	cmd.Flags().StringArray(flagEngineOverride, nil, "")
	cmd.Flags().String(flagIntegrityRuntimeIdentityTier, constants.IntegrityRuntimeIdentityTierImage, "")
	cmd.Flags().String("duration", "", "")
	cmd.Flags().String("part-size", "", "")
	cmd.Flags().Int("mpu-concurrent-objects", 0, "")
	cmd.Flags().Int("mpu-concurrent-parts", 0, "")
	cmd.Flags().Bool("cleanup", false, "")
	cmd.Flags().Bool("save-items", false, "")
	cmd.Flags().String("object-size", "", "")
	cmd.Flags().Float64("object-data-compressibility", 0, "")
	cmd.Flags().Bool("object-data-dedupable", true, "")
	cmd.Flags().Int("seed-objects", 2500, "")
	cmd.Flags().Bool("create-prefix", false, "")
	cmd.Flags().Int(flagPrefixShards, prefixShardsAuto, "")
	return cmd
}

func TestValidateIntegrityWorkloadAcceptanceMatrix(t *testing.T) {
	tests := []struct {
		name        string
		workload    string
		flag        string
		value       string
		wantErr     bool
		errContains string
	}{
		{name: "read allow empty", workload: WorkloadTypeReadVerify, flag: "allow-empty-selection", value: "true"},
		{name: "write rejects allow empty", workload: WorkloadTypeWriteVerify, flag: "allow-empty-selection", value: "true", wantErr: true, errContains: "valid only for read-verify"},
		{name: "write rejects explicit false allow empty", workload: WorkloadTypeWriteVerify, flag: "allow-empty-selection", value: "false", wantErr: true, errContains: "valid only for read-verify"},
		{name: "read requires auto results", workload: WorkloadTypeReadVerify, flag: "auto-results", value: "false", wantErr: true, errContains: "require --auto-results=true"},
		{name: "write requires auto results", workload: WorkloadTypeWriteVerify, flag: "auto-results", value: "false", wantErr: true, errContains: "require --auto-results=true"},
		{name: "read accepts items file", workload: WorkloadTypeReadVerify, flag: "items-file", value: "items.csv"},
		{name: "write rejects items file", workload: WorkloadTypeWriteVerify, flag: "items-file", value: "items.csv", wantErr: true, errContains: "not supported for write-verify"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := newIntegrityValidationCommand(t)
			if err := cmd.Flags().Set(test.flag, test.value); err != nil {
				t.Fatal(err)
			}
			err := validateIntegrityWorkloadFlags(cmd, test.workload)
			if (err != nil) != test.wantErr {
				t.Fatalf("validation error = %v, wantErr %t", err, test.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), test.errContains) {
				t.Fatalf("validation error = %q, want containing %q", err, test.errContains)
			}
		})
	}
}

func TestIntegrityCommandValidationStopsBeforeExecutionSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		workload string
		args     []string
	}{
		{name: "auto results disabled", workload: WorkloadTypeReadVerify, args: []string{"--auto-results=false"}},
		{name: "write items file", workload: WorkloadTypeWriteVerify, args: []string{"--items-file=items.csv"}},
		{name: "excluded copy override", workload: WorkloadTypeReadVerify, args: []string{"--engine-override=load.op.type=copy"}},
		{name: "unsupported duration", workload: WorkloadTypeReadVerify, args: []string{"--duration=1m"}},
		{name: "negative object count", workload: WorkloadTypeWriteVerify, args: []string{"--object-count=-1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := newIntegrityValidationCommand(t)
			cmd.Use = "run <type>"
			cmd.Args = cobra.ExactArgs(1)
			cmd.SilenceUsage = true
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.PreRunE = ValidateRunCommand
			var schemaProbes, scenarioPosts, objectIOStarts int
			cmd.RunE = func(*cobra.Command, []string) error {
				schemaProbes++
				scenarioPosts++
				objectIOStarts++
				return nil
			}
			cmd.SetArgs(append([]string{test.workload}, test.args...))

			if err := cmd.Execute(); err == nil {
				t.Fatal("incompatible verification command unexpectedly passed validation")
			}
			if schemaProbes != 0 || scenarioPosts != 0 || objectIOStarts != 0 {
				t.Fatalf("rejected command crossed execution boundary: schema=%d post=%d io=%d",
					schemaProbes, scenarioPosts, objectIOStarts)
			}
		})
	}
}

func TestValidateIntegrityRejectsExcludedEngineOverrides(t *testing.T) {
	for _, override := range []string{
		"item.data.input.file=payload.bin",
		"item.input.file=copy.csv",
		"item.data.ranges.concat=0-1",
		"item.data.ranges.random=1",
		"item.data.ranges.fixed=0-1",
		"load.op.recycle.mode=true",
		"load.op.recycle.content.update=true",
		"load.op.type=update",
		"item-data-ranges-random=1",
	} {
		t.Run(strings.ReplaceAll(override, "/", "_"), func(t *testing.T) {
			cmd := newIntegrityValidationCommand(t)
			if err := cmd.Flags().Set(flagEngineOverride, override); err != nil {
				t.Fatal(err)
			}
			err := validateIntegrityWorkloadFlags(cmd, WorkloadTypeReadVerify)
			if err == nil || !strings.Contains(err.Error(), "excluded from verification workloads") {
				t.Fatalf("override %q validation error = %v", override, err)
			}
		})
	}
}

func TestValidateReadVerifyRejectsUnsupportedOptions(t *testing.T) {
	for _, option := range []struct {
		name  string
		value string
	}{
		{name: "duration", value: "1m"},
		{name: "part-size", value: "5MiB"},
		{name: "mpu-concurrent-objects", value: "1"},
		{name: "mpu-concurrent-parts", value: "1"},
		{name: "cleanup", value: "true"},
		{name: "save-items", value: "true"},
		{name: "object-size", value: "1MiB"},
		{name: "object-data-compressibility", value: "10"},
		{name: "object-data-dedupable", value: "false"},
		{name: "seed-objects", value: "1"},
		{name: "create-prefix", value: "true"},
		{name: flagPrefixShards, value: "1"},
	} {
		t.Run(option.name, func(t *testing.T) {
			cmd := newIntegrityValidationCommand(t)
			if err := cmd.Flags().Set(option.name, option.value); err != nil {
				t.Fatal(err)
			}
			err := validateIntegrityWorkloadFlags(cmd, WorkloadTypeReadVerify)
			if err == nil || !strings.Contains(err.Error(), "not supported for read-verify workload") {
				t.Fatalf("option --%s validation error = %v", option.name, err)
			}
		})
	}
}

func TestValidateReadVerifyRejectsEffectiveOSEnvironmentOptions(t *testing.T) {
	tests := []struct {
		name   string
		envKey string
		value  string
		flag   string
	}{
		{name: "part size", envKey: constants.EnvPartSize, value: "5MiB", flag: "part-size"},
		{name: "compressibility", envKey: constants.EnvObjectDataCompressibility, value: "25", flag: "object-data-compressibility"},
		{name: "dedupable", envKey: constants.EnvObjectDataDedupable, value: "false", flag: "object-data-dedupable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := newIntegrityValidationCommand(t)
			t.Setenv(test.envKey, test.value)
			if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
				t.Fatal(err)
			}
			err := validateIntegrityWorkloadFlags(cmd, WorkloadTypeReadVerify)
			if err == nil || !strings.Contains(err.Error(), "--"+test.flag) {
				t.Fatalf("effective environment validation error = %v, want --%s rejection", err, test.flag)
			}
		})
	}
}

func TestValidateIntegrityRuntimeIdentityTier(t *testing.T) {
	newCommand := func(value string, changed bool) *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().Bool("allow-empty-selection", false, "")
		cmd.Flags().Int("integrity-max-console-failures", 20, "")
		cmd.Flags().Bool("auto-results", true, "")
		cmd.Flags().String("items-file", "", "")
		cmd.Flags().StringArray(flagEngineOverride, nil, "")
		cmd.Flags().String(flagIntegrityRuntimeIdentityTier, constants.IntegrityRuntimeIdentityTierImage, "")
		if changed {
			if err := cmd.Flags().Set(flagIntegrityRuntimeIdentityTier, value); err != nil {
				t.Fatal(err)
			}
		}
		return cmd
	}
	if err := validateIntegrityWorkloadFlags(newCommand("payload", true), WorkloadTypeWriteVerify); err != nil {
		t.Fatalf("payload tier should be valid for verification: %v", err)
	}
	if err := validateIntegrityWorkloadFlags(newCommand("unknown", true), WorkloadTypeReadVerify); err == nil {
		t.Fatal("unknown identity tier should fail")
	}
	if err := validateIntegrityWorkloadFlags(newCommand("payload", true), WorkloadTypeWrite); err == nil {
		t.Fatal("identity tier on ordinary workload should fail")
	}

	for _, ordinary := range []string{WorkloadTypeWrite, WorkloadTypeRead, WorkloadTypeList, WorkloadTypeMixed, WorkloadTypeMock, WorkloadTypeTables} {
		cmd := newCommand("payload", true)
		markEnvApplied(cmd, flagIntegrityRuntimeIdentityTier, "payload")
		if err := cmd.Flags().Set("integrity-max-console-failures", "5"); err != nil {
			t.Fatal(err)
		}
		markEnvApplied(cmd, "integrity-max-console-failures", "5")
		if err := validateIntegrityWorkloadFlags(cmd, ordinary); err != nil {
			t.Errorf("environment integrity defaults rejected ordinary %s workload: %v", ordinary, err)
		}
	}
}

func TestValidateRunCommand(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		endpoint     string
		accessKey    string
		secretKey    string
		bucket       string
		prefix       string
		objectCount  int
		authVersion  int
		duration     string
		cleanup      bool
		createPrefix bool
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid write command with count",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  100,
			duration:     "",
			wantErr:      false,
		},
		{
			name:         "valid write command with duration",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  0,
			duration:     "5m",
			wantErr:      false,
		},
		{
			name:         "missing S3 endpoint",
			workloadType: "write",
			endpoint:     "",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  100,
			duration:     "",
			wantErr:      true,
			errContains:  "--endpoint flag is required for write workload",
		},
		{
			name:         "missing duration and count",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  0,
			duration:     "",
			wantErr:      false, // This is now allowed - defaults to 100 objects
		},
		{
			name:         "both duration and count specified",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  100,
			duration:     "5m",
			wantErr:      true,
			errContains:  "cannot specify both --object-count and --duration",
		},
		{
			name:         "valid mock command",
			workloadType: "mock",
			endpoint:     "",
			accessKey:    "",
			secretKey:    "",
			bucket:       "",
			objectCount:  100,
			duration:     "",
			wantErr:      false,
		},
		{
			name:         "mock command with missing duration/count",
			workloadType: "mock",
			endpoint:     "",
			accessKey:    "",
			secretKey:    "",
			bucket:       "",
			objectCount:  0,
			duration:     "",
			wantErr:      false, // This is now allowed - defaults to 100 objects
		},
		{
			name:         "valid list command",
			workloadType: "list",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			prefix:       "reports/",
			objectCount:  0,
			duration:     "",
			wantErr:      false,
		},
		{
			name:         "list command rejects cleanup",
			workloadType: "list",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			cleanup:      true,
			wantErr:      true,
			errContains:  "--cleanup flag is not supported for list workload",
		},
		{
			name:         "list command rejects create-prefix",
			workloadType: "list",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			createPrefix: true,
			wantErr:      true,
			errContains:  "--create-prefix flag is not supported for list workload",
		},
		{
			name:         "invalid auth version",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  10,
			authVersion:  5,
			wantErr:      true,
			errContains:  "invalid auth version",
		},
		{
			name:         "auth version override to v2",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  10,
			authVersion:  2,
			wantErr:      false,
		},
		{
			name:         "object-data-compressibility out of range",
			workloadType: "write",
			endpoint:     "http://s3.example.com",
			accessKey:    "access123",
			secretKey:    "secret456",
			bucket:       "mybucket",
			objectCount:  10,
			wantErr:      true,
			errContains:  "--object-data-compressibility must be in range [0, 100]",
		},
		{
			name:         "mock ignores auth version",
			workloadType: "mock",
			endpoint:     "",
			accessKey:    "",
			secretKey:    "",
			bucket:       "",
			objectCount:  0,
			authVersion:  7,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().String("endpoint", tt.endpoint, "")
			cmd.Flags().StringSlice("endpoints", []string{}, "")
			cmd.Flags().String("access-key", tt.accessKey, "")
			cmd.Flags().String("secret-key", tt.secretKey, "")
			cmd.Flags().String("bucket", tt.bucket, "")
			cmd.Flags().String("prefix", tt.prefix, "")
			cmd.Flags().Int("auth-version", 4, "")
			cmd.Flags().Int("object-count", tt.objectCount, "")
			cmd.Flags().String("duration", tt.duration, "")
			cmd.Flags().Bool("cleanup", false, "")
			cmd.Flags().Bool("create-prefix", false, "")
			cmd.Flags().Float64("object-data-compressibility", 0.0, "")
			cmd.Flags().String("part-size", "", "")
			cmd.Flags().String("object-size", "", "")
			if tt.authVersion != 0 {
				if err := cmd.Flags().Set("auth-version", strconv.Itoa(tt.authVersion)); err != nil {
					t.Fatalf("failed to set auth-version flag: %v", err)
				}
			}

			if tt.cleanup {
				if err := cmd.Flags().Set("cleanup", "true"); err != nil {
					t.Fatalf("failed to set cleanup flag: %v", err)
				}
			}
			if tt.createPrefix {
				if err := cmd.Flags().Set("create-prefix", "true"); err != nil {
					t.Fatalf("failed to set create-prefix flag: %v", err)
				}
			}
			if strings.Contains(tt.name, "compressibility out of range") {
				if err := cmd.Flags().Set("object-data-compressibility", "101"); err != nil {
					t.Fatalf("failed to set object-data-compressibility flag: %v", err)
				}
			}

			args := []string{tt.workloadType}
			err := ValidateRunCommand(cmd, args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRunCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateRunCommand() error = %v, want error containing %v", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestValidatePartSize(t *testing.T) {
	tests := []struct {
		name        string
		partSize    string
		objectSize  string
		mpuObjects  int
		mpuParts    int
		wantErr     bool
		errContains string
	}{
		{
			name:     "empty part size is allowed",
			partSize: "",
			wantErr:  false,
		},
		{
			name:     "valid part size 5MB",
			partSize: "5MB",
			wantErr:  false,
		},
		{
			name:     "valid part size 64MB",
			partSize: "64MB",
			wantErr:  false,
		},
		{
			name:     "valid part size 1GB",
			partSize: "1GB",
			wantErr:  false,
		},
		{
			name:        "invalid part size string",
			partSize:    "notasize",
			wantErr:     true,
			errContains: "invalid --part-size",
		},
		{
			name:        "part size must be positive",
			partSize:    "0",
			wantErr:     true,
			errContains: "positive size",
		},
		{
			name:        "part size >= object size rejected",
			partSize:    "64MB",
			objectSize:  "10MB",
			wantErr:     true,
			errContains: "must be smaller than --object-size",
		},
		{
			name:        "part size equal to object size rejected",
			partSize:    "10MB",
			objectSize:  "10MB",
			wantErr:     true,
			errContains: "must be smaller than --object-size",
		},
		{
			name:       "part size smaller than object size is valid",
			partSize:   "64MB",
			objectSize: "1GB",
			wantErr:    false,
		},
		{
			name:     "part size with no object size is valid",
			partSize: "64MB",
			wantErr:  false,
		},
		{
			name:        "mpu objects requires part size",
			partSize:    "",
			mpuObjects:  10,
			wantErr:     true,
			errContains: "--mpu-concurrent-objects can only be used when --part-size is set",
		},
		{
			name:        "mpu parts requires part size",
			partSize:    "",
			mpuParts:    5,
			wantErr:     true,
			errContains: "--mpu-concurrent-parts can only be used when --part-size is set",
		},
		{
			name:        "mpu objects negative",
			partSize:    "5MB",
			mpuObjects:  -1,
			wantErr:     true,
			errContains: "--mpu-concurrent-objects must be >= 0",
		},
		{
			name:        "mpu parts negative",
			partSize:    "5MB",
			mpuParts:    -1,
			wantErr:     true,
			errContains: "--mpu-concurrent-parts must be >= 0",
		},
		{
			name:       "valid mpu limits with part size",
			partSize:   "5MB",
			objectSize: "20MB",
			mpuObjects: 5,
			mpuParts:   10,
		},
		{
			name:       "zero mpu limits are allowed with part size",
			partSize:   "5MB",
			objectSize: "20MB",
			mpuObjects: 0,
			mpuParts:   0,
		},
		{
			name:       "zero mpu limits are allowed without part size",
			partSize:   "",
			objectSize: "20MB",
			mpuObjects: 0,
			mpuParts:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().String("part-size", tt.partSize, "")
			cmd.Flags().String("object-size", tt.objectSize, "")
			cmd.Flags().Int("mpu-concurrent-objects", tt.mpuObjects, "")
			cmd.Flags().Int("mpu-concurrent-parts", tt.mpuParts, "")

			err := validatePartSize(cmd)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePartSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("validatePartSize() error = %q, want containing %q", err.Error(), tt.errContains)
				}
			}
		})
	}
}
