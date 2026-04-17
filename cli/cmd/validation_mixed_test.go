package cmd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// buildMixedCmd constructs a cobra.Command with all mixed-workload flags set as provided.
func buildMixedCmd(get, put, del, stat int, readItems, deleteItems, duration string, seedObjects int, cleanup bool) *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "http://s3.example.com", "")
	cmd.Flags().StringSlice("endpoints", []string{}, "")
	cmd.Flags().String("access-key", "ak", "")
	cmd.Flags().String("secret-key", "sk", "")
	cmd.Flags().String("bucket", "b", "")
	cmd.Flags().String("duration", duration, "")
	cmd.Flags().Int("object-count", 0, "")
	cmd.Flags().Int("seed-objects", seedObjects, "")
	cmd.Flags().Int("get-distrib", get, "")
	cmd.Flags().Int("put-distrib", put, "")
	cmd.Flags().Int("delete-distrib", del, "")
	cmd.Flags().Int("stat-distrib", stat, "")
	cmd.Flags().String("read-items-file", readItems, "")
	cmd.Flags().String("delete-items-file", deleteItems, "")
	cmd.Flags().Bool("cleanup", false, "")
	if cleanup {
		if err := cmd.Flags().Set("cleanup", "true"); err != nil {
			panic(fmt.Sprintf("failed to set cleanup: %v", err))
		}
	}
	return cmd
}

func TestValidateMixedFlags(t *testing.T) {
	const (
		dur  = "2m"
		seed = 2500
	)

	tests := []struct {
		name        string
		get         int
		put         int
		del         int
		stat        int
		readItems   string
		deleteItems string
		duration    string
		seedObjects int
		cleanup     bool
		wantErr     bool
		errContains string
	}{
		{
			name: "valid default distribution (4-op)",
			get:  45, put: 15, del: 10, stat: 30,
			duration: dur, seedObjects: seed,
		},
		{
			name: "valid 3-op distribution GET+PUT+DELETE",
			get:  45, put: 40, del: 15, stat: 0,
			duration: dur, seedObjects: seed,
		},
		{
			name: "valid two-op distribution GET+PUT",
			get:  70, put: 30, del: 0, stat: 0,
			duration: dur, seedObjects: seed,
		},
		{
			name: "valid distribution with STAT only (GET+STAT)",
			get:  60, put: 0, del: 0, stat: 40,
			duration: dur, seedObjects: seed,
		},
		{
			name: "sum too low",
			get:  45, put: 15, del: 10, stat: 0,
			duration: dur, seedObjects: seed,
			wantErr:     true,
			errContains: "sum to 70",
		},
		{
			name: "sum too high",
			get:  50, put: 30, del: 25, stat: 0,
			duration: dur, seedObjects: seed,
			wantErr:     true,
			errContains: "must equal 100",
		},
		{
			name: "negative distribution value",
			get:  105, put: 0, del: -5, stat: 0,
			duration: dur, seedObjects: seed,
			wantErr:     true,
			errContains: "negative",
		},
		{
			name: "delete exceeds put without seed file",
			get:  50, put: 10, del: 20, stat: 20,
			duration: dur, seedObjects: seed,
			wantErr:     true,
			errContains: "delete-distrib (20) exceeds put-distrib (10)",
		},
		{
			name: "delete exceeds put with seed file - constraint relaxed",
			get:  50, put: 10, del: 20, stat: 20,
			deleteItems: "/some/path/items.csv",
			duration:    dur, seedObjects: seed,
		},
		{
			name: "stat-distrib non-zero accepted with valid sum",
			get:  40, put: 20, del: 10, stat: 30,
			duration: dur, seedObjects: seed,
		},
		{
			name: "only one non-zero distribution",
			get:  100, put: 0, del: 0, stat: 0,
			duration: dur, seedObjects: seed,
			wantErr:     true,
			errContains: "at least two",
		},
		{
			name: "duration required",
			get:  45, put: 15, del: 10, stat: 30,
			duration: "", seedObjects: seed,
			wantErr:     true,
			errContains: "--duration",
		},
		{
			name: "seed-objects required without read-items-file",
			get:  45, put: 15, del: 10, stat: 30,
			duration: dur, seedObjects: 0,
			wantErr:     true,
			errContains: "--seed-objects",
		},
		{
			name: "seed-objects not required with read-items-file",
			get:  45, put: 15, del: 10, stat: 30,
			readItems: "/some/path/items.csv",
			duration:  dur, seedObjects: 0,
		},
		{
			name: "cleanup with read-items-file is an error",
			get:  45, put: 15, del: 10, stat: 30,
			readItems: "/some/path/items.csv",
			duration:  dur, seedObjects: 0, cleanup: true,
			wantErr:     true,
			errContains: "--cleanup",
		},
		{
			name: "cleanup with delete-items-file is an error",
			get:  45, put: 15, del: 10, stat: 30,
			deleteItems: "/some/path/items.csv",
			duration:    dur, seedObjects: seed, cleanup: true,
			wantErr:     true,
			errContains: "--cleanup",
		},
		{
			name: "zero PUT with nonzero DELETE and no seed file",
			get:  50, put: 0, del: 30, stat: 20,
			duration: dur, seedObjects: seed,
			wantErr:     true,
			errContains: "delete-distrib (30) exceeds put-distrib (0)",
		},
		{
			name: "zero PUT with nonzero DELETE and seed file - ok",
			get:  50, put: 0, del: 30, stat: 20,
			deleteItems: "/some/path/items.csv",
			duration:    dur, seedObjects: seed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := buildMixedCmd(
				tt.get, tt.put, tt.del, tt.stat,
				tt.readItems, tt.deleteItems,
				tt.duration, tt.seedObjects, tt.cleanup,
			)

			err := ValidateMixedFlags(cmd)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMixedFlags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateMixedFlags() error = %q, want containing %q", err.Error(), tt.errContains)
				}
			}
		})
	}
}
