/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
package cmd

import (
	"strings"
	"testing"
	"time"
)

func TestParseTablesTimeoutMs(t *testing.T) {
	cases := []struct {
		input   string
		wantMs  int64
		wantErr bool
	}{
		{"4h", int64(4 * time.Hour / time.Millisecond), false},
		{"30m", int64(30 * time.Minute / time.Millisecond), false},
		{"90s", 90000, false},
		{"0s", 0, false},
		{"", 0, false},
		{"invalid", 0, true},
	}
	for _, c := range cases {
		got, err := parseTablesTimeoutMs(c.input)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseTablesTimeoutMs(%q): expected error, got nil", c.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseTablesTimeoutMs(%q): unexpected error: %v", c.input, err)
			continue
		}
		if got != c.wantMs {
			t.Errorf("parseTablesTimeoutMs(%q) = %d, want %d", c.input, got, c.wantMs)
		}
	}
}

func TestValidateWorkloadType_Tables(t *testing.T) {
	if err := ValidateWorkloadType(WorkloadTypeTables); err != nil {
		t.Errorf("ValidateWorkloadType(%q) returned unexpected error: %v", WorkloadTypeTables, err)
	}
}

func TestBuildScenarioParams_Tables(t *testing.T) {
	isTesting = true

	cmd := runCmd
	args := []string{"tables"}

	if err := cmd.Flags().Set("endpoint", "http://172.17.0.2:4566"); err != nil {
		t.Fatalf("set endpoint: %v", err)
	}
	if err := cmd.Flags().Set("access-key", "testkey"); err != nil {
		t.Fatalf("set access-key: %v", err)
	}
	if err := cmd.Flags().Set("secret-key", "testsecret"); err != nil {
		t.Fatalf("set secret-key: %v", err)
	}
	if err := cmd.Flags().Set("test-vector", "tps"); err != nil {
		t.Fatalf("set test-vector: %v", err)
	}
	if err := cmd.Flags().Set("table-bucket", "my-tables"); err != nil {
		t.Fatalf("set table-bucket: %v", err)
	}
	if err := cmd.Flags().Set("namespace", "testns"); err != nil {
		t.Fatalf("set namespace: %v", err)
	}
	if err := cmd.Flags().Set("concurrent-writers", "8"); err != nil {
		t.Fatalf("set concurrent-writers: %v", err)
	}
	if err := cmd.Flags().Set("target-file-size", "1MB"); err != nil {
		t.Fatalf("set target-file-size: %v", err)
	}
	if err := cmd.Flags().Set("compaction-timeout", "2h"); err != nil {
		t.Fatalf("set compaction-timeout: %v", err)
	}
	if err := cmd.Flags().Set("duration", "60s"); err != nil {
		t.Fatalf("set duration: %v", err)
	}

	_ = args
	params, err := buildScenarioParams(WorkloadTypeTables, cmd)
	if err != nil {
		t.Fatalf("buildScenarioParams failed: %v", err)
	}

	if params.WorkloadType != WorkloadTypeTables {
		t.Errorf("WorkloadType = %q, want %q", params.WorkloadType, WorkloadTypeTables)
	}
	if params.Tables.TestVector != "tps" {
		t.Errorf("Tables.TestVector = %q, want tps", params.Tables.TestVector)
	}
	if params.Tables.TableBucket != "my-tables" {
		t.Errorf("Tables.TableBucket = %q, want my-tables", params.Tables.TableBucket)
	}
	if params.Tables.Namespace != "testns" {
		t.Errorf("Tables.Namespace = %q, want testns", params.Tables.Namespace)
	}
	if params.Tables.ConcurrentWriters != 8 {
		t.Errorf("Tables.ConcurrentWriters = %d, want 8", params.Tables.ConcurrentWriters)
	}
	if params.Tables.TargetFileSizeBytes != 1048576 {
		t.Errorf("Tables.TargetFileSizeBytes = %d, want 1048576", params.Tables.TargetFileSizeBytes)
	}
	wantTimeoutMs := int64(2 * time.Hour / time.Millisecond)
	if params.Tables.CompactionTimeoutMs != wantTimeoutMs {
		t.Errorf("Tables.CompactionTimeoutMs = %d, want %d", params.Tables.CompactionTimeoutMs, wantTimeoutMs)
	}
	if params.Duration != "60s" {
		t.Errorf("Duration = %q, want 60s", params.Duration)
	}
	// ObjectSize should be empty for tables (no default set)
	if params.ObjectSize != "" {
		t.Errorf("ObjectSize should be empty for tables workload, got %q", params.ObjectSize)
	}
}

func TestBuildScenarioParams_Tables_NoProvision(t *testing.T) {
	isTesting = true

	cmd := runCmd
	if err := cmd.Flags().Set("no-provision", "true"); err != nil {
		t.Fatalf("set no-provision: %v", err)
	}

	params, err := buildScenarioParams(WorkloadTypeTables, cmd)
	if err != nil {
		t.Fatalf("buildScenarioParams failed: %v", err)
	}
	if !params.Tables.NoProvision {
		t.Error("Tables.NoProvision should be true")
	}
}

func TestValidateS3Flags_TablesSkipsBucket(t *testing.T) {
	isTesting = true

	cmd := runCmd
	if err := cmd.Flags().Set("endpoint", "http://172.17.0.2:4566"); err != nil {
		t.Fatalf("set endpoint: %v", err)
	}
	if err := cmd.Flags().Set("access-key", "testkey"); err != nil {
		t.Fatalf("set access-key: %v", err)
	}
	if err := cmd.Flags().Set("secret-key", "testsecret"); err != nil {
		t.Fatalf("set secret-key: %v", err)
	}
	// Deliberately do NOT set --bucket — tables should not require it
	if err := cmd.Flags().Set("bucket", ""); err != nil {
		t.Fatalf("clear bucket: %v", err)
	}

	if err := ValidateS3Flags(cmd, WorkloadTypeTables); err != nil {
		t.Errorf("ValidateS3Flags should not require --bucket for tables workload, got: %v", err)
	}
}

func TestValidateS3Flags_NonTablesBucketRequired(t *testing.T) {
	isTesting = true

	cmd := runCmd
	if err := cmd.Flags().Set("endpoint", "http://172.17.0.2:4566"); err != nil {
		t.Fatalf("set endpoint: %v", err)
	}
	if err := cmd.Flags().Set("access-key", "testkey"); err != nil {
		t.Fatalf("set access-key: %v", err)
	}
	if err := cmd.Flags().Set("secret-key", "testsecret"); err != nil {
		t.Fatalf("set secret-key: %v", err)
	}
	if err := cmd.Flags().Set("bucket", ""); err != nil {
		t.Fatalf("clear bucket: %v", err)
	}

	err := ValidateS3Flags(cmd, WorkloadTypeWrite)
	if err == nil {
		t.Error("ValidateS3Flags should require --bucket for write workload")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("error should mention bucket, got: %v", err)
	}
}
