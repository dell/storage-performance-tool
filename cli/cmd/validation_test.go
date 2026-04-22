package cmd

import (
	"strconv"
	"strings"
	"testing"

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
			name:         "valid delete workload",
			workloadType: "delete",
			wantErr:      false,
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
