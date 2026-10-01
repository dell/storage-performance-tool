package cmd

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"gopkg.in/yaml.v3"
)

func TestRegionFlagToEngineYAML(t *testing.T) {
	flag := runCmd.Flags().Lookup("region")
	if flag == nil || flag.DefValue != "" {
		t.Fatal("run must register --region with an empty default")
	}
	for _, driver := range []string{"default", "aws", "rdma"} {
		for _, region := range []string{"", "us-west-2"} {
			t.Run(driver+"/"+region, func(t *testing.T) {
				cmd := newRunLikeCmd()
				cmd.Flags().String("region", flag.DefValue, flag.Usage)
				if err := cmd.ParseFlags([]string{"--endpoints", "https://s3.example.com", "--s3-driver", driver, "--region", region}); err != nil {
					t.Fatal(err)
				}
				params, err := buildScenarioParams("write", cmd)
				if err != nil {
					t.Fatal(err)
				}
				data, err := scenario.GenerateDefaults(params)
				if err != nil {
					t.Fatal(err)
				}
				var config map[string]interface{}
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatal(err)
				}
				storage := config["storage"].(map[string]interface{})
				got, exists := storage["region"]
				if region == "" {
					if exists {
						t.Fatalf("empty region must preserve engine default, got %v", got)
					}
				} else if got != region {
					t.Fatalf("storage.region = %v, want %s", got, region)
				}
			})
		}
	}
}
