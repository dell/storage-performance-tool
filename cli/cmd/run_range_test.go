package cmd

import (
	"github.com/spf13/cobra"
	"testing"
)

func TestBuildScenarioParamsRangeFlags(t *testing.T) {
	for _, tc := range []struct {
		workload, size, offset, align, driver string
		valid                                 bool
	}{
		{"read", "64KiB", "0", "4KiB", "netty", true},
		{"read", "3", "", "", "", true},
		{"write", "3", "", "", "", false},
		{"read-verify", "3", "", "", "", false},
		{"mixed", "3", "", "", "", false},
		{"read", "3", "", "", "aws", false},
		{"read", "3", "", "", "rdma", false},
		{"read", "", "0", "", "", false},
		{"read", "3", "1", "2", "", false},
	} {
		c := &cobra.Command{}
		c.Flags().String("range-size", "", "")
		c.Flags().String("range-offset", "", "")
		c.Flags().String("range-align", "", "")
		c.Flags().String("s3-driver", "", "")
		for k, v := range map[string]string{"range-size": tc.size, "range-offset": tc.offset, "range-align": tc.align, "s3-driver": tc.driver} {
			if v != "" {
				if err := c.Flags().Set(k, v); err != nil {
					t.Fatal(err)
				}
			}
		}
		p, err := buildScenarioParams(tc.workload, c)
		if (err == nil) != tc.valid {
			t.Fatalf("%+v: %v", tc, err)
		}
		if err == nil && (p.RangeSize != tc.size || p.RangeOffset != tc.offset || p.RangeAlign != tc.align) {
			t.Fatalf("lost range parameters: %+v", p)
		}
	}
	c := &cobra.Command{}
	c.Flags().String("range-size", "", "")
	if err := c.Flags().Set("range-size", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := buildScenarioParams("read", c); err == nil {
		t.Fatal("explicit empty size accepted")
	}
}
