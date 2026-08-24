package scenario

import (
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestGenerateDeleteScenarioWiresIndependentVerificationPhases(t *testing.T) {
	tests := []struct {
		name      string
		params    Params
		pre, post bool
	}{
		{name: "neither", params: Params{}},
		{name: "post only", params: Params{VerifyDelete: true, VerifyDeleteExplicit: true}, post: true},
		{name: "validation defaults post", params: Params{ValidateDeleteInventory: true}, pre: true, post: true},
		{name: "validation explicit post false", params: Params{ValidateDeleteInventory: true, VerifyDeleteExplicit: true}, pre: true},
		{name: "validation explicit post true", params: Params{ValidateDeleteInventory: true, VerifyDelete: true, VerifyDeleteExplicit: true}, pre: true, post: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := test.params
			params.WorkloadType = workload.Delete
			params.RunID = 77
			params.ItemsFile = "/spt-input/items/verify-input.csv"
			params.Threads = 1
			params.DeleteBatchSize = 1
			params.VerificationTimeout = 30 * time.Second
			generated, err := GenerateDeleteScenario(params)
			if err != nil {
				t.Fatal(err)
			}
			for fragment, want := range map[string]bool{
				`"preValidation": true`:              test.pre,
				`"postVerification": true`:           test.post,
				`"verificationTimeoutMillis": 30000`: test.pre || test.post,
			} {
				if strings.Contains(generated, fragment) != want {
					t.Fatalf("fragment %q presence = %t, want %t:\n%s", fragment, strings.Contains(generated, fragment), want, generated)
				}
			}
		})
	}
}
