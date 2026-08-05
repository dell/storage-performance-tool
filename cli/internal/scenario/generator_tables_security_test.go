package scenario

import (
	"strings"
	"testing"
)

func TestTablesCredentialsAreCarriedOnlyInDefaults(t *testing.T) {
	const accessKey = "ROUND12_TABLES_ACCESS_7f3a"
	const secretKey = "ROUND12_TABLES_SECRET_91bc"
	for _, vector := range []string{
		tablesTestVectorTPS,
		tablesTestVectorCompaction,
		tablesTestVectorCatalog,
	} {
		t.Run(vector, func(t *testing.T) {
			params := baseTablesParams()
			params.AccessKey = accessKey
			params.SecretKey = secretKey
			params.Tables.TestVector = vector
			scenarioJS, err := GenerateTablesScenario(params)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(scenarioJS, accessKey) || strings.Contains(scenarioJS, secretKey) {
				t.Fatalf("%s scenario retained credentials", vector)
			}
			defaults, err := GenerateDefaults(params)
			if err != nil {
				t.Fatal(err)
			}
			defaultsText := string(defaults)
			if !strings.Contains(defaultsText, accessKey) || !strings.Contains(defaultsText, secretKey) {
				t.Fatalf("%s defaults did not carry launch authentication", vector)
			}
		})
	}
}
