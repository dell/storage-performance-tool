package constants

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// The engine keeps a finished step's metrics row for api.linger.sec, and the CLI never sets it.
// A run whose metrics went unread for less than the retention cannot have missed a step only if
// the CLI's retention is the one the engine ships with.
func TestEngineTerminalRowRetentionIsTheEngineDefault(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve engine defaults source")
	}
	path := filepath.Join(filepath.Dir(source), "../../../engine/core/spt-base/src/main/resources/config/defaults.yaml")
	content, err := os.ReadFile(path) // #nosec G304 -- path is rooted at this test source file
	if err != nil {
		t.Fatalf("read engine defaults: %v", err)
	}
	var defaults struct {
		API struct {
			Linger struct {
				Sec *int `yaml:"sec"`
			} `yaml:"linger"`
		} `yaml:"api"`
	}
	if err = yaml.Unmarshal(content, &defaults); err != nil {
		t.Fatalf("decode engine defaults: %v", err)
	}
	if defaults.API.Linger.Sec == nil {
		t.Fatal("engine defaults have no api.linger.sec")
	}
	if got := time.Duration(*defaults.API.Linger.Sec) * time.Second; got != EngineTerminalRowRetention {
		t.Fatalf("engine api.linger.sec = %s, EngineTerminalRowRetention = %s", got, EngineTerminalRowRetention)
	}
	if AutoResultsDriverStatsInterval >= EngineTerminalRowRetention/2 {
		t.Fatalf("driver statistics are read every %s, not well within the retention of %s",
			AutoResultsDriverStatsInterval, EngineTerminalRowRetention)
	}
}
