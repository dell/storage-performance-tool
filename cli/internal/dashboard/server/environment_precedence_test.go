package server

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestConfigValuesPrecedenceAndEmptyOverrides(t *testing.T) {
	const prefix = "SPT_DASHBOARD_ENV_TEST_"
	keys := []string{"HOME_ONLY", "PROJECT_ONLY", "PROCESS_ONLY", "PROJECT_WINS", "PROCESS_WINS", "EMPTY_WINS", "INHERITED_REF", "PROCESS_EQUALS"}
	for _, name := range keys {
		unsetEnvFixture(t, prefix+name)
	}
	t.Setenv(prefix+"PROCESS_ONLY", "process-only")
	t.Setenv(prefix+"PROCESS_WINS", "process-value")
	t.Setenv(prefix+"EMPTY_WINS", "")
	t.Setenv(prefix+"PROCESS_EQUALS", "first=second")
	root := t.TempDir()
	a := &app{home: filepath.Join(root, "home"), root: filepath.Join(root, "project")}
	writeEnvFixture(t, a.home, prefix+"HOME_ONLY=home-only\n"+prefix+"PROJECT_WINS=home-value\n"+prefix+"PROCESS_WINS=home-value\n"+prefix+"EMPTY_WINS=home-value\n")
	writeEnvFixture(t, a.root, prefix+"PROJECT_ONLY=project-only\n"+prefix+"PROJECT_WINS=project-value\n"+prefix+"PROCESS_WINS=project-value\n"+prefix+"EMPTY_WINS=project-value\n"+prefix+"INHERITED_REF=${"+prefix+"HOME_ONLY}/objects\n")
	got := a.configValues()
	want := map[string]string{
		"HOME_ONLY": "home-only", "PROJECT_ONLY": "project-only", "PROCESS_ONLY": "process-only",
		"PROJECT_WINS": "project-value", "PROCESS_WINS": "process-value", "EMPTY_WINS": "",
		"INHERITED_REF": "home-only/objects", "PROCESS_EQUALS": "first=second",
	}
	// Check only synthetic fixture keys; never dump the developer's environment.
	for name, value := range want {
		if actual, exists := got[prefix+name]; !exists || actual != value {
			t.Errorf("%s = %q (present: %v), want %q", name, actual, exists, value)
		}
	}
}

func TestConfigValuesMissingFilesAndDeterministicEnvironment(t *testing.T) {
	const key = "SPT_DASHBOARD_ENV_TEST_PROCESS_ONLY"
	t.Setenv(key, "process-only")
	a := &app{home: filepath.Join(t.TempDir(), "missing-home"), root: filepath.Join(t.TempDir(), "missing-project")}
	if got := a.configValues()[key]; got != "process-only" {
		t.Errorf("missing optional files hid the process setting: %q", got)
	}
	values := map[string]string{"Z_LAST": "", "A_FIRST": "value=with=equals", "M_MIDDLE": "space retained"}
	before := map[string]string{"Z_LAST": "", "A_FIRST": "value=with=equals", "M_MIDDLE": "space retained"}
	want := []string{"A_FIRST=value=with=equals", "M_MIDDLE=space retained", "Z_LAST="}
	if got := envList(values); !reflect.DeepEqual(got, want) {
		t.Errorf("child environment entries = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(values, before) {
		t.Error("converting environment entries mutated their source values")
	}
	if got := envList(nil); len(got) != 0 {
		t.Errorf("empty environment produced entries: %#v", got)
	}
}
