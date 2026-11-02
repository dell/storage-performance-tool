package server

import (
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestStorageReloadRestoresVerification(t *testing.T) {
	for _, status := range []string{verifyPassed, verifyFailed, verifyRunning} {
		t.Run(status, func(t *testing.T) {
			a, _ := newVerificationTestApp(t)
			cfg := storageConfig{Hosts: []string{"node-a", "root@node-b"}, Endpoint: "https://storage.invalid"}
			stored := storageConfig{Hosts: []string{" node-a ", "root@node-b"}, Endpoint: " https://storage.invalid "}
			v := verification{Status: status, Signature: configSignature(cfg.Hosts, cfg.Endpoint), Summary: "saved summary", StartedAt: "2026-10-30T12:00:00Z",
				Nodes:    []verifyNode{{Host: "node-a", Status: verifyPassed, Checks: []verifyCheck{{Name: "Docker available", Status: "pass"}}}},
				Endpoint: verifyEndpoint{Status: verifyPassed}}
			if err := writeFileAtomic(a.storageFile, stored); err != nil {
				t.Fatal(err)
			}
			if err := writeFileAtomic(a.verifyFile, v); err != nil {
				t.Fatal(err)
			}
			reloaded := &app{storageFile: a.storageFile, verifyFile: a.verifyFile}
			reloaded.loadStorage()
			if status == verifyRunning {
				v.Status = verifyFailed
				v.Summary = "The dashboard restarted before verification finished."
			}
			if !reflect.DeepEqual(reloaded.storage, cfg) || !reflect.DeepEqual(reloaded.verification, v) {
				t.Fatalf("reload = %+v, %+v", reloaded.storage, reloaded.verification)
			}
			target := map[string]any{"hosts": cfg.Hosts, "endpoint": cfg.Endpoint}
			if reloaded.verified(target) != (status == verifyPassed) {
				t.Fatal("restart incorrectly authorized a run")
			}
			target["endpoint"] = "https://changed.invalid"
			if reloaded.currentVerification(target).Status != "idle" || reloaded.verified(target) {
				t.Fatal("stale verification authorized a changed target")
			}
			// Callers must not mutate saved configuration or nested check lists
			// through the snapshots used to construct API responses.
			snapshot := reloaded.storageSnapshot()
			snapshot.Hosts[0] = "changed"
			copy := reloaded.verification.clone()
			copy.Nodes[0].Checks[0].Status = "changed"
			if reloaded.storage.Hosts[0] != "node-a" || reloaded.verification.Nodes[0].Checks[0].Status != "pass" {
				t.Fatal("snapshot mutated stored state")
			}
			if runtime.GOOS != "windows" {
				for _, path := range []string{a.storageFile, a.verifyFile} {
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != 0600 {
						t.Fatalf("%s permissions = %o", path, info.Mode().Perm())
					}
				}
			}
		})
	}
}

func TestStorageReloadIgnoresInvalidSavedFiles(t *testing.T) {
	for _, body := range []string{"", "{", `{"hosts":[]}`, `{"hosts":["node;id"]}`, `{"hosts":["node-a"],"endpoint":"https://key:secret@storage.invalid"}`} {
		t.Run(body, func(t *testing.T) {
			a, _ := newVerificationTestApp(t)
			before := a.storageSnapshot()
			if body != "" {
				if err := os.WriteFile(a.storageFile, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(a.verifyFile, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			a.loadStorage()
			if !reflect.DeepEqual(a.storageSnapshot(), before) || a.verification.Status != "" {
				t.Fatal("invalid saved data replaced safe defaults")
			}
		})
	}
}

func TestAtomicVerificationSaveRejectsUnencodableValue(t *testing.T) {
	a, _ := newVerificationTestApp(t)
	if err := writeFileAtomic(a.verifyFile, verification{Status: verifyPassed}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(a.verifyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(a.verifyFile, make(chan int)); err == nil {
		t.Fatal("unencodable value accepted")
	}
	after, err := os.ReadFile(a.verifyFile)
	if err != nil || string(after) != string(before) || !strings.Contains(string(after), `"status":"passed"`) {
		t.Fatal("failed save replaced previous verification")
	}
}
