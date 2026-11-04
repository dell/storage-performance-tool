package server

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func newJobStorageFixture(t *testing.T) *app {
	t.Helper()
	root := t.TempDir()
	return &app{stateDir: root, stateFile: filepath.Join(root, "latest-job.json")}
}

func storageJob(id string) map[string]any {
	return map[string]any{keyRunID: id, keyStatus: jobCompleted, "returncode": 0, keyMessage: "saved outcome",
		keyOptions: map[string]any{"object_count": 10, "object_size": "64KiB"},
		"pid":      987, "results_dir": "/PRIVATE_RESULTS", "log_file": "/PRIVATE_LOG", "secret": "PRIVATE_CANARY"}
}

func TestJobStorageAtomicFailuresPreservePreviousFiles(t *testing.T) {
	for _, kind := range []string{"latest state", "run record"} {
		for _, failure := range []string{"encoding", "temporary file blocked"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				a := newJobStorageFixture(t)
				job := storageJob("mt-storage-atomic")
				write, path := a.writeState, a.stateFile
				if kind == "run record" {
					write, path = a.writeRunRecord, a.runRecordPath(str(job[keyRunID]))
				}
				if err := write(job); err != nil {
					t.Fatal(err)
				}
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				bad := cloneMap(job)
				bad[keyStatus] = jobFailed
				if failure == "encoding" {
					bad[keyMessage] = make(chan int)
				} else if err := os.Mkdir(path+".tmp", 0700); err != nil {
					t.Fatal(err)
				}
				if err := write(bad); err == nil {
					t.Fatal("failed write unexpectedly succeeded")
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(after, before) {
					t.Fatal("failed write replaced previous durable outcome")
				}
				if failure == "temporary file blocked" {
					if err := os.Remove(path + ".tmp"); err != nil {
						t.Fatal(err)
					}
				}
				good := cloneMap(job)
				good[keyStatus] = jobFailed
				if err := write(good); err != nil {
					t.Fatalf("retry after repair: %v", err)
				}
				data, err := os.ReadFile(path)
				var saved map[string]any
				if err != nil || json.Unmarshal(data, &saved) != nil || saved[keyStatus] != jobFailed {
					t.Fatal("repaired write did not persist the new outcome")
				}
			})
		}
	}
}

func TestJobStorageRenameFailureAllowsRetry(t *testing.T) {
	for _, kind := range []string{"latest state", "run record"} {
		t.Run(kind, func(t *testing.T) {
			a := newJobStorageFixture(t)
			job := storageJob("mt-storage-rename")
			write, path := a.writeState, a.stateFile
			if kind == "run record" {
				write, path = a.writeRunRecord, a.runRecordPath(str(job[keyRunID]))
			}
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(path, "sentinel")
			if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := write(job); err == nil {
				t.Fatal("renamed a file onto a nonempty directory")
			}
			if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
				t.Fatal("failed rename damaged an existing directory")
			}
			if err := os.Remove(sentinel); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := write(job); err != nil {
				t.Fatalf("retry after failed rename: %v", err)
			}
			if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
				t.Fatal("successful rename left a temporary file")
			}
		})
	}
}

func TestJobStorageRepairsTemporaryFilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix file permission contract")
	}
	for _, kind := range []string{"latest state", "run record"} {
		t.Run(kind, func(t *testing.T) {
			a := newJobStorageFixture(t)
			job := storageJob("mt-storage-permissions")
			write, path := a.writeState, a.stateFile
			if kind == "run record" {
				write, path = a.writeRunRecord, a.runRecordPath(str(job[keyRunID]))
			}
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+".tmp", []byte("stale"), 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path+".tmp", 0644); err != nil {
				t.Fatal(err)
			}
			if err := write(job); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("saved file permissions = %v, %v; want 0600", info, err)
			}
		})
	}
}

func TestJobStorageRejectsDamagedAndMismatchedRecords(t *testing.T) {
	a := newJobStorageFixture(t)
	first, second := "mt-storage-first", "mt-storage-second"
	for _, id := range []string{first, second} {
		if err := a.writeRunRecord(storageJob(id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, body := range []string{"", "{", "null", "[]", "1", `{"run_id":"mt-storage-other","status":"completed"}`, `{} {}`} {
		if err := os.WriteFile(a.runRecordPath(first), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if got := a.readRunRecord(first); got != nil {
			t.Errorf("damaged or mismatched record %q was accepted: %#v", body, got)
		}
		if got := a.readRunRecord(second); got[keyRunID] != second || got[keyStatus] != jobCompleted {
			t.Fatal("damaged neighboring record affected a valid outcome")
		}
	}
	for _, id := range []string{"", "../outside", "mt-../outside", `mt-..\outside`, "mt-bad\x00id"} {
		if err := a.writeRunRecord(storageJob(id)); err == nil || a.readRunRecord(id) != nil {
			t.Errorf("unsafe record id %q was accepted", id)
		}
	}
	if got := a.readRunRecord("mt-storage-missing"); got != nil {
		t.Fatal("missing record returned another run's outcome")
	}
	data, err := os.ReadFile(a.runRecordPath(second))
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range [][]byte{[]byte("pid"), []byte("results_dir"), []byte("log_file"), []byte("PRIVATE_CANARY")} {
		if bytes.Contains(data, private) {
			t.Errorf("public run record persisted private field %q", private)
		}
	}
}

func TestJobStorageRejectsDamagedLatestState(t *testing.T) {
	a := newJobStorageFixture(t)
	if got := a.readStateFile(); got != nil {
		t.Fatal("missing latest state returned a job")
	}
	for _, body := range []string{"", "{", "null", "[]", "1", `{} {}`} {
		if err := os.WriteFile(a.stateFile, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if got := a.readStateFile(); got != nil {
			t.Errorf("damaged latest state %q was accepted: %#v", body, got)
		}
	}
}

func TestJobStorageParentFailureAllowsRetry(t *testing.T) {
	for _, kind := range []string{"latest state", "run record"} {
		t.Run(kind, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "state")
			if err := os.WriteFile(parent, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			a := &app{stateDir: parent, stateFile: filepath.Join(parent, "latest-job.json")}
			write := a.writeState
			if kind == "run record" {
				write = a.writeRunRecord
			}
			job := storageJob("mt-storage-parent")
			if err := write(job); err == nil {
				t.Fatal("saved through a non-directory parent")
			}
			if data, err := os.ReadFile(parent); err != nil || string(data) != "keep" {
				t.Fatal("failed save overwrote the blocking parent")
			}
			if err := os.Remove(parent); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			if err := write(job); err != nil {
				t.Fatalf("save after parent repair: %v", err)
			}
		})
	}
}
