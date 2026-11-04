package server

import "testing"

// A mock run has only the create phase. Once it is no longer the active job,
// Run history must still call it completed, not partial (Dev VM, Oct 23), and
// its saved options must name the mock driver.
func TestMockRunHistoryIsCompletedWhenNotActive(t *testing.T) {
	root := t.TempDir()
	a := newHistoryApp(t, root)
	const withSummary = "mt-dashboard-20261023T214759-de8f1520"
	const noSummary = "mt-dashboard-20261023T214850-24866f7d"
	const shortMock = "mt-dashboard-20261023T215136-b32ab882"
	const notStarted = "mt-dashboard-20261023T215226-520e6984"
	const writeVerifyOnePhase = "mt-dashboard-20261023T215300-0a0b0c0d"
	writeSavedRun(t, a.results, withSummary, driverMock, 25, 25, true)
	writeSavedRun(t, a.results, noSummary, driverMock, 25, 25, false)
	writeSavedRun(t, a.results, shortMock, driverMock, 25, 7, false)
	writeSavedRun(t, a.results, notStarted, driverMock, 25, -1, false)
	writeSavedRun(t, a.results, writeVerifyOnePhase, driverWriteVerify, 25, 25, false)
	got := historyStatuses(t, a)
	want := map[string]string{
		withSummary: jobCompleted, noSummary: jobCompleted, shortMock: jobPartial,
		notStarted: jobPartial, writeVerifyOnePhase: jobPartial,
	}
	for id, status := range want {
		if got[id] != status {
			t.Errorf("%s: status %q, want %q (all: %v)", id, got[id], status, got)
		}
	}
	job := savedRunStatusOf(t, a, noSummary)
	if opts, _ := job[keyOptions].(map[string]any); opts["driver"] != driverMock {
		t.Errorf("saved mock run options must name the mock driver: %#v", job[keyOptions])
	}
	job = savedRunStatusOf(t, a, writeVerifyOnePhase)
	if opts, _ := job[keyOptions].(map[string]any); opts["driver"] != driverS3 {
		t.Errorf("saved write-verify run options must name the S3 driver: %#v", job[keyOptions])
	}
}
