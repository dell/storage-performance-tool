package driverstats

import (
	"reflect"
	"testing"
)

func rdmaStats(counters map[string]int64) *Stats {
	return &Stats{Driver: DriverS3RDMA, Counters: counters}
}

func TestRDMADataPathCountsTransfersAndPayloadRequestsOverHTTP(t *testing.T) {
	tests := map[string]struct {
		counters map[string]int64
		want     RDMADataPath
	}{
		"every payload over RDMA": {
			counters: map[string]int64{rdmaRequestsSent: 500, rdmaTransferred: 500},
			want:     RDMADataPath{Transferred: 500},
		},
		"both paths": {
			counters: map[string]int64{
				rdmaRequestsSent: 403, rdmaTransferred: 400, rdmaDeclinedServedOverHTTP: 3,
				httpRDMAUnavailable: 1, httpBelowThreshold: 20, httpIneligible: 300, httpOversize: 4000,
				httpFallback: 50000,
			},
			want: RDMADataPath{Transferred: 400, OverHTTP: 54324},
		},
		"nothing over RDMA": {
			counters: map[string]int64{httpRDMAUnavailable: 64},
			want:     RDMADataPath{OverHTTP: 64},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			path, ok := rdmaStats(tc.counters).RDMADataPath()
			if !ok {
				t.Fatal("S3-RDMA statistics gave no data path")
			}
			if !reflect.DeepEqual(path, tc.want) {
				t.Fatalf("data path = %+v, want %+v", path, tc.want)
			}
			if !path.Reported() {
				t.Fatal("a step that sent payload requests has nothing to report")
			}
		})
	}
}

func TestRDMADataPathReportsEachProblemCountAlone(t *testing.T) {
	for _, problem := range rdmaProblems {
		t.Run(problem.counter, func(t *testing.T) {
			path, ok := rdmaStats(map[string]int64{problem.counter: 3}).RDMADataPath()
			if !ok {
				t.Fatal("S3-RDMA statistics gave no data path")
			}
			want := []RDMAProblem{{Count: 3, What: problem.what}}
			if !reflect.DeepEqual(path.Problems, want) {
				t.Fatalf("problems = %+v, want %+v", path.Problems, want)
			}
			if !path.Reported() {
				t.Fatal("a step that met a problem has nothing to report")
			}
		})
	}
}

func TestRDMADataPathReportsRequestsWithoutAnOutcome(t *testing.T) {
	counters := map[string]int64{rdmaRequestsSent: 10}
	for _, outcome := range rdmaOutcomes {
		counters[outcome] = 1
	}
	path, _ := rdmaStats(counters).RDMADataPath()
	var withoutOutcome []RDMAProblem
	for _, problem := range path.Problems {
		if problem.What == rdmaWithoutOutcome {
			withoutOutcome = append(withoutOutcome, problem)
		}
	}
	want := []RDMAProblem{{Count: 4, What: rdmaWithoutOutcome}}
	if !reflect.DeepEqual(withoutOutcome, want) {
		t.Fatalf("requests without an outcome = %+v, want %+v (all problems %+v)", withoutOutcome, want, path.Problems)
	}
}

func TestRDMADataPathNeverReportsANegativeCount(t *testing.T) {
	// An outcome can be counted before its request is counted as sent.
	path, ok := rdmaStats(map[string]int64{rdmaRequestsSent: 9, rdmaTransferred: 10}).RDMADataPath()
	if !ok {
		t.Fatal("S3-RDMA statistics gave no data path")
	}
	if len(path.Problems) != 0 {
		t.Fatalf("outcomes ahead of requests sent were reported as a problem: %+v", path.Problems)
	}
}

func TestRDMADataPathKeepsProblemsInReportOrder(t *testing.T) {
	counters := map[string]int64{rdmaRequestsSent: 100}
	for _, problem := range rdmaProblems {
		counters[problem.counter] = 2
	}
	path, _ := rdmaStats(counters).RDMADataPath()
	if len(path.Problems) != len(rdmaProblems)+1 {
		t.Fatalf("problems = %d, want every problem counter and the requests without an outcome", len(path.Problems))
	}
	for i, problem := range rdmaProblems {
		if path.Problems[i].What != problem.what {
			t.Fatalf("problem %d = %q, want %q", i, path.Problems[i].What, problem.what)
		}
	}
	if last := path.Problems[len(path.Problems)-1]; last.What != rdmaWithoutOutcome {
		t.Fatalf("last problem = %q, want the requests without an outcome", last.What)
	}
}

func TestRDMADataPathIsOnlyForTheS3RDMADriver(t *testing.T) {
	var absent *Stats
	if _, ok := absent.RDMADataPath(); ok {
		t.Fatal("absent statistics gave an RDMA data path")
	}
	other := &Stats{Driver: "test-driver", Counters: map[string]int64{rdmaTransferred: 5}}
	if _, ok := other.RDMADataPath(); ok {
		t.Fatal("another driver's statistics gave an RDMA data path")
	}
}

func TestRDMADataPathHasNothingToReportWithoutPayloadRequests(t *testing.T) {
	// A step that only deletes or lists objects sends no payload request.
	path, ok := rdmaStats(map[string]int64{rdmaTransferred: 0, "pool_hits": 0}).RDMADataPath()
	if !ok {
		t.Fatal("S3-RDMA statistics gave no data path")
	}
	if path.Reported() {
		t.Fatalf("a step without payload requests reports %+v", path)
	}
}
