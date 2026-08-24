package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

func TestParseDeleteTotalsV1PrefersDurableObjectAndBatchDimensions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.metrics.total.csv")
	content := "schema_version,request_unit,object_unit,batch_unit,mode,configured_batch_size,selection_order,requests_attempted,requests_full_success,requests_partial,requests_failed,requests_unresolved,objects_selected,objects_attempted,objects_accepted,objects_failed,objects_unattempted,objects_unresolved,batch_actual_requests,batch_actual_objects,batch_full_count,batch_partial_count,terminal_reconciled\n" +
		"1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,2,1,1,0,0,4,4,3,1,0,0,2,4,2,0,true\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := parseDeleteTotalsV1(path)
	if err != nil {
		t.Fatalf("parseDeleteTotalsV1: %v", err)
	}
	if got.Units.Requests != deletemetrics.RequestUnit || got.Units.Objects != deletemetrics.ObjectUnit {
		t.Fatalf("units = %#v", got.Units)
	}
	if got.Requests.Attempted != 2 || got.Objects.Accepted != 3 || got.Batches.ActualObjectCount != 4 {
		t.Fatalf("durable totals = %#v", got)
	}
}

func TestLoaderValidatesCompleteDeleteEvidenceAndRendersRecoveryInventory(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "delete-artifacts")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID,
		MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,2,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	artifacts := map[string]string{
		constants.ResultsArtifactSuffixDeleteMetricsTotal: strings.Join(deleteTotalsColumns, ",") + "\n" +
			"1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,0,1,0,0,2,2,1,1,0,0,1,2,1,0,true\n",
		constants.ResultsArtifactSuffixDeleteRequests: "schema_version,request_id,batch_id,target_count,outcome,node,start_us,duration_us,latency_us\n" +
			"1,request-1,batch-1,2,partial,local,1,2,1\n",
		constants.ResultsArtifactSuffixDeleteObjects: "schema_version,request_id,target_id,target_index,bucket,key,size,version_id,outcome,error_classification,error\n" +
			"1,request-1,target-1,0,b,a,1,,accepted,none,\n" +
			"1,request-1,target-2,1,b,b,1,,failed,operational,failure\n",
		constants.ResultsArtifactSuffixItems:       "bucket,key,size,version_id\nb,b,1,\n",
		constants.ResultsArtifactSuffixVerifyInput: "bucket,key,size,version_id\nb,a,1,\nb,b,1,\n",
	}
	selection := []byte(artifacts[constants.ResultsArtifactSuffixVerifyInput])
	selectionHash := sha256.Sum256(selection)
	selectionCompletion, err := json.Marshal(map[string]any{
		"version": 2, "status": "complete", "run_id": 77,
		"producer_kind": "cli_stager", "producer_id": "delete-selection",
		"artifact": "verify-input.csv", "source_record_count": 2,
		"unique_record_count": 2, "selected_record_count": 2,
		"excluded_delete_marker_count": 0, "manifest_bytes": len(selection),
		"manifest_sha256": hex.EncodeToString(selectionHash[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts[constants.ResultsArtifactSuffixVerifyInputCompletion] = string(selectionCompletion) + "\n"
	sha := make(map[string]string)
	for suffix, content := range artifacts {
		name := suffix
		hash := sha256.Sum256([]byte(content))
		sha[name] = hex.EncodeToString(hash[:])
		path := filepath.Join(runDir, stepID+"."+suffix)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		manifest.Steps[0].Files = append(manifest.Steps[0].Files, results.FileStatus{
			Name: stepID + "." + suffix, Size: int64(len(content)), Status: "ok",
		})
	}
	deleteCompletion, err := json.Marshal(map[string]any{
		"version": 1, "status": "complete", "schema_version": "1", "mode": "batch",
		"configured_batch_size": 2, "selection_order": "canonical",
		"contributors": []string{"local"}, "request_rows": 1, "target_rows": 2,
		"residual_rows": 1, "sha256": sha,
	})
	if err != nil {
		t.Fatal(err)
	}
	completionName := stepID + "." + constants.ResultsArtifactSuffixDeleteCompletion
	if err := os.WriteFile(filepath.Join(runDir, completionName), append(deleteCompletion, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest.Steps[0].Files = append(manifest.Steps[0].Files, results.FileStatus{
		Name: completionName, Size: int64(len(deleteCompletion) + 1), Status: "ok",
	})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{
		WorkloadType: "delete", ResultsRoot: runDir, ExpectedStepIDs: []string{stepID},
		DeleteArtifactsVersion: constants.ResultsDeleteArtifactsVersion,
		DeleteArtifactStepIDs:  []string{stepID},
		DeleteMetrics: map[string]*deletemetrics.Metrics{stepID: {
			Units: deletemetrics.Units{
				Requests: deletemetrics.RequestUnit, Objects: deletemetrics.ObjectUnit,
				Batches: deletemetrics.RequestUnit,
			},
			Requests: deletemetrics.Requests{Attempted: 1, Partial: 1, PerSecond: 12.5},
			Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 1, Failed: 1, PerSecond: 25},
			Batches: deletemetrics.Batches{
				ConfiguredSize: 2, ActualRequestCount: 1, ActualObjectCount: 2,
				MeanObjectsPerRequest: 2, FullBatchCount: 1, FullBatchPercent: 100,
			},
			Identity: deletemetrics.Identity{
				Mode: constants.DeleteIdentityModeBatch, ConfiguredBatchSize: 2,
				SelectionOrder: constants.DeleteSelectionOrderCanonical,
			},
			Versions: deletemetrics.Versions{CurrentKey: 2},
			Buckets: []deletemetrics.Bucket{{
				Bucket: "b", Selected: 2, Attempted: 2, Accepted: 1, Failed: 1,
			}},
			FailurePolicy:      deletemetrics.FailurePolicy{OperationalFailedObjects: 1},
			Completion:         deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
			TerminalReconciled: true,
		}},
	})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	step := data.Steps[stepID]
	if step.Delete == nil || step.Delete.Objects.Accepted != 1 {
		t.Fatalf("durable DELETE totals were not preferred: %#v", step.Delete)
	}
	if step.DeleteEvidence == nil || step.DeleteEvidence.ResidualRows != 1 || step.DeleteEvidence.SelectionRows != 2 {
		t.Fatalf("DELETE evidence = %#v", step.DeleteEvidence)
	}
	result, err := Aggregate(data)
	if err != nil {
		t.Fatal(err)
	}
	report := NewRenderer(RenderOptions{}).FullReport(result)
	if !strings.Contains(report, "Recovery inventory") || !strings.Contains(report, "1 residual") {
		t.Fatalf("DELETE evidence did not reach renderer:\n%s", report)
	}
	metadataBytes, err := os.ReadFile(filepath.Join(runDir, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatal(err)
	}
	var persisted RunParams
	if err := json.Unmarshal(metadataBytes, &persisted); err != nil {
		t.Fatal(err)
	}
	matching := persisted.DeleteMetrics[stepID]
	persisted.DeleteMetrics = nil
	writeParams(t, runDir, &persisted)
	if _, err := NewLoader().Load(context.Background(), runDir); err == nil ||
		!strings.Contains(err.Error(), "schema-v4 DELETE metrics unavailable") {
		t.Fatalf("current DELETE evidence without schema-v4 metrics should fail closed: %v", err)
	}
	for _, tc := range []struct {
		name    string
		wantErr string
		mutate  func(*deletemetrics.Metrics)
	}{
		{
			name: "failure classifications", wantErr: "failure classifications",
			mutate: func(metrics *deletemetrics.Metrics) {
				metrics.FailurePolicy.OperationalFailedObjects = 0
				metrics.FailurePolicy.ExcludedFailedObjects = 1
			},
		},
		{
			name: "version dimensions", wantErr: "version dimensions",
			mutate: func(metrics *deletemetrics.Metrics) {
				metrics.Versions = deletemetrics.Versions{CurrentKey: 1, ExactVersion: 1}
			},
		},
		{
			name: "bucket dimensions", wantErr: "bucket dimensions",
			mutate: func(metrics *deletemetrics.Metrics) {
				metrics.Buckets = []deletemetrics.Bucket{{
					Bucket: "wrong", Selected: 2, Attempted: 2, Accepted: 1, Failed: 1,
				}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mismatched := *matching
			tc.mutate(&mismatched)
			persisted.DeleteMetrics = map[string]*deletemetrics.Metrics{stepID: &mismatched}
			writeParams(t, runDir, &persisted)
			if _, err := NewLoader().Load(context.Background(), runDir); err == nil ||
				!strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("schema-v4 and raw %s should reconcile exactly: %v", tc.name, err)
			}
		})
	}
}

func TestDeleteEvidenceHTTPFetchLoadAggregateAndRenderCanary(t *testing.T) {
	const stepID = "mt-001-delete"
	artifacts := deleteHTTPArtifacts(t)
	server := serveDeleteHTTPArtifacts(t, stepID, artifacts)
	defer server.Close()
	runDir := t.TempDir()
	fetcher := results.NewFetcher(server.URL, runDir)
	fetcher.Retries = 1
	fetcher.Artifacts = deleteCanaryArtifactSpecs()
	if _, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{stepID}); err != nil {
		t.Fatalf("fetch DELETE artifact set: %v", err)
	}
	writeMatchingDeleteParams(t, runDir, stepID)

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("load fetched DELETE artifact set: %v", err)
	}
	result, err := Aggregate(data)
	if err != nil {
		t.Fatalf("aggregate fetched DELETE result: %v", err)
	}
	report := NewRenderer(RenderOptions{}).FullReport(result)
	if !strings.Contains(report, "Recovery inventory") || !strings.Contains(report, "0 residual") {
		t.Fatalf("fetched DELETE evidence did not reach final rendering:\n%s", report)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewLoader().Load(canceled, runDir); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("canceled DELETE evidence load = %v, want context cancellation", err)
	}
}

func TestDeleteEvidenceHTTPFetchFailsClosedBeforeCompletionPublication(t *testing.T) {
	const stepID = "mt-001-delete"
	artifacts := deleteHTTPArtifacts(t)
	delete(artifacts, "DeleteCompletion")
	server := serveDeleteHTTPArtifacts(t, stepID, artifacts)
	defer server.Close()
	runDir := t.TempDir()
	fetcher := results.NewFetcher(server.URL, runDir)
	fetcher.Retries = 1
	fetcher.Artifacts = deleteCanaryArtifactSpecs()
	if _, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{stepID}); err != nil {
		t.Fatalf("fetch partial DELETE artifact set: %v", err)
	}
	writeMatchingDeleteParams(t, runDir, stepID)

	data, err := NewLoader().Load(context.Background(), runDir)
	if err == nil || data.Steps[stepID].Status != StepStatusError {
		t.Fatalf("partial fetched DELETE publication should fail closed: status=%s err=%v", data.Steps[stepID].Status, err)
	}
}

func TestValidateDeleteEvidenceRowsReconcilesOutcomesToTotalsAndContributors(t *testing.T) {
	temp := t.TempDir()
	contents := map[string]string{
		constants.ResultsArtifactSuffixDeleteRequests: strings.Join(deleteRequestColumns, ",") + "\n" +
			"1,request-1,batch-1,2,partial,local,1,2,1\n",
		constants.ResultsArtifactSuffixDeleteObjects: strings.Join(deleteObjectColumns, ",") + "\n" +
			"1,request-1,target-1,0,b,a,1,,accepted,none,\n" +
			"1,request-1,target-2,1,b,b,1,,failed,operational,failure\n",
		constants.ResultsArtifactSuffixItems:       "bucket,key,size,version_id\nb,b,1,\n",
		constants.ResultsArtifactSuffixVerifyInput: "bucket,key,size,version_id\nb,a,1,\nb,b,1,\n",
	}
	paths := make(map[string]string, len(contents))
	for suffix, content := range contents {
		path := filepath.Join(temp, suffix)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		paths[suffix] = path
	}
	totals := &deletemetrics.Metrics{
		Requests: deletemetrics.Requests{Attempted: 1, Partial: 1},
		Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 1, Failed: 1},
		Identity: deletemetrics.Identity{ConfiguredBatchSize: 2},
	}

	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, totals); err != nil {
		t.Fatalf("valid reconciled rows: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := validateDeleteEvidenceRows(canceled, paths, []string{"local"}, totals); err == nil ||
		!strings.Contains(err.Error(), "canceled") {
		t.Fatalf("canceled bounded reconciliation = %v, want cancellation", err)
	}
	totals.Objects.Failed = 0
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, totals); err == nil {
		t.Fatal("target outcome mismatch should fail closed")
	}
	totals.Objects.Failed = 1
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local", "local"}, totals); err == nil {
		t.Fatal("duplicate contributor identity should fail closed")
	}
}

func TestValidateDeleteEvidenceRowsRejectsPerRequestContradictionsAndClassifications(t *testing.T) {
	temp := t.TempDir()
	requests := strings.Join(deleteRequestColumns, ",") + "\n" +
		"1,request-1,batch-1,1,full_success,local,1,2,1\n" +
		"1,request-2,batch-2,1,failed,local,2,2,1\n"
	objects := strings.Join(deleteObjectColumns, ",") + "\n" +
		"1,request-1,target-1,0,b,a,1,,accepted,none,\n" +
		"1,request-2,target-2,1,b,b,1,,failed,operational,failure\n"
	contents := map[string]string{
		constants.ResultsArtifactSuffixDeleteRequests: requests,
		constants.ResultsArtifactSuffixDeleteObjects:  objects,
		constants.ResultsArtifactSuffixItems:          "bucket,key,size,version_id\nb,b,1,\n",
		constants.ResultsArtifactSuffixVerifyInput:    "bucket,key,size,version_id\nb,a,1,\nb,b,1,\n",
	}
	paths := make(map[string]string, len(contents))
	for suffix, content := range contents {
		path := filepath.Join(temp, suffix)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		paths[suffix] = path
	}
	totals := &deletemetrics.Metrics{
		Requests: deletemetrics.Requests{Attempted: 2, FullSuccess: 1, Failed: 1},
		Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 1, Failed: 1},
		Identity: deletemetrics.Identity{ConfiguredBatchSize: 1},
	}
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, totals); err != nil {
		t.Fatalf("valid per-request reconciliation: %v", err)
	}

	partialRequests := strings.Join(deleteRequestColumns, ",") + "\n" +
		"1,request-1,batch-1,2,partial,local,1,2,1\n"
	protocolPartial := strings.Join(deleteObjectColumns, ",") + "\n" +
		"1,request-1,target-1,0,b,a,1,,accepted,none,\n" +
		"1,request-1,target-2,1,b,b,1,,failed,protocol,failure\n"
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteRequests], []byte(partialRequests), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteObjects], []byte(protocolPartial), 0o600); err != nil {
		t.Fatal(err)
	}
	partialTotals := &deletemetrics.Metrics{
		Requests: deletemetrics.Requests{Attempted: 1, Partial: 1},
		Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 1, Failed: 1},
		Identity: deletemetrics.Identity{ConfiguredBatchSize: 2},
	}
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, partialTotals); err == nil {
		t.Fatal("partial request with protocol-classified target should fail closed")
	}
	failedRequests := strings.Join(deleteRequestColumns, ",") + "\n" +
		"1,request-1,batch-1,2,failed,local,1,2,1\n"
	protocolObjects := strings.Join(deleteObjectColumns, ",") + "\n" +
		"1,request-1,target-1,0,b,a,1,,failed,protocol,failure\n" +
		"1,request-1,target-2,1,b,b,1,,failed,protocol,failure\n"
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteRequests], []byte(failedRequests), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteObjects], []byte(protocolObjects), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		paths[constants.ResultsArtifactSuffixItems], []byte("bucket,key,size,version_id\nb,a,1,\nb,b,1,\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	failedTotals := &deletemetrics.Metrics{
		Requests: deletemetrics.Requests{Attempted: 1, Failed: 1},
		Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Failed: 2},
		Identity: deletemetrics.Identity{ConfiguredBatchSize: 2},
	}
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, failedTotals); err != nil {
		t.Fatalf("uniform protocol-classified failed request: %v", err)
	}
	mixedClassifications := strings.Replace(protocolObjects, ",failed,protocol,", ",failed,operational,", 1)
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteObjects], []byte(mixedClassifications), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, failedTotals); err == nil {
		t.Fatal("failed request with mixed failure classifications should fail closed")
	}
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteRequests], []byte(requests), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteObjects], []byte(objects), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		paths[constants.ResultsArtifactSuffixItems], []byte("bucket,key,size,version_id\nb,b,1,\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	contradictory := strings.Replace(
		strings.Replace(requests, ",full_success,", ",swapped,", 1),
		",failed,", ",full_success,", 1,
	)
	contradictory = strings.Replace(contradictory, ",swapped,", ",failed,", 1)
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteRequests], []byte(contradictory), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, totals); err == nil {
		t.Fatal("balanced contradictory request and target outcomes should fail closed")
	}

	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteRequests], []byte(requests), 0o600); err != nil {
		t.Fatal(err)
	}
	incompatible := strings.Replace(objects, ",accepted,none,", ",accepted,operational,", 1)
	if err := os.WriteFile(paths[constants.ResultsArtifactSuffixDeleteObjects], []byte(incompatible), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateDeleteEvidenceRows(context.Background(), paths, []string{"local"}, totals); err == nil {
		t.Fatal("target outcome and failure classification mismatch should fail closed")
	}
}

func TestLoaderFailsClosedWhenDeletePublicationIsIncomplete(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "partial-delete-artifacts")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID,
		MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	name := stepID + "." + constants.ResultsArtifactSuffixDeleteRequests
	if err := os.WriteFile(filepath.Join(runDir, name), []byte("partial\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest.Steps[0].Files = append(manifest.Steps[0].Files, results.FileStatus{Name: name, Status: "ok"})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{WorkloadType: "delete", ExpectedStepIDs: []string{stepID}})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err == nil || data.Steps[stepID].Status != StepStatusError {
		t.Fatalf("partial DELETE publication should fail closed: status=%s err=%v", data.Steps[stepID].Status, err)
	}
}

func TestDeriveDeleteEvidenceDimensionsBoundsBucketsIntoCanonicalOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.objects.by-manifest.csv")
	var content strings.Builder
	content.WriteString(strings.Join(deleteObjectColumns, ",") + "\n")
	for i := 0; i < deletemetrics.MaxBucketMetrics+2; i++ {
		_, _ = fmt.Fprintf(
			&content, "1,request-%03d,target-%03d,0,bucket-%03d,key,1,,accepted,none,\n", i, i, i,
		)
	}
	if err := os.WriteFile(path, []byte(content.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	dimensions, err := deriveDeleteEvidenceDimensions(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if dimensions.versions != (deletemetrics.Versions{CurrentKey: deletemetrics.MaxBucketMetrics + 2}) {
		t.Fatalf("versions = %#v", dimensions.versions)
	}
	if len(dimensions.buckets) != deletemetrics.MaxBucketMetrics+1 ||
		dimensions.buckets[deletemetrics.MaxBucketMetrics-1].Bucket != "bucket-099" ||
		dimensions.buckets[deletemetrics.MaxBucketMetrics] != (deletemetrics.Bucket{
			Bucket: deletemetrics.OverflowBucket, Selected: 2, Attempted: 2, Accepted: 2,
		}) {
		t.Fatalf("bounded buckets = %#v", dimensions.buckets)
	}
}

func TestDeriveDeleteEvidenceDimensionsTreatsReservedBucketAsOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.objects.by-manifest.csv")
	var content strings.Builder
	content.WriteString(strings.Join(deleteObjectColumns, ",") + "\n")
	content.WriteString("1,request-overflow,target-overflow,0,__other__,key,1,,accepted,none,\n")
	for i := 0; i < deletemetrics.MaxBucketMetrics; i++ {
		_, _ = fmt.Fprintf(
			&content, "1,request-%03d,target-%03d,0,bucket-%03d,key,1,,accepted,none,\n", i, i, i,
		)
	}
	if err := os.WriteFile(path, []byte(content.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	dimensions, err := deriveDeleteEvidenceDimensions(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(dimensions.buckets) != deletemetrics.MaxBucketMetrics ||
		dimensions.buckets[deletemetrics.MaxBucketMetrics-2].Bucket != "bucket-098" ||
		dimensions.buckets[deletemetrics.MaxBucketMetrics-1] != (deletemetrics.Bucket{
			Bucket: deletemetrics.OverflowBucket, Selected: 2, Attempted: 2, Accepted: 2,
		}) {
		t.Fatalf("reserved overflow bucket mapping = %#v", dimensions.buckets)
	}
}

func TestLoaderFailsClosedOnSelectionOnlyStandaloneDeletePublication(t *testing.T) {
	runDir := t.TempDir()
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID,
		MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	for _, suffix := range []string{
		constants.ResultsArtifactSuffixVerifyInput,
		constants.ResultsArtifactSuffixVerifyInputCompletion,
	} {
		content := sharedDeleteArtifactFixture(t, suffix)
		name := stepID + "." + suffix
		if err := os.WriteFile(filepath.Join(runDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		manifest.Steps[0].Files = append(manifest.Steps[0].Files, results.FileStatus{
			Name: name, Size: int64(len(content)), Status: fileStatusOK,
		})
	}
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{
		WorkloadType: "delete", ExpectedStepIDs: []string{stepID},
		DeleteArtifactsVersion: constants.ResultsDeleteArtifactsVersion,
		DeleteArtifactStepIDs:  []string{stepID},
		DeleteMetrics:          map[string]*deletemetrics.Metrics{stepID: {}},
	})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err == nil || data.Steps[stepID].Status != StepStatusError ||
		!strings.Contains(err.Error(), "DELETE artifact publication is incomplete") {
		t.Fatalf("selection-only standalone DELETE publication should fail closed: status=%s err=%v",
			data.Steps[stepID].Status, err)
	}
}

func TestLoaderScopesDeleteArtifactExpectationToNamedStep(t *testing.T) {
	for _, setupRole := range []string{"seed", "list"} {
		t.Run(setupRole, func(t *testing.T) {
			runDir := t.TempDir()
			setupStep := "mt-001-" + setupRole
			const deleteStep = "mt-002-delete"
			metrics := sampleMetricsCSV([]string{
				`"2026-08-24T05:00:00Z",CREATE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
			})
			manifest := makeManifest(t, runDir, []stepFixture{
				{ID: setupStep, MetricsContent: metrics},
				{ID: deleteStep, MetricsContent: metrics},
			})
			writeManifest(t, runDir, manifest)
			metadata := `{"workloadType":"delete","expectedStepIds":["` + setupStep +
				`","` + deleteStep + `"],"deleteArtifactsVersion":1,"deleteArtifactStepIds":["` +
				deleteStep + `"]}`
			if err := os.WriteFile(
				filepath.Join(runDir, constants.ResultsMetadataFileName), []byte(metadata), 0o600,
			); err != nil {
				t.Fatal(err)
			}

			data, err := NewLoader().Load(context.Background(), runDir)
			if err == nil || data.Steps[deleteStep].Status != StepStatusError {
				t.Fatalf("artifact-bearing DELETE step did not fail closed: status=%s err=%v",
					data.Steps[deleteStep].Status, err)
			}
			if data.Steps[setupStep].Status != StepStatusComplete {
				t.Fatalf("ordinary %s step inherited DELETE artifact expectation: status=%s notes=%v",
					setupRole, data.Steps[setupStep].Status, data.Steps[setupStep].Notes)
			}
		})
	}
}

func TestLoaderRejectsExplicitUnsupportedDeleteArtifactVersion(t *testing.T) {
	runDir := t.TempDir()
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID, MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{
		WorkloadType: "delete", ExpectedStepIDs: []string{stepID}, DeleteArtifactsVersion: 2,
	})

	if _, err := NewLoader().Load(context.Background(), runDir); err == nil ||
		!strings.Contains(err.Error(), "unsupported DELETE artifact version") {
		t.Fatalf("explicit unsupported DELETE artifact version should fail closed: %v", err)
	}
}

func TestLoaderRejectsDeleteArtifactStepIdentityThatDoesNotResolveExactlyOnce(t *testing.T) {
	const runtimeStep = "mt-001-20260824.050000.001-delete"
	metrics := sampleMetricsCSV([]string{
		`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
	})
	for _, tc := range []struct {
		name     string
		fixtures []stepFixture
		marker   string
	}{
		{
			name: "planned identity orphaned by runtime timestamp binding",
			fixtures: []stepFixture{{
				ID: runtimeStep, MetricsContent: metrics,
			}},
			marker: "mt-001-delete",
		},
		{
			name: "runtime identity appears twice",
			fixtures: []stepFixture{
				{ID: runtimeStep, MetricsContent: metrics},
				{ID: runtimeStep, MetricsContent: metrics},
			},
			marker: runtimeStep,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runDir := t.TempDir()
			writeManifest(t, runDir, makeManifest(t, runDir, tc.fixtures))
			writeParams(t, runDir, &RunParams{
				WorkloadType: "delete", ExpectedStepIDs: []string{runtimeStep},
				DeleteArtifactsVersion: constants.ResultsDeleteArtifactsVersion,
				DeleteArtifactStepIDs:  []string{tc.marker},
			})

			if _, err := NewLoader().Load(context.Background(), runDir); err == nil ||
				!strings.Contains(err.Error(), "does not resolve exactly once") {
				t.Fatalf("invalid runtime DELETE step binding should fail closed: %v", err)
			}
		})
	}
}

func TestLoaderPreservesOrdinaryVersionlessItemsNodeArtifact(t *testing.T) {
	runDir := t.TempDir()
	const stepID = "mt-001-create"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID, MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",CREATE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	name := stepID + ".items.node-000.csv"
	if err := os.WriteFile(filepath.Join(runDir, name), []byte("bucket,key,size,version_id\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest.Steps[0].Files = append(manifest.Steps[0].Files, results.FileStatus{Name: name, Status: fileStatusOK})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{WorkloadType: "write", ExpectedStepIDs: []string{stepID}})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil || data.Steps[stepID].Status != StepStatusComplete {
		t.Fatalf("ordinary versionless items node lost compatibility: status=%s err=%v",
			data.Steps[stepID].Status, err)
	}
}

func TestLoaderPreservesOlderDeleteMetricsWithoutArtifactExpectation(t *testing.T) {
	runDir := t.TempDir()
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID,
		MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,1,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{
		WorkloadType: "delete", ExpectedStepIDs: []string{stepID},
		DeleteMetrics: map[string]*deletemetrics.Metrics{stepID: {}},
	})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil || data.Steps[stepID].Status != StepStatusComplete {
		t.Fatalf("older DELETE result lost compatibility: status=%s err=%v", data.Steps[stepID].Status, err)
	}
}

func TestParseDeleteTotalsV1RejectsMixedIdentityAndIncompleteTerminalEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.metrics.total.csv")
	content := "schema_version,request_unit,object_unit,batch_unit,mode,configured_batch_size,selection_order,requests_attempted,requests_full_success,requests_partial,requests_failed,requests_unresolved,objects_selected,objects_attempted,objects_accepted,objects_failed,objects_unattempted,objects_unresolved,batch_actual_requests,batch_actual_objects,batch_full_count,batch_partial_count,terminal_reconciled\n" +
		"1,logical_api_requests,object_identities,logical_api_requests,single,2,canonical,1,1,0,0,0,2,2,2,0,0,0,1,2,1,0,false\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseDeleteTotalsV1(path); err == nil {
		t.Fatal("expected fail-closed identity/reconciliation error")
	}
}

func TestParseDeleteTotalsV1RejectsNoncanonicalScalars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.metrics.total.csv")
	canonical := strings.Join(deleteTotalsColumns, ",") + "\n" +
		"1,logical_api_requests,object_identities,logical_api_requests,single,1,canonical,1,1,0,0,0,1,1,1,0,0,0,1,1,1,0,true\n"
	for name, content := range map[string]string{
		"spaced integer": strings.Replace(canonical, ",1,canonical,", ",01,canonical,", 1),
		"boolean case":   strings.Replace(canonical, ",true\n", ",TRUE\n", 1),
		"extra column": strings.Replace(
			strings.Replace(canonical, ",terminal_reconciled\n", ",terminal_reconciled,extra\n", 1),
			",true\n", ",true,unexpected\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := parseDeleteTotalsV1(path); err == nil {
				t.Fatal("expected noncanonical DELETE totals scalar to fail closed")
			}
		})
	}
}

func TestParseDeleteTotalsV1RejectsCounterOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.metrics.total.csv")
	content := strings.Join(deleteTotalsColumns, ",") + "\n" +
		"1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,9223372036854775807,9223372036854775807,3,0,2,2,2,0,0,0,1,2,1,0,true\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseDeleteTotalsV1(path); err == nil {
		t.Fatal("overflowing DELETE totals reconciliation should fail closed")
	}
}

func TestParseDeleteTotalsV1RejectsImpossibleBatchShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.metrics.total.csv")
	content := strings.Join(deleteTotalsColumns, ",") + "\n" +
		"1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,1,0,0,0,2,2,2,0,0,0,1,2,0,1,true\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseDeleteTotalsV1(path); err == nil {
		t.Fatal("a full-size request cannot be reported as a partial batch")
	}
}

func TestPreferDeleteTotalsPreservesRatesAndRejectsConflictingSchemaV4Evidence(t *testing.T) {
	existing := &deletemetrics.Metrics{
		Units:              deletemetrics.Units{Requests: deletemetrics.RequestUnit, Objects: deletemetrics.ObjectUnit, Batches: deletemetrics.RequestUnit},
		Requests:           deletemetrics.Requests{Attempted: 1, FullSuccess: 1, PerSecond: 12.5},
		Objects:            deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 2, PerSecond: 25},
		Batches:            deletemetrics.Batches{ConfiguredSize: 2, ActualRequestCount: 1, ActualObjectCount: 2, FullBatchCount: 1},
		Identity:           deletemetrics.Identity{Mode: constants.DeleteIdentityModeBatch, ConfiguredBatchSize: 2, SelectionOrder: constants.DeleteSelectionOrderCanonical},
		Completion:         deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		TerminalReconciled: true,
	}
	durable := &deletemetrics.Metrics{
		Units: existing.Units, Requests: deletemetrics.Requests{Attempted: 1, FullSuccess: 1},
		Objects: existing.Objects, Batches: existing.Batches, Identity: existing.Identity,
		Completion: existing.Completion, TerminalReconciled: true,
	}
	merged, err := preferDeleteTotalsV1(existing, durable, &DeleteArtifactEvidence{})
	if err != nil {
		t.Fatalf("prefer matching totals: %v", err)
	}
	if merged.Requests.PerSecond != 12.5 || merged.Objects.PerSecond != 25 {
		t.Fatalf("durable totals erased schema-v4 rates: %#v", merged)
	}
	durable.Objects.Accepted = 1
	if _, err := preferDeleteTotalsV1(existing, durable, &DeleteArtifactEvidence{}); err == nil {
		t.Fatal("conflicting schema-v4 and totals-v1 counters should fail closed")
	}
}

func TestLoaderFailsClosedOnDeleteNodeSourceWithoutAggregateCompletion(t *testing.T) {
	runDir := t.TempDir()
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{ID: stepID, MetricsContent: sampleMetricsCSV(nil)}})
	manifest.Steps[0].Files = append(manifest.Steps[0].Files, results.FileStatus{
		Name: stepID + ".delete.requests.node-000.csv", Status: "ok",
	})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{WorkloadType: "delete", ExpectedStepIDs: []string{stepID}})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err == nil || data.Steps[stepID].Status != StepStatusError {
		t.Fatalf("node-source-only DELETE evidence should fail closed: status=%s err=%v", data.Steps[stepID].Status, err)
	}
}

func deleteHTTPArtifacts(t *testing.T) map[string]string {
	t.Helper()
	artifacts := map[string]string{
		"metrics.FileTotal": sampleMetricsCSV([]string{
			`"2026-08-24T05:00:00Z",DELETE,1,1,1,1,1,0,0,1,2,1,1,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}
	for logger, name := range map[string]string{
		"DeleteMetricsTotal":        "delete.metrics.total.csv",
		"DeleteRequests":            "delete.requests.csv",
		"DeleteObjects":             "delete.objects.csv",
		"DeleteResidual":            "items.csv",
		"DeleteSelection":           "verify-input.csv",
		"DeleteSelectionCompletion": "verify-input.complete.json",
		"DeleteCompletion":          "delete.complete.json",
	} {
		artifacts[logger] = sharedDeleteArtifactFixture(t, name)
	}
	return artifacts
}

func sharedDeleteArtifactFixture(t *testing.T, name string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve shared DELETE artifact fixture source")
	}
	path := filepath.Join(
		filepath.Dir(source), "../../../../engine/core/spt-base/src/test/resources/delete-artifacts-v1", name,
	)
	content, err := os.ReadFile(path) // #nosec G304 -- path is rooted at this test source file
	if err != nil {
		t.Fatalf("read shared DELETE artifact fixture %s: %v", name, err)
	}
	return string(content)
}

func serveDeleteHTTPArtifacts(t *testing.T, stepID string, artifacts map[string]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/logs/"+stepID+"/index.json", func(w http.ResponseWriter, _ *http.Request) {
		items := make([]map[string]any, 0, len(artifacts))
		for logger, content := range artifacts {
			items = append(items, map[string]any{
				"logger": logger, "href": "/logs/" + stepID + "/" + logger,
				"size": len(content), "content_type": "text/plain",
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"step_id": stepID, "items": items})
	})
	mux.HandleFunc("/logs/"+stepID+"/", func(w http.ResponseWriter, request *http.Request) {
		logger := strings.TrimPrefix(request.URL.Path, "/logs/"+stepID+"/")
		content, ok := artifacts[logger]
		if !ok {
			http.NotFound(w, request)
			return
		}
		_, _ = w.Write([]byte(content))
	})
	return httptest.NewServer(mux)
}

func deleteCanaryArtifactSpecs() []results.ArtifactSpec {
	return []results.ArtifactSpec{
		{Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true},
		{Loggers: []string{"DeleteMetricsTotal"}, Suffix: constants.ResultsArtifactSuffixDeleteMetricsTotal},
		{Loggers: []string{"DeleteRequests"}, Suffix: constants.ResultsArtifactSuffixDeleteRequests},
		{Loggers: []string{"DeleteObjects"}, Suffix: constants.ResultsArtifactSuffixDeleteObjects},
		{Loggers: []string{"DeleteResidual"}, Suffix: constants.ResultsArtifactSuffixItems},
		{Loggers: []string{"DeleteSelection"}, Suffix: constants.ResultsArtifactSuffixVerifyInput},
		{Loggers: []string{"DeleteSelectionCompletion"}, Suffix: constants.ResultsArtifactSuffixVerifyInputCompletion},
		{Loggers: []string{"DeleteCompletion"}, Suffix: constants.ResultsArtifactSuffixDeleteCompletion},
	}
}

func writeMatchingDeleteParams(t *testing.T, runDir, stepID string) {
	t.Helper()
	writeParams(t, runDir, &RunParams{
		WorkloadType: "delete", ResultsRoot: runDir, ExpectedStepIDs: []string{stepID},
		DeleteArtifactsVersion: constants.ResultsDeleteArtifactsVersion,
		DeleteArtifactStepIDs:  []string{stepID},
		DeleteMetrics: map[string]*deletemetrics.Metrics{stepID: {
			Units: deletemetrics.Units{
				Requests: deletemetrics.RequestUnit, Objects: deletemetrics.ObjectUnit,
				Batches: deletemetrics.RequestUnit,
			},
			Requests: deletemetrics.Requests{Attempted: 1, FullSuccess: 1, PerSecond: 12.5},
			Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 2, PerSecond: 25},
			Batches: deletemetrics.Batches{
				ConfiguredSize: 2, ActualRequestCount: 1, ActualObjectCount: 2,
				MeanObjectsPerRequest: 2, FullBatchCount: 1, FullBatchPercent: 100,
			},
			Identity: deletemetrics.Identity{
				Mode: constants.DeleteIdentityModeBatch, ConfiguredBatchSize: 2,
				SelectionOrder: constants.DeleteSelectionOrderCanonical,
			},
			Versions: deletemetrics.Versions{CurrentKey: 1, ExactVersion: 1},
			Buckets: []deletemetrics.Bucket{{
				Bucket: "bucket", Selected: 2, Attempted: 2, Accepted: 2,
			}},
			Completion:         deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
			TerminalReconciled: true,
		}},
	})
}
