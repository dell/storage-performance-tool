package server

import (
	"reflect"
	"testing"
)

func TestCSVExportPhaseAndWindow(t *testing.T) {
	a := newCSVExportApp(t, true)
	for _, tt := range []struct {
		name, query, suffix string
		timestamps          []string
	}{
		{"create", "?kind=phase&phase=CREATE", "create-phase", []string{"02", "04", "06"}},
		{"case insensitive phase", "?kind=phase&phase=create", "create-phase", []string{"02", "04", "06"}},
		{"verify includes READ operations", "?kind=phase&phase=VERIFY", "verify-phase", []string{"08", "10"}},
		{"READ alias", "?kind=phase&phase=READ", "read-phase", []string{"08", "10"}},
		{"delete", "?kind=phase&phase=DELETE", "delete-phase", []string{"12"}},
		{"inclusive boundaries", "?kind=window&phase=CREATE&start=2&end=4", "create-window", []string{"02", "04"}},
		{"fractional lower boundary", "?kind=window&phase=CREATE&start=3.5&end=6", "create-window", []string{"04", "06"}},
		{"single instant", "?kind=window&phase=CREATE&start=4&end=4", "create-window", []string{"04"}},
		// Each phase file starts elapsed_seconds at zero. Window offsets instead
		// follow the run's start time, matching the chart's shared time axis.
		{"verify uses run start", "?kind=window&phase=VERIFY&start=8&end=10", "verify-window", []string{"08", "10"}},
		{"READ window alias", "?kind=window&phase=read&start=8&end=8", "read-window", []string{"08"}},
		{"window excludes other phases", "?kind=window&phase=VERIFY&start=0&end=12", "verify-window", []string{"08", "10"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rows := decodeCSVExport(t, requestCSVExport(a, csvExportRunID, tt.query), "spt-"+csvExportRunID+"-"+tt.suffix+".csv")
			var got []string
			var want []string
			for _, row := range rows {
				got = append(got, row["timestamp"])
			}
			for _, second := range tt.timestamps {
				want = append(want, "2026-10-22T12:00:"+second+"Z")
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("exported timestamps = %v, want %v", got, want)
			}
		})
	}
}
