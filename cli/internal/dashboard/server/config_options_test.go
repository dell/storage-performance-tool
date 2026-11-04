package server

import (
	"reflect"
	"testing"
)

func customWorkloadSettings(t *testing.T) dashboardSettings {
	t.Helper()
	// Deliberately different from defaults: tests must detect hard-coded limits.
	s, err := loadSettings(settingsFixture(t, `{"limits":{
		"guided":{"object_counts":[3,7],"object_sizes":["1MiB","2KiB"],"threads":[3],"max_runtime_seconds":[20]},
		"expert":{"object_count_min":2,"object_count_max":12,"object_size_bytes_min":1024,"object_size_bytes_max":4194304,
		"threads_min":2,"threads_max":4,"max_runtime_seconds_min":10,"max_runtime_seconds_max":30,"max_total_bytes":8388608}
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCustomGuidedChoices(t *testing.T) {
	a := &app{settings: customWorkloadSettings(t)}
	for _, mode := range []string{"", "guided"} {
		for _, count := range []int{3, 7} {
			for _, size := range []string{"1MiB", "2KiB"} {
				o := options{Mode: mode, ObjectCount: count, ObjectSize: size, Threads: 3, MaxRuntimeSeconds: 20}
				if err := a.validateOptions(o); err != nil {
					t.Errorf("rejected configured choice %+v: %v", o, err)
				}
			}
		}
	}
	for _, tt := range []struct {
		name   string
		change func(*options)
	}{
		{"default count excluded", func(o *options) { o.ObjectCount = 10 }},
		{"default size excluded", func(o *options) { o.ObjectSize = "64KiB" }},
		{"default threads excluded", func(o *options) { o.Threads = 1 }},
		{"default runtime excluded", func(o *options) { o.MaxRuntimeSeconds = 60 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := options{Mode: "guided", ObjectCount: 3, ObjectSize: "2KiB", Threads: 3, MaxRuntimeSeconds: 20}
			tt.change(&o)
			if err := a.validateOptions(o); err == nil {
				t.Fatalf("accepted unconfigured choice %+v", o)
			}
		})
	}
}

func TestCustomExpertBoundaries(t *testing.T) {
	a := &app{settings: customWorkloadSettings(t)}
	for _, tt := range []struct {
		name  string
		opts  options
		valid bool
	}{
		{"minimums", options{Mode: "expert", ObjectCount: 2, ObjectSize: "1KiB", Threads: 2, MaxRuntimeSeconds: 10}, true},
		{"count threads runtime maximums", options{Mode: "expert", ObjectCount: 12, ObjectSize: "2KiB", Threads: 4, MaxRuntimeSeconds: 30}, true},
		{"size maximum and exact total", options{Mode: "expert", ObjectCount: 2, ObjectSize: "4MiB", Threads: 2, MaxRuntimeSeconds: 10}, true},
		{"exact total with more objects", options{Mode: "expert", ObjectCount: 8, ObjectSize: "1MiB", Threads: 2, MaxRuntimeSeconds: 10}, true},
		{"count below minimum", options{Mode: "expert", ObjectCount: 1, ObjectSize: "1KiB", Threads: 2, MaxRuntimeSeconds: 10}, false},
		{"count above maximum", options{Mode: "expert", ObjectCount: 13, ObjectSize: "1KiB", Threads: 2, MaxRuntimeSeconds: 10}, false},
		{"size above maximum", options{Mode: "expert", ObjectCount: 2, ObjectSize: "4097KiB", Threads: 2, MaxRuntimeSeconds: 10}, false},
		{"threads below minimum", options{Mode: "expert", ObjectCount: 2, ObjectSize: "1KiB", Threads: 1, MaxRuntimeSeconds: 10}, false},
		{"threads above maximum", options{Mode: "expert", ObjectCount: 2, ObjectSize: "1KiB", Threads: 5, MaxRuntimeSeconds: 10}, false},
		{"runtime below minimum", options{Mode: "expert", ObjectCount: 2, ObjectSize: "1KiB", Threads: 2, MaxRuntimeSeconds: 9}, false},
		{"runtime above maximum", options{Mode: "expert", ObjectCount: 2, ObjectSize: "1KiB", Threads: 2, MaxRuntimeSeconds: 31}, false},
		// Every individual field is valid; only the product exceeds the cap.
		{"total above cap", options{Mode: "expert", ObjectCount: 9, ObjectSize: "1MiB", Threads: 2, MaxRuntimeSeconds: 10}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := a.validateOptions(tt.opts); (err == nil) != tt.valid {
				t.Fatalf("validateOptions(%+v) = %v; want valid=%v", tt.opts, err, tt.valid)
			}
		})
	}
	// Use a higher custom minimum to test a canonical, positive undersized value.
	a.settings.Limits.Expert.ObjectSizeMin = 2 << 10
	if err := a.validateOptions(options{Mode: "expert", ObjectCount: 2, ObjectSize: "1KiB", Threads: 2, MaxRuntimeSeconds: 10}); err == nil {
		t.Fatal("accepted size below configured minimum")
	}
}

func TestCustomLimitsPayload(t *testing.T) {
	a := &app{settings: customWorkloadSettings(t)}
	want := map[string]any{
		"guided": map[string]any{"object_counts": []int{3, 7}, "object_sizes": []string{"2KiB", "1MiB"}, "threads": []int{3}, "max_runtime_seconds": []int{20}},
		"expert": map[string]any{
			"object_count":        map[string]int{"min": 2, "max": 12},
			"object_size_bytes":   map[string]int64{"min": 1024, "max": 4194304},
			"threads":             map[string]int{"min": 2, "max": 4},
			"max_runtime_seconds": map[string]int{"min": 10, "max": 30}, "max_total_bytes": int64(8388608),
		},
	}
	if got := a.limitsPayload(); !reflect.DeepEqual(got, want) {
		t.Fatalf("published limits = %#v; want %#v", got, want)
	}
	if got := a.settings.Limits.Guided.ObjectSizes; !reflect.DeepEqual(got, []string{"1MiB", "2KiB"}) {
		t.Fatalf("publishing limits reordered settings: %v", got)
	}
}
