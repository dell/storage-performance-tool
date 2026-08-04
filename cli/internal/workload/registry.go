// Package workload owns the public run-workload registry shared by CLI validation and scenario generation.
package workload

// Write and the constants in this block are the public CLI workload names.
const (
	Write       = "write"
	Read        = "read"
	WriteVerify = "write-verify"
	ReadVerify  = "read-verify"
	Mixed       = "mixed"
	Delete      = "delete"
	List        = "list"
	Mock        = "mock"
	Tables      = "tables"
)

// Spec describes one accepted workload and whether scenario generation is implemented.
type Spec struct {
	Name        string
	Description string
	Implemented bool
}

var registry = []Spec{
	{Write, "Perform a write-only test, creating new objects.", true},
	{Read, "Perform a read benchmark on pre-existing objects.", true},
	{WriteVerify, "Write objects and verify each successful write now or in a later campaign.", true},
	{ReadVerify, "Discover or load self-verifying objects and verify each once.", true},
	{Mixed, "Run a configured mix of S3 operations.", true},
	{Delete, "Measure object deletion performance.", false},
	{List, "Benchmark object listing throughput.", true},
	{Mock, "Run with the dummy-mock driver.", true},
	{Tables, "Benchmark S3 Tables operations.", true},
}

// All returns a copy of the registry in help-display order.
func All() []Spec {
	return append([]Spec(nil), registry...)
}

// Names returns every accepted workload name.
func Names() []string {
	names := make([]string, 0, len(registry))
	for _, spec := range registry {
		names = append(names, spec.Name)
	}
	return names
}

// Lookup resolves one exact, case-sensitive workload name.
func Lookup(name string) (Spec, bool) {
	for _, spec := range registry {
		if spec.Name == name {
			return spec, true
		}
	}
	return Spec{}, false
}
