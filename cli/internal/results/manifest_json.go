package results

import "encoding/json"

// manifestFields avoids recursively invoking the Manifest JSON methods.
type manifestFields Manifest

// UnmarshalJSON retains independent top-level fields for every index writer.
func (m *Manifest) UnmarshalJSON(data []byte) error {
	var fields manifestFields
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var independent map[string]json.RawMessage
	if err := json.Unmarshal(data, &independent); err != nil {
		return err
	}
	// Known fields belong to the typed model, including omitted optional values.
	// Retaining their raw values would resurrect fields a publisher clears.
	for _, name := range []string{"baseUrl", "outputDir", "generatedAt", "steps", "runFiles", "integrity"} {
		delete(independent, name)
	}
	fields.independentFields = independent
	*m = Manifest(fields)
	return nil
}

// MarshalJSON preserves independent values without converting large integers
// or future schemas through floating-point representations.
func (m Manifest) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(manifestFields(m))
	if err != nil || len(m.independentFields) == 0 {
		return data, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for name, value := range m.independentFields {
		if _, owned := fields[name]; !owned {
			fields[name] = value
		}
	}
	return json.Marshal(fields)
}
