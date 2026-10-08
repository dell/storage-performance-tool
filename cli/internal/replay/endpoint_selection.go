package replay

import (
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// archivedSelections collects the endpoint-selection mode and timeouts an archive declares. They
// are never written into replayed steps: the effective settings go into the generated defaults
// only, after merging with the replay flags, so the settings that are validated are the ones run.
type archivedSelections struct {
	selection *scenario.EndpointSelection
	conflict  bool
}

func (a *archivedSelections) add(sel scenario.EndpointSelection) {
	if a.selection == nil {
		copied := sel
		a.selection = &copied
		return
	}
	if *a.selection != sel {
		a.conflict = true
	}
}

// diagnostics reports archive steps that disagree. That blocks replay unless the replay flags
// choose the mode explicitly, in which case the archived settings are not used.
func (a *archivedSelections) diagnostics(opts Options) []Diagnostic {
	if !a.conflict {
		return nil
	}
	if opts.EndpointSelection.Mode == "" {
		return []Diagnostic{{Severity: severityError, Code: failureInvalidEndpointSelection,
			Message: "archived steps use different endpoint selection settings; choose one with --endpoint-selection"}}
	}
	return []Diagnostic{{Severity: severityWarning,
		Message: "archived steps use different endpoint selection settings; replay uses the --endpoint-selection flags"}}
}

// archivedEndpointSelectionFromConfig reads storage.net.endpoint from a merged JSON step config.
// Hostname and DNS server are environment-specific and are not read.
func archivedEndpointSelectionFromConfig(config map[string]any, vars map[string]string) (scenario.EndpointSelection, bool) {
	mode := resolveString(getPath(config, legacyKeyStorage, legacyKeyNet, legacyKeyEndpoint, "selection"), vars)
	if mode == "" || mode == scenario.EndpointSelectionDefault {
		return scenario.EndpointSelection{}, false
	}
	return scenario.EndpointSelection{
		Mode:                 mode,
		DNSTimeoutMillis:     intValue(getPath(config, legacyKeyStorage, legacyKeyNet, legacyKeyEndpoint, "dns", "timeoutMilliSec"), vars),
		ConnectTimeoutMillis: intValue(getPath(config, legacyKeyStorage, legacyKeyNet, legacyKeyEndpoint, "connect", "timeoutMilliSec"), vars),
	}, true
}

// mergeEndpointSelection applies the replay flags over archived settings. Explicit flags win: the
// archived mode applies only when --endpoint-selection is not given, and archived timeouts only
// fill timeouts the flags leave unset for that same mode. Hostname and DNS server come only from
// the flags.
func mergeEndpointSelection(flags scenario.EndpointSelection, archived *scenario.EndpointSelection) (scenario.EndpointSelection, []Diagnostic) {
	if archived == nil {
		return flags, nil
	}
	merged := flags
	var diagnostics []Diagnostic
	if merged.Mode == "" {
		merged.Mode = archived.Mode
		diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning,
			Message: fmt.Sprintf("replay uses the archived endpoint selection %s", archived.Mode)})
	} else if merged.Mode != archived.Mode {
		diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning,
			Message: fmt.Sprintf("--endpoint-selection %s overrides the archived endpoint selection %s", merged.Mode, archived.Mode)})
	}
	if merged.Mode == archived.Mode {
		if merged.DNSTimeoutMillis == 0 {
			merged.DNSTimeoutMillis = archived.DNSTimeoutMillis
		}
		if merged.ConnectTimeoutMillis == 0 {
			merged.ConnectTimeoutMillis = archived.ConnectTimeoutMillis
		}
	}
	return merged, diagnostics
}
