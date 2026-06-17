package replay

import (
	"errors"
	"fmt"
	"strings"
)

const (
	failureUnknownError              = "unknown_error"
	failureUnsupportedProtocol       = "unsupported_protocol"
	failureParallelUnimplemented     = "parallel_unimplemented"
	failureRunScriptMissingScenario  = "run_script_missing_scenario"
	failureScenarioMissing           = "scenario_missing"
	failureScenarioNotFound          = "scenario_not_found"
	failureScenarioNoStepsOrJobs     = "scenario_no_steps_or_jobs"
	failureScenarioNoLoadSteps       = "scenario_no_load_steps"
	failureUnsupportedCommandStep    = "unsupported_command_step"
	failureUnsupportedJSProcess      = "unsupported_js_processbuilder"
	failureUnsupportedJSLoadFactory  = "unsupported_js_load_factory"
	failureScenarioFormatUnsupported = "unsupported_scenario_format"
	failureLegacyJSONParse           = "legacy_json_parse_error"
	failureMissingRunScript          = "missing_run_script"
	failureAmbiguousRunScript        = "ambiguous_run_script"
	failureInvalidSourceURL          = "invalid_source_url"
	failureEscapedArtifactLink       = "escaped_artifact_link"
	failureHTTPFetchError            = "http_fetch_error"
	failureArtifactTooLarge          = "artifact_too_large"
	failureUnknownItemPathLabel      = "unknown_item_path_label"
	failureUnsupportedLoadOperation  = "unsupported_load_operation"
	failureMissingStepLimit          = "missing_step_limit"
	failureUnresolvedVariable        = "unresolved_variable"
	failureMissingLocalEndpoints     = "missing_local_endpoints"
	failureMissingBucket             = "missing_bucket"
	failureTimeout                   = "timeout"
	failureInvalidArtifactLink       = "invalid_artifact_link"
	failureReplayDefaultsGeneration  = "replay_defaults_generation_error"
)

type ClassifiedError struct {
	Class string
	Msg   string
	Err   error
}

func (e *ClassifiedError) Error() string {
	return e.Msg
}

func (e *ClassifiedError) Unwrap() error {
	return e.Err
}

func newClassifiedError(class, msg string, cause error) error {
	return &ClassifiedError{
		Class: class,
		Msg:   msg,
		Err:   cause,
	}
}

func classifyMessage(msg string) string {
	text := strings.ToLower(strings.TrimSpace(msg))
	switch {
	case strings.Contains(text, "scenario contains no load steps"):
		return failureScenarioNoLoadSteps
	case strings.Contains(text, "scenario contains no steps or jobs"):
		return failureScenarioNoStepsOrJobs
	case strings.Contains(text, "references unknown item-file path label") || strings.Contains(text, "unknown item-file path label"):
		return failureUnknownItemPathLabel
	case strings.Contains(text, "unsupported load operation"):
		return failureUnsupportedLoadOperation
	case strings.Contains(text, "has no time or count limit"):
		return failureMissingStepLimit
	case strings.Contains(text, "parallel scenarios are not implemented for replay"):
		return failureParallelUnimplemented
	case strings.Contains(text, "unsupported storage.driver.type"):
		return failureUnsupportedProtocol
	case strings.Contains(text, " replay is not implemented") && (strings.Contains(text, "nfs") || strings.Contains(text, "fs") || strings.Contains(text, "atmos") || strings.Contains(text, "swift") || strings.Contains(text, "cas")):
		return failureUnsupportedProtocol
	case strings.Contains(text, "unresolved variable"):
		return failureUnresolvedVariable
	case strings.Contains(text, "unresolved item output file path"):
		return failureUnresolvedVariable
	case strings.Contains(text, "unresolved item input file path"):
		return failureUnresolvedVariable
	case strings.Contains(text, "unsupported javascript command"):
		return failureUnsupportedJSProcess
	case strings.Contains(text, "unsupported javascript processbuilder form"):
		return failureUnsupportedJSProcess
	case strings.Contains(text, "only sleep processbuilder commands are supported"):
		return failureUnsupportedJSProcess
	case strings.Contains(text, "unsupported javascript load factory"):
		return failureUnsupportedJSLoadFactory
	case strings.Contains(text, "unsupported command step"):
		return failureUnsupportedCommandStep
	case strings.Contains(text, "unsupported command argument"):
		return failureUnsupportedCommandStep
	case strings.Contains(text, "unsupported command working directory"):
		return failureUnsupportedCommandStep
	case strings.Contains(text, "command step references unknown path label"):
		return failureUnsupportedCommandStep
	case strings.Contains(text, "multiple run script candidates found") || strings.Contains(text, "multiple shell script candidates found"):
		return failureAmbiguousRunScript
	case strings.Contains(text, "no .sh run script found in replay folder"):
		return failureMissingRunScript
	case strings.Contains(text, "scenario") && strings.Contains(text, "was not found in replay folder"):
		return failureScenarioNotFound
	case strings.Contains(text, "multiple scenario candidates match"):
		return failureScenarioNotFound
	case strings.Contains(text, "no json scenario artifact found") || strings.Contains(text, "no js scenario artifact found"):
		return failureScenarioMissing
	case strings.Contains(text, "run script does not reference a scenario file"):
		return failureRunScriptMissingScenario
	case strings.Contains(text, "local endpoints are required for replay"):
		return failureMissingLocalEndpoints
	case strings.Contains(text, "bucket is required; set --bucket"):
		return failureMissingBucket
	case strings.Contains(text, "unsupported replay url scheme") || strings.Contains(text, "invalid --from url"):
		return failureInvalidSourceURL
	case strings.Contains(text, "artifact link") && strings.Contains(text, "escapes replay folder"):
		return failureEscapedArtifactLink
	case strings.Contains(text, "artifact link") && strings.Contains(text, "does not name a file"):
		return failureInvalidArtifactLink
	case strings.Contains(text, "http ") && strings.Contains(text, "fetch"):
		return failureHTTPFetchError
	case strings.Contains(text, "artifact exceeds max size"):
		return failureArtifactTooLarge
	case strings.Contains(text, "timed out") || strings.Contains(text, "deadline exceeded"):
		return failureTimeout
	case strings.Contains(text, "parse legacy json scenario"):
		return failureLegacyJSONParse
	case strings.Contains(text, "scenario format") && strings.Contains(text, "is not implemented for replay"):
		return failureScenarioFormatUnsupported
	case strings.Contains(text, "generate replay defaults"):
		return failureReplayDefaultsGeneration
	default:
		return failureUnknownError
	}
}

func normalizeDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	out := make([]Diagnostic, 0, len(diagnostics))
	for _, d := range diagnostics {
		normalized := d
		if strings.TrimSpace(normalized.Code) == "" && normalized.Severity == severityError {
			normalized.Code = classifyMessage(normalized.Message)
		}
		out = append(out, normalized)
	}
	return out
}

func firstErrorClass(diagnostics []Diagnostic) string {
	for _, d := range diagnostics {
		if d.Severity != severityError {
			continue
		}
		if strings.TrimSpace(d.Code) != "" {
			return d.Code
		}
	}
	for _, d := range diagnostics {
		if d.Severity == severityError {
			return classifyMessage(d.Message)
		}
	}
	return failureUnknownError
}

func ensureClassifiedError(err error) error {
	if err == nil {
		return nil
	}
	var typed *ClassifiedError
	if errors.As(err, &typed) {
		return err
	}
	return newClassifiedError(classifyMessage(err.Error()), err.Error(), err)
}

func classifiedErrorf(class, format string, args ...any) error {
	return newClassifiedError(class, fmt.Sprintf(format, args...), nil)
}

func ErrorClass(err error) string {
	if err == nil {
		return ""
	}
	var typed *ClassifiedError
	if errors.As(err, &typed) {
		if strings.TrimSpace(typed.Class) != "" {
			return typed.Class
		}
	}
	return classifyMessage(err.Error())
}
