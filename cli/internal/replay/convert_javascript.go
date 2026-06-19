package replay

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type jsReplacement struct {
	start int
	end   int
	text  string
}

var (
	jsProcessBuilderRe        = regexp.MustCompile(`(?s)var\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*new\s+java\.lang\.ProcessBuilder\(\)\s*\.command\(\s*"((?:\\.|[^"\\])*)"\s*,\s*"((?:\\.|[^"\\])*)"\s*,\s*"((?:\\.|[^"\\])*)"\s*\)\s*\.inheritIO\(\)\s*\.start\(\)\s*;\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.waitFor\(\)\s*;`)
	jsDriverTypeRe            = regexp.MustCompile(`(?s)"driver"\s*:\s*\{\s*"type"\s*:\s*"([^"]+)"`)
	jsFactoryRe               = regexp.MustCompile(`\b(PreconditionLoad|ReadLoad|UpdateLoad|DeleteLoad|Load)\b`)
	jsAnyLoadFactoryRe        = regexp.MustCompile(`\b([A-Z][A-Za-z]*Load)\b`)
	jsIDFieldRe               = regexp.MustCompile(`"id"\s*:\s*"([^"]+)"`)
	jsObjectKeyRe             = regexp.MustCompile(`"([^"]+)"\s*:`)
	jsParentConfigRe          = regexp.MustCompile(`(?:var\s+)?(parentConfig_[A-Za-z0-9_]+)\s*=\s*\{`)
	jsBraceVarRe              = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
	jsLogPathRe               = regexp.MustCompile(`MONGOOSE_DIR\s*\+\s*"\/log\/([^/"]+)\/([^"]+)"`)
	jsMongoosePathRe          = regexp.MustCompile(`MONGOOSE_DIR\s*\+\s*"\/([^"]+)"`)
	jsOutputKeyRe             = regexp.MustCompile(`"output"\s*:\s*\{`)
	jsPathKeyRe               = regexp.MustCompile(`"path"\s*:`)
	jsIdentifierRe            = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	jsParentConfigAllowedKeys = map[string]struct{}{
		"storage":      {},
		"net":          {},
		"node":         {},
		"port":         {},
		"driver":       {},
		"type":         {},
		"ssl":          {},
		"enabled":      {},
		"ciphers":      {},
		"protocols":    {},
		"provider":     {},
		"jsseProvider": {},
		"namedGroups":  {},
		"pqcMode":      {},
	}
	jsParentSSLAllowedKeys = map[string]struct{}{
		"enabled":      {},
		"ciphers":      {},
		"protocols":    {},
		"provider":     {},
		"jsseProvider": {},
		"namedGroups":  {},
		"pqcMode":      {},
	}
)

// ConvertJS adapts a generated legacy JavaScript scenario for replay against local S3 defaults.
func ConvertJS(raw []byte, runScript RunScript, opts Options) (*Generated, error) {
	var diagnostics []Diagnostic
	var commandOps []CommandOperation
	var pathRewrites []PathRewrite
	var processHelpersNeeded bool
	effectiveVars, variableDiagnostics := effectiveVariables(runScript.Exports, opts)
	diagnostics = append(diagnostics, variableDiagnostics...)
	if len(endpointHosts(opts.Endpoints)) == 0 {
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: "local endpoints are required for replay"})
	}
	bucket := effectiveBucket(opts.Bucket, runScript)
	if bucket == "" {
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: "bucket is required; set --bucket, S3_BUCKET, or archived BUCKET"})
	} else {
		effectiveVars["BUCKET"] = bucket
	}

	body := string(raw)
	diagnostics = append(diagnostics, detectUnsupportedJSProtocols(body, effectiveVars)...)
	diagnostics = normalizeDiagnostics(diagnostics)
	if hasErrors(diagnostics) {
		return &Generated{
			Diagnostics:     diagnostics,
			EffectiveBucket: bucket,
		}, diagnosticsError(diagnostics)
	}

	label := sanitizeLabel(opts.Label)
	if label == "" {
		label = defaultLabel
	}
	baseTS := strings.TrimSpace(opts.BaseTimestamp)
	if baseTS == "" {
		baseTS = baseTimestamp()
	}

	steps, idReplacements, archiveToStepID, stepDiagnostics := extractJSSteps(body, effectiveVars, label, baseTS)
	body = applyJSReplacements(body, idReplacements)
	var commandPathRewrites []PathRewrite
	body, commandOps, commandPathRewrites, processHelpersNeeded, diagnostics = convertJSProcessBuilders(body, effectiveVars, archiveToStepID, diagnostics)
	diagnostics = append(diagnostics, stepDiagnostics...)
	body = expandJSStringPlaceholders(body, effectiveVars)
	diagnostics = append(diagnostics, detectUnsupportedJSLoadFactories(body)...)

	pathRewrites = append(pathRewrites, commandPathRewrites...)
	var pathDiagnostics []Diagnostic
	var scenarioPathRewrites []PathRewrite
	body, scenarioPathRewrites, pathDiagnostics = rewriteJSMongoosePaths(body, archiveToStepID)
	pathRewrites = append(pathRewrites, scenarioPathRewrites...)
	diagnostics = append(diagnostics, pathDiagnostics...)

	var parentDiagnostics []Diagnostic
	body, parentDiagnostics = rewriteJSParentConfigs(body, opts, bucket, effectiveVars)
	diagnostics = append(diagnostics, parentDiagnostics...)
	if bucket != "" {
		body = rewriteJSOutputPaths(body, bucket)
	}
	diagnostics = append(diagnostics, detectSensitiveJSConfig(body)...)

	pathRewrites = uniquePathRewrites(pathRewrites)
	diagnostics = normalizeDiagnostics(diagnostics)
	if hasErrors(diagnostics) {
		return &Generated{
			Diagnostics:     diagnostics,
			Steps:           steps,
			PathRewrites:    pathRewrites,
			CommandOps:      commandOps,
			EffectiveBucket: bucket,
		}, diagnosticsError(diagnostics)
	}

	var out strings.Builder
	writeJSReplayPrelude(&out, effectiveVars, processHelpersNeeded)
	out.WriteString(body)
	if !strings.HasSuffix(body, "\n") {
		out.WriteByte('\n')
	}
	return &Generated{
		ScenarioJS:      []byte(out.String()),
		Diagnostics:     diagnostics,
		Steps:           steps,
		PathRewrites:    pathRewrites,
		CommandOps:      commandOps,
		EffectiveBucket: bucket,
	}, nil
}

func writeJSReplayPrelude(b *strings.Builder, vars map[string]string, includeProcessHelpers bool) {
	b.WriteString("// Generated by spt replay\n")
	b.WriteString("var sptHomeDir = org.apache.logging.log4j.ThreadContext.get(\"home_dir\");\n")
	b.WriteString("if (!sptHomeDir) { sptHomeDir = java.lang.System.getProperty(\"user.dir\"); }\n")
	b.WriteString("var MONGOOSE_DIR = sptHomeDir;\n")
	writePreludeVars(b, vars)
	b.WriteString("\n")
	b.WriteString("function pauseSeconds(seconds, message) {\n")
	b.WriteString("  if (message) { print(\"[\" + new Date().toISOString() + \"] \" + message); }\n")
	b.WriteString("  java.lang.Thread.sleep(Number(seconds) * 1000);\n")
	b.WriteString("}\n\n")
	if includeProcessHelpers {
		writeReplayProcessHelpers(b)
	}
}

func detectUnsupportedJSProtocols(source string, vars map[string]string) []Diagnostic {
	var diagnostics []Diagnostic
	seen := map[string]struct{}{}
	for _, match := range jsDriverTypeRe.FindAllStringSubmatch(source, -1) {
		if len(match) != 2 {
			continue
		}
		rawDriver := strings.TrimSpace(match[1])
		if missing := unresolvedVars(rawDriver, vars); len(missing) > 0 {
			key := "unresolved:" + missing[0]
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("storage.driver.type has unresolved variable %s", missing[0])})
			continue
		}
		driver := strings.ToLower(strings.TrimSpace(expandWithExports(rawDriver, vars)))
		if _, ok := seen[driver]; ok {
			continue
		}
		seen[driver] = struct{}{}
		switch driver {
		case "", storageDriverTypeS3, storageDriverTypeEMCS3, storageDriverTypeS3AWS, storageDriverTypeS3RDMA:
		case "fs":
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: "NFS/FS replay is not implemented for storage.driver.type fs"})
		case "atmos", "cas", "swift":
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("%s replay is not implemented", strings.ToUpper(driver))})
		default:
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("unsupported storage.driver.type %q", driver)})
		}
	}
	return diagnostics
}

func convertJSProcessBuilders(source string, vars map[string]string, archiveToStepID map[string]string, diagnostics []Diagnostic) (string, []CommandOperation, []PathRewrite, bool, []Diagnostic) {
	var replacements []jsReplacement
	var commandOps []CommandOperation
	var pathRewrites []PathRewrite
	processHelpersNeeded := false
	waitNumber := 0
	matches := jsProcessBuilderRe.FindAllStringSubmatchIndex(source, -1)
	for _, match := range matches {
		if len(match) != 12 {
			continue
		}
		varName := source[match[2]:match[3]]
		waitVar := source[match[10]:match[11]]
		shell := unquoteJSString(source[match[4]:match[5]])
		flag := unquoteJSString(source[match[6]:match[7]])
		command := unquoteJSString(source[match[8]:match[9]])
		if varName == waitVar && shell == "/bin/sh" && flag == "-c" {
			if seconds, ok := parseSleepCommand(command, vars); ok {
				waitNumber++
				replacements = append(replacements, jsReplacement{
					start: match[0],
					end:   match[1],
					text:  fmt.Sprintf("pauseSeconds(%d, %s);\n", seconds, jsQuote(fmt.Sprintf("Archived wait %d", waitNumber))),
				})
				commandOps = append(commandOps, CommandOperation{
					Action:  commandActionConverted,
					Command: command,
					Detail:  "sleep converted to pauseSeconds",
				})
				continue
			}
			converted, supported, err := convertCommandStep(command, vars, archiveToStepID)
			if err == nil && supported {
				replacements = append(replacements, jsReplacement{
					start: match[0],
					end:   match[1],
					text:  converted.js,
				})
				commandOps = append(commandOps, CommandOperation{
					Action:  commandActionConverted,
					Command: command,
					Detail:  "safe file command(s) run without a shell",
				})
				pathRewrites = append(pathRewrites, converted.rewrites...)
				processHelpersNeeded = true
				continue
			}
			detail := "not recognized by replay command allowlist"
			message := fmt.Sprintf("unsupported JavaScript command: %s", command)
			if err != nil {
				detail = err.Error()
				message = fmt.Sprintf("unsupported JavaScript command: %s; %s", command, err.Error())
			}
			replacements = append(replacements, jsReplacement{
				start: match[0],
				end:   match[1],
				text:  fmt.Sprintf("// Unsupported archived JavaScript command skipped: %s\n", jsLineComment(command)),
			})
			commandOps = append(commandOps, CommandOperation{
				Action:  commandActionRejected,
				Command: command,
				Detail:  detail,
			})
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: message})
			continue
		}
		replacements = append(replacements, jsReplacement{
			start: match[0],
			end:   match[1],
			text:  fmt.Sprintf("// Unsupported archived JavaScript command skipped: %s\n", jsLineComment(command)),
		})
		detail := "only sleep ProcessBuilder commands are supported for JavaScript replay"
		commandOps = append(commandOps, CommandOperation{
			Action:  commandActionRejected,
			Command: command,
			Detail:  detail,
		})
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("unsupported JavaScript command: %s", command)})
	}
	out := applyJSReplacements(source, replacements)
	if strings.Contains(out, "new java.lang.ProcessBuilder") {
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: "unsupported JavaScript ProcessBuilder form"})
	}
	return out, commandOps, pathRewrites, processHelpersNeeded, diagnostics
}

func extractJSSteps(source string, vars map[string]string, label, baseTS string) ([]StepSummary, []jsReplacement, map[string]string, []Diagnostic) {
	var diagnostics []Diagnostic
	var replacements []jsReplacement
	var steps []StepSummary
	archiveToStepID := map[string]string{}
	offset := 0
	stepNumber := 0
	for {
		match := jsFactoryRe.FindStringSubmatchIndex(source[offset:])
		if match == nil {
			break
		}
		factoryStart := offset + match[0]
		factoryEnd := offset + match[1]
		factory := source[offset+match[2] : offset+match[3]]
		runRel := strings.Index(source[factoryEnd:], ".run()")
		if runRel < 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("load block at byte %d has no .run()", factoryStart)})
			offset = factoryEnd
			continue
		}
		runStart := factoryEnd + runRel
		configOpen := findLastJSObjectConfigOpen(source, factoryEnd, runStart)
		if configOpen < 0 {
			offset = runStart + len(".run()")
			continue
		}
		configClose := findMatchingJSBrace(source, configOpen)
		if configClose < 0 || configClose > runStart {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("load block at byte %d has an unterminated config object", factoryStart)})
			offset = runStart + len(".run()")
			continue
		}
		configText := source[configOpen : configClose+1]
		stepNumber++
		opSuffix := jsStepOperationForConfig(factory, configText, vars)
		stepID := formatStepID(label, stepNumber, baseTS, opSuffix)
		archiveID := fmt.Sprintf("archive-step-%03d", stepNumber)
		if idMatch := jsIDFieldRe.FindStringSubmatchIndex(configText); len(idMatch) == 4 {
			archiveID = configText[idMatch[2]:idMatch[3]]
			replacements = append(replacements, jsReplacement{
				start: configOpen + idMatch[2],
				end:   configOpen + idMatch[3],
				text:  stepID,
			})
		} else {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning, Message: fmt.Sprintf("load block %d has no step id; assigned %s", stepNumber, stepID)})
		}
		archiveToStepID[archiveID] = stepID

		limitTimeRaw, missingTimeVars := jsLimitFieldValue(configText, "time", vars)
		if len(missingTimeVars) > 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("step %s has unresolved variable %s in time limit", archiveID, missingTimeVars[0])})
		}
		limitCountRaw, missingCountVars := jsLimitFieldValue(configText, "count", vars)
		if len(missingCountVars) > 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("step %s has unresolved variable %s in count limit", archiveID, missingCountVars[0])})
		}
		duration := durationValue(limitTimeRaw, nil)
		var count int64
		if duration == "" {
			count = int64Value(limitCountRaw, nil)
		}
		limitOp := opSuffix
		if limitOp == opTypeSeed {
			limitOp = opTypeCreate
		}
		if requiresExplicitLimit(limitOp) && duration == "" && count == 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("step %s has no time or count limit", archiveID)})
		}
		_, concurrencyExplicit := jsFieldToken(configText, "concurrency")
		steps = append(steps, StepSummary{
			ArchiveID:           archiveID,
			StepID:              stepID,
			Operation:           opSuffix,
			Size:                jsStringField(configText, "size", vars),
			Concurrency:         intValue(jsFieldValue(configText, "concurrency", vars), nil),
			concurrencyExplicit: concurrencyExplicit,
			Duration:            duration,
			Count:               count,
		})
		offset = runStart + len(".run()")
	}
	if stepNumber == 0 {
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: "scenario contains no load steps"})
	}
	return steps, replacements, archiveToStepID, diagnostics
}

func jsStepOperationForConfig(factory, configText string, vars map[string]string) string {
	if factory == loadFactoryPrecondition {
		return opTypeSeed
	}
	if factory == loadFactoryRead {
		return opTypeRead
	}
	if factory == loadFactoryUpdate {
		return opTypeUpdate
	}
	if factory == loadFactoryDelete {
		return opTypeDelete
	}
	opConfig := jsObjectForKey(configText, "op")
	switch strings.ToLower(jsStringField(opConfig, "type", vars)) {
	case opTypeRead:
		return opTypeRead
	case opTypeDelete:
		return opTypeDelete
	default:
		return opTypeCreate
	}
}

func detectUnsupportedJSLoadFactories(source string) []Diagnostic {
	var diagnostics []Diagnostic
	seen := map[string]struct{}{}
	for _, match := range jsAnyLoadFactoryRe.FindAllStringSubmatchIndex(source, -1) {
		if len(match) != 4 {
			continue
		}
		factory := source[match[2]:match[3]]
		switch factory {
		case loadFactoryPrecondition, loadFactoryRead, loadFactoryUpdate, loadFactoryDelete, loadFactoryLoad:
			continue
		}
		if _, ok := seen[factory]; ok {
			continue
		}
		runRel := strings.Index(source[match[1]:], ".run()")
		configRel := strings.Index(source[match[1]:], ".config")
		if runRel < 0 || configRel < 0 || configRel > runRel {
			continue
		}
		seen[factory] = struct{}{}
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("unsupported JavaScript load factory %s", factory)})
	}
	return diagnostics
}

func findLastJSObjectConfigOpen(source string, start, end int) int {
	last := -1
	search := start
	for search < end {
		rel := strings.Index(source[search:end], ".config")
		if rel < 0 {
			break
		}
		i := search + rel + len(".config")
		i = skipJSWhitespace(source, i)
		if i >= end || source[i] != '(' {
			search = i
			continue
		}
		i = skipJSWhitespace(source, i+1)
		if i < end && source[i] == '{' {
			last = i
		}
		search = i + 1
	}
	return last
}

func rewriteJSMongoosePaths(source string, archiveToStepID map[string]string) (string, []PathRewrite, []Diagnostic) {
	var diagnostics []Diagnostic
	var replacements []jsReplacement
	var rewrites []PathRewrite
	for _, match := range jsLogPathRe.FindAllStringSubmatchIndex(source, -1) {
		if len(match) != 6 {
			continue
		}
		archiveID := source[match[2]:match[3]]
		fileName := source[match[4]:match[5]]
		stepID := archiveToStepID[archiveID]
		if stepID == "" {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("scenario references unknown item-file path label %s", archiveID)})
			continue
		}
		replacements = append(replacements, jsReplacement{
			start: match[0],
			end:   match[1],
			text:  `sptHomeDir + "/log/" + ` + jsQuote(stepID) + ` + "/` + jsEscape(fileName) + `"`,
		})
		rewrites = append(rewrites, PathRewrite{
			ArchiveID: archiveID,
			StepID:    stepID,
			From:      "${MONGOOSE_DIR}/log/" + archiveID + "/" + fileName,
			To:        "${MONGOOSE_DIR}/log/" + stepID + "/" + fileName,
		})
	}
	out := applyJSReplacements(source, replacements)
	out = jsMongoosePathRe.ReplaceAllStringFunc(out, func(match string) string {
		parts := jsMongoosePathRe.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		return `sptHomeDir + "/` + jsEscape(parts[1]) + `"`
	})
	return out, rewrites, diagnostics
}

func rewriteJSParentConfigs(source string, opts Options, bucket string, vars map[string]string) (string, []Diagnostic) {
	var diagnostics []Diagnostic
	var replacements []jsReplacement
	matches := jsParentConfigRe.FindAllStringSubmatchIndex(source, -1)
	for _, match := range matches {
		if len(match) != 4 {
			continue
		}
		name := source[match[2]:match[3]]
		open := match[1] - 1
		closeIdx := findMatchingJSBrace(source, open)
		if closeIdx < 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("%s has an unterminated config object", name)})
			continue
		}
		configText := source[open : closeIdx+1]
		sslConfig, unsupportedSSLKeys := extractJSParentSSLConfig(configText, vars)
		if enabled, ok := sslConfig["enabled"].(bool); ok {
			if warning, mismatch := archivedSSLMismatchWarning(enabled, opts.Endpoints, name); mismatch {
				diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning, Message: warning})
			}
		}
		if len(unsupportedSSLKeys) > 0 {
			diagnostics = append(diagnostics, Diagnostic{
				Severity: severityWarning,
				Message:  fmt.Sprintf("%s contained unsupported archived ssl setting(s) %s; replay stripped them and uses local defaults for those fields", name, strings.Join(unsupportedSSLKeys, ", ")),
			})
		}
		if parentConfigHasExtraKeys(configText) {
			diagnostics = append(diagnostics, Diagnostic{
				Severity: severityWarning,
				Message:  fmt.Sprintf("%s contained archived parent settings outside replay's modeled parent-config subset; replay preserved representable storage.net.ssl* settings, stripped unsupported settings, and uses local defaults where needed", name),
			})
		}
		end := closeIdx + 1
		for end < len(source) && isJSWhitespace(source[end]) {
			end++
		}
		if end < len(source) && source[end] == ';' {
			end++
		}
		replacements = append(replacements, jsReplacement{
			start: match[0],
			end:   end,
			text:  sanitizedJSParentConfig(name, opts, bucket, sslConfig),
		})
	}
	return applyJSReplacements(source, replacements), diagnostics
}

func parentConfigHasExtraKeys(configText string) bool {
	for _, match := range jsObjectKeyRe.FindAllStringSubmatch(configText, -1) {
		if len(match) != 2 {
			continue
		}
		if _, ok := jsParentConfigAllowedKeys[match[1]]; !ok {
			return true
		}
	}
	return false
}

func sanitizedJSParentConfig(name string, opts Options, bucket string, sslConfig map[string]any) string {
	config := map[string]any{
		"item": map[string]any{
			"output": map[string]any{
				"path": jsBucketPath(bucket),
			},
		},
		"storage": map[string]any{
			"driver": map[string]any{
				"type": s3DriverType(opts.S3Driver),
			},
		},
	}
	if len(sslConfig) > 0 {
		setPath(config, sslConfig, "storage", "net", "ssl")
	}
	configJS, err := marshalJSConfig(config)
	if err != nil {
		return fmt.Sprintf(`var %s = {
  "item" : {
    "output" : {
      "path" : %s
    }
  },
  "storage" : {
    "driver" : {
      "type" : %s
    }
  }
};`, name, jsQuote(jsBucketPath(bucket)), jsQuote(s3DriverType(opts.S3Driver)))
	}
	return fmt.Sprintf("var %s = %s;", name, configJS)
}

func extractJSParentSSLConfig(configText string, vars map[string]string) (map[string]any, []string) {
	storageText := jsObjectForKey(configText, "storage")
	if storageText == "" {
		return nil, nil
	}
	netText := jsObjectForKey(storageText, "net")
	if netText == "" {
		return nil, nil
	}
	sslText := jsObjectForKey(netText, "ssl")
	if sslText == "" {
		if enabled, ok := boolValue(jsFieldValue(netText, "ssl", vars), vars); ok {
			return map[string]any{"enabled": enabled}, nil
		}
		return nil, nil
	}

	sslConfig := map[string]any{}
	if enabled, ok := boolValue(jsFieldValue(sslText, "enabled", vars), vars); ok {
		sslConfig["enabled"] = enabled
	}
	if ciphers, ok := jsStringListField(sslText, "ciphers", vars); ok {
		sslConfig["ciphers"] = ciphers
	}
	if protocols, ok := jsStringListField(sslText, "protocols", vars); ok {
		sslConfig["protocols"] = protocols
	}
	if namedGroups, ok := jsStringListField(sslText, "namedGroups", vars); ok {
		sslConfig["namedGroups"] = namedGroups
	}
	if provider := jsStringField(sslText, "provider", vars); provider != "" {
		sslConfig["provider"] = provider
	}
	if jsseProvider := jsStringField(sslText, "jsseProvider", vars); jsseProvider != "" {
		sslConfig["jsseProvider"] = jsseProvider
	}
	if pqcMode := jsStringField(sslText, "pqcMode", vars); pqcMode != "" {
		sslConfig["pqcMode"] = pqcMode
	}
	if len(sslConfig) == 0 {
		sslConfig = nil
	}
	return sslConfig, jsUnsupportedObjectKeys(sslText, jsParentSSLAllowedKeys)
}

func jsUnsupportedObjectKeys(objectText string, allowed map[string]struct{}) []string {
	seen := map[string]struct{}{}
	var unsupported []string
	for _, match := range jsObjectKeyRe.FindAllStringSubmatch(objectText, -1) {
		if len(match) != 2 {
			continue
		}
		key := match[1]
		if _, ok := allowed[key]; ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unsupported = append(unsupported, key)
	}
	sort.Strings(unsupported)
	return unsupported
}

func rewriteJSOutputPaths(source, bucket string) string {
	bucketPath := jsQuote(jsBucketPath(bucket))
	var replacements []jsReplacement
	for _, match := range jsOutputKeyRe.FindAllStringSubmatchIndex(source, -1) {
		if len(match) != 2 {
			continue
		}
		open := match[1] - 1
		closeIdx := findMatchingJSBrace(source, open)
		if closeIdx < 0 {
			continue
		}
		objectText := source[open : closeIdx+1]
		if pathMatch := jsPathKeyRe.FindStringIndex(objectText); len(pathMatch) == 2 {
			valueStart := open + pathMatch[1]
			valueStart = skipJSWhitespace(source, valueStart)
			valueEnd := findJSValueEnd(source, valueStart, closeIdx)
			if valueEnd > valueStart {
				replacements = append(replacements, jsReplacement{start: valueStart, end: valueEnd, text: bucketPath})
			}
			continue
		}
		replacements = append(replacements, jsReplacement{start: open + 1, end: open + 1, text: jsOutputPathInsertion(source, open, bucketPath)})
	}
	return applyJSReplacements(source, replacements)
}

func jsOutputPathInsertion(source string, open int, bucketPath string) string {
	lineStart := strings.LastIndex(source[:open], "\n") + 1
	linePrefix := source[lineStart:open]
	parentIndent := linePrefix[:len(linePrefix)-len(strings.TrimLeft(linePrefix, " \t"))]
	childIndent := parentIndent + "  "
	return "\n" + childIndent + `"path" : ` + bucketPath + ","
}

func jsBucketPath(bucket string) string {
	return "/" + strings.TrimPrefix(strings.TrimSpace(bucket), "/")
}

func detectSensitiveJSConfig(source string) []Diagnostic {
	seen := map[string]struct{}{}
	var diagnostics []Diagnostic
	for _, match := range jsObjectKeyRe.FindAllStringSubmatch(source, -1) {
		if len(match) != 2 {
			continue
		}
		key := match[1]
		if !isSensitiveName(key) {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		diagnostics = append(diagnostics, Diagnostic{Severity: severityError, Message: fmt.Sprintf("generated JavaScript still contains sensitive configuration key %q; inline archived auth is not supported for replay", key)})
	}
	return diagnostics
}

func jsObjectForKey(source, key string) string {
	re := regexp.MustCompile(`"` + regexp.QuoteMeta(key) + `"\s*:\s*\{`)
	match := re.FindStringIndex(source)
	if len(match) != 2 {
		return ""
	}
	open := match[1] - 1
	closeIdx := findMatchingJSBrace(source, open)
	if closeIdx < 0 {
		return ""
	}
	return source[open : closeIdx+1]
}

func expandJSStringPlaceholders(source string, vars map[string]string) string {
	return jsBraceVarRe.ReplaceAllStringFunc(source, func(match string) string {
		parts := jsBraceVarRe.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		name := parts[1]
		if name == legacyMongooseDirVar || isSensitiveName(name) {
			return match
		}
		value, ok := vars[name]
		if !ok {
			return match
		}
		return value
	})
}

func jsStringField(configText, name string, vars map[string]string) string {
	value := jsFieldValue(configText, name, vars)
	if s, ok := value.(string); ok {
		return s
	}
	return resolveString(value, nil)
}

func jsFieldValue(configText, name string, vars map[string]string) any {
	raw, ok := jsFieldToken(configText, name)
	if !ok {
		return nil
	}
	if strings.HasPrefix(raw, `"`) {
		return expandWithExports(unquoteJSString(strings.Trim(raw, `"`)), vars)
	}
	if value, ok := vars[raw]; ok {
		return value
	}
	if raw == "true" || raw == "false" {
		return raw
	}
	return raw
}

func jsStringListField(configText, name string, vars map[string]string) ([]string, bool) {
	raw, ok := jsFieldRawValue(configText, name)
	if !ok {
		return nil, false
	}
	raw = strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]"):
		parts := splitJSArrayValues(raw[1 : len(raw)-1])
		values := make([]string, 0, len(parts))
		for _, part := range parts {
			value, ok := resolveJSStringListValue(part, vars)
			if ok {
				values = append(values, value...)
			}
		}
		return values, len(values) > 0
	default:
		values, ok := resolveJSStringListValue(raw, vars)
		return values, ok
	}
}

func jsLimitFieldValue(configText, name string, vars map[string]string) (any, []string) {
	raw, ok := jsFieldToken(configText, name)
	if !ok {
		return nil, nil
	}
	if strings.HasPrefix(raw, `"`) {
		unquoted := unquoteJSString(strings.Trim(raw, `"`))
		if missing := unresolvedVars(unquoted, vars); len(missing) > 0 {
			return "", missing
		}
		return expandWithExports(unquoted, vars), nil
	}
	if raw == "true" || raw == "false" {
		return raw, nil
	}
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return raw, nil
	}
	if jsIdentifierRe.MatchString(raw) {
		value, ok := vars[raw]
		if !ok || strings.TrimSpace(value) == "" {
			return "", []string{raw}
		}
		return value, nil
	}
	return raw, nil
}

func jsFieldToken(configText, name string) (string, bool) {
	re := regexp.MustCompile(`"` + regexp.QuoteMeta(name) + `"\s*:\s*("(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z0-9_]*|-?[0-9]+(?:\.[0-9]+)?|true|false)`)
	match := re.FindStringSubmatch(configText)
	if len(match) != 2 {
		return "", false
	}
	return strings.TrimSpace(match[1]), true
}

func jsFieldRawValue(configText, name string) (string, bool) {
	re := regexp.MustCompile(`"` + regexp.QuoteMeta(name) + `"\s*:`)
	match := re.FindStringIndex(configText)
	if len(match) != 2 {
		return "", false
	}
	valueStart := skipJSWhitespace(configText, match[1])
	valueEnd := findJSValueEnd(configText, valueStart, len(configText)-1)
	if valueEnd <= valueStart {
		return "", false
	}
	return strings.TrimSpace(configText[valueStart:valueEnd]), true
}

func splitJSArrayValues(raw string) []string {
	var parts []string
	start := 0
	depth := 0
	var quote byte
	escaped := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			quote = c
		case '[', '{', '(':
			depth++
		case ']', '}', ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(raw[start:i]))
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(raw[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func resolveJSStringListValue(raw string, vars map[string]string) ([]string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) {
		return splitCSV(expandWithExports(unquoteJSString(strings.Trim(raw, `"`)), vars)), true
	}
	if strings.HasPrefix(raw, `'`) && strings.HasSuffix(raw, `'`) {
		return splitCSV(strings.Trim(raw, `'`)), true
	}
	if value, ok := vars[raw]; ok && strings.TrimSpace(value) != "" {
		return splitCSV(value), true
	}
	return splitCSV(raw), true
}

func unquoteJSString(raw string) string {
	unquoted, err := strconv.Unquote(`"` + raw + `"`)
	if err != nil {
		return raw
	}
	return unquoted
}

func applyJSReplacements(source string, replacements []jsReplacement) string {
	if len(replacements) == 0 {
		return source
	}
	sort.SliceStable(replacements, func(i, j int) bool {
		return replacements[i].start > replacements[j].start
	})
	out := source
	for _, replacement := range replacements {
		if replacement.start < 0 || replacement.end < replacement.start || replacement.end > len(out) {
			continue
		}
		out = out[:replacement.start] + replacement.text + out[replacement.end:]
	}
	return out
}

func findJSValueEnd(source string, start, objectClose int) int {
	var quote byte
	escaped := false
	depth := 0
	for i := start; i < objectClose && i < len(source); i++ {
		c := source[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			quote = c
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				return trimJSValueEnd(source, start, i)
			}
		}
	}
	return trimJSValueEnd(source, start, objectClose)
}

func trimJSValueEnd(source string, start, end int) int {
	for end > start && isJSWhitespace(source[end-1]) {
		end--
	}
	return end
}

func findMatchingJSBrace(source string, open int) int {
	if open < 0 || open >= len(source) || source[open] != '{' {
		return -1
	}
	depth := 0
	var quote byte
	escaped := false
	for i := open; i < len(source); i++ {
		c := source[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			quote = c
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func skipJSWhitespace(source string, i int) int {
	for i < len(source) && isJSWhitespace(source[i]) {
		i++
	}
	return i
}

func isJSWhitespace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}
