# Engine Build Information

SPT records the engine and CLI builds that produced a benchmark so humans can
interpret performance results without relying on a mutable image tag or
configuration value. Engine Build Information is enabled by default and has no
runtime feature flag.

## Engine-owned snapshot

The engine distribution owns one immutable build snapshot for the lifetime of
the process. Gradle generates `META-INF/spt-build-info.properties`; effective
configuration, the CLI, Docker, and OCI metadata are not identity authorities.
The snapshot contains:

- product and semantic version;
- the full source revision;
- a UTC build timestamp;
- development state; and
- nullable source-dirty state.

`run.version` remains in effective configuration for compatibility, but the
engine projects its build version into that field. It is only a configured
version hint in legacy evidence, never proof of a build.

## Engine surfaces

`java -jar spt.jar --version` and the single normal startup build line render
the shared snapshot. In node mode, `GET /version` returns the same schema as
`application/json` as soon as the HTTP server responds. Schema 1 is:

```json
{
  "schema_version": 1,
  "product": "spt-engine",
  "version": "5.15.0",
  "revision": "0123456789abcdef0123456789abcdef01234567",
  "build_time": "2030-01-02T03:04:05Z",
  "development": false,
  "source_dirty": false
}
```

A direct engine run atomically writes an identical `engine.build.json` in its
initialization directory and each workload step directory. Those files remain
available locally and through the engine logs interface. CLI-managed collection
deliberately does not fetch or duplicate them.

## CLI-managed results

After every planned engine HTTP endpoint responds and before scenario
submission, the CLI queries `GET /version` with bounded concurrency. Normal
headless and full-TUI output contains one fleet line. Per-node success, retry,
and response details appear only with `--verbose`; an indeterminate warning
still names affected nodes and concise reasons.

The result root contains one combined `engine.info.json`, even for fleets with
dozens of workers. It identifies the `entry`, `worker`, or `standalone` role of
each participant, groups identical build records behind document-local IDs,
records the run ID and finalization time, and stores one consistency result:

- `consistent`: every participant supplied complete comparable identity and
  the comparison fields agree;
- `mismatch`: known comparison fields prove that different builds participated;
- `indeterminate`: no mismatch is proven, but at least one participant lacks
  complete comparable identity.

The Environment section of the human summary shows compact, separate CLI and
engine identities. `spt_run_params.json` extends its `cli` object with the CLI
version, revision, and build time, then references `engine.info.json` and its
consistency status without duplicating participants or builds. `index.json`
lists `engine.info.json` as a run-level artifact.

## Gate and force policy

A known mismatch stops `spt run` or `spt replay` before scenario submission and
returns a nonzero status. `--force` permits that known mismatch and preserves
`mismatch` plus `forced: true` in the manifest; the summary prominently warns
that performance results combine different engine builds. The same flag still
resolves supported port conflicts.

`--force` does not override malformed or contract-invalid supported build
information, exhausted collection failures, or cancellation. It does not turn
unknown evidence into a verified match.

Replay evaluates only the current participants executing the replay. Build
information from the source result remains historical provenance and is not an
equality requirement for the new run.

## Compatibility

Compatibility states are explicit in participant records:

- `legacy_endpoint_unavailable`: an older engine returned HTTP 404 or 405;
- `unsupported_schema`: a readable future schema is newer than the CLI;
- `incomplete_build_info`: supported schema-1 JSON explicitly reports `unknown`
  for `version` or `revision`, or `null` for `source_dirty`; and
- `collection_failed`: a required request failed, or supported schema-1 JSON is
  malformed, omits a required field, or otherwise violates the contract.

The first three states make consistency indeterminate unless other records
already prove a mismatch. `collection_failed` stops submission. Older result
bundles without Engine Build Information continue to load and show an explicit
unavailable legacy state. A replay does not require source-run identity.

## Privacy and scope

Build information is public provenance. The endpoint and artifacts exclude
configuration, credentials, environment variables, filesystem paths, host
inventory, container identifiers, and storage-driver plugin versions.
Participant IDs contain only a sanitized host and control port. Unsupported
response bodies are not copied into artifacts or normal logs.

Engine identity does not prove byte-for-byte image or payload equality. Use the
existing distributed image and payload checks when that stronger guarantee is
required.
