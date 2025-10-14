# Deadfinder – Dead Code and Duplicate Detection

Deadfinder is a lightweight static analysis tool for this repository that:
- Finds high‑confidence dead functions (0 references, uncovered by tests)
- Detects duplicate function bodies across packages (by normalized hash)
- Produces a JSON report suitable for CI and local review

It is module‑aware, method‑aware (via go/packages), and applies conservative filters to reduce false positives.

## Build and Run

Option A — via Makefile (from repo root):

```
make test-deadcode
```

This builds the analyzer and writes the report to:
```
tools/codehealth/deadfinder/report.json
```

Option B — manually:

```
(cd tools/codehealth/deadfinder && go build ./...)
./tools/codehealth/deadfinder/deadfinder -root . -cover coverage.out > tools/codehealth/deadfinder/report.json
```

- `-root` points to the project root to analyze (typically `.` when run from repo root).
- `-cover` is optional; when provided, covered ranges are used to avoid flagging covered code as dead.
- `-json` is true by default; set `-json=false` for a human‑readable summary to stdout.

## Flags

- `-root string`: project root to analyze (default: `.`)
- `-cover string`: optional Go coverage profile (e.g. `coverage.out`) to overlay
- `-json bool`: output JSON report to stdout (default: true)

## Report Format (JSON)

Top‑level fields:
- `dead_candidates`: array of function records considered dead (see filters below)
- `duplicate_sets`: map from body hash to array of function keys with identical code
- `all_functions`: array of all discovered function records

Each function record includes:
- `key.pkg_path`: module‑aware import path for the package
- `key.name`: function name
- `key.recv`: receiver type (for methods), fully qualified (e.g., `*github.com/dell/storage-performance-tool/cli/tui.GenericChartRenderer`)
- `file`: repo‑relative source file
- `pos_line`: starting line number
- `exported`: whether the function is exported
- `has_body`: whether a body exists (excludes forward declarations)
- `ref_count`: number of references found across the project
- `covered`: true if the function’s declaration line falls within any covered range
- `hash`: normalized body hash (used for duplicate detection)
- `stmt_count`: number of top‑level statements in the body
- `trivial_wrapper`: heuristic flag for single‑call/single‑return wrappers

## Identity and Reference Counting

- Uses `golang.org/x/tools/go/packages` to load packages with types, syntax, and deps.
- Computes a stable key per function: `(pkg_path, recv, name)`; receiver strings include full package paths.
- Counts references via:
  - `types.Info.Uses` for identifiers (including package‑qualified functions)
  - `types.Info.Selections` for method selections
  - Fallback traversal inside method bodies to count `receiverIdent.Method(...)` calls (pointer/non‑pointer variants handled)

## Dead Candidate Selection

A function is considered a dead candidate when all are true:
- Has a body
- Not named `main`
- Not in a `*_test.go` file
- Not exported
- `ref_count == 0`
- `covered == false` (if coverage profile provided)

## Duplicate Detection Rules

- Functions are clustered by normalized body hash.
- Excludes from clustering:
  - Trivial wrappers (single call or `return call(...)` style)
  - Functions defined in `*_test.go` files

## Coverage Overlay

If `-cover` points to a coverage profile (e.g., produced by `go test -coverprofile=coverage.out`), declaration lines within covered ranges mark `covered=true`. Covered functions are never included in `dead_candidates`.

## CI Integration Tips

- Add `make test-deadcode` to a CI job to generate/update the report artifact.
- Optionally, track a baseline and fail CI when new dead candidates appear.
- Prefer human review for removal; keep changes small and run the full test suite between deletions.

## Limitations / Notes

- Dynamic or reflective invocations may not be detected by static analysis.
- Interface calls are handled via Selections when type info is available; complex indirection patterns may still be missed.
- Hash‑based duplicates ignore comments and whitespace but do not perform semantic equivalence; near‑duplicates won’t cluster unless bodies match after normalization.

## Where to Look

- Analyzer source: `tools/codehealth/deadfinder/main.go`
- Report output: `tools/codehealth/deadfinder/report.json`
- Make target: `make test-deadcode`
