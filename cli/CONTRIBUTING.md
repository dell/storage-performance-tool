# Contributing to spt

Thanks for your interest in contributing! This guide outlines the basics: commit style, lint/format policy, and the quickest way to run local CI. Contributions of any size are welcome as long as tests stay green.

## Commit Style
- Prefer small, focused commits (1–3 files, ≤200 LOC) with clear scope.
- Use an imperative subject line, e.g.:
  - "tui: extract key handling helpers"
  - "verification: harden file perms (0600)"
- Keep the subject short; add a brief body when helpful (wrap at ~72 cols).
- Avoid mentioning internal tools in commit messages.

## Code Formatting & Lint
- Format with `gofmt -s`.
- Lint with `golangci-lint` v2.2.0+.
  - Production code: `golangci-lint run --tests=false`
  - Tests (advisory): `golangci-lint run -c tools/golangci-tests.yml --tests -D errcheck -D staticcheck -D govet -D ineffassign`
- CI policy:
  - Formatting and the production lint are required to pass.
  - Tests lint only enforces `unused` to keep mocks flexible.

### Pre-commit Hook (recommended)
Run the hook locally to catch issues before pushing.

1) Make the hook executable and point Git to the hooks path:
```
chmod +x .githooks/pre-commit
git config --local core.hooksPath .githooks
```
2) Commit as usual. The hook will:
- Format staged Go files with `gofmt -s` and re-stage them
- Run `golangci-lint run --tests=false`

Bypass (if needed) with `git commit --no-verify` (use sparingly).

### Installing golangci-lint
See the official docs for your platform:
- https://golangci-lint.run/welcome/install/
(We require v2.2.0 or newer; our configs use `version: 2`.)

## Local CI (quick, consistent checks)
For a more thorough check that mirrors CI:
```
bash tools/local_ci.sh
```
This script:
- Verifies `go.mod`/`go.sum` are tidy and valid
- Checks formatting and `go vet`, builds all packages
- Runs the production linter (if installed) and compiles tests
- Optional test run:
  - `LOCAL_CI_RUN_TESTS=1` to run tests
  - `LOCAL_CI_INCLUDE_INTEGRATION=1` to try full test suite (may be slow)

## Pull Requests
- Keep PRs focused and reviewable; one logical theme per PR.
- Ensure `go build ./...` and tests pass locally.
- If adding a new package or larger feature, include a short design note in the PR description.

## Questions / Help
Open a draft PR or issue with your question; maintainers are happy to help refine scope and approach.

