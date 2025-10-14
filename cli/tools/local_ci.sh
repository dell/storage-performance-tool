#!/usr/bin/env bash

# Local CI-like checks for spt
# - No external infra required
# - Fails on formatting, module drift, vet/build issues
# - Lints if golangci-lint is installed
# - Compiles tests by default; run tests with LOCAL_CI_RUN_TESTS=1

set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; GRAY='\033[0;90m'; NC='\033[0m'
if [ ! -t 1 ] || [ "${TERM:-dumb}" = "dumb" ]; then BLUE=''; GREEN=''; YELLOW=''; RED=''; BOLD=''; GRAY=''; NC=''; fi

fail() { echo -e "${RED}✗ $*${NC}"; exit 1; }
ok() { echo -e "${GREEN}✓ $*${NC}"; }
section() { echo -e "\n${BLUE}${BOLD}▶ $*${NC}\n"; }

section "Environment"
go version || fail "Go toolchain not found"
echo -e "${GRAY}GOPATH=$(go env GOPATH)  GOMODCACHE=$(go env GOMODCACHE)${NC}"

section "Module sanity (tidy/verify)"
tmpdir=$(mktemp -d)
cp go.mod "$tmpdir/go.mod.bak" || true
cp go.sum "$tmpdir/go.sum.bak" || true
go mod tidy
changed=0
cmp -s "$tmpdir/go.mod.bak" go.mod || changed=1
cmp -s "$tmpdir/go.sum.bak" go.sum || changed=1
if [ $changed -ne 0 ]; then
  echo -e "${RED}go.mod/go.sum not tidy. Diff:${NC}"
  diff -u "$tmpdir/go.mod.bak" go.mod || true
  diff -u "$tmpdir/go.sum.bak" go.sum || true
  # restore originals so we don't leave the tree dirty
  cp "$tmpdir/go.mod.bak" go.mod
  cp "$tmpdir/go.sum.bak" go.sum
  rm -rf "$tmpdir"
  fail "Run 'go mod tidy' and commit the resulting changes"
fi
rm -rf "$tmpdir"
ok "Modules tidy"

go mod verify && ok "Module verification"

section "Formatting (gofmt)"
fmt_list=$(gofmt -l . | grep -v '^vendor/' || true)
if [ -n "$fmt_list" ]; then
  echo -e "${RED}Unformatted files:${NC}\n$fmt_list"
  echo -e "${YELLOW}Tip:${NC} run: make fmt"
  fail "Formatting check failed"
fi
ok "Formatting clean"

section "go vet"
if go vet ./... 2> /tmp/_vet_stderr.txt; then
  ok "go vet clean"
else
  cat /tmp/_vet_stderr.txt
  fail "go vet reported issues"
fi

section "Build"
go build ./... && ok "All packages build"

section "Lint (optional)"
if command -v golangci-lint >/dev/null 2>&1; then
  # Enforce minimum golangci-lint version that understands config version 2
  MIN_VER="2.2.0"
  ACTUAL_VER=$(golangci-lint version 2>&1 | sed -n 's/.*version \([0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/p' | head -n1)
  if [ -z "$ACTUAL_VER" ]; then
    echo -e "${RED}Unable to determine golangci-lint version.${NC}"
    fail "Please install golangci-lint v${MIN_VER}+"
  fi
  ver_lt() { [ "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" != "$2" ]; }
  if ver_lt "$ACTUAL_VER" "$MIN_VER"; then
    echo -e "${RED}golangci-lint $ACTUAL_VER is too old.${NC}"
    echo -e "${YELLOW}Required: v${MIN_VER}+ (supports config version 2).${NC}"
    echo -e "${GRAY}Install: https://golangci-lint.run/welcome/install/#linux${NC}"
    fail "Please upgrade golangci-lint and re-run"
  fi

  # Pass 1: Production code — full linters from .golangci.yml (skip tests)
  section "Lint: production (full set incl. unused)"
  set +e
  PROD_LINT_OUT=$(golangci-lint run -c .golangci.yml --tests=false --timeout=5m 2>&1)
  PROD_LINT_CODE=$?
  set -e
  [ -n "$PROD_LINT_OUT" ] && echo "$PROD_LINT_OUT"
  [ $PROD_LINT_CODE -ne 0 ] && fail "Production lint failed"
  ok "Production lint clean"

  # Pass 2: Tests — enforce only 'unused' to keep mocks flexible
  section "Lint: tests (unused only)"
  # Use shared test-only config to avoid drift
  test_cfg=tools/golangci-tests.yml
  set +e
  TEST_LINT_OUT=$(golangci-lint run -c "$test_cfg" --tests --timeout=5m -D errcheck -D staticcheck -D govet -D ineffassign 2>&1)
  TEST_LINT_CODE=$?
  set -e
  [ -n "$TEST_LINT_OUT" ] && echo "$TEST_LINT_OUT"
  [ $TEST_LINT_CODE -ne 0 ] && fail "Test lint (unused) failed"
  ok "Test lint clean (unused only)"
else
  echo -e "${GRAY}golangci-lint not installed — skipping (install to enable).${NC}"
fi

section "Tests"
if [ "${LOCAL_CI_RUN_TESTS:-0}" = "1" ]; then
  echo -e "${GRAY}Running tests with -short (set LOCAL_CI_INCLUDE_INTEGRATION=1 to attempt full).${NC}"
  if [ "${LOCAL_CI_INCLUDE_INTEGRATION:-0}" = "1" ]; then
    go test ./...
  else
    go test -short ./...
  fi
else
  echo -e "${GRAY}Compiling tests only (no execution). Set LOCAL_CI_RUN_TESTS=1 to run.${NC}"
  go test -run '^$' ./...
fi
ok "Test phase completed"

echo -e "\n${GREEN}${BOLD}All local CI checks passed.${NC}"
