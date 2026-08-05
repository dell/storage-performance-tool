#!/usr/bin/env bash

# Report engine implementation and test source statistics.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

if ! command -v cloc >/dev/null 2>&1; then
	echo "error: cloc is required but was not found in PATH" >&2
	exit 1
fi

cd "$ENGINE_ROOT"

readonly SOURCE_LANGUAGES="Java,Scala,C"
readonly TEST_SOURCE_PATTERN='/src/test(/|$)'
CLOC_ARGS=(
	--exclude-dir=.git,.gradle,build,out,target
	--include-lang="$SOURCE_LANGUAGES"
	--fullpath
	--quiet
	--csv
)

cloc_summary() {
	local output
	local summary

	output="$(cloc . "${CLOC_ARGS[@]}" "$@")"
	summary="$(awk -F',' '$2 == "SUM" { print $1, $3, $4, $5 }' <<<"$output")"
	if [[ -z "$summary" ]]; then
		echo "error: cloc did not return a summary row" >&2
		return 1
	fi
	printf '%s\n' "$summary"
}

percentage() {
	awk -v part="$1" -v total="$2" 'BEGIN {
		if (total == 0) {
			printf "0.0"
		} else {
			printf "%.1f", (part / total) * 100
		}
	}'
}

ratio() {
	awk -v tests="$1" -v production="$2" 'BEGIN {
		if (production == 0) {
			printf "N/A"
		} else {
			printf "%.2f:1", tests / production
		}
	}'
}

echo "=== Engine Code Statistics with cloc ==="
echo
echo "Languages: Java, Scala, and C"
echo

read -r files blank comment code <<<"$(cloc_summary)"

echo "### Overall:"
printf -- '- **Total actual code**: %'\''d lines (excluding blanks and comments)\n' "$code"
printf -- '- **Blank lines**: %'\''d\n' "$blank"
printf -- '- **Comment lines**: %'\''d\n' "$comment"
printf -- '- **Total files**: %d\n' "$files"
echo

read -r production_files production_blank production_comment production_code \
	<<<"$(cloc_summary --not-match-d="$TEST_SOURCE_PATTERN")"
read -r test_files test_blank test_comment test_code \
	<<<"$(cloc_summary --match-d="$TEST_SOURCE_PATTERN")"

production_pct="$(percentage "$production_code" "$code")"
test_pct="$(percentage "$test_code" "$code")"
test_ratio="$(ratio "$test_code" "$production_code")"

echo "### Production vs Test:"
printf -- '- **Production code**: %'\''d lines (%s%%)\n' "$production_code" "$production_pct"
printf -- '- **Test code**: %'\''d lines (%s%%)\n' "$test_code" "$test_pct"
printf -- '- **Test-to-code ratio**: %s\n' "$test_ratio"
echo

echo "### Largest files by actual code:"
cloc . "${CLOC_ARGS[@]}" --by-file |
	awk -F',' '$1 == "Java" || $1 == "Scala" || $1 == "C"' |
	sort -t',' -k5,5nr |
	awk -F',' 'NR <= 4 { printf "%d. `%s`: %d lines\n", NR, $2, $5 }'
