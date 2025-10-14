#!/bin/bash

# cloc_it.sh - Generate code statistics report for spt_poc

echo "=== Accurate Code Statistics with cloc ==="
echo

# Overall statistics
echo "### Overall:"
OVERALL=$(cloc . --exclude-dir=vendor,.git --include-lang=Go --quiet --csv | tail -1)
CODE=$(echo $OVERALL | cut -d',' -f5)
COMMENT=$(echo $OVERALL | cut -d',' -f4)
BLANK=$(echo $OVERALL | cut -d',' -f3)
FILES=$(echo $OVERALL | cut -d',' -f1)

echo "- **Total actual code**: $(printf "%'d" $CODE) lines (excluding blanks and comments)"
echo "- **Blank lines**: $(printf "%'d" $BLANK)"
echo "- **Comment lines**: $(printf "%'d" $COMMENT)"
echo "- **Total files**: $FILES"
echo

# Production vs Test statistics
echo "### Production vs Test:"
PROD=$(cloc . --exclude-dir=vendor,.git --include-lang=Go --not-match-f="_test.go" --quiet --csv | tail -1)
PROD_CODE=$(echo $PROD | cut -d',' -f5)

TEST=$(cloc . --exclude-dir=vendor,.git --include-lang=Go --match-f="_test.go" --quiet --csv | tail -1)
TEST_CODE=$(echo $TEST | cut -d',' -f5)

PROD_PCT=$(awk "BEGIN {printf \"%.1f\", ($PROD_CODE / $CODE) * 100}")
TEST_PCT=$(awk "BEGIN {printf \"%.1f\", ($TEST_CODE / $CODE) * 100}")
RATIO=$(awk "BEGIN {printf \"%.2f\", $TEST_CODE / $PROD_CODE}")

echo "- **Production code**: $(printf "%'d" $PROD_CODE) lines ($PROD_PCT%)"
echo "- **Test code**: $(printf "%'d" $TEST_CODE) lines ($TEST_PCT%)"
echo "- **Test-to-code ratio**: ${RATIO}:1 $(if awk "BEGIN {exit !($RATIO > 1)}"; then echo "(excellent coverage!)"; else echo ""; fi)"
echo

# Largest files
echo "### Largest files by actual code:"
cloc . --exclude-dir=vendor,.git --include-lang=Go --by-file --quiet --csv | \
    tail -n +2 | head -n -1 | \
    sort -t',' -k5 -nr | \
    head -4 | \
    awk -F',' '{printf "%d. `%s`: %d lines", NR, $2, $5; if (NR==1) print " (core TUI logic)"; else print ""}'
echo