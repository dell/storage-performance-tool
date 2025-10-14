// Package main provides a small helper that summarizes Go coverage
// profiles by package and overall totals, with optional pretty output.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type pkgStat struct {
	Covered int `json:"covered"`
	Total   int `json:"total"`
}

type pkgEntry struct {
	Pkg     string  `json:"pkg"`
	Covered int     `json:"covered"`
	Total   int     `json:"total"`
	Pct     float64 `json:"pct"`
}

type summary struct {
	Overall struct {
		Covered int     `json:"covered"`
		Total   int     `json:"total"`
		Pct     float64 `json:"pct"`
	} `json:"overall"`
	Packages []pkgEntry `json:"packages"`
}

func main() {
	jsonOnly := flag.Bool("json", false, "print JSON instead of pretty summary")
	profile := flag.String("profile", "coverage.out", "coverage profile file")
	sortBy := flag.String("sort", "pkg", "sort packages by: pkg|pct|covered")
	flag.Parse()

	f, err := os.Open(*profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", *profile, err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	pkgTotals := make(map[string]*pkgStat)
	var gCovered, gTotal int

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		// Expected format: <importpath>/file.go:line.col,line.col <numstmt> <count>
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		loc := fields[0]
		// Extract pkg path by trimming ":..." then removing filename
		colon := strings.IndexByte(loc, ':')
		if colon == -1 {
			continue
		}
		filePath := loc[:colon]
		// Normalize to forward slashes
		filePath = filepath.ToSlash(filePath)
		slash := strings.LastIndexByte(filePath, '/')
		if slash == -1 {
			continue
		}
		pkg := filePath[:slash]

		numStmt, err1 := strconv.Atoi(fields[1])
		count, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			continue
		}

		st := pkgTotals[pkg]
		if st == nil {
			st = &pkgStat{}
			pkgTotals[pkg] = st
		}
		st.Total += numStmt
		if count > 0 {
			st.Covered += numStmt
		}
		gTotal += numStmt
		if count > 0 {
			gCovered += numStmt
		}
	}
	if err := s.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed reading %s: %v\n", *profile, err)
		os.Exit(1)
	}

	var out summary
	out.Overall.Covered = gCovered
	out.Overall.Total = gTotal
	if gTotal > 0 {
		out.Overall.Pct = float64(gCovered) * 100.0 / float64(gTotal)
	}
	out.Packages = make([]pkgEntry, 0, len(pkgTotals))
	for p, st := range pkgTotals {
		pct := 0.0
		if st.Total > 0 {
			pct = float64(st.Covered) * 100.0 / float64(st.Total)
		}
		out.Packages = append(out.Packages, pkgEntry{Pkg: p, Covered: st.Covered, Total: st.Total, Pct: pct})
	}
	// Sort packages
	switch *sortBy {
	case "pct":
		sort.Slice(out.Packages, func(i, j int) bool { return out.Packages[i].Pct < out.Packages[j].Pct })
	case "covered":
		sort.Slice(out.Packages, func(i, j int) bool { return out.Packages[i].Covered < out.Packages[j].Covered })
	default:
		sort.Slice(out.Packages, func(i, j int) bool { return out.Packages[i].Pkg < out.Packages[j].Pkg })
	}

	if *jsonOnly {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(os.Stderr, "failed to encode summary: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Pretty output with colors/boxes
	printPretty(out)
}

func printPretty(out summary) {
	// ANSI colors (disable if not TTY)
	useColor := isTTY()
	red, green, yellow, blue, bold, gray, nc := "", "", "", "", "", "", ""
	if useColor {
		red = "\033[0;31m"
		green = "\033[0;32m"
		yellow = "\033[1;33m"
		blue = "\033[0;34m"
		bold = "\033[1m"
		gray = "\033[0;90m"
		nc = "\033[0m"
	}
	drawBox := func(title string) {
		width := 54
		line := strings.Repeat("=", width)
		fmt.Printf("%s╔%s╗%s\n", blue, line, nc)
		pad := (width - len(title)) / 2
		fmt.Printf("%s║%s%s%s%s║%s\n", blue, strings.Repeat(" ", pad), bold, title, nc, blue)
		fmt.Printf("%s╚%s╝%s\n\n", blue, line, nc)
	}

	drawBox("COVERAGE SUMMARY")
	fmt.Printf("Overall: %s%.2f%%%s (%d/%d statements)\n\n", bold, out.Overall.Pct, nc, out.Overall.Covered, out.Overall.Total)

	drawBox("PER-PACKAGE")
	for _, p := range out.Packages {
		color := green
		if p.Pct < 50 {
			color = red
		} else if p.Pct < 80 {
			color = yellow
		}
		fmt.Printf("%s✓ %s:%s %.2f%% %s(%d/%d)%s\n", color, p.Pkg, nc, p.Pct, gray, p.Covered, p.Total, nc)
	}
	fmt.Printf("\n%s💡 Open coverage.html for detailed, per-line coverage.%s\n", gray, nc)
}

func isTTY() bool {
	// rudimentary: colorize unless explicitly disabled
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
