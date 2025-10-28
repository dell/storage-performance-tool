//go:build tools
// +build tools

package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/printer"
	"go/types"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go/token"
	"golang.org/x/tools/go/packages"
)

type FuncKey struct {
	PkgPath string `json:"pkg_path"`
	Name    string `json:"name"`
	Recv    string `json:"recv"`
}

type FuncInfo struct {
	Key        FuncKey `json:"key"`
	File       string  `json:"file"`
	PosLine    int     `json:"pos_line"`
	Exported   bool    `json:"exported"`
	HasBody    bool    `json:"has_body"`
	RefCount   int     `json:"ref_count"`
	Covered    bool    `json:"covered"`
	Hash       string  `json:"hash"`
	DocSnippet string  `json:"doc,omitempty"`
	StmtCount  int     `json:"stmt_count"`
	Trivial    bool    `json:"trivial_wrapper"`
}

type CoverBlock struct {
	File      string
	StartLine int
	EndLine   int
	Count     int
}

type Report struct {
	DeadCandidates []FuncInfo           `json:"dead_candidates"`
	DuplicateSets  map[string][]FuncKey `json:"duplicate_sets"`
	All            []FuncInfo           `json:"all_functions"`
}

var (
	flagJSON  = flag.Bool("json", true, "output JSON report")
	flagCover = flag.String("cover", "", "optional go cover profile to overlay (e.g. coverage.out)")
	flagRoot  = flag.String("root", ".", "project root to analyze")
)

func main() {
	flag.Parse()
	cov := loadCoverage(*flagCover)

	declByObj := map[types.Object]*FuncInfo{}
	declByKey := map[string]*FuncInfo{}
	hashMap := map[string][]*FuncInfo{}
	modulePath := readModulePath(*flagRoot)

	// Load packages via go/packages for module-aware types across the repo
	rootAbs, _ := filepath.Abs(*flagRoot)
	cfg := &packages.Config{
		Dir:   *flagRoot,
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedModule | packages.NeedDeps,
		Tests: true,
	}
	pkgsRaw, _ := packages.Load(cfg, "./...")
	type PkgCtx struct {
		Files      []*ast.File
		Info       *types.Info
		Pkg        *types.Package
		ImportPath string
		Fset       *token.FileSet
	}
	var pkgs []PkgCtx

	for _, p := range pkgsRaw {
		if p.Types == nil || len(p.Syntax) == 0 {
			continue
		}
		if p.Module == nil || modulePath == "" || p.Module.Path != modulePath {
			continue
		}
		// Ensure we only analyze packages from the target module dir
		modDir := p.Module.Dir
		if abs, err := filepath.Abs(modDir); err == nil {
			modDir = abs
		}
		if modDir != rootAbs {
			continue
		}
		files := p.Syntax
		info := p.TypesInfo
		importPath := p.PkgPath
		fset := p.Fset

		for _, f := range files {
			ast.Inspect(f, func(n ast.Node) bool {
				fn, ok := n.(*ast.FuncDecl)
				if !ok {
					return true
				}
				obj := info.Defs[fn.Name]
				if obj == nil {
					obj = p.Types.Scope().Lookup(fn.Name.Name)
				}
				recv := ""
				// Canonical receiver via types: try receiver type first
				if fn.Recv != nil && len(fn.Recv.List) > 0 {
					if tv, ok := info.Types[fn.Recv.List[0].Type]; ok && tv.Type != nil {
						recv = types.TypeString(tv.Type, func(p *types.Package) string { return p.Path() })
					}
				}
				// Fallback to object signature if available
				if recv == "" && obj != nil {
					if tf, ok := obj.(*types.Func); ok {
						if sig, ok2 := tf.Type().(*types.Signature); ok2 && sig.Recv() != nil {
							recv = types.TypeString(sig.Recv().Type(), func(p *types.Package) string { return p.Path() })
							recv = normalizeRecvForPkg(recv, pkgPathFromTypes(tf.Pkg(), modulePath))
						}
					}
				}
				// Fallback to AST printing if types info is missing
				if recv == "" && fn.Recv != nil && len(fn.Recv.List) > 0 {
					var b strings.Builder
					if err := printer.Fprint(&b, fset, fn.Recv.List[0].Type); err == nil {
						recv = b.String()
					}
				}
				recv = normalizeRecvForPkg(recv, importPath)
				key := FuncKey{PkgPath: importPath, Name: fn.Name.Name, Recv: recv}
				pos := fset.Position(fn.Pos())
				filePath := pos.Filename
				// Skip synthetic/testmain files outside the repo root
				if abs, err := filepath.Abs(filePath); err == nil {
					if !strings.HasPrefix(abs, rootAbs+string(filepath.Separator)) {
						return true
					}
				}
				exported := ast.IsExported(fn.Name.Name)
				h := ""
				hasBody := fn.Body != nil
				stmtCount := 0
				trivial := false
				if fn.Body != nil {
					stmtCount = len(fn.Body.List)
					trivial = isTrivialWrapper(fn)
					var b strings.Builder
					if err := printer.Fprint(&b, fset, fn.Body); err == nil {
						norm := normalizeCode(b.String())
						sum := sha256.Sum256([]byte(norm))
						h = hex.EncodeToString(sum[:])
					}
				}
				fi := &FuncInfo{Key: key, File: relTo(rootAbs, filePath), PosLine: pos.Line, Exported: exported, HasBody: hasBody, Hash: h, Covered: coveredBy(cov, filePath, pos.Line), StmtCount: stmtCount, Trivial: trivial}
				if existing := declByKey[keyString(key)]; existing != nil {
					if obj != nil {
						declByObj[obj] = existing
					}
				} else {
					if obj != nil {
						declByObj[obj] = fi
					}
					declByKey[keyString(key)] = fi
					// Only consider substantial, non-test functions for duplicate detection
					if h != "" && !fi.Trivial && !strings.HasSuffix(fi.File, "_test.go") {
						hashMap[h] = append(hashMap[h], fi)
					}
				}
				return true
			})
		}
		// Stash package context for second pass reference counting
		pkgs = append(pkgs, PkgCtx{Files: files, Info: info, Pkg: p.Types, ImportPath: importPath, Fset: fset})
	}

	// Second pass: count references now that declByKey is fully populated
	for _, pc := range pkgs {
		for _, f := range pc.Files {
			aliasToPath := importAliases(f)
			ast.Inspect(f, func(n ast.Node) bool {
				// Direct identifier uses
				if id, ok := n.(*ast.Ident); ok {
					if obj := pc.Info.Uses[id]; obj != nil {
						if fi, ok2 := declByObj[obj]; ok2 {
							fi.RefCount++
						} else if fn, ok3 := obj.(*types.Func); ok3 {
							p := fn.Pkg()
							recv := ""
							if sig, ok4 := fn.Type().(*types.Signature); ok4 && sig.Recv() != nil {
								recv = types.TypeString(sig.Recv().Type(), func(p *types.Package) string { return p.Path() })
								recv = normalizeRecvForPkg(recv, pkgPathFromTypes(p, modulePath))
							}
							k := FuncKey{PkgPath: pkgPathFromTypes(p, modulePath), Name: fn.Name(), Recv: recv}
							if fi2 := declByKey[keyString(k)]; fi2 != nil {
								fi2.RefCount++
							}
						}
					}
				}
				// Selector expressions: method selections via Selections info
				if sel, ok := n.(*ast.SelectorExpr); ok {
					// Method selection
					if selInfo, okS := pc.Info.Selections[sel]; okS && selInfo != nil {
						if fn, okF := selInfo.Obj().(*types.Func); okF {
							p := fn.Pkg()
							recv := ""
							if sig, ok4 := fn.Type().(*types.Signature); ok4 && sig.Recv() != nil {
								recv = types.TypeString(sig.Recv().Type(), func(p *types.Package) string { return p.Path() })
							}
							k := FuncKey{PkgPath: pkgPathFromTypes(p, modulePath), Name: fn.Name(), Recv: recv}
							if fi2 := declByKey[keyString(k)]; fi2 != nil {
								fi2.RefCount++
							}
						}
					} else if idX, okX := sel.X.(*ast.Ident); okX { // package-qualified function
						if pkgName, okP := pc.Info.Uses[idX].(*types.PkgName); okP {
							if p := pkgName.Imported(); p != nil {
								k := FuncKey{PkgPath: pkgPathFromTypes(p, modulePath), Name: sel.Sel.Name, Recv: ""}
								if fi2 := declByKey[keyString(k)]; fi2 != nil {
									fi2.RefCount++
								}
							} else {
								if impPath, okA := aliasToPath[idX.Name]; okA {
									k := FuncKey{PkgPath: impPath, Name: sel.Sel.Name, Recv: ""}
									if fi2 := declByKey[keyString(k)]; fi2 != nil {
										fi2.RefCount++
									}
								} else {
									// Fallback: derive receiver type from expression type info
									if tv, okT := pc.Info.Types[sel.X]; okT && tv.Type != nil {
										recv := types.TypeString(tv.Type, func(p *types.Package) string { return p.Path() })
										// attempt to derive package from named type
										base := tv.Type
										if ptr, okP := base.(*types.Pointer); okP {
											base = ptr.Elem()
										}
										if named, okN := base.(*types.Named); okN {
											pkg := named.Obj().Pkg()
											pkgPath := pkgPathFromTypes(pkg, modulePath)
											recv = normalizeRecvForPkg(recv, pkgPath)
											k := FuncKey{PkgPath: pkgPath, Name: sel.Sel.Name, Recv: recv}
											if fi2 := declByKey[keyString(k)]; fi2 != nil {
												fi2.RefCount++
											}
										}
									}
								}
							}
						} else if impPath, okA := aliasToPath[idX.Name]; okA {
							k := FuncKey{PkgPath: impPath, Name: sel.Sel.Name, Recv: ""}
							if fi2 := declByKey[keyString(k)]; fi2 != nil {
								fi2.RefCount++
							}
						}
					}
				}
				return true
			})

			// Fallback: within each method, count selector calls on the receiver identifier
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 || fn.Body == nil {
					continue
				}
				recvName := ""
				if len(fn.Recv.List[0].Names) > 0 {
					recvName = fn.Recv.List[0].Names[0].Name
				}
				if recvName == "" {
					continue
				}
				recv := ""
				if tv, ok := pc.Info.Types[fn.Recv.List[0].Type]; ok && tv.Type != nil {
					recv = types.TypeString(tv.Type, func(p *types.Package) string { return p.Path() })
				}
				if recv == "" { // AST fallback
					var b strings.Builder
					if err := printer.Fprint(&b, pc.Fset, fn.Recv.List[0].Type); err == nil {
						recv = b.String()
					}
				}
				recv = normalizeRecvForPkg(recv, pc.ImportPath)
				ast.Inspect(fn.Body, func(n2 ast.Node) bool {
					sel, ok2 := n2.(*ast.SelectorExpr)
					if !ok2 {
						return true
					}
					if idX, okX := sel.X.(*ast.Ident); okX && idX.Name == recvName {
						// Try exact receiver match first
						k := FuncKey{PkgPath: pc.ImportPath, Name: sel.Sel.Name, Recv: recv}
						if fi2 := declByKey[keyString(k)]; fi2 != nil {
							fi2.RefCount++
						} else {
							// Try pointer/non-pointer variant to account for method set rules
							if strings.HasPrefix(recv, "*") {
								alt := strings.TrimPrefix(recv, "*")
								k2 := FuncKey{PkgPath: pc.ImportPath, Name: sel.Sel.Name, Recv: alt}
								if fi3 := declByKey[keyString(k2)]; fi3 != nil {
									fi3.RefCount++
								}
							} else {
								alt := "*" + recv
								k2 := FuncKey{PkgPath: pc.ImportPath, Name: sel.Sel.Name, Recv: alt}
								if fi3 := declByKey[keyString(k2)]; fi3 != nil {
									fi3.RefCount++
								}
							}
						}
					}
					return true
				})
			}
		}
		// Also count all method selections captured in types.Info.Selections
		for _, selInfo := range pc.Info.Selections {
			if fn, okF := selInfo.Obj().(*types.Func); okF {
				if fi, ok := declByObj[fn]; ok {
					fi.RefCount++
					continue
				}
				p := fn.Pkg()
				recv := ""
				if sig, ok4 := fn.Type().(*types.Signature); ok4 && sig.Recv() != nil {
					recv = types.TypeString(sig.Recv().Type(), func(p *types.Package) string { return p.Path() })
					recv = normalizeRecvForPkg(recv, pkgPathFromTypes(p, modulePath))
				}
				k := FuncKey{PkgPath: pkgPathFromTypes(p, modulePath), Name: fn.Name(), Recv: recv}
				if fi2 := declByKey[keyString(k)]; fi2 != nil {
					fi2.RefCount++
				}
			}
		}
	}

	// build report
	var all []FuncInfo
	for _, fi := range declByKey {
		all = append(all, *fi)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Key.PkgPath == all[j].Key.PkgPath {
			return all[i].PosLine < all[j].PosLine
		}
		return all[i].Key.PkgPath < all[j].Key.PkgPath
	})

	dupSets := map[string][]FuncKey{}
	for h, arr := range hashMap {
		if len(arr) > 1 {
			for _, fi := range arr {
				dupSets[h] = append(dupSets[h], fi.Key)
			}
		}
	}

	dead := make([]FuncInfo, 0)
	for _, fi := range all {
		if !fi.HasBody {
			continue
		}
		if fi.Key.Name == "main" {
			continue
		}
		if strings.HasSuffix(fi.File, "_test.go") {
			continue
		}
		if fi.Exported {
			continue
		}
		if fi.RefCount == 0 && !fi.Covered {
			dead = append(dead, fi)
		}
	}

	rep := Report{DeadCandidates: dead, DuplicateSets: dupSets, All: all}
	if *flagJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return
	}
	fmt.Printf("Dead candidates: %d\n", len(dead))
	for _, d := range dead {
		fmt.Printf("- %s %s (%s:%d)\n", d.Key.PkgPath, formatName(d.Key), d.File, d.PosLine)
	}
}

func normalizeCode(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func formatName(k FuncKey) string {
	if k.Recv != "" {
		return fmt.Sprintf("(%s).%s", k.Recv, k.Name)
	}
	return k.Name
}

func relTo(root, p string) string {
	r, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return filepath.ToSlash(r)
}

func loadCoverage(path string) []CoverBlock {
	if path == "" {
		return nil
	}
	f, err := os.Open(path) // #nosec G304 -- developer tool path from CLI flag
	if err != nil {
		return nil
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	blocks := []CoverBlock{}
	first := true
	for s.Scan() {
		line := s.Text()
		if first {
			first = false
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}
		loc := parts[0]
		cnt := parts[len(parts)-1]
		file := loc[:strings.Index(loc, ":")]
		rangePart := loc[strings.Index(loc, ":")+1:]
		ranges := strings.Split(rangePart, ",")
		if len(ranges) != 2 {
			continue
		}
		start := strings.Split(ranges[0], ".")
		end := strings.Split(ranges[1], ".")
		if len(start) < 2 || len(end) < 2 {
			continue
		}
		startLine := atoiSafe(start[0])
		endLine := atoiSafe(end[0])
		blocks = append(blocks, CoverBlock{File: file, StartLine: startLine, EndLine: endLine, Count: atoiSafe(cnt)})
	}
	return blocks
}

func coveredBy(blocks []CoverBlock, file string, line int) bool {
	if len(blocks) == 0 {
		return false
	}
	for _, b := range blocks {
		if sameFile(b.File, file) && line >= b.StartLine && line <= b.EndLine && b.Count > 0 {
			return true
		}
	}
	return false
}

func sameFile(a, b string) bool {
	if a == b {
		return true
	}
	return filepath.Base(a) == filepath.Base(b)
}

func atoiSafe(s string) int {
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			break
		}
		n = n*10 + int(ch-'0')
	}
	return n
}

func keyString(k FuncKey) string { return k.PkgPath + "::" + k.Recv + ":" + k.Name }

// isTrivialWrapper heuristically identifies simple wrapper functions that
// either just make a single call or immediately return a call result.
func isTrivialWrapper(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Body == nil {
		return false
	}
	if len(fn.Body.List) == 0 {
		return true
	}
	if len(fn.Body.List) > 2 {
		return false
	}
	// Accept patterns:
	// - return someCall(...)
	// - someCall(...)
	for _, s := range fn.Body.List {
		switch st := s.(type) {
		case *ast.ReturnStmt:
			if len(st.Results) == 1 {
				if _, ok := st.Results[0].(*ast.CallExpr); ok {
					return true
				}
			}
		case *ast.ExprStmt:
			if _, ok := st.X.(*ast.CallExpr); ok {
				return true
			}
		case *ast.AssignStmt:
			// x := someCall(...)
			if len(st.Rhs) == 1 {
				if _, ok := st.Rhs[0].(*ast.CallExpr); ok {
					return true
				}
			}
		}
	}
	return false
}

// normalizeRecvForPkg ensures receiver types include a package path qualifier
// (e.g., *Type -> *pkg/path.Type) for stable cross-package matching.
func normalizeRecvForPkg(recv, pkgPath string) string {
	if recv == "" || pkgPath == "" {
		return recv
	}
	stars := 0
	for stars < len(recv) && recv[stars] == '*' {
		stars++
	}
	base := recv[stars:]
	if strings.Contains(base, ".") { // already qualified
		return recv
	}
	return strings.Repeat("*", stars) + pkgPath + "." + base
}

// importAliases builds a mapping from import alias/name to full import path for a file.
func importAliases(f *ast.File) map[string]string {
	m := map[string]string{}
	for _, is := range f.Imports {
		p, err := strconv.Unquote(is.Path.Value)
		if err != nil {
			continue
		}
		name := ""
		if is.Name != nil {
			name = is.Name.Name
		} else {
			name = path.Base(p)
		}
		if name != "." && name != "_" { // skip dot and blank imports
			m[name] = p
		}
	}
	return m
}

func readModulePath(root string) string {
	data, err := os.ReadFile(filepath.Join(root, "go.mod")) // #nosec G304 -- developer tool, root provided by trusted caller
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	return ""
}

func pkgPathFromTypes(p *types.Package, module string) string {
	if p == nil {
		return ""
	}
	path := p.Path()
	if module != "" && (strings.HasPrefix(path, module+"/") || path == module) {
		return path
	}
	return path
}
