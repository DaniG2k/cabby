// Command inventory statically enumerates Go test cases (tests, nested t.Run
// subtests, benchmarks, examples, fuzz targets) with source spans, skips,
// assertion counts and build constraints. It is a test-only reference adapter
// for Cabby (SPEC.md §4); runtime enumeration from `go test -json` complements it
// for subtest names that are only known at run time (table-driven cases).
//
// Usage: inventory -root <checkout> <relative dir or file>... > ast.json
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Case struct {
	File       string  `json:"file"`
	Kind       string  `json:"kind"` // test | benchmark | example | fuzz
	Path       []Seg   `json:"path"` // top-level func then nested t.Run names
	StartLine  int     `json:"start_line"`
	EndLine    int     `json:"end_line"`
	Assertions int     `json:"assertions"` // direct (not nested) assertion-like calls
	Skips      []Skip  `json:"skips,omitempty"`
	InLoop     bool    `json:"in_loop,omitempty"` // t.Run inside a for/range: table-driven
	Children   []*Case `json:"children,omitempty"`
}

type Seg struct {
	Name    string `json:"name"`              // literal name, or source of the name expression
	Dynamic bool   `json:"dynamic,omitempty"` // name is computed at run time
}

type Skip struct {
	Line   int    `json:"line"`
	Call   string `json:"call"`
	Reason string `json:"reason,omitempty"`
}

type File struct {
	Path        string  `json:"path"`
	SHA256      string  `json:"sha256"`
	Package     string  `json:"package"`
	BuildTags   string  `json:"build_constraint,omitempty"`
	Helpers     []Func  `json:"helpers,omitempty"` // non-test funcs in test files (dependencies)
	Cases       []*Case `json:"cases"`
	ParseErrors string  `json:"parse_error,omitempty"`
}

type Func struct {
	Name      string `json:"name"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
}

var fset = token.NewFileSet()

// Build outputs and third-party trees are never part of the upstream corpus.
var skipDirs = map[string]bool{"vendor": true, "testdata": true, "mruby-build": true, "tmp": true, "dist": true, "node_modules": true}

func main() {
	root := flag.String("root", ".", "reference checkout root")
	flag.Parse()
	// WalkDir does not descend into a symlinked root (e.g. a worktree's
	// compat/reference linking to a shared checkout).
	if resolved, err := filepath.EvalSymlinks(*root); err == nil {
		*root = resolved
	}
	var files []File
	for _, arg := range flag.Args() {
		base := filepath.Join(*root, arg)
		err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if d != nil && d.IsDir() && p != base {
					return filepath.SkipDir // e.g. root-owned tmp/ from container runs
				}
				return err
			}
			if d.IsDir() && p != base && skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			if !d.IsDir() && strings.HasSuffix(p, "_test.go") {
				rel, _ := filepath.Rel(*root, p)
				files = append(files, inspect(p, filepath.ToSlash(rel)))
			}
			return nil
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(files); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func inspect(abs, rel string) File {
	src, err := os.ReadFile(abs)
	if err != nil {
		return File{Path: rel, ParseErrors: err.Error()}
	}
	sum := sha256.Sum256(src)
	out := File{Path: rel, SHA256: hex.EncodeToString(sum[:])}
	f, err := parser.ParseFile(fset, abs, src, parser.ParseComments)
	if err != nil {
		out.ParseErrors = err.Error()
		return out
	}
	out.Package = f.Name.Name
	for _, cg := range f.Comments {
		if cg.Pos() > f.Package {
			break
		}
		for _, c := range cg.List {
			if strings.HasPrefix(c.Text, "//go:build ") {
				out.BuildTags = strings.TrimPrefix(c.Text, "//go:build ")
			}
		}
	}
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		kind, param := classify(fd)
		if kind == "" {
			out.Helpers = append(out.Helpers, Func{fd.Name.Name, line(fd.Pos()), line(fd.End())})
			continue
		}
		c := &Case{File: rel, Kind: kind, Path: []Seg{{Name: fd.Name.Name}}, StartLine: line(fd.Pos()), EndLine: line(fd.End())}
		walk(fd.Body, c, param, false)
		out.Cases = append(out.Cases, c)
	}
	return out
}

// classify reports the test kind of a top-level function and the name of its
// *testing.T/B/F parameter.
func classify(fd *ast.FuncDecl) (string, string) {
	if fd.Recv != nil {
		return "", ""
	}
	name := fd.Name.Name
	params := fd.Type.Params.List
	paramOf := func(typ string) string {
		if len(params) != 1 || len(params[0].Names) != 1 {
			return ""
		}
		star, ok := params[0].Type.(*ast.StarExpr)
		if !ok {
			return ""
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != typ {
			return ""
		}
		return params[0].Names[0].Name
	}
	switch {
	case strings.HasPrefix(name, "Test") && name != "TestMain":
		if p := paramOf("T"); p != "" {
			return "test", p
		}
	case strings.HasPrefix(name, "Benchmark"):
		if p := paramOf("B"); p != "" {
			return "benchmark", p
		}
	case strings.HasPrefix(name, "Fuzz"):
		if p := paramOf("F"); p != "" {
			return "fuzz", p
		}
	case strings.HasPrefix(name, "Example") && len(params) == 0:
		return "example", ""
	}
	return "", ""
}

// walk records assertions, skips and nested t.Run/b.Run subtests in n, attributing
// them to cur. param is the identifier of the current *testing.T.
func walk(n ast.Node, cur *Case, param string, inLoop bool) {
	ast.Inspect(n, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.ForStmt:
			walk(x.Body, cur, param, true)
			return false
		case *ast.RangeStmt:
			walk(x.Body, cur, param, true)
			return false
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv := exprString(sel.X)
			if sel.Sel.Name == "Run" && len(x.Args) == 2 && recv == param {
				if lit, ok := x.Args[1].(*ast.FuncLit); ok {
					child := &Case{File: cur.File, Kind: cur.Kind, StartLine: line(x.Pos()), EndLine: line(x.End()), InLoop: inLoop}
					child.Path = append(append([]Seg{}, cur.Path...), nameSeg(x.Args[0]))
					childParam := param
					if ps := lit.Type.Params.List; len(ps) == 1 && len(ps[0].Names) == 1 {
						childParam = ps[0].Names[0].Name
					}
					walk(lit.Body, child, childParam, false)
					cur.Children = append(cur.Children, child)
					return false
				}
			}
			if recv == param && strings.HasPrefix(sel.Sel.Name, "Skip") {
				s := Skip{Line: line(x.Pos()), Call: sel.Sel.Name}
				if len(x.Args) > 0 {
					s.Reason = exprString(x.Args[0])
				}
				cur.Skips = append(cur.Skips, s)
			}
			if isAssertion(recv, sel.Sel.Name, param) {
				cur.Assertions++
			}
		}
		return true
	})
}

func isAssertion(recv, method, param string) bool {
	switch recv {
	case "assert", "require":
		return true
	}
	if recv == param {
		switch method {
		case "Error", "Errorf", "Fatal", "Fatalf", "Fail", "FailNow":
			return true
		}
	}
	// mock expectations (testify/mock) count as assertions
	return method == "AssertExpectations" || method == "AssertCalled" || method == "AssertNotCalled" || method == "AssertNumberOfCalls"
}

func nameSeg(e ast.Expr) Seg {
	if lit, ok := e.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		if s, err := strconv.Unquote(lit.Value); err == nil {
			return Seg{Name: s}
		}
	}
	return Seg{Name: exprString(e), Dynamic: true}
}

func exprString(e ast.Expr) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, e)
	return b.String()
}

func line(p token.Pos) int { return fset.Position(p).Line }
