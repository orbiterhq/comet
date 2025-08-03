package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// stateLint checks for direct access to s.state field which should use atomic helpers
func main() {
	var dir = flag.String("dir", ".", "directory to analyze")
	flag.Parse()

	var allIssues []string
	hasError := false

	err := filepath.Walk(*dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !strings.HasSuffix(path, ".go") || strings.Contains(path, "_test.go") {
			return nil
		}

		issues := checkFile(path)
		if len(issues) > 0 {
			allIssues = append(allIssues, issues...)
			hasError = true
		}
		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	for _, issue := range allIssues {
		fmt.Println(issue)
	}

	if hasError {
		os.Exit(1)
	}
}

func checkFile(filename string) []string {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return nil
	}

	var issues []string
	var currentFunc string

	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			// Track current function name
			if x.Name != nil {
				currentFunc = x.Name.Name
			}
		case *ast.SelectorExpr:
			// Check for s.state access
			if ident, ok := x.X.(*ast.Ident); ok && ident.Name == "s" && x.Sel.Name == "state" {
				// Allow in loadState and storeState methods
				if currentFunc == "loadState" || currentFunc == "storeState" {
					return true
				}
				// Check if this is an allowed atomic helper call
				if !isAllowedStateAccess(x) {
					pos := fset.Position(x.Pos())
					issues = append(issues, fmt.Sprintf("%s:%d:%d: direct access to s.state is forbidden, use s.loadState() or s.storeState() instead",
						filename, pos.Line, pos.Column))
				}
			}
		}
		return true
	})

	return issues
}

// isAllowedStateAccess checks if this is an allowed atomic helper function
func isAllowedStateAccess(sel *ast.SelectorExpr) bool {
	// Walk up the AST to see if this is part of an assignment from loadState() or storeState()
	// For now, we'll be strict and only allow these patterns:
	// - s.storeState(x)
	// - atomic operations inside loadState/storeState helper methods

	// This is a simplified check - in practice you might want more sophisticated analysis
	// to allow the atomic helper methods themselves
	return false
}
