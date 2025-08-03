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

// stateLint checks for direct nil comparisons on state fields
// These should use loadState() != nil to avoid race conditions
func main() {
	var dir = flag.String("dir", ".", "directory to analyze")
	flag.Parse()

	var allIssues []string
	hasError := false

	err := filepath.Walk(*dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !strings.HasSuffix(path, ".go") {
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
	src, err := os.ReadFile(filename)
	if err != nil {
		return nil
	}

	node, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return nil
	}

	var issues []string
	
	// Find all nil comparisons with state fields
	ast.Inspect(node, func(n ast.Node) bool {
		switch expr := n.(type) {
		case *ast.BinaryExpr:
			// Look for != nil or == nil comparisons
			if expr.Op == token.NEQ || expr.Op == token.EQL {
				// Check if right side is nil
				if ident, ok := expr.Y.(*ast.Ident); ok && ident.Name == "nil" {
					// Check if left side is a state field access
					if sel, ok := expr.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "state" {
						if varIdent, ok := sel.X.(*ast.Ident); ok {
							if isShardVariable(varIdent.Name) {
								pos := fset.Position(sel.Pos())
								op := "!="
								if expr.Op == token.EQL {
									op = "=="
								}
								issues = append(issues, fmt.Sprintf("%s:%d:%d: direct nil check '%s.state %s nil' should use '%s.loadState() %s nil'",
									filename, pos.Line, pos.Column, varIdent.Name, op, varIdent.Name, op))
							}
						}
					}
				}
				// Also check if left side is nil
				if ident, ok := expr.X.(*ast.Ident); ok && ident.Name == "nil" {
					// Check if right side is a state field access
					if sel, ok := expr.Y.(*ast.SelectorExpr); ok && sel.Sel.Name == "state" {
						if varIdent, ok := sel.X.(*ast.Ident); ok {
							if isShardVariable(varIdent.Name) {
								pos := fset.Position(sel.Pos())
								op := "!="
								if expr.Op == token.EQL {
									op = "=="
								}
								issues = append(issues, fmt.Sprintf("%s:%d:%d: direct nil check 'nil %s %s.state' should use 'nil %s %s.loadState()'",
									filename, pos.Line, pos.Column, op, varIdent.Name, op, varIdent.Name))
							}
						}
					}
				}
			}
		case *ast.IfStmt:
			// Check for if shard.state { ... } patterns
			if sel, ok := expr.Cond.(*ast.SelectorExpr); ok && sel.Sel.Name == "state" {
				if varIdent, ok := sel.X.(*ast.Ident); ok {
					if isShardVariable(varIdent.Name) {
						pos := fset.Position(sel.Pos())
						issues = append(issues, fmt.Sprintf("%s:%d:%d: direct boolean check 'if %s.state' should use 'if %s.loadState() != nil'",
							filename, pos.Line, pos.Column, varIdent.Name, varIdent.Name))
					}
				}
			}
		}
		return true
	})

	return issues
}

// isShardVariable checks if the variable name refers to a shard
func isShardVariable(name string) bool {
	// Common shard variable names in the codebase
	shardNames := []string{"s", "shard", "shard2", "sh", "shd"}
	for _, sn := range shardNames {
		if name == sn {
			return true
		}
	}
	// Also check for variables that start/end with shard patterns
	return strings.HasPrefix(name, "shard") || 
	       strings.HasSuffix(name, "Shard") ||
	       strings.HasSuffix(name, "shard")
}