package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// Startup callbacks alone cannot detect accidental rewiring to the independent
// drain context. Check the real composition root's context provenance as well.
func TestProviderWorkUsesSignalContextAndCallbacksRetainDrainContext(t *testing.T) {
	source, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	require.NoError(t, err)
	var run *ast.FuncDecl
	for _, decl := range source.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "run" {
			run = fn
		}
	}
	require.NotNil(t, run)
	parents := map[string]string{}
	ast.Inspect(run.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) == 0 || len(assignment.Rhs) != 1 {
			return true
		}
		name, ok := assignment.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := method.X.(*ast.Ident)
		if !ok || owner.Name != "context" || (method.Sel.Name != "WithCancel" && method.Sel.Name != "WithTimeout") {
			return true
		}
		if parent, ok := call.Args[0].(*ast.Ident); ok {
			parents[name.Name] = parent.Name
		}
		return true
	})
	require.Equal(t, "startupCtx", parents["workCtx"])
	resolvesToWork := func(name string) bool {
		for range len(parents) + 1 {
			if name == "workCtx" {
				return true
			}
			name = parents[name]
		}
		return false
	}
	workCalls, drainCalls := 0, 0
	ast.Inspect(run.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		work, drain := false, false
		switch function := call.Fun.(type) {
		case *ast.Ident:
			work = function.Name == "runInitialProviderWork"
		case *ast.SelectorExpr:
			owner, ok := function.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch owner.Name {
			case "eventSub", "eventBridge", "leaseWatcher", "withdrawScheduler", "reconciler", "maintenanceService":
				work = function.Sel.Name == "Start" || function.Sel.Name == "RunOnce" || function.Sel.Name == "WithdrawOnce"
			case "chain":
				work = function.Sel.Name == "EnsureGrantsWithRetry" || function.Sel.Name == "EnsureGrants" || function.Sel.Name == "EnsureFunding"
			case "provisionMgr":
				drain = function.Sel.Name == "Start"
			}
		}
		if !work && !drain {
			return true
		}
		ctx, ok := call.Args[0].(*ast.Ident)
		require.True(t, ok, "lifecycle call must carry an owned context")
		if work {
			workCalls++
			require.True(t, resolvesToWork(ctx.Name), "work call uses %s instead of signal-derived work context", ctx.Name)
		}
		if drain {
			drainCalls++
			require.Equal(t, "ctx", ctx.Name)
		}
		return true
	})
	require.Positive(t, workCalls)
	require.Equal(t, 1, drainCalls)
}
