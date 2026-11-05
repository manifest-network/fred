package main

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestComponents(t *testing.T) (*components, context.Context) {
	t.Helper()
	work, cancelWork := context.WithCancel(t.Context())
	t.Cleanup(cancelWork)
	return newComponents(work, cancelWork), work
}

func waitForComponents(t *testing.T, comps *components) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.True(t, comps.Wait(ctx), "components did not stop")
	require.True(t, comps.WaitStoreReaders(ctx), "store readers did not stop")
}

func TestComponentFailureIsRecordedAndStartsShutdown(t *testing.T) {
	failure := errors.New("listener closed")
	for _, tc := range []struct {
		name  string
		start func(*components, func() error)
	}{
		{"runtime component", func(c *components, fn func() error) { c.Go("api server", fn) }},
		{"store reader", func(c *components, fn func() error) { c.GoStoreReader("placement snapshots", fn) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			comps, work := newTestComponents(t)
			tc.start(comps, func() error { return failure })
			waitForComponents(t, comps)
			require.ErrorIs(t, comps.Err(), failure)
			require.Error(t, work.Err(), "a component failure must start shutdown")
		})
	}
}

func TestComponentPanicIsAFailure(t *testing.T) {
	comps, work := newTestComponents(t)
	comps.Go("reconciler", func() error { panic("nil map write") })
	waitForComponents(t, comps)
	require.ErrorContains(t, comps.Err(), "reconciler panic: nil map write")
	require.Error(t, work.Err())
}

func TestComponentThatStopsBeforeShutdownIsAFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"canceled", context.Canceled},
		{"wrapped canceled", fmt.Errorf("router: %w", context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			comps, work := newTestComponents(t)
			comps.Go("event bridge", func() error { return tc.err })
			waitForComponents(t, comps)
			require.ErrorContains(t, comps.Err(), "event bridge stopped before shutdown",
				"a component that silently stops leaves providerd running without it")
			require.Error(t, work.Err())
		})
	}
}

func TestComponentsStoppingForShutdownAreNotFailures(t *testing.T) {
	comps, work := newTestComponents(t)
	started := make(chan struct{}, 3)
	comps.Go("reconciler", func() error { started <- struct{}{}; <-work.Done(); return work.Err() })
	comps.Go("scheduler", func() error {
		started <- struct{}{}
		<-work.Done()
		return fmt.Errorf("scheduler: %w", context.Canceled)
	})
	comps.GoStoreReader("placement snapshots", func() error { started <- struct{}{}; <-work.Done(); return nil })
	for range 3 {
		<-started
	}
	comps.stop()
	waitForComponents(t, comps)
	require.NoError(t, comps.Err())
}

func TestComponentErrorDuringShutdownIsStillAFailure(t *testing.T) {
	comps, work := newTestComponents(t)
	failure := errors.New("router close timed out")
	comps.Go("provision manager", func() error { <-work.Done(); return failure })
	comps.stop()
	waitForComponents(t, comps)
	require.ErrorIs(t, comps.Err(), failure)
}

func TestEveryComponentFailureIsReported(t *testing.T) {
	comps, _ := newTestComponents(t)
	first, second := errors.New("first"), errors.New("second")
	comps.Go("api server", func() error { return first })
	comps.Go("watcher", func() error { return second })
	waitForComponents(t, comps)
	require.ErrorIs(t, comps.Err(), first)
	require.ErrorIs(t, comps.Err(), second)
}

func TestComponentsWaitReportsATimeout(t *testing.T) {
	comps, work := newTestComponents(t)
	release := make(chan struct{})
	comps.Go("watcher", func() error { <-release; <-work.Done(); return nil })
	comps.stop()
	expired, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.False(t, comps.Wait(expired), "a component still running is reported")
	close(release)
	waitForComponents(t, comps)
	require.NoError(t, comps.Err())
}

// TestProviderdGoroutinesStartThroughComponents keeps every long-lived
// goroutine of providerd under the supervisor. One started any other way could
// fail or stop without reaching the exit status. Two bounded goroutines are
// reviewed: joinWithin's single waiter, and the startup identity probe's
// errgroup, which its caller joins before startup continues.
func TestProviderdGoroutinesStartThroughComponents(t *testing.T) {
	allowed := map[string]int{"supervise": 1, "joinWithin": 1, "attestPinnedBackendIdentitiesWithin": 1}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	started := make(map[string]int)
	var violations []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			owner := "<package scope>"
			if function, ok := declaration.(*ast.FuncDecl); ok {
				owner = function.Name.Name
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				var start bool
				switch node := node.(type) {
				case *ast.GoStmt:
					start = true
				case *ast.CallExpr:
					selector, ok := node.Fun.(*ast.SelectorExpr)
					start = ok && selector.Sel.Name == "Go" && !isComponentsReceiver(selector.X)
				}
				if !start {
					return true
				}
				started[owner]++
				if _, ok := allowed[owner]; !ok {
					violations = append(violations, fmt.Sprintf("%s: goroutine started in %s", fset.Position(node.Pos()), owner))
				}
				return true
			})
		}
	}
	require.Empty(t, violations, "start long-lived goroutines through components")
	for owner, count := range allowed {
		require.Equal(t, count, started[owner], "reviewed goroutines in %s changed; review the new one", owner)
	}
}

// isComponentsReceiver reports a call on the supervisor itself, comps.Go.
func isComponentsReceiver(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "comps"
}
