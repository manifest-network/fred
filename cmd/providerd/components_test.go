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
	comps, work := newComponents(t.Context())
	t.Cleanup(comps.BeginShutdown)
	return comps, work
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
		{"runtime component", func(c *components, fn func() error) { c.Start("api server", fn) }},
		{"store reader", func(c *components, fn func() error) { c.StartStoreReader("placement snapshots", fn) }},
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
	comps.Start("reconciler", func() error { panic("nil map write") })
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
			comps.Start("event bridge", func() error { return tc.err })
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
	comps.Start("reconciler", func() error { started <- struct{}{}; <-work.Done(); return work.Err() })
	comps.Start("scheduler", func() error {
		started <- struct{}{}
		<-work.Done()
		return fmt.Errorf("scheduler: %w", context.Canceled)
	})
	comps.StartStoreReader("placement snapshots", func() error { started <- struct{}{}; <-work.Done(); return nil })
	for range 3 {
		<-started
	}
	comps.BeginShutdown()
	waitForComponents(t, comps)
	require.NoError(t, comps.Err())
}

func TestComponentErrorDuringShutdownIsStillAFailure(t *testing.T) {
	comps, work := newTestComponents(t)
	failure := errors.New("router close timed out")
	comps.Start("provision manager", func() error { <-work.Done(); return failure })
	comps.BeginShutdown()
	waitForComponents(t, comps)
	require.ErrorIs(t, comps.Err(), failure)
}

func TestEveryComponentFailureIsReported(t *testing.T) {
	comps, _ := newTestComponents(t)
	first, second := errors.New("first"), errors.New("second")
	comps.Start("api server", func() error { return first })
	comps.Start("watcher", func() error { return second })
	waitForComponents(t, comps)
	require.ErrorIs(t, comps.Err(), first)
	require.ErrorIs(t, comps.Err(), second)
}

func TestComponentsWaitReportsATimeout(t *testing.T) {
	comps, work := newTestComponents(t)
	release := make(chan struct{})
	comps.Start("watcher", func() error { <-release; <-work.Done(); return nil })
	comps.BeginShutdown()
	expired, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.False(t, comps.Wait(expired), "a component still running is reported")
	close(release)
	waitForComponents(t, comps)
	require.NoError(t, comps.Err())
}

func TestTheWorkContextEndsWithItsParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(t.Context())
	comps, work := newComponents(parent)
	cancelParent()
	<-work.Done()
	comps.Start("watcher", func() error { return work.Err() })
	waitForComponents(t, comps)
	require.NoError(t, comps.Err(), "a signal ends the work context, so stopping after it is not a failure")
}

// TestProviderdGoroutinesStartThroughComponents keeps every long-lived
// goroutine of providerd under the supervisor. One started any other way could
// fail or stop without reaching the exit status. Two bounded goroutines are
// reviewed: joinWithin's single waiter, and the startup identity probe's
// errgroup, which its caller joins before startup continues. The check reads
// syntax: it rejects every go statement and every call to a method named Go,
// whatever the receiver, so the supervisor has no Go method. It cannot see a
// goroutine started inside a library call.
func TestProviderdGoroutinesStartThroughComponents(t *testing.T) {
	allowed := map[string]int{"supervise": 1, "joinWithin": 1, "attestPinnedBackendIdentitiesWithin": 1}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		files = append(files, file)
	}
	started, violations := goroutineStarts(fset, files, allowed)
	require.Empty(t, violations, "start long-lived goroutines through components")
	for owner, count := range allowed {
		require.Equal(t, count, started[owner], "reviewed goroutines in %s changed; review the new one", owner)
	}
}

// The guard must not trust a receiver's name: a WaitGroup called comps is
// still an unsupervised goroutine.
func TestGoroutineGuardIgnoresReceiverNames(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "probe.go", `package main

import "sync"

func probe() {
	var comps sync.WaitGroup
	comps.Go(func() {})
	go func() {}()
}
`, 0)
	require.NoError(t, err)
	started, violations := goroutineStarts(fset, []*ast.File{file}, nil)
	require.Equal(t, 2, started["probe"])
	require.Len(t, violations, 2)
}

// goroutineStarts counts, per enclosing top-level declaration, the go
// statements and calls to a method named Go, and lists every one outside
// allowed.
func goroutineStarts(fset *token.FileSet, files []*ast.File, allowed map[string]int) (map[string]int, []string) {
	started := make(map[string]int)
	var violations []string
	for _, file := range files {
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
					start = ok && selector.Sel.Name == "Go"
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
	return started, violations
}
