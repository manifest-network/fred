package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func snapshotStores(t *testing.T) (*config.Config, *placement.Store, *payload.Store) {
	t.Helper()
	liveDir := t.TempDir()
	cfg := &config.Config{
		ProviderUUID:              placementstore.ProviderUUID,
		PlacementStoreDBPath:      filepath.Join(liveDir, "placements.db"),
		PayloadStoreDBPath:        filepath.Join(liveDir, "payloads.db"),
		PlacementSnapshotInterval: time.Hour,
		PlacementSnapshotRetain:   24,
	}
	placements, err := placementstore.NewStore(cfg.PlacementStoreDBPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = placements.Close() })
	payloads, err := payload.NewStore(payload.StoreConfig{DBPath: cfg.PayloadStoreDBPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })
	return cfg, placements, payloads
}

func TestPlacementSnapshotsAreOffUnlessADirectoryIsConfigured(t *testing.T) {
	cfg, placements, payloads := snapshotStores(t)
	service, closeSnapshots, err := newPlacementSnapshots(cfg, placements, payloads)
	require.NoError(t, err)
	assert.Nil(t, service)
	assert.NoError(t, closeSnapshots())
}

func TestPlacementSnapshotsBindTheConfiguredDirectory(t *testing.T) {
	cfg, placements, payloads := snapshotStores(t)
	cfg.PlacementSnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	require.NoError(t, os.Mkdir(cfg.PlacementSnapshotDir, 0o700))
	service, closeSnapshots, err := newPlacementSnapshots(cfg, placements, payloads)
	require.NoError(t, err)
	assert.NotNil(t, service)
	assert.NoError(t, closeSnapshots())

	require.NoError(t, os.Chmod(cfg.PlacementSnapshotDir, 0o770))
	_, closeSnapshots, err = newPlacementSnapshots(cfg, placements, payloads)
	assert.ErrorContains(t, err, "not writable by group or others")
	assert.NoError(t, closeSnapshots())
}

// TestShutdownJoinsPlacementSnapshotsBeforeTheStoresClose pins the composition
// root: the snapshot loop runs on its own WaitGroup, and shutdown waits for it
// before the provision manager closes the payload store.
func TestShutdownJoinsPlacementSnapshotsBeforeTheStoresClose(t *testing.T) {
	fset := token.NewFileSet()
	source, err := parser.ParseFile(fset, "main.go", nil, 0)
	require.NoError(t, err)
	var run *ast.FuncDecl
	for _, decl := range source.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "run" {
			run = fn
		}
	}
	require.NotNil(t, run)
	var started, joined, closed []token.Pos
	ast.Inspect(run.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			if function.Name == "safeGo" && len(call.Args) > 0 {
				if group, ok := call.Args[0].(*ast.UnaryExpr); ok {
					if name, ok := group.X.(*ast.Ident); ok && name.Name == "snapshotWG" {
						started = append(started, call.Pos())
					}
				}
			}
		case *ast.SelectorExpr:
			owner, ok := function.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch {
			case owner.Name == "snapshotWG" && function.Sel.Name == "Wait":
				joined = append(joined, call.Pos())
			case owner.Name == "provisionMgr" && function.Sel.Name == "Close":
				closed = append(closed, call.Pos())
			}
		}
		return true
	})
	require.Len(t, started, 1, "the snapshot loop starts on snapshotWG")
	require.Len(t, joined, 1)
	require.Len(t, closed, 1)
	assert.Less(t, started[0], joined[0])
	assert.Less(t, joined[0], closed[0], "shutdown joins the snapshot loop before the stores close")
}
