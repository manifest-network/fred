package placement

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/payload"
)

const (
	snapshotLeaseBefore = "00000000-0000-4000-8000-000000000a01"
	snapshotLeaseAfter  = "00000000-0000-4000-8000-000000000a02"
)

func newSnapshotStores(t *testing.T) (*Store, *payload.Store) {
	t.Helper()
	store := newTestStore(t)
	payloads, err := payload.NewStore(payload.StoreConfig{DBPath: filepath.Join(t.TempDir(), "payloads.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })
	return store, payloads
}

// copiedBucketHas opens a copied database read-only and reports whether key is
// in bucket.
func copiedBucketHas(t *testing.T, copied []byte, bucket []byte, key string) bool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "copy.db")
	require.NoError(t, os.WriteFile(path, copied, 0o600))
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	found := false
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bucket); b != nil {
			found = b.Get([]byte(key)) != nil
		}
		return nil
	}))
	return found
}

func TestConsistentCutCopiesBothStoresAsOfCapture(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	requireConfirmedPlacement(t, store, snapshotLeaseBefore, "backend-a")
	require.True(t, payloads.Store(snapshotLeaseBefore, []byte("before")))

	cut, err := store.CaptureConsistentCut(payloads, time.Minute)
	require.NoError(t, err)
	// Writers proceed while the cut is open, and their writes are not in it.
	requireConfirmedPlacement(t, store, snapshotLeaseAfter, "backend-a")
	require.True(t, payloads.Store(snapshotLeaseAfter, []byte("after")))

	var placements, payloadsCopy bytes.Buffer
	receipt, err := cut.Stream(t.Context(), &placements, &payloadsCopy)
	require.NoError(t, err)
	require.True(t, receipt.Valid())
	assert.Equal(t, freshTestProviderUUID, receipt.ProviderUUID())
	assert.Equal(t, cut.Size(), receipt.Placements().Size+receipt.Payloads().Size)
	assert.Equal(t, int64(placements.Len()), receipt.Placements().Size)
	assert.Equal(t, sha256.Sum256(placements.Bytes()), receipt.Placements().SHA256)
	assert.Equal(t, int64(payloadsCopy.Len()), receipt.Payloads().Size)
	assert.Equal(t, sha256.Sum256(payloadsCopy.Bytes()), receipt.Payloads().SHA256)

	assert.True(t, copiedBucketHas(t, placements.Bytes(), bucketName, snapshotLeaseBefore))
	assert.False(t, copiedBucketHas(t, placements.Bytes(), bucketName, snapshotLeaseAfter))
	assert.True(t, copiedBucketHas(t, payloadsCopy.Bytes(), []byte("payloads"), snapshotLeaseBefore))
	assert.False(t, copiedBucketHas(t, payloadsCopy.Bytes(), []byte("payloads"), snapshotLeaseAfter))
}

// copiedBucketKeys opens a copied database read-only and returns bucket's keys.
func copiedBucketKeys(t *testing.T, copied []byte, bucket []byte) map[string]bool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys.db")
	require.NoError(t, os.WriteFile(path, copied, 0o600))
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	keys := make(map[string]bool)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bucket); b != nil {
			return b.ForEach(func(key, _ []byte) error {
				keys[string(key)] = true
				return nil
			})
		}
		return nil
	}))
	return keys
}

// TestConsistentCutIsACrashState captures while a writer stores each lease's
// payload and then its placement, the order provisioning uses. A cut is a state
// a crash could leave only if no placement in it lacks the payload written
// before it.
func TestConsistentCutIsACrashState(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	const leases, captureAfter = 40, 10
	start := make(chan struct{})
	type captured struct {
		cut ConsistentCut
		err error
	}
	cutCh := make(chan captured, 1)
	go func() {
		<-start
		cut, err := store.CaptureConsistentCut(payloads, time.Minute)
		cutCh <- captured{cut: cut, err: err}
	}()
	for i := range leases {
		lease := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		require.True(t, payloads.Store(lease, []byte(lease)))
		requireConfirmedPlacement(t, store, lease, "backend-a")
		if i == captureAfter-1 {
			close(start)
		}
	}
	result := <-cutCh
	require.NoError(t, result.err)

	var placements, payloadsCopy bytes.Buffer
	_, err := result.cut.Stream(t.Context(), &placements, &payloadsCopy)
	require.NoError(t, err)
	placed := copiedBucketKeys(t, placements.Bytes(), bucketName)
	stored := copiedBucketKeys(t, payloadsCopy.Bytes(), []byte("payloads"))
	assert.GreaterOrEqual(t, len(placed), captureAfter)
	for lease := range placed {
		assert.True(t, stored[lease], "lease %s is placed in the cut without its payload", lease)
	}

	path := filepath.Join(t.TempDir(), "placements.db")
	require.NoError(t, os.WriteFile(path, placements.Bytes(), 0o600))
	store.mu.RLock()
	topology := slices.Clone(store.backendTopology)
	store.mu.RUnlock()
	expectation, err := NewAuthorityExpectation(freshTestProviderUUID, topology)
	require.NoError(t, err)
	report, err := InspectAuthorityFile(path, expectation)
	require.NoError(t, err)
	assert.Equal(t, AuthorityPreparedCurrent, report.Classification, report.Diagnostics)
}

// blockedWriter never returns from Write until released, like a stuck disk.
type blockedWriter struct{ release <-chan struct{} }

func (w blockedWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func TestConsistentCutEndsItsTransactionsAtTheDeadline(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	cut, err := store.CaptureConsistentCut(payloads, 100*time.Millisecond)
	require.NoError(t, err)
	release := make(chan struct{})
	defer close(release)

	started := time.Now()
	_, err = cut.Stream(t.Context(), blockedWriter{release}, io.Discard)
	require.ErrorIs(t, err, ErrSnapshotDeadline)
	assert.Less(t, time.Since(started), 5*time.Second)
	// The destination write is still blocked, yet both transactions ended.
	requireStoresClose(t, store, payloads)
}

// requireStoresClose proves no read transaction is open: bbolt's Close waits
// for every open read transaction.
func requireStoresClose(t *testing.T, store *Store, payloads *payload.Store) {
	t.Helper()
	closed := make(chan error, 2)
	go func() { closed <- store.Close() }()
	go func() { closed <- payloads.Close() }()
	for range 2 {
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("a snapshot read transaction outlived its deadline")
		}
	}
}

func TestAnUnstreamedCutExpiresAtItsDeadline(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	cut, err := store.CaptureConsistentCut(payloads, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Positive(t, cut.Size())

	requireStoresClose(t, store, payloads)
	_, err = cut.Stream(t.Context(), io.Discard, io.Discard)
	assert.ErrorContains(t, err, "already used or expired")
	assert.NoError(t, cut.Discard())
}

func TestConsistentCutIsUsedOnce(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	cut, err := store.CaptureConsistentCut(payloads, time.Minute)
	require.NoError(t, err)
	_, err = cut.Stream(t.Context(), io.Discard, io.Discard)
	require.NoError(t, err)
	_, err = cut.Stream(t.Context(), io.Discard, io.Discard)
	assert.ErrorContains(t, err, "already used or expired")
	assert.NoError(t, cut.Discard(), "discarding a streamed cut is a no-op")

	unused, err := store.CaptureConsistentCut(payloads, time.Minute)
	require.NoError(t, err)
	require.NoError(t, unused.Discard())
	_, err = unused.Stream(t.Context(), io.Discard, io.Discard)
	assert.ErrorContains(t, err, "already used or expired")

	var zero ConsistentCut
	_, err = zero.Stream(t.Context(), io.Discard, io.Discard)
	assert.Error(t, err)
	assert.NoError(t, zero.Discard())
	assert.False(t, CutReceipt{}.Valid())
}

func TestCaptureConsistentCutRefusesWithdrawnAuthority(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	_, err := store.CaptureConsistentCut(nil, time.Minute)
	assert.Error(t, err)
	_, err = store.CaptureConsistentCut(payloads, 0)
	assert.Error(t, err)

	withdrawn := errors.New("test withdrawal")
	_ = store.latchRuntimeAuthorityFailure(withdrawn)
	_, err = store.CaptureConsistentCut(payloads, time.Minute)
	assert.ErrorIs(t, err, withdrawn)
}

// TestLiveWritesDoNotWaitForAnOpenCut grows the live payload database while a
// cut holds its read transaction. bbolt remaps a growing file only after every
// open read ends, so the write finishing well before the cut's deadline proves
// the live store reserved its mapping up front.
func TestLiveWritesDoNotWaitForAnOpenCut(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	cut, err := store.CaptureConsistentCut(payloads, 30*time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, cut.Discard()) }()
	stored := make(chan bool, 1)
	go func() { stored <- payloads.Store(snapshotLeaseAfter, bytes.Repeat([]byte{'p'}, 4<<20)) }()
	select {
	case ok := <-stored:
		require.True(t, ok)
	case <-time.After(10 * time.Second):
		t.Fatal("a live payload write waited for the open cut")
	}
}

// TestOpenStoreReservesTheSnapshotMapping covers the production open the
// in-package fixture does not use: OpenStore must reserve the same mapping.
func TestOpenStoreReservesTheSnapshotMapping(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "store.go", nil, 0)
	require.NoError(t, err)
	reserved := false
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "OpenStore" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			field, ok := node.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, keyOK := field.Key.(*ast.Ident)
			value, valueOK := field.Value.(*ast.Ident)
			if keyOK && valueOK && key.Name == "InitialMmapSize" && value.Name == "liveStoreInitialMmapSize" {
				reserved = true
			}
			return true
		})
	}
	assert.True(t, reserved, "OpenStore's bolt options set InitialMmapSize: liveStoreInitialMmapSize")
}

func TestCaptureRefusesAStoreItsPathNoLongerNames(t *testing.T) {
	store, payloads := newSnapshotStores(t)
	path := store.db.Path()
	require.NoError(t, os.Rename(path, path+".moved"))
	_, err := store.CaptureConsistentCut(payloads, time.Minute)
	assert.Error(t, err, "authority is checked before the gate is held")
}

func TestStreamChecksBothStoresAgainAfterTheCopy(t *testing.T) {
	for _, moved := range []string{"placements", "payloads"} {
		t.Run(moved, func(t *testing.T) {
			store := newTestStore(t)
			payloadPath := filepath.Join(t.TempDir(), "payloads.db")
			payloads, err := payload.NewStore(payload.StoreConfig{DBPath: payloadPath})
			require.NoError(t, err)
			t.Cleanup(func() { _ = payloads.Close() })
			cut, err := store.CaptureConsistentCut(payloads, time.Minute)
			require.NoError(t, err)
			path := map[string]string{"placements": store.db.Path(), "payloads": payloadPath}[moved]
			require.NoError(t, os.Rename(path, path+".moved"))
			_, err = cut.Stream(t.Context(), io.Discard, io.Discard)
			assert.Error(t, err, "a copy of a store its path stopped naming is not a snapshot of it")
		})
	}
}

// TestBothSnapshotReadsBeginInsideTheGateHold pins the consistency argument:
// no placement write can land between the two Begins only if both run inside
// the function literal the gate holds.
func TestBothSnapshotReadsBeginInsideTheGateHold(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "online_snapshot.go", nil, 0)
	require.NoError(t, err)
	var capture *ast.FuncDecl
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "CaptureConsistentCut" {
			capture = function
		}
	}
	require.NotNil(t, capture)
	begins := func(node ast.Node) (placementBegins, payloadBegins int) {
		ast.Inspect(node, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					switch selector.Sel.Name {
					case "Begin":
						placementBegins++
					case "BeginSnapshotRead":
						payloadBegins++
					}
				}
			}
			return true
		})
		return placementBegins, payloadBegins
	}
	var held *ast.FuncLit
	ast.Inspect(capture.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Hold" && len(call.Args) == 1 {
				held, _ = call.Args[0].(*ast.FuncLit)
			}
		}
		return true
	})
	require.NotNil(t, held, "CaptureConsistentCut holds the gate around a function literal")
	heldPlacement, heldPayload := begins(held.Body)
	allPlacement, allPayload := begins(capture.Body)
	assert.Equal(t, 1, heldPlacement)
	assert.Equal(t, 1, heldPayload)
	assert.Equal(t, allPlacement, heldPlacement, "every placement Begin is inside the gate hold")
	assert.Equal(t, allPayload, heldPayload, "every payload Begin is inside the gate hold")
}

// snapshotReadCallers are the only production functions allowed to begin a
// snapshot read or hold a store's write gate: anything else could hold every
// placement writer behind arbitrary work.
var snapshotReadCallers = map[string]map[string]bool{
	"Hold":              {"internal/provisioner/placement/online_snapshot.go:CaptureConsistentCut": true},
	"BeginSnapshotRead": {"internal/provisioner/placement/online_snapshot.go:CaptureConsistentCut": true},
}

// TestSnapshotCapabilitiesAreMintedOnlyByTheirConstructors keeps the minting
// claims on ConsistentCut and CutReceipt true: a non-empty literal of either
// appears only in its constructor.
func TestSnapshotCapabilitiesAreMintedOnlyByTheirConstructors(t *testing.T) {
	minters := map[string]string{
		"ConsistentCut": "CaptureConsistentCut",
		"cutState":      "CaptureConsistentCut",
		"CutReceipt":    "Stream",
	}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
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
				literal, ok := node.(*ast.CompositeLit)
				if !ok || len(literal.Elts) == 0 {
					return true
				}
				name, ok := literal.Type.(*ast.Ident)
				if !ok {
					return true
				}
				if minter, guarded := minters[name.Name]; guarded && owner != minter {
					t.Errorf("%s: %s minted in %s", fset.Position(literal.Pos()), name.Name, owner)
				}
				return true
			})
		}
	}
}

func TestSnapshotReadsBeginOnlyInsideTheConsistentCut(t *testing.T) {
	repositoryRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fset := token.NewFileSet()
	for _, topLevel := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(repositoryRoot, topLevel), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			for _, declaration := range file.Decls {
				owner := "<package scope>"
				if function, ok := declaration.(*ast.FuncDecl); ok {
					owner = function.Name.Name
				}
				ast.Inspect(declaration, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if allowed, guarded := snapshotReadCallers[selector.Sel.Name]; guarded && !allowed[relative+":"+owner] {
						t.Errorf("%s calls %s outside the consistent cut (in %s)",
							fset.Position(call.Pos()), selector.Sel.Name, owner)
					}
					return true
				})
			}
			return nil
		})
		require.NoError(t, err)
	}
}
