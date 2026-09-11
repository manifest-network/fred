package payload

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/storeauthority"
)

type fakePayloadWriteTransaction struct {
	commitErr     error
	rollbackErr   error
	commitCalls   int
	rollbackCalls int
}

func (tx *fakePayloadWriteTransaction) Commit() error {
	tx.commitCalls++
	return tx.commitErr
}

func (tx *fakePayloadWriteTransaction) Rollback() error {
	tx.rollbackCalls++
	return tx.rollbackErr
}

func TestFinishPayloadWriteTransactionClassifiesOnlyCommitErrorsAsOutcomeUnknown(t *testing.T) {
	mutationErr := errors.New("synthetic payload mutation failure")
	mutationTx := &fakePayloadWriteTransaction{}
	err := finishPayloadWriteTransaction(mutationTx, func() error { return mutationErr })
	require.ErrorIs(t, err, mutationErr)
	assert.NotErrorIs(t, err, ErrStoreMutationOutcomeUnknown)
	assert.Zero(t, mutationTx.commitCalls)
	assert.Equal(t, 1, mutationTx.rollbackCalls)

	commitErr := errors.New("synthetic payload commit failure")
	commitTx := &fakePayloadWriteTransaction{commitErr: commitErr}
	err = finishPayloadWriteTransaction(commitTx, func() error { return nil })
	require.ErrorIs(t, err, commitErr)
	require.ErrorIs(t, err, ErrStoreMutationOutcomeUnknown)
	assert.Equal(t, 1, commitTx.commitCalls)
	assert.Equal(t, 1, commitTx.rollbackCalls)

	successTx := &fakePayloadWriteTransaction{}
	require.NoError(t, finishPayloadWriteTransaction(successTx, func() error { return nil }))
	assert.Equal(t, 1, successTx.commitCalls)
	assert.Zero(t, successTx.rollbackCalls)
}

func TestPayloadAuthorityWithdrawalLinearizesWithAdmittedCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payloads.db")
	store, err := NewStore(StoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.Chmod(path, 0o600)
		_ = store.Close()
	})

	key := []byte("admitted-before-withdrawal")
	entered := make(chan struct{})
	release := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.update(func(tx *bolt.Tx) error {
			if err := tx.Bucket(payloadBucketName).Put(key, []byte("committed")); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	require.NoError(t, os.Chmod(path, 0o640))
	healthDone := make(chan error, 1)
	healthStarted := make(chan struct{})
	go func() {
		close(healthStarted)
		healthDone <- store.Healthy()
	}()
	<-healthStarted
	select {
	case err := <-healthDone:
		t.Fatalf("authority withdrawal overtook admitted commit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	writeErr := <-writeDone
	require.ErrorIs(t, writeErr, ErrStoreAuthorityUnavailable)
	require.ErrorIs(t, writeErr, ErrStoreAuthorityPathChanged)
	healthErr := <-healthDone
	require.ErrorIs(t, healthErr, ErrStoreAuthorityUnavailable)
	require.ErrorIs(t, healthErr, ErrStoreAuthorityPathChanged)

	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, []byte("committed"), tx.Bucket(payloadBucketName).Get(key),
			"the write admitted before withdrawal may finish")
		return nil
	}))
	lateMutationRan := false
	err = store.update(func(*bolt.Tx) error {
		lateMutationRan = true
		return nil
	})
	require.ErrorIs(t, err, ErrStoreAuthorityUnavailable)
	assert.False(t, lateMutationRan, "no mutation may begin after withdrawal is published")
}

func TestPayloadBoundaryPanicWithdrawsAuthorityAndFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payloads.db")
	store, err := NewStore(StoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	panicValue := errors.New("synthetic payload mutation panic")
	func() {
		defer func() { require.Same(t, panicValue, recover()) }()
		_ = store.update(func(*bolt.Tx) error { panic(panicValue) })
	}()

	latched := store.authorityFailure()
	require.ErrorIs(t, latched, storeauthority.ErrWriteBoundaryPanicked)
	require.ErrorIs(t, latched, panicValue)

	_, err = store.Get("must-not-read-after-unknown-write")
	require.ErrorIs(t, err, storeauthority.ErrWriteBoundaryPanicked)
	_, err = store.count()
	require.ErrorIs(t, err, storeauthority.ErrWriteBoundaryPanicked)
	require.Error(t, store.Put("must-not-write-after-unknown-write", []byte("payload")))
}

func TestNewStoreSynchronizesParentBeforePublication(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "store.go", nil, 0)
	require.NoError(t, err)

	var newStore *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "NewStore" {
			newStore = function
			break
		}
	}
	require.NotNil(t, newStore)

	var parentSyncPosition token.Pos
	for _, statement := range newStore.Body.List {
		conditional, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		assignment, ok := conditional.Init.(*ast.AssignStmt)
		if !ok || len(assignment.Rhs) != 1 {
			continue
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok {
			continue
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Sync" {
			continue
		}
		receiver, ok := selector.X.(*ast.Ident)
		if ok && receiver.Name == "directory" {
			parentSyncPosition = call.Pos()
			break
		}
	}
	require.NotEqual(t, token.NoPos, parentSyncPosition,
		"NewStore must unconditionally sync the retained parent directory")

	var publicationPosition token.Pos
	ast.Inspect(newStore.Body, func(node ast.Node) bool {
		result, ok := node.(*ast.ReturnStmt)
		if !ok || len(result.Results) != 2 {
			return true
		}
		storeResult, storeOK := result.Results[0].(*ast.Ident)
		errorResult, errorOK := result.Results[1].(*ast.Ident)
		if storeOK && errorOK && storeResult.Name == "s" && errorResult.Name == "nil" {
			publicationPosition = result.Pos()
		}
		return true
	})
	require.NotEqual(t, token.NoPos, publicationPosition)
	assert.Less(t, parentSyncPosition, publicationPosition,
		"the parent directory must be synced before NewStore publishes the Store")
}
