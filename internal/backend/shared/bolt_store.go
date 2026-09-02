package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
	"github.com/manifest-network/fred/internal/util"
)

var (
	storeIdentityBucketName    = []byte("_fred_backend_storage_identity")
	storeIdentitySchemaKey     = []byte("schema")
	storeIdentityStorageIDKey  = []byte("backend_storage_id")
	storeIdentityStoreKindKey  = []byte("store_kind")
	storeIdentitySchemaVersion = []byte("1")

	// ErrStoreIdentityUnbound means an authoritative database predates the
	// storage-lineage seal (or was replaced by an unbound file). Only the explicit
	// pending initializer may add the binding; ordinary startup must fail closed.
	ErrStoreIdentityUnbound = errors.New("authoritative backend store is not bound to storage identity")

	// ErrStoreIdentityMismatch means a bound database belongs to another storage
	// generation or another authoritative store role.
	ErrStoreIdentityMismatch = errors.New("authoritative backend store identity mismatch")
)

const (
	maxAuthoritativeInspectionRows  = 100_000
	maxAuthoritativeRecordBytes     = backend.MaxStoredReleaseHistoryBytes
	maxAuthoritativeInspectionBytes = 256 << 20
)

type authoritativeInspectionLimits struct {
	maxRows        int
	maxRecordBytes int
	maxTotalBytes  int64
}

var stoppedAuthoritativeInspectionLimits = authoritativeInspectionLimits{
	maxRows:        maxAuthoritativeInspectionRows,
	maxRecordBytes: maxAuthoritativeRecordBytes,
	maxTotalBytes:  maxAuthoritativeInspectionBytes,
}

type authoritativeInspectionBudget struct {
	limits authoritativeInspectionLimits
	rows   int
	bytes  int64
}

func newStoppedAuthoritativeInspectionBudget() authoritativeInspectionBudget {
	return newAuthoritativeInspectionBudget(stoppedAuthoritativeInspectionLimits)
}

func newAuthoritativeInspectionBudget(
	limits authoritativeInspectionLimits,
) authoritativeInspectionBudget {
	return authoritativeInspectionBudget{limits: limits}
}

func validateAuthoritativeRecord(key, value []byte, maxRecordBytes int) error {
	if maxRecordBytes <= 0 {
		return errors.New("authoritative store record limit must be positive")
	}
	if len(key) > maxRecordBytes || len(value) > maxRecordBytes {
		return fmt.Errorf(
			"authoritative store record exceeds %d bytes (key_length=%d value_length=%d)",
			maxRecordBytes, len(key), len(value),
		)
	}
	return nil
}

// ValidateDistinctStorePaths rejects lexical, symlink-parent, and existing
// inode aliases before any authoritative file is created or opened. Names are
// used only for bounded operator diagnostics.
func ValidateDistinctStorePaths(paths map[string]string) error {
	names := make([]string, 0, len(paths))
	for name := range paths {
		names = append(names, name)
	}
	slices.Sort(names)
	type resolvedPath struct {
		name string
		path string
		info os.FileInfo
	}
	resolved := make([]resolvedPath, 0, len(names))
	for _, name := range names {
		path := paths[name]
		if path == "" {
			return fmt.Errorf("backend storage path %s is empty", name)
		}
		absolute, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return fmt.Errorf("resolve backend storage path %s: %w", name, err)
		}
		canonicalParent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
		if err != nil {
			return fmt.Errorf("resolve backend storage path %s parent: %w", name, err)
		}
		canonical := filepath.Join(canonicalParent, filepath.Base(absolute))
		var info os.FileInfo
		if stat, statErr := os.Stat(canonical); statErr == nil {
			info = stat
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("stat backend storage path %s: %w", name, statErr)
		}
		for _, earlier := range resolved {
			if canonical == earlier.path || (info != nil && earlier.info != nil && os.SameFile(info, earlier.info)) {
				return fmt.Errorf("backend storage paths %s and %s alias the same durable location", earlier.name, name)
			}
		}
		resolved = append(resolved, resolvedPath{name: name, path: canonical, info: info})
	}
	return nil
}

func (budget *authoritativeInspectionBudget) observe(key, value []byte) error {
	if budget == nil {
		return errors.New("authoritative inspection budget is required")
	}
	if budget.limits.maxRows <= 0 || budget.limits.maxTotalBytes <= 0 {
		return errors.New("stopped authoritative inspection aggregate limits must be positive")
	}
	if err := validateAuthoritativeRecord(key, value, budget.limits.maxRecordBytes); err != nil {
		return err
	}
	budget.rows++
	if budget.rows > budget.limits.maxRows {
		return fmt.Errorf(
			"stopped authoritative store inspection exceeds the %d-record cutover ceiling; run the matching predecessor to drain or prune completed state, then retry",
			budget.limits.maxRows,
		)
	}
	budget.bytes += int64(len(key)) + int64(len(value))
	if budget.bytes > budget.limits.maxTotalBytes {
		return fmt.Errorf(
			"stopped authoritative store inspection exceeds the %d-byte logical cutover ceiling; run the matching predecessor to drain or prune completed state, then retry",
			budget.limits.maxTotalBytes,
		)
	}
	return nil
}

type authoritativeStoreKind string

const (
	authoritativeStoreCallbacks authoritativeStoreKind = "callbacks"
	authoritativeStoreReleases  authoritativeStoreKind = "releases"
	authoritativeStoreRetention authoritativeStoreKind = "retention"
)

// BoundAuthoritativeStorePath retains the exact physical parent directory of
// one authority-bearing journal during a storage-lineage initialization. Its
// bbolt opens are descriptor-relative and do not follow a final-component
// symlink, so replacing the configured pathname cannot redirect an evidence
// read or identity-binding write into a different directory.
//
// The zero value is invalid. Call Close after the complete proof-and-
// publication transaction, not after an individual store operation.
type BoundAuthoritativeStorePath struct {
	directory *fsidentity.Directory
	entry     fsidentity.Entry
}

// BindAuthoritativeStorePath acquires a retained physical-parent capability
// for dbPath. Parent symlinks are resolved once before acquisition; all later
// filesystem operations use the retained descriptor rather than dbPath.
func BindAuthoritativeStorePath(dbPath string) (*BoundAuthoritativeStorePath, error) {
	if dbPath == "" {
		return nil, errors.New("authoritative store path is required")
	}
	absolute, err := filepath.Abs(filepath.Clean(dbPath))
	if err != nil {
		return nil, fmt.Errorf("resolve authoritative store path: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, fmt.Errorf("resolve authoritative store parent: %w", err)
	}
	directory, err := fsidentity.OpenDirectory(parent)
	if err != nil {
		return nil, fmt.Errorf("bind authoritative store parent: %w", err)
	}
	entry, err := directory.Entry(filepath.Base(absolute))
	if err != nil {
		_ = directory.Close()
		return nil, fmt.Errorf("bind authoritative store entry: %w", err)
	}
	return &BoundAuthoritativeStorePath{directory: directory, entry: entry}, nil
}

// VerifyPath proves that the acquisition pathname still names the retained
// physical parent directory.
func (path *BoundAuthoritativeStorePath) VerifyPath() error {
	if path == nil || path.directory == nil || !path.entry.Valid() {
		return errors.New("authoritative store path capability is invalid")
	}
	return path.directory.VerifyPath()
}

// Close revokes the retained parent-directory capability.
func (path *BoundAuthoritativeStorePath) Close() error {
	if path == nil || path.directory == nil {
		return nil
	}
	err := path.directory.Close()
	path.directory = nil
	path.entry = fsidentity.Entry{}
	return err
}

type authoritativeStoreFile interface {
	DisplayPath() string
	Lstat() (os.FileInfo, error)
	OpenFile(int, os.FileMode) (*os.File, error)
	SyncParent() error
}

type pathnameAuthoritativeStoreFile string

func (path pathnameAuthoritativeStoreFile) DisplayPath() string { return string(path) }

func (path pathnameAuthoritativeStoreFile) Lstat() (os.FileInfo, error) {
	return os.Lstat(string(path))
}

func (path pathnameAuthoritativeStoreFile) OpenFile(
	flag int,
	mode os.FileMode,
) (*os.File, error) {
	return os.OpenFile(string(path), flag, mode) // #nosec G304 -- operator-configured database path
}

func (path pathnameAuthoritativeStoreFile) SyncParent() error {
	return syncDirectory(filepath.Dir(string(path)))
}

func boundAuthoritativeStoreFile(path *BoundAuthoritativeStorePath) (authoritativeStoreFile, error) {
	if path == nil || path.directory == nil || !path.entry.Valid() {
		return nil, errors.New("authoritative store path capability is invalid")
	}
	return path.entry, nil
}

// boltStore provides the common lifecycle for bbolt-backed stores:
// database open/close, bucket creation, background cleanup, and health checks.
type boltStore struct {
	db        *bolt.DB
	bucket    []byte
	maxAge    time.Duration
	binding   *openedStoreIdentityBinding
	ctx       context.Context
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	closeOnce *sync.Once
	closeErr  error
	// backendAuthorityGate linearizes this store's write/Commit boundary with
	// sibling-journal and substrate authority withdrawal. It is mandatory and
	// immutable for an identity-bound store; unbound utility/test stores leave it
	// nil and never execute an authoritative identity check.
	backendAuthorityGate *backendidentity.StorageAuthorityGate
}

type openedStoreIdentityBinding struct {
	dbPath    string
	fileInfo  os.FileInfo
	kind      authoritativeStoreKind
	storageID backendidentity.ID
}

// boltStoreConfig configures a boltStore.
type boltStoreConfig struct {
	DBPath     string
	BucketName []byte
	MaxAge     time.Duration
	Label      string // for log/error messages (e.g. "callback", "diagnostics")
}

type authoritativeStoreWriteTransaction interface {
	Commit() error
	Rollback() error
}

type authoritativeStoreCommitBoundary func(commit func() error) (attempted bool, err error)

// finishAuthoritativeStoreWriteTransaction makes the durability boundary
// explicit. A mutation-function error is definitely uncommitted because Commit
// is never called. Any Commit error is outcome-unknown: bbolt may have written
// data or a meta page before surfacing it, so the store must withdraw authority
// until the process reopens and re-verifies the database.
func finishAuthoritativeStoreWriteTransaction(
	tx authoritativeStoreWriteTransaction,
	mutate func() error,
) error {
	return finishAuthoritativeStoreWriteTransactionAtBoundary(
		tx,
		mutate,
		func(commit func() error) (bool, error) { return true, commit() },
	)
}

func finishAuthoritativeStoreWriteTransactionAtBoundary(
	tx authoritativeStoreWriteTransaction,
	mutate func() error,
	commitBoundary authoritativeStoreCommitBoundary,
) error {
	if tx == nil {
		return errors.New("authoritative store write transaction is required")
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Rollback()
		}
	}()
	if mutate == nil {
		return errors.New("authoritative store mutation is required")
	}
	if commitBoundary == nil {
		return errors.New("authoritative store commit boundary is required")
	}

	if err := mutate(); err != nil {
		rollbackErr := tx.Rollback()
		finished = true
		if rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("roll back rejected authoritative store mutation: %w", rollbackErr))
		}
		return err
	}
	commitAttempted, err := commitBoundary(tx.Commit)
	if err != nil {
		if !commitAttempted {
			rollbackErr := tx.Rollback()
			finished = true
			if rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("roll back authority-refused store mutation: %w", rollbackErr))
			}
			return err
		}
		// A defensive rollback releases the writer if the failed Commit left the
		// transaction open. It cannot undo pages that may already be durable.
		_ = tx.Rollback()
		finished = true
		if errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous) {
			return err
		}
		return fmt.Errorf(
			"%w: authoritative store bbolt commit failed: %w",
			backendidentity.ErrMutationOutcomeAmbiguous,
			err,
		)
	}
	if !commitAttempted {
		return errors.New("authoritative store commit boundary returned without attempting commit")
	}
	finished = true
	return nil
}

// openBoltStore opens a bbolt database, creates the bucket, and returns the
// base store. The parent and final component are acquired through descriptor-
// relative, no-follow capabilities before bbolt can initialize a new file;
// existing and newly created databases must be regular, single-link, exact-
// 0600 files. Call startCleanup after construction if MaxAge > 0.
func openBoltStore(cfg boltStoreConfig) (*boltStore, error) {
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("%s db path is required", cfg.Label)
	}
	boundPath, err := BindAuthoritativeStorePath(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("bind %s db path: %w", cfg.Label, err)
	}
	defer func() { _ = boundPath.Close() }()
	file, err := boundAuthoritativeStoreFile(boundPath)
	if err != nil {
		return nil, fmt.Errorf("bind %s db entry: %w", cfg.Label, err)
	}

	info, err := file.Lstat()
	created := false
	switch {
	case err == nil:
		if err := validateAuthoritativeStoreFile(file.DisplayPath(), info); err != nil {
			return nil, fmt.Errorf("refuse insecure existing %s db: %w", cfg.Label, err)
		}
	case errors.Is(err, os.ErrNotExist):
		createdFile, createErr := file.OpenFile(os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if createErr != nil {
			return nil, fmt.Errorf("create %s db without following links: %w", cfg.Label, createErr)
		}
		info, err = createdFile.Stat()
		closeErr := createdFile.Close()
		if err != nil {
			return nil, fmt.Errorf("stat newly created %s db: %w", cfg.Label, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close newly created %s db: %w", cfg.Label, closeErr)
		}
		if err := validateAuthoritativeStoreFile(file.DisplayPath(), info); err != nil {
			return nil, fmt.Errorf("refuse insecure newly created %s db: %w", cfg.Label, err)
		}
		created = true
	case err != nil:
		return nil, fmt.Errorf("inspect existing %s db: %w", cfg.Label, err)
	}

	db, _, err := openBoltDBFileWithExpectedIdentity(file, info, false, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s db: %w", cfg.Label, err)
	}
	if err := boundPath.VerifyPath(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("re-attest %s db parent before schema write: %w", cfg.Label, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(cfg.BucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create %s bucket: %w", cfg.Label, err)
	}
	if err := verifyStoreFileIdentity(file, info); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("re-attest %s db after schema write: %w", cfg.Label, err)
	}
	if created {
		if err := file.SyncParent(); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sync new %s db parent: %w", cfg.Label, err)
		}
	}
	if err := boundPath.VerifyPath(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("re-attest %s db parent after schema write: %w", cfg.Label, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &boltStore{
		db:        db,
		bucket:    cfg.BucketName,
		maxAge:    cfg.MaxAge,
		ctx:       ctx,
		cancel:    cancel,
		wg:        &sync.WaitGroup{},
		closeOnce: &sync.Once{},
	}, nil
}

func verifyStoreFileIdentity(file authoritativeStoreFile, expected os.FileInfo) error {
	if file == nil || expected == nil {
		return errors.New("database file capability and expected identity are required")
	}
	current, err := file.Lstat()
	if err != nil {
		return err
	}
	if err := validateAuthoritativeStoreFile(file.DisplayPath(), current); err != nil {
		return err
	}
	if !os.SameFile(expected, current) {
		return errors.New("database pathname no longer names the opened file")
	}
	return nil
}

// openIdentityBoundBoltStore is the production opener for authority-bearing
// control state. It never creates a file, a primary bucket, or a lineage
// binding. The caller must present a capability returned from a committed marker
// pair, and the database must already carry the matching role-specific binding.
func openIdentityBoundBoltStore(
	cfg boltStoreConfig,
	kind authoritativeStoreKind,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*boltStore, error) {
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("%s db path is required", cfg.Label)
	}
	if !storage.Valid() {
		return nil, fmt.Errorf("open %s db: verified backend storage authority is required", cfg.Label)
	}
	if gate == nil || !gate.Valid() {
		return nil, fmt.Errorf("open %s db: backend storage authority gate is required", cfg.Label)
	}
	if err := gate.Error(); err != nil {
		return nil, fmt.Errorf("open %s db: backend storage authority was withdrawn: %w", cfg.Label, err)
	}
	db, fileInfo, err := openExistingBoltDB(cfg.DBPath, false, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open existing %s db: %w", cfg.Label, err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(cfg.BucketName) == nil {
			return fmt.Errorf("%s bucket %q is missing", cfg.Label, cfg.BucketName)
		}
		return verifyStoreIdentityBinding(tx, kind, storage.ID())
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("verify %s db identity: %w", cfg.Label, err)
	}
	store := newBoltStore(db, cfg)
	store.binding = &openedStoreIdentityBinding{
		dbPath:    cfg.DBPath,
		fileInfo:  fileInfo,
		kind:      kind,
		storageID: storage.ID(),
	}
	store.backendAuthorityGate = gate
	if err := gate.Error(); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open %s db: backend storage authority was withdrawn: %w", cfg.Label, err)
	}
	return store, nil
}

func initializeIdentityBoundBoltStoreBound(
	path *BoundAuthoritativeStorePath,
	bucketName []byte,
	label string,
	kind authoritativeStoreKind,
	storageID backendidentity.ID,
	allowCreate bool,
	validate func(*bolt.Tx) error,
) error {
	file, err := boundAuthoritativeStoreFile(path)
	if err != nil {
		return fmt.Errorf("initialize %s db: %w", label, err)
	}
	return initializeIdentityBoundBoltStoreFile(
		file, bucketName, label, kind, storageID, allowCreate, validate,
	)
}

func initializeIdentityBoundBoltStoreFile(
	file authoritativeStoreFile,
	bucketName []byte,
	label string,
	kind authoritativeStoreKind,
	storageID backendidentity.ID,
	allowCreate bool,
	validate func(*bolt.Tx) error,
) error {
	if !storageID.Valid() {
		return fmt.Errorf("initialize %s db: valid backend storage ID is required", label)
	}
	displayPath := file.DisplayPath()
	info, err := file.Lstat()
	switch {
	case err == nil:
		if err := validateAuthoritativeStoreFile(displayPath, info); err != nil {
			return fmt.Errorf("%s database: %w", label, err)
		}
	case errors.Is(err, os.ErrNotExist):
		if !allowCreate {
			return fmt.Errorf("%s database is missing during v0.13 adoption: %w", label, ErrStoreIdentityUnbound)
		}
		created, createErr := file.OpenFile(os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if createErr != nil {
			return fmt.Errorf("create %s database: %w", label, createErr)
		}
		if closeErr := created.Close(); closeErr != nil {
			return fmt.Errorf("close new %s database: %w", label, closeErr)
		}
	case err != nil:
		return fmt.Errorf("stat %s database: %w", label, err)
	}

	db, _, err := openExistingBoltDBFile(file, false, true)
	if err != nil {
		return fmt.Errorf("open %s database for identity binding: %w", label, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			if !allowCreate {
				return fmt.Errorf("v0.13 %s bucket %q is missing", label, bucketName)
			}
			if _, createErr := tx.CreateBucket(bucketName); createErr != nil {
				return fmt.Errorf("create %s bucket: %w", label, createErr)
			}
		}
		if validate != nil {
			if validateErr := validate(tx); validateErr != nil {
				return fmt.Errorf("validate %s database before identity binding: %w", label, validateErr)
			}
		}
		binding := tx.Bucket(storeIdentityBucketName)
		if binding != nil {
			return verifyStoreIdentityBinding(tx, kind, storageID)
		}
		binding, err = tx.CreateBucket(storeIdentityBucketName)
		if err != nil {
			return fmt.Errorf("create %s identity bucket: %w", label, err)
		}
		for key, value := range map[string][]byte{
			string(storeIdentitySchemaKey):    storeIdentitySchemaVersion,
			string(storeIdentityStorageIDKey): []byte(storageID.String()),
			string(storeIdentityStoreKindKey): []byte(kind),
		} {
			if putErr := binding.Put([]byte(key), value); putErr != nil {
				return fmt.Errorf("write %s identity field %q: %w", label, key, putErr)
			}
		}
		return nil
	})
	closeErr := db.Close()
	if err != nil {
		return fmt.Errorf("bind %s database identity: %w", label, err)
	}
	if closeErr != nil {
		return fmt.Errorf("close bound %s database: %w", label, closeErr)
	}
	if err := file.SyncParent(); err != nil {
		return fmt.Errorf("sync %s database directory after identity binding: %w", label, err)
	}
	return nil
}

// verifyIdentityBoundBoltStore opens no cleanup goroutine and performs no
// mutation. It is suitable for the committed marker-pair Verify hook.
func verifyIdentityBoundBoltStore(
	dbPath string,
	bucketName []byte,
	label string,
	kind authoritativeStoreKind,
	storage backendidentity.VerifiedStorage,
	validate func(*bolt.Tx) error,
) error {
	if !storage.Valid() {
		return fmt.Errorf("verify %s db: verified backend storage authority is required", label)
	}
	return checkIdentityBoundBoltStore(dbPath, bucketName, label, kind, storage.ID(), validate)
}

func checkIdentityBoundBoltStore(
	dbPath string,
	bucketName []byte,
	label string,
	kind authoritativeStoreKind,
	storageID backendidentity.ID,
	validate func(*bolt.Tx) error,
) error {
	return checkIdentityBoundBoltStoreFile(
		pathnameAuthoritativeStoreFile(dbPath), bucketName, label, kind, storageID, validate,
	)
}

func checkIdentityBoundBoltStoreBound(
	path *BoundAuthoritativeStorePath,
	bucketName []byte,
	label string,
	kind authoritativeStoreKind,
	storageID backendidentity.ID,
	validate func(*bolt.Tx) error,
) error {
	file, err := boundAuthoritativeStoreFile(path)
	if err != nil {
		return fmt.Errorf("check %s db: %w", label, err)
	}
	return checkIdentityBoundBoltStoreFile(file, bucketName, label, kind, storageID, validate)
}

func checkIdentityBoundBoltStoreFile(
	file authoritativeStoreFile,
	bucketName []byte,
	label string,
	kind authoritativeStoreKind,
	storageID backendidentity.ID,
	validate func(*bolt.Tx) error,
) error {
	if !storageID.Valid() {
		return fmt.Errorf("check %s db: valid backend storage ID is required", label)
	}
	db, _, err := openExistingBoltDBFile(file, true, false)
	if err != nil {
		return fmt.Errorf("open existing %s db read-only: %w", label, err)
	}
	defer func() { _ = db.Close() }()
	return db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return fmt.Errorf("%s bucket %q is missing", label, bucketName)
		}
		if err := verifyStoreIdentityBinding(tx, kind, storageID); err != nil {
			return err
		}
		if validate != nil {
			return validate(tx)
		}
		return nil
	})
}

func allowAuthoritativeStoreCreation(profile backendidentity.InitializationProfile) (bool, error) {
	switch profile {
	case backendidentity.InitializationProfileFresh:
		return true, nil
	case backendidentity.InitializationProfileExisting:
		return false, nil
	default:
		return false, fmt.Errorf("invalid backend storage initialization profile %q", profile)
	}
}

func verifyStoreIdentityBinding(
	tx *bolt.Tx,
	kind authoritativeStoreKind,
	storageID backendidentity.ID,
) error {
	binding := tx.Bucket(storeIdentityBucketName)
	if binding == nil {
		return ErrStoreIdentityUnbound
	}
	expected := map[string][]byte{
		string(storeIdentitySchemaKey):    storeIdentitySchemaVersion,
		string(storeIdentityStorageIDKey): []byte(storageID.String()),
		string(storeIdentityStoreKindKey): []byte(kind),
	}
	seen := make(map[string]struct{}, len(expected))
	if err := binding.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("%w: identity metadata contains a nested bucket (key_length=%d)", ErrStoreIdentityMismatch, len(key))
		}
		want, ok := expected[string(key)]
		if !ok {
			return fmt.Errorf("%w: identity metadata contains an unknown field (key_length=%d value_length=%d)",
				ErrStoreIdentityMismatch, len(key), len(value))
		}
		if !bytes.Equal(value, want) {
			return fmt.Errorf("%w: identity field %s differs (value_length=%d expected_length=%d)",
				ErrStoreIdentityMismatch, string(key), len(value), len(want))
		}
		seen[string(key)] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	for key := range expected {
		if _, ok := seen[key]; !ok {
			return fmt.Errorf("%w: identity field %q is missing", ErrStoreIdentityMismatch, key)
		}
	}
	return nil
}

func openExistingBoltDB(dbPath string, readOnly, allowEmpty bool) (*bolt.DB, os.FileInfo, error) {
	return openExistingBoltDBFile(pathnameAuthoritativeStoreFile(dbPath), readOnly, allowEmpty)
}

func openExistingBoltDBFile(
	file authoritativeStoreFile,
	readOnly, allowEmpty bool,
) (*bolt.DB, os.FileInfo, error) {
	info, err := file.Lstat()
	if err != nil {
		return nil, nil, err
	}
	return openBoltDBFileWithExpectedIdentity(file, info, readOnly, allowEmpty)
}

func openBoltDBFileWithExpectedIdentity(
	file authoritativeStoreFile,
	expected os.FileInfo,
	readOnly, allowEmpty bool,
) (*bolt.DB, os.FileInfo, error) {
	if file == nil || expected == nil {
		return nil, nil, errors.New("database file capability and expected identity are required")
	}
	displayPath := file.DisplayPath()
	if err := verifyStoreFileIdentity(file, expected); err != nil {
		return nil, nil, err
	}
	if expected.Size() == 0 && !allowEmpty {
		return nil, nil, errors.New("authoritative database is empty")
	}
	db, err := bolt.Open(displayPath, 0o600, &bolt.Options{
		ReadOnly: readOnly,
		Timeout:  5 * time.Second,
		OpenFile: func(name string, flag int, mode os.FileMode) (*os.File, error) {
			if name != displayPath {
				return nil, errors.New("bbolt requested an unexpected authoritative database path")
			}
			opened, openErr := file.OpenFile(flag&^os.O_CREATE, mode)
			if openErr != nil {
				return nil, openErr
			}
			openedInfo, statErr := opened.Stat()
			if statErr != nil {
				_ = opened.Close()
				return nil, fmt.Errorf("stat opened database: %w", statErr)
			}
			if validationErr := validateAuthoritativeStoreFile(displayPath, openedInfo); validationErr != nil {
				_ = opened.Close()
				return nil, validationErr
			}
			if !os.SameFile(expected, openedInfo) {
				_ = opened.Close()
				return nil, errors.New("database changed between validation and open")
			}
			return opened, nil
		},
	})
	if err != nil {
		return nil, nil, err
	}
	return db, expected, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path) // #nosec G304 -- parent of operator-configured database path
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func validateAuthoritativeStoreFile(path string, info os.FileInfo) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("authoritative database is not a regular file: %s", path)
	}
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("authoritative database %q must have exact mode 0600", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return fmt.Errorf("authoritative database %q must have exactly one hard link", path)
	}
	return nil
}

func newBoltStore(db *bolt.DB, cfg boltStoreConfig) *boltStore {
	ctx, cancel := context.WithCancel(context.Background())
	return &boltStore{
		db:        db,
		bucket:    cfg.BucketName,
		maxAge:    cfg.MaxAge,
		ctx:       ctx,
		cancel:    cancel,
		wg:        &sync.WaitGroup{},
		closeOnce: &sync.Once{},
	}
}

// VerifyStorageIdentity proves that an authoritative store's configured path
// still names the exact regular file opened by the constructor and that the
// durable binding remains intact. This catches delete/rename/replacement while
// the process still holds a writable descriptor to the old inode.
func (s *boltStore) VerifyStorageIdentity(storage backendidentity.VerifiedStorage) error {
	if s == nil || s.binding == nil {
		return errors.New("store has no authoritative storage binding")
	}
	if err := s.backendStorageAuthorityError(); err != nil {
		return err
	}
	if !storage.Valid() || storage.ID() != s.binding.storageID {
		return s.latchIdentityFailure(fmt.Errorf(
			"%w: %w: runtime storage authority differs from opened store",
			backendidentity.ErrIdentityDrift, ErrStoreIdentityMismatch,
		))
	}
	if err := s.verifyOpenedBinding(); err != nil {
		return s.latchIdentityFailure(err)
	}
	// Error is an atomic publication check. Keeping this final load after every
	// pathname/database read gives the verification a clear linearization point:
	// a withdrawal published during any I/O rejects the result, while a later
	// withdrawal orders after this completed verification.
	return s.backendStorageAuthorityError()
}

func (s *boltStore) verifyOpenedBinding() error {
	if err := s.verifyOpenedPath(); err != nil {
		return err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		return s.verifyBindingTransaction(tx)
	})
	if err != nil {
		return fmt.Errorf("%w: authoritative store binding changed: %w", backendidentity.ErrIdentityDrift, err)
	}
	return s.verifyOpenedPath()
}

func (s *boltStore) backendStorageAuthorityError() error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.backendAuthorityGate == nil {
		return nil
	}
	return s.backendAuthorityGate.Error()
}

func (s *boltStore) latchIdentityFailure(err error) error {
	if err == nil || (!errors.Is(err, backendidentity.ErrIdentityDrift) &&
		!errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous)) {
		return err
	}
	if s.backendAuthorityGate == nil {
		return errors.Join(errors.New("authoritative store has no backend storage authority gate"), err)
	}
	return s.backendAuthorityGate.Latch(err)
}

func (s *boltStore) verifyOpenedPath() error {
	current, err := os.Lstat(s.binding.dbPath)
	if err != nil {
		return fmt.Errorf("%w: stat authoritative store path: %w", backendidentity.ErrIdentityDrift, err)
	}
	if err := validateAuthoritativeStoreFile(s.binding.dbPath, current); err != nil {
		return fmt.Errorf("%w: %w", backendidentity.ErrIdentityDrift, err)
	}
	if !os.SameFile(s.binding.fileInfo, current) {
		return fmt.Errorf("%w: %w: authoritative store path no longer names the opened regular file",
			backendidentity.ErrIdentityDrift, ErrStoreIdentityMismatch)
	}
	return nil
}

func (s *boltStore) verifyBindingTransaction(tx *bolt.Tx) error {
	if tx.Bucket(s.bucket) == nil {
		return fmt.Errorf("authoritative store bucket %q is missing", s.bucket)
	}
	return verifyStoreIdentityBinding(tx, s.binding.kind, s.binding.storageID)
}

// update is the only write path for stores built on boltStore. Identity-bound
// stores re-attest their configured pathname/inode and durable binding before
// every transaction; diagnostics and explicitly unbound test stores retain the
// ordinary bbolt behavior.
func (s *boltStore) update(fn func(*bolt.Tx) error) error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.binding == nil {
		return s.db.Update(fn)
	}
	gate := s.backendAuthorityGate
	if err := gate.Error(); err != nil {
		return err
	}
	if err := s.verifyOpenedPath(); err != nil {
		return s.latchIdentityFailure(err)
	}
	tx, err := s.db.Begin(true)
	if err != nil {
		// Begin failed before a write transaction existed, so no mutation could
		// have committed and the caller may retry the ordinary store error.
		return err
	}
	commitBoundary := func(commit func() error) (bool, error) {
		commitAndVerify := func() error {
			if err := commit(); err != nil {
				return fmt.Errorf(
					"%w: authoritative store bbolt commit failed: %w",
					backendidentity.ErrMutationOutcomeAmbiguous,
					err,
				)
			}
			if err := s.verifyOpenedPath(); err != nil {
				return fmt.Errorf(
					"%w: authoritative store transaction committed before pathname re-attestation failed: %w",
					backendidentity.ErrMutationOutcomeAmbiguous,
					err,
				)
			}
			return nil
		}
		attempted := false
		err := gate.Run(func() error {
			attempted = true
			return commitAndVerify()
		})
		return attempted, err
	}
	err = finishAuthoritativeStoreWriteTransactionAtBoundary(
		tx,
		func() error {
			if err := s.verifyBindingTransaction(tx); err != nil {
				return fmt.Errorf("%w: authoritative store binding changed: %w", backendidentity.ErrIdentityDrift, err)
			}
			return fn(tx)
		},
		commitBoundary,
	)
	// Commit errors are already published by gate.Run while its boundary is
	// locked. Latch is idempotent here and handles pre-commit identity failures.
	return s.latchIdentityFailure(err)
}

// view is the read-side counterpart to update. Bound stores reject values
// read through an unlinked/replaced inode and verify their binding in the same
// transaction as the caller's read.
func (s *boltStore) view(fn func(*bolt.Tx) error) error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.binding == nil {
		return s.db.View(fn)
	}
	if err := s.backendStorageAuthorityError(); err != nil {
		return err
	}
	if err := s.verifyOpenedPath(); err != nil {
		return s.latchIdentityFailure(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if err := s.backendStorageAuthorityError(); err != nil {
			return err
		}
		if err := s.verifyBindingTransaction(tx); err != nil {
			return fmt.Errorf("%w: authoritative store binding changed: %w", backendidentity.ErrIdentityDrift, err)
		}
		if err := fn(tx); err != nil {
			return err
		}
		return s.backendStorageAuthorityError()
	}); err != nil {
		return s.latchIdentityFailure(err)
	}
	if err := s.verifyOpenedPath(); err != nil {
		return s.latchIdentityFailure(err)
	}
	// Error is deliberately lock-free: this post-I/O load preserves the read
	// boundary's authority check without acquiring the gate underneath bbolt's
	// mmap read lock. A withdrawal observed here rejects the snapshot; a latch
	// published afterward linearizes after the completed view.
	return s.backendStorageAuthorityError()
}

// startCleanup runs an initial cleanup and starts a background loop.
// removeExpired is the store-specific function that deletes old entries.
// onPanic (may be nil) is invoked if a cleanup iteration panics; callers
// typically inject a metrics-increment closure here. Kept as a parameter
// rather than a package import so internal/backend/shared stays free of
// the internal/metrics dependency.
func (s *boltStore) startCleanup(label string, cleanupInterval time.Duration, removeExpired func(time.Duration) (int, error), onPanic util.PanicHandler) {
	// Initial cleanup to clear stale entries from a previous run.
	if removed, err := removeExpired(s.maxAge); err != nil {
		slog.Warn("initial "+label+" cleanup failed", "error", err)
	} else if removed > 0 {
		slog.Info("removed expired "+label+" on startup", "count", removed, "max_age", s.maxAge)
	}

	interval := cleanupInterval
	if interval <= 0 {
		interval = s.maxAge
	}
	s.wg.Go(func() {
		util.StartCleanupLoop(s.ctx, interval, func() error {
			removed, err := removeExpired(s.maxAge)
			if err != nil {
				return err
			}
			if removed > 0 {
				slog.Debug("cleaned up expired "+label, "count", removed)
			}
			return nil
		}, label, onPanic)
	})
}

// removeOlderThan is a generic cleanup helper for bbolt stores that store
// JSON-encoded entries with a timestamp field. It iterates all entries in the
// bucket, unmarshals each as T, extracts the timestamp using getTime, and
// deletes entries older than maxAge. Malformed entries are also removed.
func removeOlderThan[T any](db *bolt.DB, bucket []byte, maxAge time.Duration, getTime func(*T) time.Time) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket %q not found", string(bucket))
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry T
			if err := json.Unmarshal(v, &entry); err != nil {
				slog.Warn("removing malformed entry",
					"bucket", string(bucket), "key", string(k), "error", err)
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
				continue
			}
			if getTime(&entry).Before(cutoff) {
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
			}
		}
		return nil
	})

	return removed, err
}

// Healthy checks that the bbolt database is accessible and the bucket exists.
func (s *boltStore) Healthy() error {
	return s.view(func(tx *bolt.Tx) error {
		if tx.Bucket(s.bucket) == nil {
			return errors.New("bucket missing")
		}
		return nil
	})
}

// Close shuts down the store gracefully. It is idempotent: the first call
// closes the database and captures any error; subsequent calls return the same error.
func (s *boltStore) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
