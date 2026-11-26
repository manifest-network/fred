package placement

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// ExactBackupTarget is an opaque capability for one absent entry in one exact
// physical directory. Binding the parent before remote evidence collection
// prevents an unmount, rename, or same-path directory replacement from
// redirecting the mandatory rollback image.
//
// The zero value is invalid. The caller owns the capability and must Close it
// after the complete proof, backup, and mutation sequence has finished.
type ExactBackupTarget struct {
	directory *fsidentity.Directory
	entry     fsidentity.Entry
	path      string

	mu          sync.RWMutex
	publication *exactBackupPublication
}

type exactBackupPublication struct {
	info   os.FileInfo
	size   int64
	digest [sha256.Size]byte
}

// BindExactBackupTarget canonicalizes path, retains its exact physical parent,
// and proves the destination entry is absent without following a final
// symlink. All publication later occurs relative to the retained descriptor.
func BindExactBackupTarget(path string) (*ExactBackupTarget, error) {
	canonicalPath, err := canonicalNewPlacementPath(path)
	if err != nil {
		return nil, fmt.Errorf("canonicalize exact backup target: %w", err)
	}
	directory, err := fsidentity.OpenDirectory(filepath.Dir(canonicalPath))
	if err != nil {
		return nil, fmt.Errorf("bind exact backup parent directory: %w", err)
	}
	closeOnError := func(cause error) (*ExactBackupTarget, error) {
		if closeErr := directory.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close exact backup parent directory: %w", closeErr))
		}
		return nil, cause
	}
	entry, err := directory.Entry(filepath.Base(canonicalPath))
	if err != nil {
		return closeOnError(fmt.Errorf("bind exact backup entry: %w", err))
	}
	exists, err := entry.Exists()
	if err != nil {
		return closeOnError(fmt.Errorf("inspect exact backup destination: %w", err))
	}
	if exists {
		return closeOnError(errors.New("backup destination already exists"))
	}
	if err := directory.VerifyPath(); err != nil {
		return closeOnError(fmt.Errorf("verify exact backup parent directory: %w", err))
	}
	return &ExactBackupTarget{
		directory: directory,
		entry:     entry,
		path:      canonicalPath,
	}, nil
}

// Path returns the canonical diagnostic pathname. Authority-bearing I/O uses
// the retained entry instead.
func (target *ExactBackupTarget) Path() string {
	if target == nil {
		return ""
	}
	return target.path
}

// ParentIdentity returns the physical parent identity bound by this target.
// It is included in causal-capability digests so another directory capability
// at the same pathname cannot be substituted.
func (target *ExactBackupTarget) ParentIdentity() fsidentity.Identity {
	if target == nil || target.directory == nil {
		return fsidentity.Identity{}
	}
	return target.directory.Identity()
}

// Verify proves the acquisition pathname still names the retained parent.
func (target *ExactBackupTarget) Verify() error {
	if target == nil || target.directory == nil || !target.entry.Valid() || target.path == "" {
		return errors.New("exact backup target is absent or closed")
	}
	if err := target.directory.VerifyPath(); err != nil {
		return fmt.Errorf("exact backup parent directory changed: %w", err)
	}
	return nil
}

// VerifyPublished proves both that the parent pathname remains bound and that
// the destination entry still names the exact no-overwrite inode published by
// writeExactBackup.
func (target *ExactBackupTarget) VerifyPublished() error {
	if err := target.Verify(); err != nil {
		return err
	}
	publication, err := target.publishedSnapshot()
	if err != nil {
		return err
	}
	if err := verifyExactBackupEntry(target.entry, publication); err != nil {
		return err
	}
	file, err := target.entry.OpenFile(os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open published exact backup: %w", err)
	}
	closeOnError := func(cause error) error {
		if closeErr := file.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close published exact backup: %w", closeErr))
		}
		return cause
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return closeOnError(fmt.Errorf("stat opened exact backup: %w", err))
	}
	if err := validateExactBackupFileInfo(openedInfo, publication); err != nil {
		return closeOnError(err)
	}
	hash := sha256.New()
	copied, err := io.CopyN(hash, file, publication.size)
	if err != nil {
		return closeOnError(fmt.Errorf(
			"hash published exact backup: read %d of %d bytes: %w",
			copied,
			publication.size,
			err,
		))
	}
	var extra [1]byte
	extraBytes, readErr := file.Read(extra[:])
	if extraBytes != 0 || !errors.Is(readErr, io.EOF) {
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return closeOnError(fmt.Errorf("check exact backup length: %w", readErr))
		}
		return closeOnError(errors.New("published exact backup grew after its verified snapshot"))
	}
	var actualDigest [sha256.Size]byte
	copy(actualDigest[:], hash.Sum(nil))
	if actualDigest != publication.digest {
		return closeOnError(errors.New("published exact backup bytes changed after verification"))
	}
	afterReadInfo, err := file.Stat()
	if err != nil {
		return closeOnError(fmt.Errorf("restat opened exact backup: %w", err))
	}
	if err := validateExactBackupFileInfo(afterReadInfo, publication); err != nil {
		return closeOnError(fmt.Errorf("revalidate read exact backup: %w", err))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close published exact backup: %w", err)
	}
	if err := verifyExactBackupEntry(target.entry, publication); err != nil {
		return err
	}
	return target.Verify()
}

// Close revokes the retained parent-directory capability. It is idempotent.
func (target *ExactBackupTarget) Close() error {
	if target == nil || target.directory == nil {
		return nil
	}
	return target.directory.Close()
}

func (target *ExactBackupTarget) valid() bool {
	return target != nil && target.directory != nil && target.entry.Valid() &&
		target.path != "" && target.ParentIdentity().Valid()
}

func (target *ExactBackupTarget) createTemporary() (string, *os.File, error) {
	if !target.valid() {
		return "", nil, errors.New("exact backup target is invalid")
	}
	return target.directory.CreateTemp(".fred-placement-backup-", 0o600)
}

func (target *ExactBackupTarget) markPublished(
	info os.FileInfo,
	size int64,
	digest [sha256.Size]byte,
) error {
	publication := exactBackupPublication{info: info, size: size, digest: digest}
	if !target.valid() {
		return errors.New("valid published exact-backup target is required")
	}
	if err := validateExactBackupFileInfo(info, publication); err != nil {
		return err
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.publication != nil {
		if !os.SameFile(target.publication.info, info) ||
			target.publication.size != size || target.publication.digest != digest {
			return errors.New("exact backup target was already bound to another published image")
		}
		return nil
	}
	target.publication = &publication
	return nil
}

func (target *ExactBackupTarget) publishedSnapshot() (exactBackupPublication, error) {
	if !target.valid() {
		return exactBackupPublication{}, errors.New("exact backup target is invalid")
	}
	target.mu.RLock()
	defer target.mu.RUnlock()
	if target.publication == nil {
		return exactBackupPublication{}, errors.New("exact backup has not been durably published")
	}
	return *target.publication, nil
}

func verifyExactBackupEntry(
	entry fsidentity.Entry,
	publication exactBackupPublication,
) error {
	if !entry.Valid() {
		return errors.New("published exact backup entry is invalid")
	}
	info, err := entry.Lstat()
	if err != nil {
		return fmt.Errorf("stat published exact backup: %w", err)
	}
	return validateExactBackupFileInfo(info, publication)
}

func validateExactBackupFileInfo(
	info os.FileInfo,
	publication exactBackupPublication,
) error {
	if info == nil || publication.info == nil || publication.size < 0 {
		return errors.New("published exact backup identity is invalid")
	}
	if info.Mode() != 0o600 || !os.SameFile(publication.info, info) ||
		info.Size() != publication.size {
		return errors.New("published exact backup inode, size, or mode changed after verification")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("published exact backup must have exactly one directory entry")
	}
	return nil
}

// ErrExactBackupPublished means the atomic no-overwrite publication succeeded before a
// later verification, durability, close, or cleanup step failed. The target is
// an operator artifact and must be preserved; a retry requires a new path.
var ErrExactBackupPublished = errors.New("exact placement backup published")

func writeExactBackup(
	db *bolt.DB,
	sourceInfo os.FileInfo,
	target *ExactBackupTarget,
) error {
	if db == nil || sourceInfo == nil {
		return errors.New("placement db and source identity are required")
	}
	authority, err := bindOfflinePlacementAuthority(db.Path())
	if err != nil {
		return fmt.Errorf("bind placement db backup source: %w", err)
	}
	defer authority.close() //nolint:errcheck // the database close reports command durability
	if !os.SameFile(sourceInfo, authority.info) {
		return errors.New("placement db changed before exact backup")
	}
	return writeBoundExactBackup(db, authority, target)
}

// writeBoundExactBackup copies the already-open offline authority through its
// retained parent entry.  This keeps the rollback image bound to the inode
// locked by bbolt even if an attacker swaps the configured pathname.
func writeBoundExactBackup(
	db *bolt.DB,
	authority *offlinePlacementAuthority,
	target *ExactBackupTarget,
) (resultErr error) {
	published := false
	defer func() {
		resultErr = classifyExactBackupResult(published, resultErr)
	}()
	if db == nil {
		return errors.New("placement db is required")
	}
	if !authority.valid() {
		return errors.New("bound placement db source authority is required")
	}
	sourceInfo := authority.info
	if err := authority.verify(); err != nil {
		return fmt.Errorf("verify placement db backup source: %w", err)
	}
	if !target.valid() {
		return errors.New("bound exact backup target is required")
	}
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		return fmt.Errorf("validate placement db before exact backup: %w", err)
	}
	if err := target.Verify(); err != nil {
		return fmt.Errorf("verify bound exact backup target: %w", err)
	}
	if exists, err := target.entry.Exists(); err != nil {
		return fmt.Errorf("inspect bound backup destination: %w", err)
	} else if exists {
		return errors.New("backup destination already exists")
	}
	tempName, temp, err := target.createTemporary()
	if err != nil {
		return fmt.Errorf("create backup temporary file: %w", err)
	}
	tempOpen := true
	defer func() {
		if tempOpen {
			if err := temp.Close(); resultErr == nil && err != nil {
				resultErr = fmt.Errorf("close backup temporary file: %w", err)
			}
		}
		if err := target.directory.Remove(tempName); resultErr == nil && err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = fmt.Errorf("remove backup temporary file: %w", err)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("set backup temporary file permissions: %w", err)
	}
	// The caller holds bbolt's exclusive stopped-database lock and has not yet
	// opened a write transaction. Copy the validated source inode byte-for-byte,
	// rather than Tx.WriteTo's logically equivalent snapshot: WriteTo rewrites
	// the two meta pages and therefore is not an exact old-binary rollback image.
	// Recheck the inode captured before bolt.Open so a pathname replacement can
	// never redirect the backup to different authority.
	source, err := authority.entry.OpenFile(os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open placement db for exact backup: %w", err)
	}
	defer func() {
		if err := source.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("close placement db backup source: %w", err)
		}
	}()
	openedSourceInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("stat placement db backup source: %w", err)
	}
	if !os.SameFile(sourceInfo, openedSourceInfo) {
		return errors.New("placement db changed between validation and exact backup")
	}
	snapshotHash := sha256.New()
	snapshotBytes, err := io.Copy(io.MultiWriter(temp, snapshotHash), source)
	if err != nil {
		return fmt.Errorf("copy exact placement database bytes: %w", err)
	}
	if snapshotBytes != openedSourceInfo.Size() {
		return fmt.Errorf(
			"copy exact placement database bytes: copied %d bytes, expected %d",
			snapshotBytes,
			openedSourceInfo.Size(),
		)
	}
	afterCopyInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("restat placement db backup source: %w", err)
	}
	if !os.SameFile(sourceInfo, afterCopyInfo) ||
		afterCopyInfo.Size() != openedSourceInfo.Size() ||
		!afterCopyInfo.ModTime().Equal(openedSourceInfo.ModTime()) {
		return errors.New("placement db changed while exact backup was copied")
	}
	if err := authority.verify(); err != nil {
		return fmt.Errorf("reverify placement db backup source: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync backup temporary file: %w", err)
	}
	closeErr := temp.Close()
	tempOpen = false
	if closeErr != nil {
		return fmt.Errorf("close backup temporary file: %w", closeErr)
	}
	temporaryInfo, err := target.directory.Lstat(tempName)
	if err != nil {
		return fmt.Errorf("stat verified backup temporary file: %w", err)
	}
	if err := target.directory.RenameNoReplace(tempName, target.entry.Name()); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("backup destination already exists")
		}
		return fmt.Errorf("publish placement backup: %w", err)
	}
	published = true
	if err := verifyPublishedEntryIdentity(target.entry, temporaryInfo); err != nil {
		return fmt.Errorf("verify exact backup publication identity: %w", err)
	}
	backup, err := target.entry.OpenFile(os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open published placement backup: %w", err)
	}
	backupInfo, err := backup.Stat()
	if err != nil {
		_ = backup.Close()
		return fmt.Errorf("stat opened placement backup: %w", err)
	}
	if !os.SameFile(temporaryInfo, backupInfo) {
		_ = backup.Close()
		return errors.New("opened placement backup is not the verified source inode")
	}
	publishedHash := sha256.New()
	publishedBytes, err := io.Copy(publishedHash, backup)
	if err != nil {
		_ = backup.Close()
		return fmt.Errorf("verify published placement backup bytes: %w", err)
	}
	snapshotDigest := snapshotHash.Sum(nil)
	if publishedBytes != snapshotBytes ||
		!bytes.Equal(publishedHash.Sum(nil), snapshotDigest) {
		_ = backup.Close()
		return errors.New("published placement backup bytes differ from validated snapshot")
	}
	if err := backup.Sync(); err != nil {
		_ = backup.Close()
		return fmt.Errorf("sync published placement backup: %w", err)
	}
	if err := backup.Close(); err != nil {
		return fmt.Errorf("close published placement backup: %w", err)
	}
	if err := target.entry.SyncParent(); err != nil {
		return fmt.Errorf("sync backup directory: %w", err)
	}
	if err := verifyBoltEntryPhysicalConsistency(target.entry, temporaryInfo); err != nil {
		return fmt.Errorf("validate published placement backup physical consistency: %w", err)
	}
	if err := verifyPublishedEntryIdentity(target.entry, temporaryInfo); err != nil {
		return fmt.Errorf("reverify exact backup publication identity: %w", err)
	}
	if err := target.Verify(); err != nil {
		return fmt.Errorf("reverify bound exact backup parent after publication: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], snapshotDigest)
	if err := target.markPublished(temporaryInfo, snapshotBytes, digest); err != nil {
		return fmt.Errorf("bind published exact backup inode: %w", err)
	}
	return nil
}

func classifyExactBackupResult(published bool, resultErr error) error {
	if published && resultErr != nil && !errors.Is(resultErr, ErrExactBackupPublished) {
		return fmt.Errorf("%w: %w", ErrExactBackupPublished, resultErr)
	}
	return resultErr
}
