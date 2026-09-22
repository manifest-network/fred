package placement

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

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

func (target *ExactBackupTarget) publishedIdentity() (os.FileInfo, error) {
	publication, err := target.publishedSnapshot()
	if err != nil {
		return nil, err
	}
	return publication.info, nil
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
