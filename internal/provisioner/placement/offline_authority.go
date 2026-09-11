package placement

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// offlinePlacementAuthority retains both the exact directory that contained a
// stopped placement file and the exact regular inode opened by an offline
// command.  A display path is supplied only because bbolt requires one; every
// actual open is descriptor-relative through entry.
//
// The capability is intentionally package-private.  Offline operations must
// not be able to downgrade a checked path back into an arbitrary pathname.
type offlinePlacementAuthority struct {
	directory *fsidentity.Directory
	entry     fsidentity.Entry
	info      os.FileInfo
	path      string
}

// bindOfflinePlacementAuthority rejects a final symlink, a non-regular file,
// non-private permissions, and multiply linked authority before bbolt sees
// the file.  Parent symlinks are resolved once, then all subsequent I/O uses
// the retained physical parent rather than resolving the operator path again.
func bindOfflinePlacementAuthority(path string) (*offlinePlacementAuthority, error) {
	if err := requireAbsoluteCleanPlacementDBPath(path); err != nil {
		return nil, err
	}
	canonicalPath, err := canonicalNewPlacementPath(path)
	if err != nil {
		return nil, fmt.Errorf("resolve placement db parent: %w", err)
	}
	directory, err := fsidentity.OpenDirectory(filepath.Dir(canonicalPath))
	if err != nil {
		return nil, fmt.Errorf("open placement db parent without following links: %w", err)
	}
	closeOnError := func(cause error) (*offlinePlacementAuthority, error) {
		if closeErr := directory.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close placement db parent: %w", closeErr))
		}
		return nil, cause
	}
	entry, err := directory.Entry(filepath.Base(canonicalPath))
	if err != nil {
		return closeOnError(fmt.Errorf("bind placement db entry: %w", err))
	}
	info, err := entry.Lstat()
	if err != nil {
		return closeOnError(fmt.Errorf("stat placement db without following links: %w", err))
	}
	if err := validatePlacementAuthorityFile(canonicalPath, info); err != nil {
		return closeOnError(err)
	}
	if err := directory.VerifyPath(); err != nil {
		return closeOnError(fmt.Errorf("verify placement db parent: %w", err))
	}
	return &offlinePlacementAuthority{
		directory: directory,
		entry:     entry,
		info:      info,
		path:      entry.DisplayPath(),
	}, nil
}

func (authority *offlinePlacementAuthority) valid() bool {
	return authority != nil && authority.directory != nil && authority.entry.Valid() &&
		authority.info != nil && authority.path != ""
}

// verify proves the still-open bbolt inode remains the exact, private,
// singly-linked file published at the original bound directory entry.
func (authority *offlinePlacementAuthority) verify() error {
	if !authority.valid() {
		return errors.New("offline placement authority is unavailable")
	}
	if err := authority.directory.VerifyPath(); err != nil {
		return fmt.Errorf("placement db parent changed: %w", err)
	}
	info, err := authority.entry.Lstat()
	if err != nil {
		return fmt.Errorf("stat bound placement db: %w", err)
	}
	if err := validatePlacementAuthorityFile(authority.path, info); err != nil {
		return err
	}
	if !os.SameFile(authority.info, info) {
		return errors.New("placement authority path or inode changed")
	}
	return nil
}

func (authority *offlinePlacementAuthority) close() error {
	if authority == nil || authority.directory == nil {
		return nil
	}
	return authority.directory.Close()
}

// openBolt forces bbolt through the retained entry.  O_CREATE is never
// permitted because the offline tools operate only on an existing authority.
func (authority *offlinePlacementAuthority) openBolt(
	readOnly bool,
) (*bolt.DB, error) {
	if err := authority.verify(); err != nil {
		return nil, err
	}
	db, err := bolt.Open(authority.path, 0o600, &bolt.Options{
		ReadOnly: readOnly,
		Timeout:  time.Second,
		OpenFile: func(requested string, flag int, mode os.FileMode) (*os.File, error) {
			if filepath.Clean(requested) != authority.path {
				return nil, errors.New("bbolt requested an unexpected placement authority path")
			}
			file, openErr := authority.entry.OpenFile(flag&^os.O_CREATE, mode)
			if openErr != nil {
				return nil, openErr
			}
			openedInfo, statErr := file.Stat()
			if statErr != nil || !os.SameFile(authority.info, openedInfo) {
				_ = file.Close()
				if statErr != nil {
					return nil, fmt.Errorf("stat opened placement db: %w", statErr)
				}
				return nil, errors.New("placement db changed between validation and open")
			}
			if modeErr := validatePlacementAuthorityFile(authority.path, openedInfo); modeErr != nil {
				_ = file.Close()
				return nil, modeErr
			}
			return file, nil
		},
	})
	if err != nil {
		return nil, err
	}
	if err := authority.verify(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
