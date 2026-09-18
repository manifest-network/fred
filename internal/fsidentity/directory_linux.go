//go:build linux

package fsidentity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	// ErrDirectoryIdentityChanged means a pathname no longer names the retained
	// physical directory supplied by the caller.
	ErrDirectoryIdentityChanged = errors.New("directory identity changed")
	// ErrDirectoryClosed means an operation was attempted after capability
	// revocation through Close.
	ErrDirectoryClosed = errors.New("directory capability is closed")
	// ErrInvalidEntryName means a dirfd-relative operation was given a path
	// rather than one simple directory entry.
	ErrInvalidEntryName = errors.New("invalid directory entry name")
)

// Identity is the stable Linux device/inode pair for one physical directory.
// Its zero value is invalid.
type Identity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

// Valid reports whether identity can name a physical directory.
func (identity Identity) Valid() bool {
	return identity.Inode != 0
}

// Equal reports whether both valid values name the same physical directory.
func (identity Identity) Equal(other Identity) bool {
	return identity.Valid() && other.Valid() &&
		identity.Device == other.Device && identity.Inode == other.Inode
}

// Directory is a retained capability for one exact physical directory.
// Operations accept only a single entry name and are implemented with
// descriptor-relative syscalls plus final-component no-follow semantics. Its
// zero value is invalid.
type Directory struct {
	path     string
	identity Identity

	mu   sync.RWMutex
	file *os.File
}

// Entry is an opaque, validated name bound to one retained Directory. Its zero
// value is invalid. It is useful at APIs such as bbolt's custom opener boundary
// where carrying a raw pathname would discard the parent capability.
type Entry struct {
	directory *Directory
	name      string
}

// InspectDirectory returns the physical identity at path without retaining a
// capability. The final path component must be a directory, not a symlink.
func InspectDirectory(path string) (Identity, error) {
	directory, err := OpenDirectory(path)
	if err != nil {
		return Identity{}, err
	}
	identity := directory.Identity()
	if err := directory.Close(); err != nil {
		return Identity{}, fmt.Errorf("close inspected directory: %w", err)
	}
	return identity, nil
}

// OpenDirectory opens and retains the exact physical directory at path. The
// final path component is not followed if it is a symlink.
func OpenDirectory(path string) (*Directory, error) {
	return OpenBoundDirectory(path, Identity{})
}

// OpenBoundDirectory opens path and requires it to name expected when expected
// is valid. This is the cross-process boundary used after an operator confirms
// a previously inspected physical identity.
func OpenBoundDirectory(path string, expected Identity) (*Directory, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("directory path must be non-empty, absolute, and clean")
	}
	if !expected.Valid() && expected != (Identity{}) {
		return nil, errors.New("expected directory identity is malformed")
	}
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open directory without following links: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("construct retained directory handle")
	}
	identity, err := identityFromFD(fd)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat retained directory handle: %w", err)
	}
	if expected.Valid() && !identity.Equal(expected) {
		_ = file.Close()
		return nil, fmt.Errorf(
			"%w: expected device=%d inode=%d, got device=%d inode=%d",
			ErrDirectoryIdentityChanged,
			expected.Device,
			expected.Inode,
			identity.Device,
			identity.Inode,
		)
	}
	return &Directory{
		path:     path,
		identity: identity,
		file:     file,
	}, nil
}

func identityFromFD(fd int) (Identity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return Identity{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return Identity{}, errors.New("retained filesystem object is not a directory")
	}
	return Identity{Device: stat.Dev, Inode: stat.Ino}, nil
}

// Path returns the clean absolute pathname used to acquire this capability.
// It is a diagnostic label, not an authority-bearing handle.
func (directory *Directory) Path() string {
	if directory == nil {
		return ""
	}
	return directory.path
}

// Identity returns the immutable physical directory identity.
func (directory *Directory) Identity() Identity {
	if directory == nil {
		return Identity{}
	}
	return directory.identity
}

// DisplayPath returns a diagnostic pathname for name. Callers of APIs such as
// bbolt that accept both a display pathname and a custom file opener should use
// this value only as the display pathname and route the open through OpenFile.
func (directory *Directory) DisplayPath(name string) string {
	if directory == nil || validateEntryName(name) != nil {
		return ""
	}
	return filepath.Join(directory.path, name)
}

// Entry returns a typed capability for one simple name in this directory.
func (directory *Directory) Entry(name string) (Entry, error) {
	if directory == nil {
		return Entry{}, ErrDirectoryClosed
	}
	if err := validateEntryName(name); err != nil {
		return Entry{}, err
	}
	return Entry{directory: directory, name: name}, nil
}

// Valid reports whether entry is bound to a retained directory and valid name.
// The directory may subsequently be revoked with Close; operations then fail.
func (entry Entry) Valid() bool {
	return entry.directory != nil && validateEntryName(entry.name) == nil
}

// Name returns the validated simple directory-entry name.
func (entry Entry) Name() string {
	if !entry.Valid() {
		return ""
	}
	return entry.name
}

// DisplayPath returns a diagnostic path for entry, never an authority handle.
func (entry Entry) DisplayPath() string {
	if !entry.Valid() {
		return ""
	}
	return entry.directory.DisplayPath(entry.name)
}

// Exists reports whether any object currently occupies entry.
func (entry Entry) Exists() (bool, error) {
	if !entry.Valid() {
		return false, ErrInvalidEntryName
	}
	return entry.directory.EntryExists(entry.name)
}

// Lstat reports entry without following a final-component symlink.
func (entry Entry) Lstat() (os.FileInfo, error) {
	if !entry.Valid() {
		return nil, ErrInvalidEntryName
	}
	return entry.directory.Lstat(entry.name)
}

// OpenFile opens entry relative to its retained directory without following a
// final-component symlink.
func (entry Entry) OpenFile(flag int, perm os.FileMode) (*os.File, error) {
	if !entry.Valid() {
		return nil, ErrInvalidEntryName
	}
	return entry.directory.OpenFile(entry.name, flag, perm)
}

// SyncParent durably flushes directory-entry changes for entry's parent.
func (entry Entry) SyncParent() error {
	if !entry.Valid() {
		return ErrInvalidEntryName
	}
	return entry.directory.Sync()
}

// VerifyPath proves that the acquisition pathname still names this retained
// physical directory. The final path component is not followed.
func (directory *Directory) VerifyPath() error {
	if directory == nil || !directory.identity.Valid() {
		return ErrDirectoryClosed
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if directory.file == nil {
		return ErrDirectoryClosed
	}
	current, err := inspectDirectoryPath(directory.path)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDirectoryIdentityChanged, err)
	}
	if !directory.identity.Equal(current) {
		return fmt.Errorf(
			"%w: path %q no longer names device=%d inode=%d",
			ErrDirectoryIdentityChanged,
			directory.path,
			directory.identity.Device,
			directory.identity.Inode,
		)
	}
	return nil
}

func inspectDirectoryPath(path string) (Identity, error) {
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return Identity{}, err
	}
	defer unix.Close(fd) //nolint:errcheck // read-only identity probe
	return identityFromFD(fd)
}

// EntryExists reports whether any filesystem object, including a symlink,
// occupies name in the retained directory.
func (directory *Directory) EntryExists(name string) (bool, error) {
	if directory == nil {
		return false, ErrDirectoryClosed
	}
	if err := validateEntryName(name); err != nil {
		return false, err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return false, err
	}
	var stat unix.Stat_t
	err = unix.Fstatat(fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	return false, err
}

// Lstat opens name without following the final component and returns its file
// information. A symlink is reported as a symlink rather than followed.
func (directory *Directory) Lstat(name string) (os.FileInfo, error) {
	if directory == nil {
		return nil, ErrDirectoryClosed
	}
	if err := validateEntryName(name); err != nil {
		return nil, err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return nil, err
	}
	entryFD, err := unix.Openat(fd, name, unix.O_PATH|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	entry := os.NewFile(uintptr(entryFD), directory.DisplayPath(name))
	if entry == nil {
		_ = unix.Close(entryFD)
		return nil, errors.New("construct retained entry handle")
	}
	info, statErr := entry.Stat()
	closeErr := entry.Close()
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return info, nil
}

// OpenFile opens name relative to the retained directory and never follows a
// symlink in the final component.
func (directory *Directory) OpenFile(
	name string,
	flag int,
	perm os.FileMode,
) (*os.File, error) {
	if directory == nil {
		return nil, ErrDirectoryClosed
	}
	if err := validateEntryName(name); err != nil {
		return nil, err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return nil, err
	}
	entryFD, err := unix.Openat(
		fd,
		name,
		flag|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(perm.Perm()),
	)
	if err != nil {
		return nil, err
	}
	entry := os.NewFile(uintptr(entryFD), directory.DisplayPath(name))
	if entry == nil {
		_ = unix.Close(entryFD)
		return nil, errors.New("construct retained entry file")
	}
	return entry, nil
}

// OpenSelf returns a new descriptor for the retained physical directory. The
// caller owns the returned file; it remains bound even if the path is replaced.
func (directory *Directory) OpenSelf() (*os.File, error) {
	if directory == nil {
		return nil, ErrDirectoryClosed
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return nil, err
	}
	selfFD, err := unix.Openat(
		fd,
		".",
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, err
	}
	self := os.NewFile(uintptr(selfFD), directory.path)
	if self == nil {
		_ = unix.Close(selfFD)
		return nil, errors.New("construct duplicated directory handle")
	}
	return self, nil
}

// ReadDir reads at most n entries from a separately opened descriptor for the
// retained directory. A positive n bounds both the result and memory use.
func (directory *Directory) ReadDir(n int) ([]os.DirEntry, error) {
	if n <= 0 {
		return nil, errors.New("directory read limit must be positive")
	}
	self, err := directory.OpenSelf()
	if err != nil {
		return nil, err
	}
	entries, readErr := self.ReadDir(n)
	closeErr := self.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return entries, nil
}

// CreateTemp creates one random, exclusive, private regular file relative to
// the retained directory. prefix must itself be a valid entry-name prefix.
func (directory *Directory) CreateTemp(
	prefix string,
	perm os.FileMode,
) (string, *os.File, error) {
	if directory == nil {
		return "", nil, ErrDirectoryClosed
	}
	if prefix == "" || filepath.Base(prefix) != prefix ||
		prefix == "." || prefix == ".." {
		return "", nil, fmt.Errorf("%w: %q", ErrInvalidEntryName, prefix)
	}
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, fmt.Errorf("generate temporary entry name: %w", err)
		}
		name := prefix + hex.EncodeToString(random[:])
		file, err := directory.OpenFile(
			name,
			os.O_RDWR|os.O_CREATE|os.O_EXCL,
			perm,
		)
		if err == nil {
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, err
		}
	}
	return "", nil, errors.New("exhausted temporary entry-name attempts")
}

// LinkNoReplace publishes oldName at newName in the same retained directory.
// linkat has atomic no-overwrite semantics for the destination.
func (directory *Directory) LinkNoReplace(oldName, newName string) error {
	if directory == nil {
		return ErrDirectoryClosed
	}
	if err := validateEntryName(oldName); err != nil {
		return err
	}
	if err := validateEntryName(newName); err != nil {
		return err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return err
	}
	return unix.Linkat(fd, oldName, fd, newName, 0)
}

// RenameNoReplace atomically moves oldName to an absent newName inside the
// retained directory. Linux RENAME_NOREPLACE provides the publication
// invariant directly, avoiding the crash-visible second name inherent in a
// hard-link-then-unlink protocol.
func (directory *Directory) RenameNoReplace(oldName, newName string) error {
	if directory == nil {
		return ErrDirectoryClosed
	}
	if err := validateEntryName(oldName); err != nil {
		return err
	}
	if err := validateEntryName(newName); err != nil {
		return err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return err
	}
	return unix.Renameat2(fd, oldName, fd, newName, unix.RENAME_NOREPLACE)
}

// Rename atomically replaces newName with oldName inside the retained
// directory. Callers requiring no-overwrite semantics must use RenameNoReplace.
func (directory *Directory) Rename(oldName, newName string) error {
	if directory == nil {
		return ErrDirectoryClosed
	}
	if err := validateEntryName(oldName); err != nil {
		return err
	}
	if err := validateEntryName(newName); err != nil {
		return err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return err
	}
	return unix.Renameat(fd, oldName, fd, newName)
}

// Remove unlinks name from the retained directory without following it.
func (directory *Directory) Remove(name string) error {
	if directory == nil {
		return ErrDirectoryClosed
	}
	if err := validateEntryName(name); err != nil {
		return err
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	fd, err := directory.fdLocked()
	if err != nil {
		return err
	}
	return unix.Unlinkat(fd, name, 0)
}

// Sync durably flushes directory-entry changes on the retained descriptor.
func (directory *Directory) Sync() error {
	if directory == nil {
		return ErrDirectoryClosed
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if directory.file == nil {
		return ErrDirectoryClosed
	}
	return directory.file.Sync()
}

// Close revokes this directory capability. It is safe to call repeatedly.
func (directory *Directory) Close() error {
	if directory == nil {
		return nil
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.file == nil {
		return nil
	}
	err := directory.file.Close()
	directory.file = nil
	return err
}

func (directory *Directory) fdLocked() (int, error) {
	if directory == nil || directory.file == nil {
		return -1, ErrDirectoryClosed
	}
	return int(directory.file.Fd()), nil
}

func validateEntryName(name string) error {
	// A lone "/" is the important edge case that filepath.Base alone does not
	// reject: Base("/") == "/", and Linux *at syscalls ignore dirfd for an
	// absolute path. Reject every separator and NUL explicitly so this type can
	// never escape its retained directory capability.
	if name == "" || name == "." || name == ".." ||
		filepath.IsAbs(name) || strings.ContainsRune(name, filepath.Separator) ||
		strings.IndexByte(name, 0) >= 0 || filepath.Base(name) != name {
		return fmt.Errorf("%w: %q", ErrInvalidEntryName, name)
	}
	return nil
}
