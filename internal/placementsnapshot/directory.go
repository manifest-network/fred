package placementsnapshot

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// LiveDatabases names providerd's live database files. A snapshot directory
// must not be either file's directory, and pruning never unlinks either file's
// inode.
type LiveDatabases struct {
	Placements string
	Payloads   string
}

// Directory is the retained snapshot directory of one provider. Every file
// operation is relative to the directory descriptor bound at open, so renaming
// or replacing the configured path cannot redirect a write or a delete. Only
// OpenDirectory mints one; the zero value is invalid.
type Directory struct {
	directory *fsidentity.Directory
	names     namer
	owner     uint32
	live      []os.FileInfo
}

// OpenDirectory binds path as the snapshot directory for providerUUID. It
// refuses a directory that is not owned by the service user, is writable by
// group or others, or is the directory of a live database.
func OpenDirectory(path, providerUUID string, live LiveDatabases) (*Directory, error) {
	names, err := newNamer(providerUUID)
	if err != nil {
		return nil, err
	}
	var (
		liveFiles   []os.FileInfo
		liveParents []fsidentity.Identity
	)
	for _, livePath := range []string{live.Placements, live.Payloads} {
		if livePath == "" || !filepath.IsAbs(livePath) || filepath.Clean(livePath) != livePath {
			return nil, fmt.Errorf("live database path must be absolute and clean: %q", livePath)
		}
		info, err := os.Stat(livePath)
		if err != nil {
			return nil, fmt.Errorf("stat live database: %w", err)
		}
		parentPath, err := filepath.EvalSymlinks(filepath.Dir(livePath))
		if err != nil {
			return nil, fmt.Errorf("resolve live database directory: %w", err)
		}
		parent, err := fsidentity.InspectDirectory(parentPath)
		if err != nil {
			return nil, fmt.Errorf("inspect live database directory: %w", err)
		}
		liveFiles = append(liveFiles, info)
		liveParents = append(liveParents, parent)
	}

	directory, err := fsidentity.OpenDirectory(path)
	if err != nil {
		return nil, fmt.Errorf("open snapshot directory: %w", err)
	}
	snapshots := &Directory{
		directory: directory,
		names:     names,
		owner:     uint32(os.Geteuid()), // #nosec G115 -- a Linux euid is a uint32
		live:      liveFiles,
	}
	if err := snapshots.checkDirectory(liveParents); err != nil {
		return nil, errors.Join(err, directory.Close())
	}
	return snapshots, nil
}

func (snapshots *Directory) checkDirectory(liveParents []fsidentity.Identity) error {
	for _, parent := range liveParents {
		if snapshots.directory.Identity().Equal(parent) {
			return errors.New("snapshot directory must not be a live database's directory")
		}
	}
	self, err := snapshots.directory.OpenSelf()
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	info, statErr := self.Stat()
	if closeErr := self.Close(); statErr == nil {
		statErr = closeErr
	}
	if statErr != nil {
		return fmt.Errorf("stat snapshot directory: %w", statErr)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != snapshots.owner || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf(
			"snapshot directory %s must be owned by the service user and not writable by group or others",
			snapshots.directory.Path())
	}
	for _, liveFile := range snapshots.live {
		if liveStat, ok := liveFile.Sys().(*syscall.Stat_t); ok && liveStat.Dev == stat.Dev {
			slog.Warn("placement snapshots share a filesystem with a live database; "+
				"one disk loss takes both", "snapshot_dir", snapshots.directory.Path())
			break
		}
	}
	return nil
}

// Path is the directory's configured path, for diagnostics only.
func (snapshots *Directory) Path() string {
	if snapshots == nil || snapshots.directory == nil {
		return ""
	}
	return snapshots.directory.Path()
}

// Close releases the directory.
func (snapshots *Directory) Close() error {
	if snapshots == nil || snapshots.directory == nil {
		return nil
	}
	return snapshots.directory.Close()
}

// availableBytes is the space an unprivileged writer may still use on the
// snapshot directory's filesystem.
func (snapshots *Directory) availableBytes() (uint64, error) {
	self, err := snapshots.directory.OpenSelf()
	if err != nil {
		return 0, err
	}
	defer func() { _ = self.Close() }()
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(self.Fd()), &stat); err != nil { // #nosec G115 -- a descriptor fits in int
		return 0, fmt.Errorf("statfs snapshot directory: %w", err)
	}
	return stat.Bavail * uint64(stat.Bsize), nil // #nosec G115 -- block size is positive
}

// isLive reports whether info is a live database's inode.
func (snapshots *Directory) isLive(info os.FileInfo) bool {
	for _, liveFile := range snapshots.live {
		if os.SameFile(liveFile, info) {
			return true
		}
	}
	return false
}

// owned reports whether info is a regular file owned by the service user.
func (snapshots *Directory) owned(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && stat.Uid == snapshots.owner
}

// private reports whether info is an owned file with mode 0600 and one link,
// the shape publication creates.
func (snapshots *Directory) private(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && snapshots.owned(info) && info.Mode().Perm() == 0o600 && stat.Nlink == 1
}
