//go:build linux

package fstree

import (
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

// dirFlags opens a directory without following a final symlink, without
// blocking if a FIFO raced into its place, and without leaking the
// descriptor across exec.
const dirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK

// The wrappers below retry EINTR, which the x/sys syscall wrappers return
// unchanged. close is deliberately not retried: Linux releases a descriptor
// even when close reports EINTR, so a retry could close an unrelated one.

func openDirAt(dirfd int, name string) (int, error) {
	for {
		fd, err := unix.Openat(dirfd, name, dirFlags, 0)
		if !errors.Is(err, unix.EINTR) {
			return fd, err
		}
	}
}

func unlinkAt(dirfd int, name string, flags int) error {
	for {
		err := unix.Unlinkat(dirfd, name, flags)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func renameNoReplace(olddirfd int, oldname string, newdirfd int, newname string) error {
	for {
		err := unix.Renameat2(olddirfd, oldname, newdirfd, newname, unix.RENAME_NOREPLACE)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func getdents(fd int, buf []byte) (int, error) {
	for {
		n, err := unix.Getdents(fd, buf)
		if !errors.Is(err, unix.EINTR) {
			return n, err
		}
	}
}

func seekDir(fd int, off int64) error {
	for {
		_, err := unix.Seek(fd, off, io.SeekStart)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func dupFD(fd int) (int, error) {
	for {
		nfd, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if !errors.Is(err, unix.EINTR) {
			return nfd, err
		}
	}
}

// closeFD closes a descriptor this package opened. Every such descriptor is
// a read-only directory, whose close cannot lose data, so its error carries
// nothing to act on.
func closeFD(fd int) {
	_ = unix.Close(fd)
}

// identity is what fstree compares to decide that two descriptors or names
// denote the same directory, or lie on the same mount.
type identity struct {
	dev    uint64 // device major and minor
	ino    uint64
	mnt    uint64 // mount ID, meaningful only when hasMnt
	hasMnt bool
	mode   uint16 // S_IFMT bits
}

// statxMask asks for the type, the inode and, since Linux 5.8, the mount ID.
// statx reports the device unconditionally.
const statxMask = unix.STATX_TYPE | unix.STATX_INO | unix.STATX_MNT_ID

var errIncompleteStat = errors.New("filesystem did not report the file type and inode")

func statx(dirfd int, name string, flags int) (identity, error) {
	var st unix.Statx_t
	for {
		err := unix.Statx(dirfd, name, flags, statxMask, &st)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EINTR) {
			return identity{}, err
		}
	}
	const required = unix.STATX_TYPE | unix.STATX_INO
	if st.Mask&required != required {
		return identity{}, errIncompleteStat
	}
	return identity{
		dev:    uint64(st.Dev_major)<<32 | uint64(st.Dev_minor),
		ino:    st.Ino,
		mnt:    st.Mnt_id,
		hasMnt: st.Mask&unix.STATX_MNT_ID != 0,
		mode:   st.Mode & unix.S_IFMT,
	}, nil
}

// statFD describes the object an open descriptor refers to.
func statFD(fd int) (identity, error) {
	return statx(fd, "", unix.AT_EMPTY_PATH)
}

// statEntry describes name inside dirfd without following a final symlink.
func statEntry(dirfd int, name string) (identity, error) {
	return statx(dirfd, name, unix.AT_SYMLINK_NOFOLLOW)
}

// sameMount reports whether b lies on a's filesystem and mount. The device
// alone misses a bind mount of the same filesystem, so the mount IDs are
// compared too whenever the kernel reports them.
func (a identity) sameMount(b identity) bool {
	if a.dev != b.dev {
		return false
	}
	if a.hasMnt && b.hasMnt {
		return a.mnt == b.mnt
	}
	return true
}

// same reports whether a and b denote the same inode on the same mount.
func (a identity) same(b identity) bool {
	return a.sameMount(b) && a.ino == b.ino
}

func (a identity) isDir() bool {
	return a.mode == unix.S_IFDIR
}

// direntType maps the file type to the matching d_type value.
func (a identity) direntType() uint8 {
	switch a.mode {
	case unix.S_IFIFO:
		return unix.DT_FIFO
	case unix.S_IFCHR:
		return unix.DT_CHR
	case unix.S_IFDIR:
		return unix.DT_DIR
	case unix.S_IFBLK:
		return unix.DT_BLK
	case unix.S_IFREG:
		return unix.DT_REG
	case unix.S_IFLNK:
		return unix.DT_LNK
	case unix.S_IFSOCK:
		return unix.DT_SOCK
	default:
		return unix.DT_UNKNOWN
	}
}
