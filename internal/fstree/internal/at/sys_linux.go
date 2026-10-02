//go:build linux

package at

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// This file is the only one under internal/fstree that calls into
// golang.org/x/sys/unix or syscall, or reaches the descriptor behind an
// *os.File; fstree's confinement guard test pins that. Every flag a syscall
// here uses is fixed in this file.
//
// The wrappers retry EINTR, which the x/sys wrappers return unchanged. close
// is deliberately not retried: Linux releases a descriptor even when close
// reports EINTR, so a retry could close an unrelated one.

// dirFlags opens a directory without following a final symlink, without
// blocking if a FIFO raced into its place, and without leaking the
// descriptor across exec. Every directory this package opens uses exactly
// these flags.
const dirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK

// statxMask asks for the file type, the inode and the mount ID, which Linux
// reports from 5.8 on. statx reports the device unconditionally.
const statxMask = unix.STATX_TYPE | unix.STATX_INO | unix.STATX_MNT_ID

func openDirAt(dirfd int, name string) (int, error) {
	for {
		fd, err := unix.Openat(dirfd, name, dirFlags, 0)
		if !errors.Is(err, unix.EINTR) {
			return fd, err
		}
	}
}

// unlinkEntry removes the non-directory name inside dirfd.
func unlinkEntry(dirfd int, name string) error {
	for {
		err := unix.Unlinkat(dirfd, name, 0)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// removeDir removes the empty directory name inside dirfd.
func removeDir(dirfd int, name string) error {
	for {
		err := unix.Unlinkat(dirfd, name, unix.AT_REMOVEDIR)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// renameNoReplace moves oldname inside olddirfd to newname inside newdirfd,
// and fails with EEXIST rather than replace an existing newname.
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

func statx(dirfd int, name string, flags int) (Identity, error) {
	var st unix.Statx_t
	for {
		err := unix.Statx(dirfd, name, flags, statxMask, &st)
		if err == nil {
			return identityOf(&st)
		}
		if !errors.Is(err, unix.EINTR) {
			return Identity{}, err
		}
	}
}

// statFD describes the object an open descriptor refers to.
func statFD(fd int) (Identity, error) {
	return statx(fd, "", unix.AT_EMPTY_PATH)
}

// statEntry describes name inside dirfd without following a final symlink.
func statEntry(dirfd int, name string) (Identity, error) {
	return statx(dirfd, name, unix.AT_SYMLINK_NOFOLLOW)
}

func fstat(fd int, st *unix.Stat_t) error {
	for {
		err := unix.Fstat(fd, st)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// control runs fn with f's descriptor. SyscallConn's Control holds a
// reference on f for the whole call, so a concurrent Close does not release
// the descriptor before fn returns, and its number cannot be reused
// underneath fn. The error is f's: nil, closed, or without a descriptor.
func control(f *os.File, fn func(fd int)) error {
	raw, err := f.SyscallConn()
	if err != nil {
		return err
	}
	return raw.Control(func(fd uintptr) { fn(int(fd)) })
}
