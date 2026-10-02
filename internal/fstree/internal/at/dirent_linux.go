//go:build linux

package at

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	// direntHeader is offsetof(struct linux_dirent64, d_name): d_ino (8
	// bytes), d_off (8), d_reclen (2) and d_type (1).
	direntHeader = 19
	// direntAlign is the kernel's record alignment, sizeof(u64).
	direntAlign = 8
	// direntBufSize is the largest getdents64 read.
	direntBufSize = 32 << 10
	// minDirentRead is the smallest. It holds the two dot records plus three
	// records with NAME_MAX names, so getdents64 never finds it too small.
	minDirentRead = 1 << 10
	// batchSize bounds the names taken from one read.
	batchSize = 256
	// maxDotReads bounds consecutive non-empty reads that hold only "." and
	// "..". A sane filesystem returns those two once, at the start.
	maxDotReads = 4
)

const (
	// ErrMalformedDirent means a getdents64 buffer broke the record format.
	// The whole buffer is refused.
	ErrMalformedDirent = atError("malformed directory entry")
	// ErrOnlyDots means maxDotReads reads in a row returned only "." and
	// "..", which no sane filesystem does: the directory keeps changing
	// under the reader.
	ErrOnlyDots = atError("directory reads kept returning only the dot entries")
)

// dirent is one decoded linux_dirent64 record.
type dirent struct {
	name string
	off  int64 // d_off: the cookie that resumes the directory after this entry
	typ  uint8 // d_type
}

// parseDirents decodes the linux_dirent64 records in buf, appending to out
// every name other than "." and "..", and stops once limit names were
// appended. It returns the extended slice and the d_off of the last record it
// consumed, dot records included (0 when it consumed none).
//
// The buffer is checked strictly: a record must hold its header, fit in what
// is left, be 8-byte aligned and carry a NUL-terminated name of 1 to 255
// bytes free of '/'. Any violation fails the whole buffer, so a corrupt read
// can neither panic nor yield a name that resolves outside its directory.
func parseDirents(buf []byte, limit int, out []dirent) ([]dirent, int64, error) {
	var next int64
	for taken := 0; len(buf) > 0 && taken < limit; {
		if len(buf) < direntHeader {
			return out, next, fmt.Errorf("%w: %d trailing bytes", ErrMalformedDirent, len(buf))
		}
		reclen := int(binary.NativeEndian.Uint16(buf[16:18]))
		if reclen < direntHeader || reclen > len(buf) || reclen%direntAlign != 0 {
			return out, next, fmt.Errorf("%w: record length %d with %d bytes left",
				ErrMalformedDirent, reclen, len(buf))
		}
		name := buf[direntHeader:reclen]
		end := bytes.IndexByte(name, 0)
		if end < 0 {
			return out, next, fmt.Errorf("%w: name is not NUL-terminated", ErrMalformedDirent)
		}
		name = name[:end]
		off := int64(binary.NativeEndian.Uint64(buf[8:16]))
		typ := buf[18]
		buf = buf[reclen:]
		next = off
		if isDotName(name) {
			continue
		}
		if !validName(name) {
			return out, next, fmt.Errorf("%w: invalid name of %d bytes", ErrMalformedDirent, len(name))
		}
		out = append(out, dirent{name: string(name), off: off, typ: typ})
		taken++
	}
	return out, next, nil
}

// Reader reads one directory batch at a time into a buffer it reuses. Its
// read size adapts: full for a directory just entered, small after an
// ascent, when usually only the emptied child and its next sibling matter,
// and doubling while whole batches are consumed. That keeps the kernel's work
// per read proportional to the entries actually used.
//
// The slices a read returns are reused by the next read.
type Reader struct {
	buf     []byte
	size    int
	entries []dirent
	listed  []Listed
	viewed  []ListedView
}

// NewReader returns a Reader with one 32 KiB buffer and room for one batch
// of 256 names.
func NewReader() Reader {
	return Reader{
		buf:     make([]byte, direntBufSize),
		size:    direntBufSize,
		entries: make([]dirent, 0, batchSize),
	}
}

// Full sets the read size for a directory just entered.
func (r *Reader) Full() { r.size = len(r.buf) }

// Shrink sets the read size after an ascent.
func (r *Reader) Shrink() { r.size = minDirentRead }

// Grow doubles the read size after a batch was consumed whole.
func (r *Reader) Grow() { r.size = min(2*r.size, len(r.buf)) }

// read positions fd at off and returns up to batchSize entries other than
// "." and "..". next is the cookie that resumes the directory after the last
// entry returned. eof reports that no entry follows off.
func (r *Reader) read(fd int, off int64) (entries []dirent, next int64, eof bool, err error) {
	if err := seekDir(fd, off); err != nil {
		return nil, off, false, err
	}
	next = off
	for range maxDotReads {
		n, err := getdents(fd, r.buf[:r.size])
		if err != nil {
			return nil, next, false, err
		}
		if n == 0 {
			return nil, next, true, nil
		}
		var last int64
		r.entries, last, err = parseDirents(r.buf[:n], batchSize, r.entries[:0])
		if err != nil {
			return nil, next, false, err
		}
		next = last
		if len(r.entries) > 0 {
			return r.entries, next, false, nil
		}
	}
	return nil, next, false, ErrOnlyDots
}

// ReadBatch reads d at off and returns up to 256 of its entries, each bound
// to d. next is the cookie that resumes d after the last entry returned, and
// eof reports that no entry follows off. A directory removed while d holds
// it fails with ENOENT.
func (d *Dir) ReadBatch(r *Reader, off int64) (batch []Listed, next int64, eof bool, err error) {
	fd, err := d.fdOf()
	if err != nil {
		return nil, off, false, err
	}
	entries, next, eof, err := r.read(fd, off)
	if err != nil || eof {
		return nil, next, eof, err
	}
	r.listed = r.listed[:0]
	for _, e := range entries {
		r.listed = append(r.listed, Listed{dir: d, ent: e})
	}
	return r.listed, next, false, nil
}

// ReadBatch is (*Dir).ReadBatch for the read-only side: each entry is a
// ListedView bound to the directory.
func (v View) ReadBatch(r *Reader, off int64) (batch []ListedView, next int64, eof bool, err error) {
	fd, err := v.d.fdOf()
	if err != nil {
		return nil, off, false, err
	}
	entries, next, eof, err := r.read(fd, off)
	if err != nil || eof {
		return nil, next, eof, err
	}
	r.viewed = r.viewed[:0]
	for _, e := range entries {
		r.viewed = append(r.viewed, ListedView{dir: v.d, ent: e})
	}
	return r.viewed, next, false, nil
}

// Listed is one entry a Dir listed, bound to that Dir. Its methods act on the
// entry inside that directory and never take the directory it is in, and
// nothing converts a Listed to a Name: a listed name cannot be resolved
// against any other directory, such as the parent of the one it came from.
// Once its Dir is closed, every method fails with ErrClosed. Only ReadBatch
// makes one; the zero Listed is bound to nothing and fails the same way.
type Listed struct {
	dir *Dir
	ent dirent
}

// String returns the entry's name, for messages.
func (l Listed) String() string { return l.ent.name }

// bound returns the descriptor of l's directory, or ErrClosed when that
// directory was closed or l is the zero Listed.
func (l Listed) bound() (int, error) {
	if l.ent.name == "" {
		return -1, ErrClosed
	}
	return l.dir.fdOf()
}

// Unlink removes the entry when it is not a directory, and fails with EISDIR
// when it is one. A symlink is removed, never followed.
func (l Listed) Unlink() error {
	fd, err := l.bound()
	if err != nil {
		return err
	}
	return unlinkEntry(fd, l.ent.name)
}

// Rmdir removes the entry when it is an empty directory.
func (l Listed) Rmdir() error {
	fd, err := l.bound()
	if err != nil {
		return err
	}
	return removeDir(fd, l.ent.name)
}

// OpenDir opens the entry as a directory. It never follows a symlink: a
// symlink fails with ELOOP, and a non-directory with ENOTDIR.
func (l Listed) OpenDir() (*Dir, error) {
	fd, err := l.bound()
	if err != nil {
		return nil, err
	}
	child, err := openDirAt(fd, l.ent.name)
	if err != nil {
		return nil, err
	}
	return &Dir{fd: child}, nil
}

// RenameNoReplaceInto moves the entry into dst under name, and fails with
// EEXIST rather than replace an existing entry there.
func (l Listed) RenameNoReplaceInto(dst *Dir, name Name) error {
	fd, err := l.bound()
	if err != nil {
		return err
	}
	dstFD, err := dst.fdOf()
	if err != nil {
		return err
	}
	if !name.Valid() {
		return ErrZeroName
	}
	return renameNoReplace(fd, l.ent.name, dstFD, name.s)
}

// ListedView is one entry a View listed, bound to that directory, with only
// read-only methods. Like Listed, it converts to no Name, and fails with
// ErrClosed once its directory is closed.
type ListedView struct {
	dir *Dir
	ent dirent
}

// String returns the entry's name.
func (l ListedView) String() string { return l.ent.name }

// Offset returns the cookie that resumes the listing right after the entry.
func (l ListedView) Offset() int64 { return l.ent.off }

func (l ListedView) bound() (int, error) {
	if l.ent.name == "" {
		return -1, ErrClosed
	}
	return l.dir.fdOf()
}

// Type returns the entry's d_type. When the listing did not say (DT_UNKNOWN,
// or a value this package does not know), it asks the filesystem without
// following a symlink. present is false, with a nil error, when the entry
// vanished before that stat.
func (l ListedView) Type() (typ uint8, present bool, err error) {
	switch l.ent.typ {
	case unix.DT_FIFO, unix.DT_CHR, unix.DT_DIR, unix.DT_BLK, unix.DT_REG, unix.DT_LNK, unix.DT_SOCK, unix.DT_WHT:
		if _, err := l.bound(); err != nil {
			return 0, false, err
		}
		return l.ent.typ, true, nil
	}
	fd, err := l.bound()
	if err != nil {
		return 0, false, err
	}
	id, err := statEntry(fd, l.ent.name)
	switch {
	case err == nil:
		return id.DirentType(), true, nil
	case errors.Is(err, unix.ENOENT):
		return 0, false, nil
	default:
		return 0, false, err
	}
}

// OpenDir opens the entry as a directory View, never following a symlink.
func (l ListedView) OpenDir() (View, error) {
	fd, err := l.bound()
	if err != nil {
		return View{}, err
	}
	child, err := openDirAt(fd, l.ent.name)
	if err != nil {
		return View{}, err
	}
	return View{d: &Dir{fd: child}}, nil
}
