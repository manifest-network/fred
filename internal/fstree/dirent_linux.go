//go:build linux

package fstree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

var (
	errMalformedDirent = errors.New("malformed directory entry")
	errOnlyDots        = fmt.Errorf("%w: directory reads kept returning only the dot entries", ErrTreeChanged)
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
			return out, next, fmt.Errorf("%w: %d trailing bytes", errMalformedDirent, len(buf))
		}
		reclen := int(binary.NativeEndian.Uint16(buf[16:18]))
		if reclen < direntHeader || reclen > len(buf) || reclen%direntAlign != 0 {
			return out, next, fmt.Errorf("%w: record length %d with %d bytes left",
				errMalformedDirent, reclen, len(buf))
		}
		name := buf[direntHeader:reclen]
		end := bytes.IndexByte(name, 0)
		if end < 0 {
			return out, next, fmt.Errorf("%w: name is not NUL-terminated", errMalformedDirent)
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
			return out, next, fmt.Errorf("%w: invalid name of %d bytes", errMalformedDirent, len(name))
		}
		out = append(out, dirent{name: string(name), off: off, typ: typ})
		taken++
	}
	return out, next, nil
}

// dirReader reads one directory batch at a time into a buffer it reuses. Its
// read size adapts: full for a directory just entered, small after an
// ascent, when usually only the emptied child and its next sibling matter,
// and doubling while whole batches are consumed. That keeps the kernel's work
// per read proportional to the entries actually used.
type dirReader struct {
	buf     []byte
	size    int
	entries []dirent
}

func newDirReader() dirReader {
	return dirReader{
		buf:     make([]byte, direntBufSize),
		size:    direntBufSize,
		entries: make([]dirent, 0, batchSize),
	}
}

// full sets the read size for a directory just entered.
func (d *dirReader) full() { d.size = len(d.buf) }

// shrink sets the read size after an ascent.
func (d *dirReader) shrink() { d.size = minDirentRead }

// grow doubles the read size after a batch was consumed whole.
func (d *dirReader) grow() { d.size = min(2*d.size, len(d.buf)) }

// read positions fd at off and returns up to batchSize entries other than
// "." and "..". next is the cookie that resumes the directory after the last
// entry returned. eof reports that no entry follows off. The returned slice
// is reused by the next read.
func (d *dirReader) read(fd int, off int64) (entries []dirent, next int64, eof bool, err error) {
	if err := seekDir(fd, off); err != nil {
		return nil, off, false, err
	}
	next = off
	for range maxDotReads {
		n, err := getdents(fd, d.buf[:d.size])
		if err != nil {
			return nil, next, false, err
		}
		if n == 0 {
			return nil, next, true, nil
		}
		var last int64
		d.entries, last, err = parseDirents(d.buf[:n], batchSize, d.entries[:0])
		if err != nil {
			return nil, next, false, err
		}
		next = last
		if len(d.entries) > 0 {
			return d.entries, next, false, nil
		}
	}
	return nil, next, false, errOnlyDots
}
