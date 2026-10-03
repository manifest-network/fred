//go:build linux

package at

import (
	"sync"

	"golang.org/x/sys/unix"
)

// ErrLoanEnded means a Borrowed was used after the call it was lent for
// returned, or is the zero Borrowed.
const ErrLoanEnded = atError("fstree: directory used after the call it was lent for returned")

// Lender lends a directory to foreign code for exactly one call at a time.
// Each loan has its own generation, so a Borrowed kept from an earlier call
// is refused during a later one, and the end of a loan waits for every
// Control still running on it, so the directory's descriptor is never closed
// or reused under one.
//
// The zero Lender is ready to use. A Lender must not be copied after first
// use, and lends one directory at a time.
type Lender struct {
	mu     sync.Mutex
	idle   sync.Cond // signaled when the last running Control returns
	fd     int       // the lent descriptor, meaningful only while active
	gen    uint64    // generation of the current or last loan
	active bool
	users  int // Control calls running
}

// Lend runs fn with a Borrowed for v that is valid only until fn returns,
// and returns fn's error unchanged. A closed v is refused with ErrClosed
// before fn runs.
func (l *Lender) Lend(v View, fn func(Borrowed) error) error {
	fd, err := v.d.fdOf()
	if err != nil {
		return err
	}
	l.mu.Lock()
	if l.idle.L == nil {
		l.idle.L = &l.mu
	}
	l.gen++
	l.fd, l.active = fd, true
	lent := Borrowed{lender: l, gen: l.gen}
	l.mu.Unlock()
	defer l.end()
	return fn(lent)
}

// end closes the current loan and waits until no Control is still using
// its descriptor.
func (l *Lender) end() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active = false
	for l.users > 0 {
		l.idle.Wait()
	}
}

// Borrowed is a directory lent for the duration of one call. Its descriptor
// is reachable only inside Control, and only while that call runs. The zero
// Borrowed is never valid.
type Borrowed struct {
	lender *Lender
	gen    uint64
}

// acquire returns the lent descriptor and counts one more Control running
// on it, or fails with ErrLoanEnded once the loan is over.
func (b Borrowed) acquire() (int, error) {
	l := b.lender
	if l == nil || b.gen == 0 {
		return -1, ErrLoanEnded
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active || l.gen != b.gen {
		return -1, ErrLoanEnded
	}
	l.users++
	return l.fd, nil
}

func (b Borrowed) release() {
	l := b.lender
	l.mu.Lock()
	defer l.mu.Unlock()
	l.users--
	if l.users == 0 {
		l.idle.Broadcast()
	}
}

// Control runs fn with the directory's descriptor, as syscall.RawConn's
// Control does, and returns fn's error. It fails with ErrLoanEnded, without
// running fn, once the call the directory was lent for has returned. The
// call does not return while fn runs, so the descriptor stays open for all
// of fn.
//
// fn must not close the descriptor, keep it, or wrap it in an *os.File
// (whose finalizer would close it later): the descriptor belongs to fstree,
// and may be reused as soon as the loan ends.
func (b Borrowed) Control(fn func(fd int) error) error {
	fd, err := b.acquire()
	if err != nil {
		return err
	}
	defer b.release()
	return fn(fd)
}

// Stat returns fstat(2) of the directory.
func (b Borrowed) Stat() (unix.Stat_t, error) {
	var st unix.Stat_t
	err := b.Control(func(fd int) error { return fstat(fd, &st) })
	return st, err
}
