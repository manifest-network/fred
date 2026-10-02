//go:build linux

package at

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A Borrowed reaches the lent directory only during the call it was lent
// for: before Lend returns it works, afterwards it fails without running fn,
// and one kept from an earlier loan is refused during a later one.
func TestLenderScopesTheLoan(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, "dir"), 0o755))
	d := openOwned(t, parentPath, "dir")
	want, err := d.Stat()
	require.NoError(t, err)

	var lender Lender
	var kept Borrowed
	require.NoError(t, lender.Lend(d.View(), func(b Borrowed) error {
		require.NoError(t, b.Control(func(fd int) error {
			var st unix.Stat_t
			require.NoError(t, unix.Fstat(fd, &st))
			require.Equal(t, want.Ino(), st.Ino, "Control lends the directory itself")
			return nil
		}))
		st, err := b.Stat()
		require.NoError(t, err)
		require.Equal(t, want.Ino(), st.Ino)
		kept = b
		return nil
	}))

	ran := false
	require.ErrorIs(t, kept.Control(func(int) error { ran = true; return nil }), ErrLoanEnded)
	require.False(t, ran, "fn never runs after the loan ended")
	_, err = kept.Stat()
	require.ErrorIs(t, err, ErrLoanEnded)

	require.NoError(t, lender.Lend(d.View(), func(Borrowed) error {
		require.ErrorIs(t, kept.Control(func(int) error { ran = true; return nil }), ErrLoanEnded,
			"a Borrowed from an earlier loan is refused during a later one")
		return nil
	}))
	require.False(t, ran)

	var zero Borrowed
	require.ErrorIs(t, zero.Control(func(int) error { ran = true; return nil }), ErrLoanEnded)
	require.False(t, ran)

	stop := errors.New("stop")
	require.Same(t, stop, lender.Lend(d.View(), func(Borrowed) error { return stop }),
		"fn's error comes back unchanged")

	called := false
	require.ErrorIs(t, lender.Lend(View{}, func(Borrowed) error { called = true; return nil }), ErrClosed)
	require.False(t, called, "a closed directory is never lent")
}

// The end of a loan waits for every Control still running on it, so the
// lent descriptor cannot be closed, and its number reused, under a Control
// that a goroutine started during the call.
func TestLenderWaitsForControlInFlight(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, "dir"), 0o755))
	d := openOwned(t, parentPath, "dir")

	var lender Lender
	var finished atomic.Bool
	started := make(chan struct{})
	proceed := make(chan struct{})
	controlled := make(chan error, 1)
	require.NoError(t, lender.Lend(d.View(), func(b Borrowed) error {
		go func() {
			controlled <- b.Control(func(fd int) error {
				close(started)
				<-proceed
				var st unix.Stat_t
				err := unix.Fstat(fd, &st)
				finished.Store(true)
				return err
			})
		}()
		<-started
		go func() {
			time.Sleep(20 * time.Millisecond)
			close(proceed)
		}()
		return nil
	}))
	require.True(t, finished.Load(), "Lend returned while a Control was still running")
	require.NoError(t, <-controlled)
}
