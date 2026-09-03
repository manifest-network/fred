package placement

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

var (
	// ErrRuntimeAuthorityUnavailable means this process can no longer prove that
	// its open placement database is the authority published at the configured
	// pathname. The failure is sticky: callers must stop the process and reopen
	// the database rather than continuing from an uncertain in-memory cache.
	ErrRuntimeAuthorityUnavailable = errors.New("placement runtime authority is unavailable")

	// ErrRuntimeAuthorityPathChanged means the configured pathname no longer
	// names the exact regular file opened by this Store.
	ErrRuntimeAuthorityPathChanged = errors.New("placement database pathname changed")

	errRuntimeAuthorityClosed = errors.New("placement runtime authority is closed")
)

// openRuntimeAuthorityIdentity retains a read-only descriptor for the exact
// database inode. expected is the file identity captured before bbolt opened
// the production authority; test and offline callers may omit it.
func openRuntimeAuthorityIdentity(
	db *bolt.DB,
	expected os.FileInfo,
) (string, *os.File, error) {
	if db == nil {
		return "", nil, errors.New("placement database is not open")
	}
	path, err := filepath.Abs(db.Path())
	if err != nil {
		return "", nil, fmt.Errorf("resolve placement database path: %w", err)
	}
	path = filepath.Clean(path)

	pathInfo, err := os.Lstat(path)
	if err != nil {
		return "", nil, fmt.Errorf("stat placement database identity path: %w", err)
	}
	if err := validatePlacementAuthorityFile(path, pathInfo); err != nil {
		return "", nil, err
	}
	file, err := openPlacementAuthorityFileNoFollow(path, os.O_RDONLY, 0)
	if err != nil {
		return "", nil, fmt.Errorf("open placement database identity: %w", err)
	}
	closeOnError := func(cause error) (string, *os.File, error) {
		if closeErr := file.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close placement database identity: %w", closeErr))
		}
		return "", nil, cause
	}

	openedInfo, err := file.Stat()
	if err != nil {
		return closeOnError(fmt.Errorf("stat opened placement database identity: %w", err))
	}
	if err := validatePlacementAuthorityFile(path, openedInfo); err != nil {
		return closeOnError(err)
	}
	currentInfo, err := os.Lstat(path)
	if err != nil {
		return closeOnError(fmt.Errorf("stat placement database path: %w", err))
	}
	if err := validatePlacementAuthorityFile(path, currentInfo); err != nil {
		return closeOnError(err)
	}
	if !os.SameFile(openedInfo, currentInfo) {
		return closeOnError(fmt.Errorf(
			"%w: path %q does not name the opened regular file",
			ErrRuntimeAuthorityPathChanged,
			path,
		))
	}
	if expected != nil && !os.SameFile(expected, openedInfo) {
		return closeOnError(fmt.Errorf(
			"%w: path %q changed after bbolt opened it",
			ErrRuntimeAuthorityPathChanged,
			path,
		))
	}
	return path, file, nil
}

// runtimeAuthorityFailure returns the first process-fatal authority failure.
// It never probes the filesystem; callers use reattestRuntimeAuthority when a
// fresh pathname proof is required.
func (s *Store) runtimeAuthorityFailure() error {
	if s == nil || s.runtimeAuthorityGate == nil || !s.runtimeAuthorityGate.Valid() {
		return ErrRuntimeAuthorityUnavailable
	}
	s.runtimeAuthorityMu.RLock()
	closed := s.runtimeAuthorityClosed
	s.runtimeAuthorityMu.RUnlock()
	if closed {
		return errRuntimeAuthorityClosed
	}
	return s.runtimeAuthorityGate.Error()
}

func (s *Store) latchRuntimeAuthorityFailure(cause error) error {
	if cause == nil {
		cause = errors.New("unknown placement authority failure")
	}
	if s == nil {
		return fmt.Errorf("%w: %w", ErrRuntimeAuthorityUnavailable, cause)
	}

	s.runtimeAuthorityMu.RLock()
	if s.runtimeAuthorityClosed {
		s.runtimeAuthorityMu.RUnlock()
		return errRuntimeAuthorityClosed
	}
	gate := s.runtimeAuthorityGate
	s.runtimeAuthorityMu.RUnlock()
	if gate == nil || !gate.Valid() {
		return fmt.Errorf("%w: %w", ErrRuntimeAuthorityUnavailable, cause)
	}
	return gate.Withdraw(runtimeAuthorityTerminalFailure(cause))
}

func runtimeAuthorityTerminalFailure(cause error) error {
	if errors.Is(cause, ErrRuntimeAuthorityUnavailable) {
		return cause
	}
	return fmt.Errorf("%w: %w", ErrRuntimeAuthorityUnavailable, cause)
}

// reattestRuntimeAuthority proves that the configured pathname still names the
// exact inode retained when the Store opened. Any inability to prove identity
// permanently withdraws this process's placement authority.
func (s *Store) reattestRuntimeAuthority() error {
	if s == nil {
		return ErrRuntimeAuthorityUnavailable
	}
	if err := s.runtimeAuthorityFailure(); err != nil {
		return err
	}
	if err := s.probeRuntimeAuthority(); err != nil {
		return s.latchRuntimeAuthorityFailure(err)
	}
	return s.runtimeAuthorityFailure()
}

// probeRuntimeAuthority performs the filesystem proof without consulting or
// publishing the gate. A write admitted by updateRuntimeAuthority calls it while
// holding that gate, avoiding recursive locking if the probe itself fails.
func (s *Store) probeRuntimeAuthority() error {
	if s == nil {
		return errors.New("placement database identity is not bound")
	}
	s.runtimeAuthorityMu.RLock()
	closed := s.runtimeAuthorityClosed
	path := s.runtimeAuthorityPath
	file := s.runtimeAuthorityFile
	s.runtimeAuthorityMu.RUnlock()
	if closed {
		return errRuntimeAuthorityClosed
	}
	if path == "" || file == nil {
		return errors.New("placement database identity is not bound")
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened placement database identity: %w", err)
	}
	if err := validatePlacementAuthorityFile(path, openedInfo); err != nil {
		return fmt.Errorf("%w: %w", ErrRuntimeAuthorityPathChanged, err)
	}
	currentInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: stat %q: %w", ErrRuntimeAuthorityPathChanged, path, err)
	}
	if err := validatePlacementAuthorityFile(path, currentInfo); err != nil {
		return fmt.Errorf("%w: %w", ErrRuntimeAuthorityPathChanged, err)
	}
	if !os.SameFile(openedInfo, currentInfo) {
		return fmt.Errorf(
			"%w: path %q no longer names the opened regular file",
			ErrRuntimeAuthorityPathChanged,
			path,
		)
	}
	return nil
}

func validatePlacementAuthorityFile(path string, info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() {
		return fmt.Errorf("placement database %q is not a regular file", path)
	}
	if info.Mode() != 0o600 {
		return fmt.Errorf("placement database %q must have exact mode 0600", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return fmt.Errorf("placement database %q must have exactly one hard link", path)
	}
	return nil
}

func openPlacementAuthorityFileNoFollow(
	path string,
	flag int,
	mode os.FileMode,
) (*os.File, error) {
	fd, err := unix.Open(path, flag|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("construct placement database file")
	}
	return file, nil
}

// classifyRuntimeAuthorityUpdateError converts the only indeterminate write
// result into a sticky process-fatal authority failure. Begin/assembly errors
// are definitely uncommitted and remain ordinary retryable errors.
func (s *Store) classifyRuntimeAuthorityUpdateError(err error) error {
	if err == nil || !errors.Is(err, errBoltCommitOutcomeUnknown) {
		return err
	}
	return runtimeAuthorityTerminalFailure(err)
}

// updateRuntimeAuthority is the sole runtime placement write boundary. It
// re-attests both sides of the transaction and makes an indeterminate bbolt
// commit withdraw all authority before any caller may update its cache.
func (s *Store) updateRuntimeAuthority(mutate func(*bolt.Tx) error) error {
	if s == nil || mutate == nil {
		return errors.New("placement store and write transaction are required")
	}
	gate := s.runtimeAuthorityGate
	if gate == nil || !gate.Valid() {
		return ErrRuntimeAuthorityUnavailable
	}
	return gate.Run(func() error {
		if err := s.probeRuntimeAuthority(); err != nil {
			return runtimeAuthorityTerminalFailure(err)
		}
		updateErr := s.classifyRuntimeAuthorityUpdateError(
			updateBoltWithExplicitOutcome(s.db, mutate),
		)
		identityErr := s.probeRuntimeAuthority()
		if identityErr != nil {
			identityErr = runtimeAuthorityTerminalFailure(identityErr)
		}
		return errors.Join(updateErr, identityErr)
	})
}

func (s *Store) viewRuntimeAuthority(view func(*bolt.Tx) error) error {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return err
	}
	viewErr := s.db.View(view)
	identityErr := s.reattestRuntimeAuthority()
	if viewErr != nil && identityErr != nil {
		return errors.Join(viewErr, identityErr)
	}
	if viewErr != nil {
		return viewErr
	}
	return identityErr
}

func unavailablePlacement() Placement {
	return Placement{unusable: true}
}
