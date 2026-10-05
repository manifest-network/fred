//go:build linux

package tenantseccomp

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// memfdName labels the sealed file in /proc/<pid>/fd listings.
const memfdName = "fred-tenant-seccomp"

// Source hands out the process's tenant profile. Process returns the only
// usable Source; the zero Source refuses every request.
type Source struct{ owner *processOwner }

// process is the one owner in the process. Every Docker client shares it, so
// the process holds a single sealed file however many clients it builds.
var process processOwner

// Process returns the process-wide Source.
func Process() Source { return Source{owner: &process} }

// TenantSeccompProfile returns the verified profile once both the profile and
// its sealed file are usable. A failed build or file is attempted again on
// every call, under the owner's lock. Every error wraps ErrRefused.
func (s Source) TenantSeccompProfile() (Profile, error) {
	if s.owner == nil {
		return Profile{}, refusal("the profile source is the zero value")
	}
	return s.owner.acquire()
}

// processOwner builds the profile once and keeps its sealed file. Its zero
// value is ready; mu guards both fields.
type processOwner struct {
	mu     sync.Mutex
	record *profileRecord
	file   sealedFile
}

func (o *processOwner) acquire() (Profile, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.record == nil {
		compact, err := buildProfile()
		if err != nil {
			return Profile{}, fmt.Errorf("%w: build the profile: %w", ErrRefused, err)
		}
		o.record = &profileRecord{compact: compact, digest: digestOf(compact), owner: o}
	}
	if err := o.ensureFileLocked(); err != nil {
		return Profile{}, err
	}
	return Profile{r: o.record}, nil
}

func (o *processOwner) path(record *profileRecord) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if record == nil || record != o.record {
		return "", refusal("the profile belongs to another owner")
	}
	if err := o.ensureFileLocked(); err != nil {
		return "", err
	}
	return o.file.path(), nil
}

// ensureFileLocked keeps exactly one verified sealed file. When the recorded
// descriptor no longer names it, something else closed that descriptor and
// its number may already belong to another file, so it is never closed here;
// a new file replaces it.
func (o *processOwner) ensureFileLocked() error {
	if o.file.open {
		if o.file.verify() == nil {
			return nil
		}
		o.file = sealedFile{}
	}
	file, err := createSealedFile(o.record.compact)
	if err != nil {
		return fmt.Errorf("%w: create the sealed profile file: %w", ErrRefused, err)
	}
	o.file = file
	return nil
}

// sealedFile is a memfd holding the compact profile, sealed against every
// change, and the identity of its inode. The descriptor is kept as a raw int
// for the life of the process: an *os.File's finalizer would close it and let
// its number name an unrelated file.
type sealedFile struct {
	fd       int
	dev, ino uint64
	open     bool
}

func (f sealedFile) path() string { return "/proc/self/fd/" + strconv.Itoa(f.fd) }

// verify proves the descriptor still names the recorded inode.
func (f sealedFile) verify() error {
	if !f.open {
		return errors.New("the sealed profile file is not open")
	}
	info, err := os.Stat(f.path())
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Dev != f.dev || stat.Ino != f.ino {
		return errors.New("the profile descriptor names another file")
	}
	return nil
}

// createSealedFile writes content to a new memfd and seals it. MFD_CLOEXEC
// keeps it out of every child process, and MFD_ALLOW_SEALING is required to
// add any seal. MFD_NOEXEC_SEAL and MFD_EXEC are deliberately absent: Linux
// 5.15 rejects both.
func createSealedFile(content []byte) (sealedFile, error) {
	fd, err := unix.MemfdCreate(memfdName, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return sealedFile{}, fmt.Errorf("memfd_create: %w", err)
	}
	kept := false
	defer func() {
		if !kept {
			_ = unix.Close(fd)
		}
	}()
	for written := 0; written < len(content); {
		n, err := unix.Write(fd, content[written:])
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case err != nil:
			return sealedFile{}, fmt.Errorf("write: %w", err)
		case n <= 0:
			return sealedFile{}, errors.New("write made no progress")
		}
		written += n
	}
	const seals = unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_WRITE | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
		return sealedFile{}, fmt.Errorf("add seals: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return sealedFile{}, fmt.Errorf("fstat: %w", err)
	}
	if stat.Size != int64(len(content)) {
		return sealedFile{}, fmt.Errorf("sealed file holds %d bytes, want %d", stat.Size, len(content))
	}
	file := sealedFile{fd: fd, dev: stat.Dev, ino: stat.Ino, open: true}
	if err := file.verify(); err != nil {
		return sealedFile{}, fmt.Errorf("verify the sealed file: %w", err)
	}
	kept = true
	return file, nil
}
