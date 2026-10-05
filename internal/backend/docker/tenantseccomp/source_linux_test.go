//go:build linux

package tenantseccomp

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// newTestOwner gives a test its own owner and sealed file, so it can damage
// the descriptor without touching the process-wide one.
func newTestOwner(t *testing.T) (*processOwner, Profile) {
	t.Helper()
	owner := &processOwner{}
	profile, err := owner.acquire()
	require.NoError(t, err)
	t.Cleanup(func() {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if owner.file.open && owner.file.verify() == nil {
			_ = unix.Close(owner.file.fd)
		}
	})
	return owner, profile
}

func descriptorOf(t *testing.T, path string) int {
	t.Helper()
	number, ok := strings.CutPrefix(path, "/proc/self/fd/")
	require.True(t, ok, "%q is not a descriptor path", path)
	fd, err := strconv.Atoi(number)
	require.NoError(t, err)
	return fd
}

func TestSealedProfileFileCarriesEverySeal(t *testing.T) {
	_, profile := newTestOwner(t)
	path, err := profile.MemfdPath()
	require.NoError(t, err)
	seals, err := unix.FcntlInt(uintptr(descriptorOf(t, path)), unix.F_GET_SEALS, 0)
	require.NoError(t, err)
	const want = unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_WRITE | unix.F_SEAL_SEAL
	require.Equal(t, want, seals&want, "seals %#x", seals)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, profile.CompactJSON(), content)
	require.Equal(t, profile.Digest(), digestOf(content))

	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err == nil {
		_, err = file.Write([]byte("x"))
		_ = file.Close()
	}
	require.Error(t, err, "a sealed profile file must not accept writes")
}

// The descriptor is a raw int on purpose: nothing may finalize it.
func TestSealedProfileFileSurvivesGarbageCollection(t *testing.T) {
	_, profile := newTestOwner(t)
	before, err := profile.MemfdPath()
	require.NoError(t, err)
	for range 3 {
		runtime.GC()
	}
	after, err := profile.MemfdPath()
	require.NoError(t, err)
	require.Equal(t, before, after, "a still-valid descriptor must not be replaced")
	content, err := os.ReadFile(after)
	require.NoError(t, err)
	require.Equal(t, profile.CompactJSON(), content)
}

func TestSealedProfileFileIsRecreatedWhenItsDescriptorChanges(t *testing.T) {
	owner, profile := newTestOwner(t)
	original, err := profile.MemfdPath()
	require.NoError(t, err)
	fd := descriptorOf(t, original)

	// Point the recorded number at an unrelated file, as a stray close and
	// reuse would. The owner must notice, leave that descriptor alone, and
	// make a new sealed file.
	unrelated, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer func() { _ = unrelated.Close() }()
	require.NoError(t, unix.Dup2(int(unrelated.Fd()), fd))
	t.Cleanup(func() { _ = unix.Close(fd) })

	replaced, err := profile.MemfdPath()
	require.NoError(t, err)
	require.NotEqual(t, original, replaced)
	content, err := os.ReadFile(replaced)
	require.NoError(t, err)
	require.Equal(t, profile.CompactJSON(), content)

	var unrelatedStat, recordedStat unix.Stat_t
	require.NoError(t, unix.Fstat(int(unrelated.Fd()), &unrelatedStat))
	require.NoError(t, unix.Fstat(fd, &recordedStat))
	require.Equal(t, unrelatedStat.Ino, recordedStat.Ino, "the replaced descriptor must not be closed by the owner")

	again, err := owner.acquire()
	require.NoError(t, err)
	againPath, err := again.MemfdPath()
	require.NoError(t, err)
	require.Equal(t, replaced, againPath)
}

func TestProcessSourceSharesOneSealedFile(t *testing.T) {
	first, err := Process().TenantSeccompProfile()
	require.NoError(t, err)
	second, err := Process().TenantSeccompProfile()
	require.NoError(t, err)
	firstPath, err := first.MemfdPath()
	require.NoError(t, err)
	secondPath, err := second.MemfdPath()
	require.NoError(t, err)
	require.Equal(t, firstPath, secondPath)
	require.Equal(t, first.Digest(), second.Digest())
}

func TestZeroValuesRefuse(t *testing.T) {
	_, err := Source{}.TenantSeccompProfile()
	require.ErrorIs(t, err, ErrRefused)
	_, err = Profile{}.MemfdPath()
	require.ErrorIs(t, err, ErrRefused)
	require.Nil(t, Profile{}.CompactJSON())
	require.Equal(t, Digest{}, Profile{}.Digest())
	_, err = InlineSecurityOpt(nil, Profile{})
	require.ErrorIs(t, err, ErrRefused)
	_, err = FileSecurityOpt(nil, Profile{})
	require.ErrorIs(t, err, ErrRefused)

	_, profile := newTestOwner(t)
	body := createRequestBody(t, []string{"seccomp=" + string(profile.CompactJSON())}, false)
	require.ErrorIs(t, VerifyCreateRequest(body, Digest{}), ErrRefused, "the zero Digest matches nothing")
	require.False(t, Applied([]string{"seccomp=" + string(profile.CompactJSON())}, false, Digest{}))
}

func TestProfileFromAnotherOwnerIsRefused(t *testing.T) {
	_, mine := newTestOwner(t)
	other, _ := newTestOwner(t)
	_, err := other.path(mine.r)
	require.ErrorIs(t, err, ErrRefused)
}
