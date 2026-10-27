//go:build integration

package docker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// nonRootBackendUID and nonRootBackendGID stand in for the service account
// production runs docker-backend as (manifest-deploy's fred user).
const nonRootBackendUID, nonRootBackendGID = 65534, 65534

// asNonRootBackend runs fn with the filesystem identity of the production
// docker-backend: a non-root account holding the unit's ambient file
// capabilities (CAP_CHOWN, CAP_DAC_OVERRIDE, CAP_FOWNER). The integration suite
// runs as root, where MkdirAll creates root-owned directories and hides
// ENG-2091. The filesystem identity and capabilities are per-thread, so fn runs
// on a dedicated locked OS thread that is never unlocked and exits with its
// goroutine, taking that identity with it.
func asNonRootBackend[T any](fn func() (T, error)) (T, error) {
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		var r result
		if r.err = assumeNonRootBackendIdentity(); r.err == nil {
			r.value, r.err = fn()
		}
		done <- r
	}()
	r := <-done
	return r.value, r.err
}

func assumeNonRootBackendIdentity() error {
	if err := unix.Setfsgid(nonRootBackendGID); err != nil {
		return fmt.Errorf("setfsgid: %w", err)
	}
	if err := unix.Setfsuid(nonRootBackendUID); err != nil {
		return fmt.Errorf("setfsuid: %w", err)
	}
	// An invalid ID changes nothing and reports the current one.
	if fsuid, _ := unix.SetfsuidRetUid(-1); fsuid != nonRootBackendUID {
		return fmt.Errorf("fsuid is %d, want %d", fsuid, nonRootBackendUID)
	}
	if fsgid, _ := unix.SetfsgidRetGid(-1); fsgid != nonRootBackendGID {
		return fmt.Errorf("fsgid is %d, want %d", fsgid, nonRootBackendGID)
	}
	// Leaving fsuid 0 clears the file capabilities from the effective set.
	// They remain permitted, so raise back exactly the ones the unit grants.
	hdr := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var data [2]unix.CapUserData
	if err := unix.Capget(&hdr, &data[0]); err != nil {
		return fmt.Errorf("capget: %w", err)
	}
	for _, c := range []uint{unix.CAP_CHOWN, unix.CAP_DAC_OVERRIDE, unix.CAP_FOWNER} {
		data[c/32].Effective |= 1 << (c % 32)
	}
	if err := unix.Capset(&hdr, &data[0]); err != nil {
		return fmt.Errorf("capset: %w", err)
	}
	return nil
}

// TestIntegration_StatefulVolume_RootContainerWritesUnderNonRootBackend is
// the ENG-2091 regression. Production runs docker-backend as a non-root account
// and every tenant container with all capabilities dropped. A root container
// (image USER root, or no USER and a root-owned VOLUME, both of which resolve
// to 0:0) then has no DAC override, so it can write its stateful VOLUME only if
// the bind source is owned by root. The backend used to skip the chown for
// 0:0 and leave the 0700 subdir owned by its own account.
//
// The bind is built with the backend's real identity, then mounted into a
// container launched through the production Compose path, as root, and
// written to. The second case is a subdir an earlier deploy left owned by the
// backend: the next deploy must re-own it.
func TestIntegration_StatefulVolume_RootContainerWritesUnderNonRootBackend(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root to assume the backend's non-root identity")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	// fred provisions with PullPolicy=never, so the image must be present before Up.
	require.NoError(t, docker.PullImage(ctx, "busybox:latest", 60*time.Second))
	composeSvc, err := newComposeService("")
	require.NoError(t, err)

	for _, tc := range []struct {
		name          string
		leftByBackend bool
	}{
		{name: "fresh volume"},
		{name: "subdir left owned by the backend", leftByBackend: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostPath := t.TempDir()
			dataDir := filepath.Join(hostPath, "data")
			if tc.leftByBackend {
				require.NoError(t, os.Mkdir(dataDir, 0o700))
				require.NoError(t, os.Chown(dataDir, nonRootBackendUID, nonRootBackendGID))
			}

			binds, err := asNonRootBackend(func() (map[string]string, error) {
				return buildStatefulVolumeBinds(hostPath, []string{"/data"}, 0, 0)
			})
			require.NoError(t, err)
			require.Equal(t, map[string]string{dataDir: "/data"}, binds)
			assertBindSourcesOwnedBy(t, binds, 0, 0)

			params := baseProjectParams()
			params.LeaseUUID = fmt.Sprintf("eng2091-rootvol-%d", time.Now().UnixNano())
			params.NetworkName = "" // use compose's default network; no pre-created tenant net
			params.Stack.Services["web"] = &manifest.Manifest{Image: "busybox:latest", Command: []string{"sleep", "3600"}}
			params.ImageSetups["web"] = &imageSetup{Volumes: []string{"/data"}}
			params.VolBinds = map[string]map[int]serviceVolBinds{"web": {0: {StatefulBinds: binds}}}
			projectName := composeProjectName(params.LeaseUUID)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
				defer cancel()
				_ = composeSvc.Down(ctx, projectName, time.Second)
			})
			require.NoError(t, composeSvc.Up(ctx, buildComposeProject(params), composeUpOpts{}))

			execInContainer(t, "fred-"+params.LeaseUUID+"-web-0", []string{"touch", "/data/written-by-root"})
			require.FileExists(t, filepath.Join(dataDir, "written-by-root"))
		})
	}
}
