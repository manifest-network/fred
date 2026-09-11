package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// writerRetirementHarness uses the production maintenance executor, an exact
// Started target/source pair, real directories, and the real launch journal.
// Only Docker and the volume-manager system calls are fixture implementations.
type writerRetirementHarness struct {
	h        *maintenanceRecoveryHarness
	docker   *mockDockerClient
	sources  []ContainerInfo
	binds    []string
	events   []string
	stops    int
	removes  int
	launches int
	beforeUp func()
}

func newWriterRetirementHarness(t *testing.T) *writerRetirementHarness {
	t.Helper()
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	f := &writerRetirementHarness{h: h, docker: h.b.docker.(*mockDockerClient), sources: h.containersFor(h.source, 2, "running", "")}
	root := t.TempDir()
	h.b.cfg.VolumeDataPath = root
	h.b.cfg.ContainerReadonlyRootfs = ptrBool(false)
	paths := make(map[string]string)
	identities := make(map[string]fsidentity.Identity)
	for index := range f.sources {
		name := canonicalVolumeName(h.leaseUUID, "web", index)
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Join(path, "data"), 0o700))
		identity, err := fsidentity.InspectDirectory(path)
		require.NoError(t, err)
		paths[name], identities[name] = path, identity
		f.binds = append(f.binds, filepath.Join(path, "data", "next"))
		f.sources[index].Mounts = []ContainerMount{{Type: "bind", Source: filepath.Join(path, "data"), Target: "/data"}}
	}
	h.inventory.containers = slices.Clone(f.sources)
	h.b.volumes = &mockVolumeManager{
		CreateFn: func(_ context.Context, name string, sizeMB int64) (string, bool, error) {
			path, ok := paths[name]
			require.True(t, ok, "launch can only reuse the exact source volume set")
			require.Positive(t, sizeMB)
			return path, false, nil
		},
		AttestManagedVolumeFn: func(_ context.Context, name managedVolumeName) error {
			path, ok := paths[name.value()]
			if !ok {
				return errors.New("foreign fixture volume")
			}
			identity, err := fsidentity.InspectDirectory(path)
			if err != nil {
				return err
			}
			if !identity.Equal(identities[name.value()]) {
				return errors.New("fixture volume identity changed")
			}
			return nil
		},
	}
	f.docker.InspectImageFn = func(context.Context, string) (*ImageInfo, error) {
		// A new image requires a nested bind that the old /data writer could
		// exchange. It must not be materialized while that writer can run.
		return &ImageInfo{ID: fixtureImageID("writer-retirement-target"), User: fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), Volumes: map[string]struct{}{"/data/next": {}}}, nil
	}
	f.docker.ResolveImageUserFn = func(context.Context, string, string) (int, int, error) { return os.Getuid(), os.Getgid(), nil }
	f.docker.ListVolumeWritersFn = h.inventory.list
	f.docker.InspectContainerFn = func(_ context.Context, id string) (*ContainerInfo, error) {
		h.inventory.mu.Lock()
		defer h.inventory.mu.Unlock()
		for _, info := range h.inventory.containers {
			if info.ContainerID == id {
				f.events = append(f.events, "inspect:"+id+":"+info.Status)
				return &info, nil
			}
		}
		f.events = append(f.events, "absent:"+id)
		return nil, errdefs.ErrNotFound
	}
	f.docker.StopContainerFn = func(_ context.Context, id string, _ time.Duration) error {
		f.requireUnprepared(t)
		f.events = append(f.events, "stop:"+id)
		f.stops++
		h.inventory.mu.Lock()
		defer h.inventory.mu.Unlock()
		for index := range h.inventory.containers {
			if h.inventory.containers[index].ContainerID == id {
				h.inventory.containers[index].Status = "exited"
				return nil
			}
		}
		return errdefs.ErrNotFound
	}
	f.docker.RemoveContainerFn = func(ctx context.Context, id string) error {
		f.requireUnprepared(t)
		f.events = append(f.events, "remove:"+id)
		f.removes++
		return h.inventory.remove(ctx, id)
	}
	h.b.compose = &mockComposeExecutor{UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
		f.events = append(f.events, "up")
		f.launches++
		if f.beforeUp != nil {
			f.beforeUp()
		}
		for _, source := range f.sources {
			stop := slices.Index(f.events, "stop:"+source.ContainerID)
			stopped := slices.Index(f.events, "inspect:"+source.ContainerID+":exited")
			removed := slices.Index(f.events, "remove:"+source.ContainerID)
			absent := slices.Index(f.events, "absent:"+source.ContainerID)
			require.GreaterOrEqual(t, stop, 0)
			require.Greater(t, stopped, stop)
			require.Greater(t, removed, stopped)
			require.Greater(t, absent, removed)
			require.Greater(t, slices.Index(f.events, "up"), absent)
		}
		var mounted []string
		for _, service := range project.Services {
			for _, mount := range service.Volumes {
				if mount.Type == composetypes.VolumeTypeBind {
					mounted = append(mounted, mount.Source)
					info, err := os.Lstat(mount.Source)
					require.NoError(t, err)
					require.True(t, info.IsDir(), "a previously running writer cannot exchange the bind leaf before Up")
				}
			}
		}
		require.ElementsMatch(t, f.binds, mounted)
		return errors.New("fixture ends after observing the SDK launch boundary")
	}}
	return f
}

func (f *writerRetirementHarness) requireUnprepared(t *testing.T) {
	t.Helper()
	for _, path := range f.binds {
		_, err := os.Lstat(path)
		require.ErrorIs(t, err, os.ErrNotExist, "bind preparation must follow positive retirement of every prior writer")
	}
}

func (f *writerRetirementHarness) execute(t *testing.T) {
	t.Helper()
	execution, err := f.h.b.maintenanceSettlement.StartMaintenanceExecution(f.h.target)
	require.NoError(t, err)
	_ = f.h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution)
}

func TestVolumeWriterRetirementPrecedesBindPreparationAndLaunch(t *testing.T) {
	f := newWriterRetirementHarness(t)
	f.execute(t)
	require.Equal(t, 2, f.stops)
	require.Equal(t, 2, f.removes)
	require.Equal(t, 1, f.launches)
}

func TestVolumeWriterInterferencePreventsEveryRetirement(t *testing.T) {
	for _, scenario := range []string{"foreign tenant", "current target"} {
		t.Run(scenario, func(t *testing.T) {
			f := newWriterRetirementHarness(t)
			interference := f.sources[0]
			if scenario == "foreign tenant" {
				interference.Tenant = "foreign"
			} else {
				interference = f.h.containersFor(f.h.targetRelease, 1, "running", "")[0]
				interference.Mounts = f.sources[0].Mounts
			}
			// Sort after exact source IDs: every source is classified first,
			// making this catch retirement performed during partial validation.
			interference.ContainerID = "zz-unretirable-writer"
			f.h.inventory.containers = append(f.h.inventory.containers, interference)
			f.execute(t)
			require.Zero(t, f.stops)
			require.Zero(t, f.removes)
			require.Zero(t, f.launches)
			f.requireUnprepared(t)
		})
	}
}

func TestVolumeWriterUncertainRetirementPreventsPreparationAndLaunch(t *testing.T) {
	for _, scenario := range []string{"stop error", "still running", "remove reply lost", "absence unproven"} {
		t.Run(scenario, func(t *testing.T) {
			f := newWriterRetirementHarness(t)
			switch scenario {
			case "stop error":
				f.docker.StopContainerFn = func(context.Context, string, time.Duration) error { f.stops++; return errors.New("stop unavailable") }
			case "still running":
				f.docker.StopContainerFn = func(context.Context, string, time.Duration) error { f.stops++; return nil }
			case "remove reply lost":
				remove := f.docker.RemoveContainerFn
				f.docker.RemoveContainerFn = func(ctx context.Context, id string) error {
					require.NoError(t, remove(ctx, id))
					return errors.New("remove reply lost after daemon effect")
				}
			case "absence unproven":
				f.docker.RemoveContainerFn = func(context.Context, string) error { f.removes++; return nil }
			}
			f.execute(t)
			require.Equal(t, 1, f.stops)
			if scenario == "stop error" || scenario == "still running" {
				require.Zero(t, f.removes)
			} else {
				require.Equal(t, 1, f.removes)
			}
			require.Zero(t, f.launches)
			f.requireUnprepared(t)
		})
	}
}

func TestVolumeWriterCannotExchangeLeafDuringLaterLaunch(t *testing.T) {
	f := newWriterRetirementHarness(t)
	requests := make(chan chan error)
	stop, done := make(chan struct{}), make(chan struct{})
	outside := t.TempDir()
	go func() {
		defer close(done)
		select {
		case <-stop:
		case result := <-requests:
			// This models the old container's writable /data mount: once a
			// nested source is prepared it can replace that leaf before Docker
			// resolves the new bind, unless retirement has revoked the writer.
			err := os.Rename(f.binds[0], f.binds[0]+"-exchanged")
			if err == nil {
				err = os.Symlink(outside, f.binds[0])
			}
			result <- err
		}
	}()
	stopWriter := sync.OnceFunc(func() { close(stop); <-done })
	t.Cleanup(stopWriter)
	stopContainer := f.docker.StopContainerFn
	f.docker.StopContainerFn = func(ctx context.Context, id string, timeout time.Duration) error {
		if id == f.sources[0].ContainerID {
			stopWriter()
		}
		return stopContainer(ctx, id, timeout)
	}
	f.beforeUp = func() {
		result := make(chan error, 1)
		select {
		case <-done:
		case requests <- result:
			require.NoError(t, <-result)
			t.Fatal("old writer survived bind preparation and exchanged the source at the launch boundary")
		}
	}
	f.execute(t)
	require.Equal(t, 1, f.launches)
	require.DirExists(t, f.binds[0])
	require.NoDirExists(t, f.binds[0]+"-exchanged")
}
