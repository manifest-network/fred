package docker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/fsidentity"
)

func emptyVolumeLaunchCoordinatorForTest() *volumeLaunchCoordinator {
	return &volumeLaunchCoordinator{
		checkNamespace: func(string) error { return nil },
		pendingCount:   func() (int, error) { return 0, nil },
		check:          func(shared.VolumeLaunchOrigin, []fsidentity.Identity) error { return nil },
		compose: func(context.Context, *quiescedVolumes, imageexec.PreparedProject, composeUpOpts) error {
			return errors.New("full launch requires a real fixture journal")
		},
		source: func(context.Context, *quiescedVolumes, compensationStartup) error {
			return errors.New("full source launch requires a real fixture journal")
		},
	}
}

func quiescedVolumeForTest(t *testing.T, mutations *storageMutations, path string) *quiescedVolumes {
	t.Helper()
	installProtectedVolumeFixture(t, mutations.ops.backend, path)
	q, err := mutations.ops.backend.quiesceLaunchVolumes(context.Background(), mutations,
		map[string]string{filepath.Base(path): path}, nil, nil)
	require.NoError(t, err)
	return q
}

// These focused filesystem tests stop below the complete launch boundary.
// Their fixture attests one actual directory and has no prior Docker launches;
// attempting a full launch still requires a real operation journal.
func installProtectedVolumeFixture(t *testing.T, b *Backend, path string) {
	t.Helper()
	previousVolumes, previousLaunches, previousPath := b.volumes, b.volumeLaunches, b.cfg.VolumeDataPath
	t.Cleanup(func() {
		b.volumes, b.volumeLaunches, b.cfg.VolumeDataPath = previousVolumes, previousLaunches, previousPath
	})
	b.cfg.VolumeDataPath = filepath.Dir(path)
	b.volumes = &mockVolumeManager{
		defaultDir: filepath.Dir(path),
		AttestManagedVolumeFn: func(ctx context.Context, name managedVolumeName) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if name.value() != filepath.Base(path) {
				return errors.New("volume is outside the fixture's managed directory")
			}
			_, err := fsidentity.InspectDirectory(path)
			return err
		},
	}
	b.volumeLaunches = emptyVolumeLaunchCoordinatorForTest()
}

func (m *mockDockerClient) ListVolumeWriters(ctx context.Context) ([]ContainerInfo, error) {
	if m.ListVolumeWritersFn != nil {
		return m.ListVolumeWritersFn(ctx)
	}
	return m.ListManagedContainersStrict(ctx)
}

func (m *mockDockerClient) createCompensationContainer(ctx context.Context, image imageexec.Image, snapshot compensationContainer) (string, daemonLaunchOutcome) {
	if m.CreateCompensationOutcomeFn != nil {
		return m.CreateCompensationOutcomeFn(ctx, image, snapshot)
	}
	if m.CreateCompensationContainerFn != nil {
		id, err := m.CreateCompensationContainerFn(ctx, image, snapshot)
		return id, daemonLaunchOutcome{settled: err == nil, err: err}
	}
	return "", daemonLaunchOutcome{err: errors.New("source compensation creation is not configured in this fixture")}
}

func (m *mockDockerClient) startCompensationContainer(ctx context.Context, id string, timeout time.Duration) daemonLaunchOutcome {
	if m.StartCompensationOutcomeFn != nil {
		return m.StartCompensationOutcomeFn(ctx, id, timeout)
	}
	err := m.StartContainer(ctx, id, timeout)
	return daemonLaunchOutcome{settled: err == nil, err: err}
}

func (m *mockDockerClient) readmitCompensationImage(ctx context.Context, snapshot compensationContainerRecord) (imageexec.Image, error) {
	if m.ReadmitCompensationImageFn != nil {
		return m.ReadmitCompensationImageFn(ctx, snapshot)
	}
	return m.AdmitImage(ctx, snapshot.ImageID)
}

func (p testDockerMutationProxy) createCompensationContainer(ctx context.Context, image imageexec.Image, snapshot compensationContainer) (string, daemonLaunchOutcome) {
	sink, err := p.sink()
	if err != nil {
		return "", daemonLaunchOutcome{settled: true, err: err}
	}
	return sink.createCompensationContainer(ctx, image, snapshot)
}

func (p testDockerMutationProxy) startCompensationContainer(ctx context.Context, id string, timeout time.Duration) daemonLaunchOutcome {
	sink, err := p.sink()
	if err != nil {
		return daemonLaunchOutcome{settled: true, err: err}
	}
	return sink.startCompensationContainer(ctx, id, timeout)
}

func (p testDockerMutationProxy) readmitCompensationImage(ctx context.Context, snapshot compensationContainerRecord) (imageexec.Image, error) {
	sink, err := p.sink()
	if err != nil {
		return imageexec.Image{}, err
	}
	return sink.readmitCompensationImage(ctx, snapshot)
}
