package docker

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// actorFor resolves the lease actor for leaseUUID, creating and starting
// one if absent. Test-only: production code uses routeToLease to deliver
// messages without ever exposing an actor pointer to the caller. Tests
// retain direct access for synthetic scenario setup (installing
// workers entries, poking SM state, asserting invariants) that can't
// go through the message path.
func (b *Backend) actorFor(leaseUUID string) *leasesm.LeaseActor {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	return b.actorForLocked(leaseUUID)
}

// handleContainerDeath synchronously dispatches a container death to the
// owning lease's actor and waits for processing to complete. Exists as a
// test helper so direct-call unit tests can keep their synchronous
// assertion style; production code routes die events via
// b.routeToLease(uuid, leasesm.ContainerDiedMsg{...}).
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}
	done := make(chan struct{})
	if !b.routeToLease(leaseUUID, leasesm.ContainerDiedMsg{ContainerID: containerID, Done: done}) {
		return
	}
	select {
	case <-done:
	case <-b.stopCtx.Done():
	}
}

// --- Migration test fixtures (Task 1, plan §Task 1.4) -----------------------
//
// These fakes back the recover-time-migration tests in migrate_test.go. They
// model the three substrates that migration touches (docker engine + compose,
// volume backend, release store) so tests can hand-craft legacy-shaped state
// and assert what the migration pipeline did with it.
//
// The fakes intentionally implement just enough of each interface for the
// migration paths to compile and run end-to-end; behaviour beyond that (e.g.
// concurrent Up calls, partial Down failures) is not modelled because the
// migration tests don't exercise it.

// fakeDocker is the shared test-side state object referenced from the
// fakeDockerClient and fakeComposeExecutor wired into the Backend by
// newMigrationTestBackend. Tests set fields on this struct to control mock
// behaviour, and read them to assert what production code did.
//
// Lumping docker-engine and compose state into a single struct keeps the
// migration tests in the plan readable (one fake to set up, one fake to
// assert against).
type fakeDocker struct {
	// Docker engine side.
	containers []ContainerInfo               // returned by ListManagedContainers
	mounts     map[string][]ContainerMount   // containerID → mounts (test-only)

	// Compose side.
	composeUpErr           error  // returned by composeExecutor.Up if non-nil
	lastComposeProjectName string // captured project.Name from the most recent Up call
}

// fakeVolumeBackend records RenameVolume calls and stubs the rest of the
// volumeManager interface with no-ops. RenameVolume is the new method
// introduced by Task 10; the migration logic (Task 9) will call it via a
// type assertion so this fake compiles cleanly today.
type fakeVolumeBackend struct {
	renames [][2]string // (oldName, newName) pairs, in call order
}

// Create returns a deterministic path so any production code that calls it
// during a test does not blow up; the migration tests do not assert on it.
func (f *fakeVolumeBackend) Create(_ context.Context, id string, _ int64) (string, bool, error) {
	return filepath.Join("/var/lib/fred/volumes", id), true, nil
}

func (f *fakeVolumeBackend) Destroy(_ context.Context, _ string) error { return nil }
func (f *fakeVolumeBackend) List() ([]string, error)                   { return nil, nil }
func (f *fakeVolumeBackend) Validate() error                           { return nil }

// RenameVolume captures the rename request. Returns nil unconditionally —
// migration tests assert on the recorded renames slice rather than on a
// returned error.
func (f *fakeVolumeBackend) RenameVolume(oldName, newName string) error {
	f.renames = append(f.renames, [2]string{oldName, newName})
	return nil
}

// renamed reports whether (oldName → newName) appears in the rename log.
func (f *fakeVolumeBackend) renamed(oldName, newName string) bool {
	for _, r := range f.renames {
		if r[0] == oldName && r[1] == newName {
			return true
		}
	}
	return false
}

// HostPath returns a deterministic test path under /var/lib/fred/volumes
// matching the convention production code uses. Tests asserting on
// migration bind paths can predict the value.
func (f *fakeVolumeBackend) HostPath(name string) string {
	return filepath.Join("/var/lib/fred/volumes", name)
}

// fakeReleaseStore wraps a real *shared.ReleaseStore with the test-side
// helpers expected by the migration tests. The wrapped store is real so the
// production code path (which talks to *shared.ReleaseStore directly) is
// exercised; `releases` is a setup map that tests populate and Seed flushes
// into the backing store.
type fakeReleaseStore struct {
	Store    *shared.ReleaseStore
	releases map[string][]byte // leaseUUID → manifest payload to pre-seed
}

// Seed flushes the test-side `releases` map into the backing release store
// as "active" entries dated now. Call this after populating `releases` and
// before invoking the production code under test.
func (f *fakeReleaseStore) Seed(t *testing.T) {
	t.Helper()
	for uuid, data := range f.releases {
		require.NoError(t, f.Store.Append(uuid, shared.Release{
			Manifest:  data,
			Status:    "active",
			CreatedAt: time.Now(),
		}))
	}
}

// hasWrappedRelease reports whether the latest release for uuid carries a
// stack-shaped manifest (auto-wrapped or natively stack). Heuristic: look
// for the top-level "services" key in the stored JSON.
func (f *fakeReleaseStore) hasWrappedRelease(uuid string) bool {
	rel, err := f.Store.LatestActive(uuid)
	if err != nil || rel == nil {
		return false
	}
	return bytes.Contains(rel.Manifest, []byte(`"services"`))
}

// newMigrationTestBackend constructs a Backend wired with the migration-test
// fakes. Returns the Backend plus pointers to each fake so the test can drive
// its inputs and assert on captured outputs.
//
// The release store is real (backed by a bbolt DB in t.TempDir()) so any
// production read/write goes through the same paths as in production; closed
// automatically via t.Cleanup. Tests that seeded `fakeRel.releases` must
// invoke `fakeRel.Seed(t)` before triggering recoverState.
func newMigrationTestBackend(t *testing.T) (*Backend, *fakeDocker, *fakeVolumeBackend, *fakeReleaseStore) {
	t.Helper()

	state := &fakeDocker{mounts: make(map[string][]ContainerMount)}
	fakeVol := &fakeVolumeBackend{}

	dbPath := filepath.Join(t.TempDir(), "releases.db")
	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = relStore.Close() })
	fakeRel := &fakeReleaseStore{Store: relStore, releases: make(map[string][]byte)}

	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			// Splice in mounts from the shared state map so the
			// list payload matches what production code receives
			// from types.Container.Mounts inline.
			out := make([]ContainerInfo, len(state.containers))
			for i, c := range state.containers {
				if ms, ok := state.mounts[c.ContainerID]; ok {
					c.Mounts = append(c.Mounts, ms...)
				}
				out[i] = c
			}
			return out, nil
		},
		// StopContainer + RemoveContainer default to silent success so
		// the migration's stop-legacy / -prev-cleanup paths don't blow
		// up the test runtime (mockDockerClient.RemoveContainer panics
		// by default). RenameContainer default already returns nil
		// silently in the underlying mock.
		StopContainerFn: func(_ context.Context, _ string, _ time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(_ context.Context, _ string) error {
			return nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			for i := range state.containers {
				if state.containers[i].ContainerID == containerID {
					c := state.containers[i]
					// Splice in mounts from the shared state map.
					// Production InspectContainer populates Mounts
					// directly from resp.Mounts; the test mock has to
					// merge the test-side mounts setup since
					// state.containers entries are constructed with
					// just label-bearing fields.
					if ms, ok := state.mounts[containerID]; ok {
						c.Mounts = append(c.Mounts, ms...)
					}
					return &c, nil
				}
			}
			return nil, fmt.Errorf("not found: %s", containerID)
		},
	}

	fakeCompose := &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			if project == nil {
				return state.composeUpErr
			}
			state.lastComposeProjectName = project.Name
			if state.composeUpErr != nil {
				return state.composeUpErr
			}
			// Simulate compose successfully creating containers: append one
			// post-migration ContainerInfo per service in the project, with
			// Status:"running" so waitForHealthy doesn't block. Production
			// compose creates real containers; the test fake mirrors that
			// behaviour at the ContainerInfo abstraction so downstream code
			// (resolveContainerIDsByName, ListManagedContainers) sees a
			// post-Up state consistent with production semantics.
			for _, svc := range project.Services {
				if svc.ContainerName == "" {
					continue
				}
				state.containers = append(state.containers, ContainerInfo{
					ContainerID: "post-mig-" + svc.ContainerName,
					Name:        svc.ContainerName,
					Status:      "running",
					Health:      HealthStatusNone,
				})
			}
			return nil
		},
	}

	b := newBackendForTest(mock, nil)
	b.compose = fakeCompose
	b.volumes = fakeVol
	b.releaseStore = relStore
	// Migration tests assume managed volume sources live under this root
	// — matches the fixture mounts used by migrate_test.go.
	b.cfg.VolumeDataPath = "/var/lib/fred/volumes"
	// MigrationReadyTimeout must be short in tests so verifyStartup's
	// no-healthcheck path (fixed wait + inspect) doesn't bloat suite
	// runtime. StartupVerifyDuration covers the same bound at the
	// per-poll level.
	b.cfg.MigrationReadyTimeout = 500 * time.Millisecond
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	// MigrationGracePeriod: keep short so the background -prev cleanup
	// goroutine doesn't outlive t.Cleanup teardown.
	b.cfg.MigrationGracePeriod = 100 * time.Millisecond

	return b, state, fakeVol, fakeRel
}
