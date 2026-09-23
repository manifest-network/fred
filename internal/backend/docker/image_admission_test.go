package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// The admission runs inside a real Started operation with its journal-bound
// physical subject, so preparation must select and publish an authorized pin.
func imagePreparationExecutions(t *testing.T, m *imageCapacityManager, refs map[string]string, results chan<- string) map[string]func() {
	t.Helper()
	callbacks, err := newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = callbacks.Close() })
	releases, retentions, settlement, _, _ := operationHandoffForCallbackTest(t, callbacks)
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok)
	authority := value.(*operationIntentTestAuthority)
	b := &Backend{stopCtx: t.Context(), storageIdentity: authority.storage.ID(), storeAuthorityGate: authority.gate,
		storageVerifier: testDockerRuntimeStorageVerifier{id: authority.storage.ID()}}
	m.pins, err = shared.NewImagePinJournal(callbacks, releases, retentions)
	require.NoError(t, err)
	require.NoError(t, shared.BindOperationSubstrateExecutor(settlement, b.authorizeStorageMutation, b.completeStorageMutation,
		func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) func(context.Context) error {
			return func(ctx context.Context) error {
				return runner.Prepare(ctx, "prepare image", func(ctx context.Context) error {
					mutations := &storageMutations{operationSubject: subject, inspectionOrigin: shared.ImageInspectionForOperation(subject), leaseUUID: subject.LeaseUUID()}
					_, err := m.prepare(ctx, mutations, refs[subject.LeaseUUID()], true)
					if err != nil {
						results <- err.Error()
					} else {
						results <- subject.LeaseUUID()
					}
					return err
				})
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ shared.OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(context.Context, shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			return shared.OperationPhysicalEvidence{}, errors.New("fixture leaves parent operation pending")
		},
	))
	runs := make(map[string]func())
	for lease, ref := range refs {
		spec := dockerOperationIntentSpec(t, authority.storage.ID())
		spec.LeaseUUID = lease
		spec.Manifest = validStackManifestJSON(map[string]string{"app": ref})
		candidate, err := settlement.NewOperationIntentCandidate(spec)
		require.NoError(t, err)
		admission, err := settlement.BeginOperationIntent(candidate)
		require.NoError(t, err)
		claim, ok := admission.CreatedClaim()
		require.True(t, ok)
		release, err := settlement.PrepareOperationRelease(claim)
		require.NoError(t, err)
		execution, err := settlement.StartOperationExecution(release)
		require.NoError(t, err)
		runs[lease] = func() { _ = settlement.ExecuteOperation(t.Context(), execution) }
	}
	return runs
}

func TestImageCapacitySlowRegistryDoesNotBlockCachedTenantPreparation(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseRegistry := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseRegistry)
	registryHandler := registry.New()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/slow/manifests/") {
			close(blocked)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		registryHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseRegistry)
	slowRef := strings.TrimPrefix(server.URL, "https://") + "/slow:latest"
	warmRef := strings.TrimPrefix(server.URL, "https://") + "/warm:latest"
	fixture := imageCapacityRegistryImage(t, "verified cached content")
	for _, ref := range []string{slowRef, warmRef} {
		tag, err := name.NewTag(ref)
		require.NoError(t, err)
		require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
	}
	configID, err := fixture.ConfigName()
	require.NoError(t, err)
	m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		return &ImageInfo{ID: configID.String()}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: configID.String(), Size: imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(context.Context, io.Reader) (image.LoadResponse, error) {
		return image.LoadResponse{}, errors.New("cached image was unexpectedly imported")
	}, imagefetch.WithRegistryTransport(server.Client().Transport))
	const slowLease = "550e8400-e29b-41d4-a716-446655440001"
	const warmLease = "550e8400-e29b-41d4-a716-446655440002"
	results := make(chan string, 2)
	runs := imagePreparationExecutions(t, m, map[string]string{slowLease: slowRef, warmLease: warmRef}, results)
	var workers sync.WaitGroup
	workers.Go(runs[slowLease])
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("slow registry request was not entered")
	}
	workers.Go(runs[warmLease])
	select {
	case result := <-results:
		require.Equal(t, warmLease, result, "a registry wait must not own another lease's admission gate")
	case <-time.After(5 * time.Second):
		releaseRegistry()
		workers.Wait()
		t.Fatal("cached tenant preparation blocked behind another registry")
	}
	pins, err := m.pins.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, warmLease, pins[0].LeaseUUID)
	releaseRegistry()
	workers.Wait()
	require.Equal(t, slowLease, <-results)
	require.Zero(t, m.active)
}

func TestImageCapacityPullReusesUnpinnedLocalDigestAboveNewImageLimit(t *testing.T) {
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/existing:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, strings.Repeat("existing bytes", 200_000))
	require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
	manifestID, err := fixture.Digest()
	require.NoError(t, err)
	configID, err := fixture.ConfigName()
	require.NoError(t, err)
	digestRef := tag.Context().Digest(manifestID.String()).Name()
	for _, test := range []struct {
		name      string
		size      int64
		lowSpace  bool
		wantError string
	}{
		{name: "already extracted", size: 3 * imageMiB},
		{name: "invalid reported size", size: -1, wantError: "negative image size"},
		{name: "insufficient headroom", size: 3 * imageMiB, lowSpace: true, wantError: "image disk admission"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, daemon, fs := imageCapacityFixture(t)
			m.cfg.ImageMaxSizeMB = 1
			if test.lowSpace {
				fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
			}
			var digestInspections, imports int
			m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
				switch inspected {
				case digestRef:
					digestInspections++
				case configID.String():
				default:
					return nil, errors.New("local admission unexpectedly resolved a mutable reference")
				}
				return &ImageInfo{ID: configID.String()}, nil
			}}).imageAdmitter()
			daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
				return image.InspectResponse{ID: configID.String(), Size: test.size}, nil
			}
			attachImageCapacityLoader(t, m, func(context.Context, io.Reader) (image.LoadResponse, error) {
				imports++
				return image.LoadResponse{}, errors.New("already-local content was unexpectedly imported")
			}, imagefetch.WithRegistryTransport(server.Client().Transport))
			const lease = "550e8400-e29b-41d4-a716-446655440001"
			results := make(chan string, 1)
			runs := imagePreparationExecutions(t, m, map[string]string{lease: ref}, results)
			pins, err := m.pins.List()
			require.NoError(t, err)
			require.Empty(t, pins, "reuse must not depend on an existing pin")
			runs[lease]()
			result := <-results
			pins, err = m.pins.List()
			require.NoError(t, err)
			if test.wantError != "" {
				require.Contains(t, result, test.wantError)
				require.Empty(t, pins, "invalid content or inadequate capacity must not mint a pin")
			} else {
				require.Equal(t, lease, result)
				require.Len(t, pins, 1)
				require.Equal(t, lease, pins[0].LeaseUUID)
				require.Equal(t, configID.String(), pins[0].ImageID)
				require.Equal(t, digestRef, pins[0].PullDigest)
			}
			require.Positive(t, digestInspections)
			require.Zero(t, imports)
		})
	}
}

func TestImageCapacityAllocationOwnershipSharesReleaseAndAccountsConcurrentWork(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	first, err := m.reserveStaging(t.Context(), 10*imageMiB)
	require.NoError(t, err)
	second, err := m.reserveStaging(t.Context(), 10*imageMiB)
	require.NoError(t, err)
	copyOfFirst := first
	first.close()
	copyOfFirst.close()
	require.Equal(t, 10*imageMiB, m.staging)
	require.Len(t, m.stageSlots, 1)
	fs["/images"] = diskCapacity{available: uint64(15 * imageMiB)}
	require.Error(t, m.importHeadroom(t.Context(), 4*imageMiB), "unwritten staging bytes must be reserved against concurrent import")
	second.close()
	probe, err := m.reserveUnpack(t.Context(), 10*imageMiB)
	require.NoError(t, err)
	require.Error(t, m.importHeadroom(t.Context(), 4*imageMiB), "deferred extraction must reserve its allowance against concurrent import")
	copyOfProbe := probe
	probe.close()
	copyOfProbe.close()
	require.NoError(t, m.importHeadroom(t.Context(), 4*imageMiB))
	require.Zero(t, m.probing)
}

func TestImageImportAdmissionCoordinatesHelperStartedAfterPreflight(t *testing.T) {
	for _, purpose := range []imageInspectionPurpose{imageContentInspection, imageUnpackInspection} {
		for _, cancelWait := range []bool{false, true} {
			caseName := "content"
			if purpose == imageUnpackInspection {
				caseName = "unpack"
			}
			if cancelWait {
				caseName += " cancellation"
			}
			t.Run(caseName, func(t *testing.T) {
				m, daemon, fs := imageCapacityFixture(t)
				h := newInspectionHarness(t)
				m.docker = h.client
				m.cfg.ImageDataPath = "/containerd"
				fs["/containerd"] = fs["/images"]
				info := system.Info{DockerRootDir: "/images", Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}, OSType: "linux", Architecture: "amd64"}
				admissionEntered := make(chan struct{})
				notifyAdmission := sync.OnceFunc(func() { close(admissionEntered) })
				daemon.info = func(context.Context) (system.Info, error) {
					notifyAdmission()
					return info, nil
				}
				server := httptest.NewTLSServer(registry.New())
				t.Cleanup(server.Close)
				ref := strings.TrimPrefix(server.URL, "https://") + "/app:latest"
				tag, err := name.NewTag(ref)
				require.NoError(t, err)
				require.NoError(t, remote.Write(tag, imageCapacityRegistryImage(t, "concurrent verified bytes"), remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
				attachImageCapacityLoader(t, m, func(context.Context, io.Reader) (image.LoadResponse, error) {
					return image.LoadResponse{}, errors.New("reservation test must not dispatch ImageLoad")
				}, imagefetch.WithRegistryTransport(server.Client().Transport))
				prepared, err := m.loader.Prepare(t.Context(), ref, daemonImagePlatform(info))
				require.NoError(t, err)
				defer func() { require.NoError(t, prepared.Close()) }()
				createEntered, resume := make(chan struct{}), make(chan struct{})
				resumeCreate := sync.OnceFunc(func() { close(resume) })
				defer resumeCreate()
				h.daemon.beforeCreate = func() { close(createEntered); <-resume }
				h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
					// This is the real race: preflight succeeds, but a sibling starts
					// a helper before verified bytes reach the import reservation.
					require.NoError(t, m.requireSettledHelpers(ctx, info))
					opened := make(chan inspectionOpenResult, 1)
					go func() {
						session, err := h.owner.openFor(ctx, h.image, origin, purpose)
						opened <- inspectionOpenResult{session: session, err: err}
					}()
					<-createEntered
					waitCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					reserved := make(chan error, 1)
					go func() {
						admission, err := m.reserveImport(waitCtx, m.loader, prepared)
						if err == nil {
							err = admission.Close()
						}
						reserved <- err
					}()
					<-admissionEntered
					gateCtx, stopGateWait := context.WithTimeout(ctx, time.Second)
					defer stopGateWait()
					require.NoError(t, m.lock(gateCtx), "waiting for a helper must release the capacity gate")
					m.unlock()
					select {
					case err := <-reserved:
						t.Fatalf("a live helper became a terminal admission result: %v", err)
					case <-time.After(20 * time.Millisecond):
					}
					if cancelWait {
						cancel()
						require.ErrorIs(t, <-reserved, context.Canceled)
					}
					resumeCreate()
					result := <-opened
					require.NoError(t, result.err, "canceling an admission waiter cannot cancel the helper owner")
					if purpose == imageUnpackInspection {
						require.NoError(t, result.session.close())
					}
					if !cancelWait {
						require.NoError(t, <-reserved)
					}
					return result.session.close()
				})
				pending, err := m.loader.PendingBytes()
				require.NoError(t, err)
				require.Zero(t, pending, "unstarted or canceled reservations must not retain an import debit")
				require.NoError(t, h.client.requireImageInspectionsSettled(t.Context()))
			})
		}
	}
}

func TestImageCapacityRecoveryKeepsSavedBudgetAfterNewLimitDrops(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	for path := range fs {
		fs[path] = diskCapacity{available: uint64(100 * imageMiB), total: uint64(200 * imageMiB)}
	}
	m.cfg.ImageMaxSizeMB = 1
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/large:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, strings.Repeat("legacy bytes", 200_000))
	require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
	manifestID, err := fixture.Digest()
	require.NoError(t, err)
	configID, err := fixture.ConfigName()
	require.NoError(t, err)
	present := false
	m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		if !present {
			return nil, errdefs.NotFound(errors.New("missing pinned content"))
		}
		return &ImageInfo{ID: configID.String()}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: configID.String(), Size: 3 * imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		_, err := io.Copy(io.Discard, input)
		present = true
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, err
	}, imagefetch.WithRegistryTransport(server.Client().Transport))
	pin := &shared.ImagePin{ImageID: configID.String(), PullDigest: tag.Context().Digest(manifestID.String()).Name(), ImportBytes: 8 * imageMiB}
	pin.Platform.OS, pin.Platform.Architecture = "linux", "amd64"
	resolved, err := m.resolveImage(t.Context(), ref, pin, true)
	require.NoError(t, err)
	require.Equal(t, pin.ImageID, resolved.image.ID())
	require.Greater(t, resolved.importBytes, imageMiB)
}

func TestImageCapacityReusesVerifiedPinAcrossTenantsAndProtectsItFromCollection(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/shared:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, "shared exact content")
	require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
	configID, err := fixture.ConfigName()
	require.NoError(t, err)
	imports := 0
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
		if inspected != configID.String() || imports == 0 {
			return nil, errdefs.NotFound(errors.New("only imported immutable identity exists"))
		}
		return &ImageInfo{ID: configID.String()}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: configID.String(), Size: imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		_, err := io.Copy(io.Discard, input)
		imports++
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, err
	}, imagefetch.WithRegistryTransport(server.Client().Transport))
	const first = "550e8400-e29b-41d4-a716-446655440001"
	const second = "550e8400-e29b-41d4-a716-446655440002"
	results := make(chan string, 2)
	runs := imagePreparationExecutions(t, m, map[string]string{first: ref, second: ref}, results)
	runs[first]()
	require.Equal(t, first, <-results)
	runs[second]()
	require.Equal(t, second, <-results)
	require.Equal(t, 1, imports, "another tenant reuses the host's verified immutable pin")
	pins, err := m.pins.List()
	require.NoError(t, err)
	require.Len(t, pins, 2)
	fs["/images"] = diskCapacity{total: uint64(100 * imageMiB), available: uint64(10 * imageMiB)}
	unused := fixtureImageID("unused")
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		return []image.Summary{{ID: configID.String(), Created: 1}, {ID: unused, Created: 2}}, nil
	}
	var removed []string
	daemon.imageRemove = func(_ context.Context, id string, _ image.RemoveOptions) ([]image.DeleteResponse, error) {
		removed = append(removed, id)
		return nil, nil
	}
	require.NoError(t, m.collect(t.Context()))
	require.Equal(t, []string{unused}, removed, "pin protection must survive even with no container references")

	// Ownership must be re-read at the deletion boundary, after inventory.
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		daemon.volumeInspect = func(context.Context, string) (volume.Volume, error) {
			return volume.Volume{Name: imageCacheOwnerVolume, Driver: "local", Labels: map[string]string{imageCacheOwnerModeLabel: "shared"}}, nil
		}
		return []image.Summary{{ID: unused}}, nil
	}
	removed = nil
	require.Error(t, m.collect(t.Context()))
	require.Empty(t, removed, "a stale ownership observation must not authorize deletion")
}

func TestImageCapacityIncompleteCollectionDoesNotRefuseCachedAdmission(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	fs["/images"] = diskCapacity{total: uint64(100 * imageMiB), available: uint64(10 * imageMiB)}
	daemon.containerList = func(context.Context, container.ListOptions) ([]container.Summary, error) {
		return nil, errors.New("transient inventory failure")
	}
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/cached:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, "cached")
	require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
	configID, err := fixture.ConfigName()
	require.NoError(t, err)
	m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		return &ImageInfo{ID: configID.String()}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: configID.String(), Size: imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(context.Context, io.Reader) (image.LoadResponse, error) {
		return image.LoadResponse{}, errors.New("cached admission must not import")
	}, imagefetch.WithRegistryTransport(server.Client().Transport))
	const lease = "550e8400-e29b-41d4-a716-446655440001"
	results := make(chan string, 1)
	runs := imagePreparationExecutions(t, m, map[string]string{lease: ref}, results)
	runs[lease]()
	require.Equal(t, lease, <-results)
}
