package docker

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func TestImagePeriodicCollectionPrunesObsoletePinsWhileAdmissionRemainsActive(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	callbacks, err := newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = callbacks.Close() })
	releases, retentions, settlement, _, _ := operationHandoffForCallbackTest(t, callbacks)
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok)
	authority := value.(*operationIntentTestAuthority)
	b := &Backend{cfg: DefaultConfig(), stopCtx: t.Context(), storageIdentity: authority.storage.ID(), storeAuthorityGate: authority.gate,
		storageVerifier: testDockerRuntimeStorageVerifier{id: authority.storage.ID()}, imageCapacity: m}
	m.pins, err = shared.NewImagePinJournal(callbacks, releases, retentions)
	require.NoError(t, err)
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, ref string) (*ImageInfo, error) {
		id := testImageID
		if strings.Contains(ref, "obsolete") {
			id = otherTestImageID
		}
		return &ImageInfo{ID: id}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(_ context.Context, id string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	daemon.imageRemove = func(context.Context, string, image.RemoveOptions) ([]image.DeleteResponse, error) {
		t.Fatal("active image admission must still prevent physical deletion")
		return nil, nil
	}
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		return []image.Summary{{ID: testImageID}, {ID: otherTestImageID}}, nil
	}
	activeLease, obsoleteLease := uuid.NewString(), uuid.NewString()
	var admission imageAdmission
	require.NoError(t, shared.BindOperationSubstrateExecutor(settlement, b.authorizeStorageMutation, b.completeStorageMutation,
		func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) func(context.Context) error {
			ref := "example.invalid/active:1"
			if subject.LeaseUUID() == obsoleteLease {
				ref = "example.invalid/obsolete:1"
			}
			mutations := newOperationStorageMutations(runner, subject, storageMutationOperations{backend: b})
			return func(ctx context.Context) error {
				if _, err := mutations.admitImage(ctx, ref); err != nil {
					return err
				}
				if subject.LeaseUUID() == obsoleteLease {
					var err error
					admission, err = m.beginAdmission(ctx, mutations, ref)
					if err != nil {
						return err
					}
				}
				return runner.Step(ctx, "observe final image consumer", func(context.Context) error { return nil })
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ shared.OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(_ context.Context, subject shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			if subject.LeaseUUID() == obsoleteLease {
				return shared.NewOperationExactAbsent(subject)
			}
			return shared.NewOperationTargetReady(subject, []string{"active-container"}, map[string][]string{"app": {"active-container"}})
		},
	))
	publisher := callbackPublisherForCallbackTest(t, callbacks)
	for _, lease := range []string{activeLease, obsoleteLease} {
		spec := dockerOperationIntentSpec(t, authority.storage.ID())
		spec.LeaseUUID = lease
		_, spec.CallbackURL, spec.LifecycleCallbackURL = newTestRestoreCallbackAuthority(t)
		if lease == obsoleteLease {
			spec.Manifest = validStackManifestJSON(map[string]string{"app": "example.invalid/obsolete:1"})
		} else {
			spec.Manifest = validStackManifestJSON(map[string]string{"app": "example.invalid/active:1"})
		}
		candidate, err := settlement.NewOperationIntentCandidate(spec)
		require.NoError(t, err)
		accepted, err := settlement.BeginOperationIntent(candidate)
		require.NoError(t, err)
		claim, created := accepted.CreatedClaim()
		require.True(t, created)
		release, err := settlement.PrepareOperationRelease(claim)
		require.NoError(t, err)
		execution, err := settlement.StartOperationExecution(release)
		require.NoError(t, err)
		outcome := settlement.ExecuteOperation(t.Context(), execution)
		if lease == obsoleteLease {
			failure, ok := outcome.(shared.OperationExecutionFailure)
			require.True(t, ok, "operation outcome = %T", outcome)
			proof, err := settlement.CommitOperationFailure(failure)
			require.NoError(t, err)
			require.NoError(t, publisher.PublishOperationFailureContext(t.Context(), proof, "image consumer is absent"))
		} else {
			success, ok := outcome.(shared.OperationExecutionSuccess)
			require.True(t, ok, "operation outcome = %T", outcome)
			proof, err := settlement.CommitOperationSuccess(success)
			require.NoError(t, err)
			require.NoError(t, publisher.PublishOperationSuccessContext(t.Context(), proof))
		}
	}
	require.NotNil(t, admission.state)
	defer admission.close()
	require.Equal(t, 1, m.active)
	pins, err := m.pins.List()
	require.NoError(t, err)
	require.Len(t, pins, 2)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 5 * uint64(imageMiB)}
	require.NoError(t, b.collectImages(t.Context()), "periodic pruning remains available while image removal is busy")
	pins, err = m.pins.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, activeLease, pins[0].LeaseUUID)
	require.Equal(t, testImageID, pins[0].ImageID)
	require.Equal(t, 1, m.active, "pin pruning cannot release live image ownership")
}
