package shared

import (
	"context"
	"errors"
	"strings"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

type imagePinObserverFunc func(context.Context, ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error)

func (f imagePinObserverFunc) ObserveImagePins(ctx context.Context, subject ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error) {
	return f(ctx, subject)
}

func imagePinBackfillFixture(t *testing.T) (operationHandoffStores, *ImagePinJournal, string, Release) {
	t.Helper()
	stores := openOperationHandoffStores(t, "image-pin-backfill")
	spec := testOperationIntentSpec(t, "image-pin-backfill")
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	require.NoError(t, err)
	authority, err := NewReleaseRuntimeAuthority(operationID, spec.Tenant, spec.ProviderUUID, spec.CallbackURL, spec.LifecycleCallbackURL)
	require.NoError(t, err)
	require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, Release{Manifest: spec.Manifest, Image: "stack", OperationID: operationID, Items: spec.Items, ResourceProfiles: spec.ResourceProfiles, RuntimeAuthority: &authority}))
	release, err := stores.releases.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	return stores, journal, spec.LeaseUUID, *release
}

func TestImagePinBackfillPublishesOnlyExactActiveMissingPins(t *testing.T) {
	stores, journal, lease, release := imagePinBackfillFixture(t)
	calls := 0
	owner, err := NewImagePinBackfiller(journal, imagePinObserverFunc(func(_ context.Context, subject ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error) {
		calls++
		require.Equal(t, lease, subject.LeaseUUID())
		require.Equal(t, release, subject.Release())
		detached := subject.Release()
		detached.Manifest[0] = '!'
		require.Equal(t, release, subject.Release(), "observer cannot change the held generation")
		return []ImagePinBackfillObservation{
			{Reference: "example.invalid/app:1", ImageID: inspectionJournalTestImage, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}},
			{Reference: "unrelated:latest", ImageID: inspectionJournalTestImage, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}},
		}, nil
	}))
	require.NoError(t, err)
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.False(t, inventory.Complete())
	require.Equal(t, 1, inventory.UnpinnedActiveGenerations())
	require.Zero(t, inventory.UnpinnedRetainedGenerations())
	report, err := owner.Sweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, ImagePinBackfillReport{PinsAdded: 1}, report)
	pin, err := journal.Lookup(lease, release.Manifest, "example.invalid/app:1")
	require.NoError(t, err)
	require.Equal(t, inspectionJournalTestImage, pin.ImageID)
	require.Zero(t, pin.ImportBytes)
	require.Empty(t, pin.PullDigest, "physical observation grants no registry recovery authority")
	inventory, err = journal.Collect(t.Context())
	require.NoError(t, err)
	require.True(t, inventory.Complete())
	require.Zero(t, inventory.UnpinnedActiveGenerations(), "verified backfill resolves the exact missing active generation")
	require.Zero(t, inventory.UnpinnedRetainedGenerations())
	require.False(t, inventory.CanRemove(inspectionJournalTestImage))
	require.True(t, inventory.CanRemove("sha256:"+strings.Repeat("c", 64)))
	// Reconstruct the owner over the open journals. Existing pins cannot be
	// rewritten by a new observer, and do not require repeating observations.
	reopened, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	owner, err = NewImagePinBackfiller(reopened, imagePinObserverFunc(func(context.Context, ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error) {
		t.Fatal("existing pins must never be re-observed or overwritten")
		return nil, nil
	}))
	require.NoError(t, err)
	report, err = owner.Sweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, ImagePinBackfillReport{}, report)
	require.Equal(t, 1, calls)
}

func TestImagePinBackfillMissingOrConflictingEvidenceLeavesCollectionIncomplete(t *testing.T) {
	for _, mode := range []string{"unavailable", "incomplete", "conflicting", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			_, journal, _, _ := imagePinBackfillFixture(t)
			owner, err := NewImagePinBackfiller(journal, imagePinObserverFunc(func(context.Context, ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error) {
				observation := ImagePinBackfillObservation{Reference: "example.invalid/app:1", ImageID: inspectionJournalTestImage, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}
				switch mode {
				case "unavailable":
					return nil, errors.New("container absent")
				case "incomplete":
					return nil, nil
				case "invalid":
					observation.ImageID = "mutable:latest"
				case "conflicting":
					other := observation
					other.ImageID = "sha256:" + strings.Repeat("c", 64)
					return []ImagePinBackfillObservation{observation, other}, nil
				}
				return []ImagePinBackfillObservation{observation}, nil
			}))
			require.NoError(t, err)
			report, err := owner.Sweep(t.Context())
			require.NoError(t, err, "unavailable historical evidence must not prevent backend startup")
			require.Equal(t, ImagePinBackfillReport{UnresolvedLeases: 1}, report)
			pins, err := journal.List()
			require.NoError(t, err)
			require.Empty(t, pins)
			inventory, err := journal.Collect(t.Context())
			require.NoError(t, err)
			require.False(t, inventory.Complete())
		})
	}
}

func TestImagePinBackfillDoesNotObserveUnresolvedMutation(t *testing.T) {
	stores, journal, lease, _ := imagePinBackfillFixture(t)
	spec := testOperationIntentSpec(t, "pending-image-pin-backfill")
	spec.LeaseUUID = lease
	beginHandoffOperation(t, stores.settlement, spec)
	owner, err := NewImagePinBackfiller(journal, imagePinObserverFunc(func(context.Context, ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error) {
		t.Fatal("an unresolved mutation cannot attest the active container cohort")
		return nil, nil
	}))
	require.NoError(t, err)
	report, err := owner.Sweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, ImagePinBackfillReport{UnresolvedLeases: 1}, report)
}
