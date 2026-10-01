package docker

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

func TestImageRecoveryStagingUsesSavedVerificationOrFreshCapacityNeverLegacyAllocation(t *testing.T) {
	for _, saved := range []int64{0, 8 * imageMiB} {
		name := "legacy_allocation"
		if saved > 0 {
			name = "saved_verification"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				f := newImageFlightFixture(t, func(ctx context.Context) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, nil)
				pin := &shared.ImagePin{ImageID: f.id, PullDigest: "registry.example/rollout@" + f.manifest, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}, ImportBytes: imageMiB, VerificationBytes: saved}
				done := make(chan error, 1)
				preparation := imageTenantPreparationForTest(t, f.m)
				go func() {
					_, err := f.m.ingestRecovery(t.Context(), preparation, f.ref, pin)
					preparation.close()
					done <- err
				}()
				synctest.Wait()
				expected := saved
				if saved == 0 {
					expected = (50*imageMiB - f.m.cfg.ImageDiskMinFreeMB*imageMiB) / 3
				}
				require.Equal(t, expected, f.m.staging, "recovery must reserve its independent verification ceiling")
				close(release)
				require.NoError(t, <-done)
				require.EqualValues(t, 1, f.imports.Load())
			})
		})
	}
}

func TestImageCacheDoesNotTreatLegacyPhysicalAllowanceAsVerification(t *testing.T) {
	f := newImageFlightFixture(t, nil, nil)
	resolution, err := f.m.loader.Resolve(t.Context(), f.ref, ocispec.Platform{OS: "linux", Architecture: "amd64"})
	require.NoError(t, err)
	legacy, err := imagebudget.Decode(imagebudget.Stored{ImportBytes: imageMiB})
	require.NoError(t, err)
	lease := uuid.NewString()
	pins, runs := imagePreparationSubjects(t, map[string]string{lease: f.ref}, nil, func(_ context.Context, mutations *storageMutations) error {
		return f.m.pins.Pin(mutations.inspectionOrigin, f.ref, f.id, resolution.SourceReference(), resolution.Platform(), legacy)
	})
	f.m.pins = pins
	runs[lease]()
	saved, err := pins.List()
	require.NoError(t, err)
	require.Len(t, saved, 1)
	f.local.Store(true)
	_, cached, err := f.m.cachedImage(t.Context(), f.ref, resolution)
	require.NoError(t, err)
	require.False(t, cached, "a retained physical allocation grants no host-wide verification evidence")
}

func TestImageUnpackAdmissionUsesPhysicalAllowanceInsteadOfVerificationBytes(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	h := newInspectionHarness(t)
	m.docker = h.client
	budget, err := imagebudget.Decode(imagebudget.Stored{VerificationBytes: imageMiB, ImportBytes: 4 * imageMiB})
	require.NoError(t, err)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 5 * uint64(imageMiB)}
	err = m.verifyUnpacked(t.Context(), shared.ImageInspectionOrigin{}, resolvedImage{budget: budget})
	require.ErrorContains(t, err, "image import admission", "physical headroom must refuse before any helper can be created")
	require.Zero(t, h.daemon.creates)
	require.Zero(t, m.probing)
}
