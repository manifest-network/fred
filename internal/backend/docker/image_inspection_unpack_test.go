package docker

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestImageUnpackProbeOwnsStoppedSafeContainer(t *testing.T) {
	h := newInspectionHarness(t)
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		require.NoError(t, h.client.requireImageInspectionsSettled())
		err := h.client.verifyImageUnpacked(ctx, h.image, origin)
		require.NoError(t, err)
		return err
	})
	require.Equal(t, 1, h.daemon.creates)
	require.Equal(t, 1, h.daemon.removes)
	assert.Empty(t, h.daemon.containers)
	assert.Zero(t, h.daemon.volumes, "image VOLUME content must not be copied into anonymous volumes")
	config, host := h.daemon.createConfigs[0], h.daemon.createHosts[0]
	assert.Equal(t, h.image.ID(), config.Image)
	assert.Equal(t, "/", config.WorkingDir)
	assert.Equal(t, "0", config.User)
	assert.True(t, config.NetworkDisabled)
	assert.Equal(t, []string{"/__fred_image_probe_never_started__"}, []string(config.Entrypoint))
	assert.Equal(t, []string{"--never-start"}, []string(config.Cmd))
	require.NotNil(t, config.Healthcheck)
	assert.Equal(t, []string{"NONE"}, config.Healthcheck.Test)
	require.NotNil(t, host)
	assert.True(t, host.ReadonlyRootfs)
	assert.Equal(t, container.NetworkMode("none"), host.NetworkMode)
	assert.Equal(t, map[string]string{"/data": "rw,noexec,nosuid,nodev,size=1m"}, host.Tmpfs)
	assert.Empty(t, host.Binds)
	assert.Empty(t, host.Mounts)
	assert.Equal(t, []string{"ALL"}, []string(host.CapDrop))
	assert.Equal(t, []string{"no-new-privileges:true"}, host.SecurityOpt)
	assert.Equal(t, container.RestartPolicyDisabled, host.RestartPolicy.Name)
	require.NoError(t, h.client.requireImageInspectionsSettled())
}

func TestImageUnpackProbeRejectsForeignAdmissionBeforeJournalOrCreate(t *testing.T) {
	h := newInspectionHarness(t)
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		err := h.client.verifyImageUnpacked(ctx, imageexec.Image{}, origin)
		require.Error(t, err)
		return err
	})
	assert.Zero(t, h.daemon.creates)
	require.NoError(t, h.client.requireImageInspectionsSettled())
}

func TestImageUnpackProbeDrainsAdmittedCreateAfterWorkCancellation(t *testing.T) {
	h := newInspectionHarness(t)
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		h.daemon.afterCreate = cancel
		err := h.client.verifyImageUnpacked(ctx, h.image, origin)
		require.ErrorIs(t, err, context.Canceled)
		return err
	})
	assert.Equal(t, 1, h.daemon.creates)
	assert.Equal(t, 1, h.daemon.removes)
	assert.Empty(t, h.daemon.containers)
	require.NoError(t, h.backend.terminalStorageAuthorityError())
	require.NoError(t, h.client.requireImageInspectionsSettled())
}

func TestImageUnpackProbeUnknownCreateFencesUntilDurableRecovery(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.createErr = errors.New("response lost during unpack")
	h.daemon.delayCreate = true
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		err := h.client.verifyImageUnpacked(ctx, h.image, origin)
		require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		return err
	})
	require.ErrorIs(t, h.backend.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
	require.Error(t, h.client.requireImageInspectionsSettled())
	assert.Zero(t, h.daemon.removes)
	h.reopen(t)
	require.NoError(t, h.backend.terminalStorageAuthorityError())
	require.ErrorContains(t, h.client.requireImageInspectionsSettled(), "remains pending")
	report, err := h.owner.Recover(t.Context())
	require.NoError(t, err)
	require.Len(t, report.pending, 1, "empty inventory cannot prove deferred daemon unpack ended")
	require.Error(t, h.client.requireImageInspectionsSettled())
	h.daemon.containers[h.daemon.late.ID] = *h.daemon.late
	report, err = h.owner.Recover(t.Context())
	require.NoError(t, err)
	require.Len(t, report.pending, 1, "observing one late helper cannot settle a response-lost request")
	assert.Empty(t, h.daemon.containers)
	require.Error(t, h.client.requireImageInspectionsSettled())
}

func TestImageUnpackProbeSettledFailureCannotProveUnpack(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.createStatus = http.StatusInternalServerError
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		err := h.client.verifyImageUnpacked(ctx, h.image, origin)
		require.ErrorContains(t, err, "image unpack failed")
		return err
	})
	assert.Equal(t, 1, h.daemon.creates)
	assert.Equal(t, 1, h.daemon.removes)
	require.NoError(t, h.backend.terminalStorageAuthorityError(), "a completed daemon rejection is retryable")
	require.NoError(t, h.client.requireImageInspectionsSettled())
}

func TestImageUnpackProbeCleanupFailureExcludesNextImportAcrossRestart(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.removeErr = errors.New("remove unavailable")
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		err := h.client.verifyImageUnpacked(ctx, h.image, origin)
		require.ErrorContains(t, err, "remove unavailable")
		return err
	})
	require.Error(t, h.client.requireImageInspectionsSettled())
	h.reopen(t)
	require.Error(t, h.client.requireImageInspectionsSettled())
	h.daemon.removeErr = nil
	report, err := h.owner.Recover(t.Context())
	require.NoError(t, err)
	assert.Empty(t, report.pending)
	require.NoError(t, h.client.requireImageInspectionsSettled())
}
