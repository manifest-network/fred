package docker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func unsettledHelperGauges() (unknownCreate, cleanupPending float64) {
	return promtestutil.ToFloat64(imageHelpersUnsettled.WithLabelValues(imageHelperUnsettledUnknownCreate)),
		promtestutil.ToFloat64(imageHelpersUnsettled.WithLabelValues(imageHelperUnsettledCleanupPending))
}

func setUnsettledHelperGauges(unknownCreate, cleanupPending float64) {
	imageHelpersUnsettled.WithLabelValues(imageHelperUnsettledUnknownCreate).Set(unknownCreate)
	imageHelpersUnsettled.WithLabelValues(imageHelperUnsettledCleanupPending).Set(cleanupPending)
}

func recoverInspectionsAndReport(ctx context.Context, h *inspectionHarness) error {
	return h.owner.RecoverAndReport(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newHelperLeftByFailedRemoval leaves one settled helper whose removal failed,
// as seen by a restarted backend.
func newHelperLeftByFailedRemoval(t *testing.T) *inspectionHarness {
	t.Helper()
	h := newInspectionHarness(t)
	h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) { return inspectionTar(t, path), nil }
	h.daemon.removeErr = errors.New("remove unavailable")
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
		return err
	})
	h.reopen(t)
	return h
}

// Not parallel: the gauge is process-global, and every backend's reconcile
// pass writes it.
func TestImageInspectionRecoveryReportsUnsettledHelpersByReason(t *testing.T) {
	t.Run("cleanup pending until removal succeeds", func(t *testing.T) {
		h := newHelperLeftByFailedRemoval(t)
		setUnsettledHelperGauges(5, 0)
		require.NoError(t, recoverInspectionsAndReport(t.Context(), h))
		unknownCreate, cleanupPending := unsettledHelperGauges()
		assert.Zero(t, unknownCreate)
		assert.Equal(t, 1.0, cleanupPending)

		h.daemon.removeErr = nil
		require.NoError(t, recoverInspectionsAndReport(t.Context(), h))
		_, cleanupPending = unsettledHelperGauges()
		assert.Zero(t, cleanupPending, "a removed helper is settled")
	})

	t.Run("a pass stopped at its time cap still counts what it never reached", func(t *testing.T) {
		h := newHelperLeftByFailedRemoval(t)
		h.daemon.removeErr = nil
		setUnsettledHelperGauges(0, 0)
		capped, cancel := context.WithCancel(t.Context())
		cancel()
		require.NoError(t, recoverInspectionsAndReport(capped, h), "a deferred pass is not an error")
		_, cleanupPending := unsettledHelperGauges()
		assert.Equal(t, 1.0, cleanupPending)
	})

	t.Run("an unconfirmed Create is kept and reported separately", func(t *testing.T) {
		h := newInspectionHarness(t)
		h.daemon.createErr = errors.New("response lost after create dispatch")
		h.daemon.delayCreate = true
		h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
			_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
			require.ErrorContains(t, err, "response lost")
			return err
		})
		h.reopen(t)
		setUnsettledHelperGauges(0, 5)
		require.NoError(t, recoverInspectionsAndReport(t.Context(), h))
		unknownCreate, cleanupPending := unsettledHelperGauges()
		assert.Equal(t, 1.0, unknownCreate)
		assert.Zero(t, cleanupPending)
	})

	for _, failure := range []string{"authority withdrawn", "journal closed"} {
		t.Run("an unreadable journal keeps the previous value: "+failure, func(t *testing.T) {
			h := newHelperLeftByFailedRemoval(t)
			setUnsettledHelperGauges(3, 7)
			if failure == "journal closed" {
				require.NoError(t, h.callbacks.Close())
			} else {
				h.backend.storageVerifier = testDockerRuntimeStorageVerifier{
					id:     h.authority.storage.ID(),
					verify: func(context.Context) error { return backendidentity.ErrIdentityDrift },
				}
			}
			require.Error(t, recoverInspectionsAndReport(t.Context(), h))
			unknownCreate, cleanupPending := unsettledHelperGauges()
			assert.Equal(t, 3.0, unknownCreate)
			assert.Equal(t, 7.0, cleanupPending)
		})
	}

	t.Run("a live inspection's helper is in progress, not unsettled", func(t *testing.T) {
		h := newInspectionHarness(t)
		copying, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		h.daemon.copy = func(ctx context.Context, path string) (io.ReadCloser, error) {
			close(copying)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return inspectionTar(t, path), nil
			}
		}
		go func() {
			defer close(finished)
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				_, _, err := h.client.ResolveImageUser(ctx, h.image, "app", origin)
				assert.NoError(t, err)
				return err
			})
		}()
		select {
		case <-copying:
		case <-t.Context().Done():
			t.Fatal("inspection copy did not begin")
		}
		setUnsettledHelperGauges(5, 5)
		require.NoError(t, recoverInspectionsAndReport(t.Context(), h))
		unknownCreate, cleanupPending := unsettledHelperGauges()
		assert.Zero(t, unknownCreate)
		assert.Zero(t, cleanupPending)
		close(release)
		select {
		case <-finished:
		case <-t.Context().Done():
			t.Fatal("inspection did not finish")
		}
	})
}
