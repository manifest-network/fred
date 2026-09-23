package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

type inspectionOpenResult struct {
	session *imageInspectionSession
	err     error
}

func TestImageAdmissionWaitsForOwnedContentCreation(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "completed create keeps live content helper"
		if lost {
			name = "response lost retains exclusion"
		}
		t.Run(name, func(t *testing.T) {
			h := newInspectionHarness(t)
			entered, resume := make(chan struct{}), make(chan struct{})
			resumeCreate := sync.OnceFunc(func() { close(resume) })
			defer resumeCreate()
			h.daemon.beforeCreate = func() { close(entered); <-resume }
			if lost {
				h.daemon.createErr = errors.New("response lost during content helper creation")
				h.daemon.delayCreate = true
			}
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				opened := make(chan inspectionOpenResult, 1)
				go func() {
					session, err := h.client.openImageInspection(ctx, h.image, origin)
					opened <- inspectionOpenResult{session: session, err: err}
				}()
				<-entered
				waited := make(chan error, 1)
				go func() { waited <- h.client.requireImageInspectionsSettled(ctx) }()
				select {
				case err := <-waited:
					t.Fatalf("image admission returned before the owned Create settled: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				resumeCreate()
				result := <-opened
				admissionErr := <-waited
				if lost {
					require.ErrorContains(t, result.err, "response lost")
					require.ErrorContains(t, admissionErr, "remains pending")
					return result.err
				}
				require.NoError(t, result.err)
				require.NoError(t, admissionErr, "another lease may prepare images while this helper remains open")
				receipts, err := h.owner.journal.List()
				require.NoError(t, err)
				require.Len(t, receipts, 1)
				require.True(t, receipts[0].CreationSettled())
				require.NoError(t, h.client.requireImageInspectionsSettled(ctx))
				return result.session.close()
			})
			if lost {
				h.reopen(t)
				require.ErrorContains(t, h.client.requireImageInspectionsSettled(t.Context()), "remains pending")
			} else {
				require.NoError(t, h.client.requireImageInspectionsSettled(t.Context()))
			}
		})
	}
}

func TestImageAdmissionWaitCancellationDoesNotReleaseContentOwner(t *testing.T) {
	h := newInspectionHarness(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	resumeCreate := sync.OnceFunc(func() { close(resume) })
	defer resumeCreate()
	h.daemon.beforeCreate = func() { close(entered); <-resume }
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		opened := make(chan inspectionOpenResult, 1)
		go func() {
			session, err := h.client.openImageInspection(ctx, h.image, origin)
			opened <- inspectionOpenResult{session: session, err: err}
		}()
		<-entered
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		err := h.client.requireImageInspectionsSettled(waitCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		resumeCreate()
		result := <-opened
		require.NoError(t, result.err)
		require.NoError(t, h.client.requireImageInspectionsSettled(ctx))
		return result.session.close()
	})
	require.NoError(t, h.client.requireImageInspectionsSettled(t.Context()))
}

func TestImageAdmissionRejectsAbandonedSettledContentHelper(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.removeErr = errors.New("retain abandoned content helper")
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		session, err := h.client.openImageInspection(ctx, h.image, origin)
		require.NoError(t, err)
		require.NoError(t, h.client.requireImageInspectionsSettled(ctx))
		err = session.close()
		require.ErrorContains(t, err, "retain abandoned")
		return err
	})
	require.ErrorContains(t, h.client.requireImageInspectionsSettled(t.Context()), "remains pending")
	h.reopen(t)
	require.ErrorContains(t, h.client.requireImageInspectionsSettled(t.Context()), "remains pending")
	h.daemon.removeErr = nil
	report, err := h.owner.Recover(t.Context())
	require.NoError(t, err)
	assert.Empty(t, report.pending)
	require.NoError(t, h.client.requireImageInspectionsSettled(t.Context()))
}

func TestImageAdmissionDoesNotShareLiveUnpackProbe(t *testing.T) {
	h := newInspectionHarness(t)
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		session, err := h.owner.openFor(ctx, h.image, origin, imageUnpackInspection)
		require.NoError(t, err)
		require.ErrorContains(t, h.client.requireImageInspectionsSettled(ctx), "remains pending")
		return session.close()
	})
	require.NoError(t, h.client.requireImageInspectionsSettled(t.Context()))
}
