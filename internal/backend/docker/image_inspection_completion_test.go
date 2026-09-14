package docker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestImageInspectionCreateCompletionSeparatesDaemonFailureFromUnknown(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusInternalServerError, http.StatusBadGateway, 0} {
		for _, materialized := range []bool{false, true} {
			t.Run(fmt.Sprintf("status=%d/materialized=%v", status, materialized), func(t *testing.T) {
				h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
					return newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
						if !strings.HasSuffix(req.URL.Path, "/containers/create") {
							return daemon.request(t, req)
						}
						if materialized {
							response, err := daemon.request(t, req)
							require.NoError(t, err)
							require.NoError(t, response.Body.Close())
						}
						if status == 0 {
							return nil, errors.New("lost helper create response")
						}
						return imageSecurityResponse(status, `{"message":"helper create rejected"}`), nil
					})
				})
				h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
					_, err := h.client.openImageInspection(ctx, h.image, origin)
					require.Error(t, err)
					return err
				})
				receipts, err := h.owner.journal.List()
				require.NoError(t, err)
				if status == http.StatusConflict || status == http.StatusInternalServerError {
					require.Empty(t, receipts, "completed failure plus exact absence retires its receipt")
				} else {
					require.Len(t, receipts, 1, "current absence cannot settle a lost or gateway response")
					require.False(t, receipts[0].CreationSettled())
				}
				require.Empty(t, h.daemon.containers)
				if materialized {
					require.Equal(t, 1, h.daemon.removes, "response completion never replaces exact owned-helper cleanup")
				}
			})
		}
	}
}

func TestImageInspectionCompletedResponseWithUnreadableBodyCanCleanUp(t *testing.T) {
	h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
		return newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
			response, err := daemon.request(t, req)
			if err != nil || !strings.HasSuffix(req.URL.Path, "/containers/create") {
				return response, err
			}
			require.NoError(t, response.Body.Close())
			return imageSecurityResponse(http.StatusCreated, `{"Id":`), nil
		})
	})
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.openImageInspection(ctx, h.image, origin)
		require.Error(t, err)
		return err
	})
	require.Equal(t, 1, h.daemon.removes)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Empty(t, receipts, "response-body decoding does not undo the completed daemon handler")
}

func TestImageInspectionCreatePanicRetainsDebtAndWithdrawsAuthority(t *testing.T) {
	h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
		return newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/containers/create") {
				panic("helper SDK failed after dispatch")
			}
			return daemon.request(t, req)
		})
	})
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.openImageInspection(ctx, h.image, origin)
		require.ErrorContains(t, err, "helper SDK failed after dispatch")
		return err
	})
	require.Error(t, h.backend.requireMutationAdmission(t.Context(), "continue after helper panic"))
	h.reopen(t)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.False(t, receipts[0].CreationSettled())
}

func TestImageInspectionCreateCompletionRequiresStoragePostcheck(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.afterCreate = func() {
		h.backend.storageVerifier = testDockerRuntimeStorageVerifier{id: h.authority.storage.ID(), verify: func(context.Context) error {
			return backendidentity.ErrIdentityDrift
		}}
	}
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.openImageInspection(ctx, h.image, origin)
		require.ErrorIs(t, err, backendidentity.ErrIdentityDrift)
		return err
	})
	require.Zero(t, h.daemon.removes)
	h.reopen(t)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.False(t, receipts[0].CreationSettled(), "known HTTP response alone cannot commit completion after identity withdrawal")
	require.Empty(t, receipts[0].ContainerID())
	_, err = h.owner.Recover(t.Context())
	require.NoError(t, err)
	require.Empty(t, h.daemon.containers)
	receipts, err = h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1, "failed postcheck retains the offline fence obligation")
}

func TestImageInspectionRefusedCreationDoesNotReserveHelperDebt(t *testing.T) {
	h := newInspectionHarness(t)
	h.owner.authorize = func(context.Context, string) (context.Context, func(), error) {
		return nil, nil, errors.New("helper dispatch refused")
	}
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.openImageInspection(ctx, h.image, origin)
		require.ErrorContains(t, err, "helper dispatch refused")
		return err
	})
	require.Zero(t, h.daemon.creates)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Empty(t, receipts)
}

func TestImageInspectionCompletedFailureDoesNotAuthorizeForeignCleanup(t *testing.T) {
	h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
		return newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
			response, err := daemon.request(t, req)
			if err != nil || !strings.HasSuffix(req.URL.Path, "/containers/create") {
				return response, err
			}
			require.NoError(t, response.Body.Close())
			for _, actual := range daemon.containers {
				actual.Config.Labels["fred.inspection.id"] = "foreign"
			}
			return imageSecurityResponse(http.StatusInternalServerError, `{"message":"helper create rejected"}`), nil
		})
	})
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.openImageInspection(ctx, h.image, origin)
		require.ErrorContains(t, err, "differs")
		return err
	})
	require.Zero(t, h.daemon.removes)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.True(t, receipts[0].CreationSettled())
	report, err := h.owner.Recover(t.Context())
	require.NoError(t, err)
	require.Len(t, report.pending, 1)
	require.Zero(t, h.daemon.removes)
}
