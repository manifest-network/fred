package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	restoreapp "github.com/manifest-network/fred/internal/provisioner/restore"
	"github.com/manifest-network/fred/internal/testutil"
)

func TestRestoreLeaseSemanticConflictReasonsPreserveNumericCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome restoreapp.Outcome
		status  int
		body    string
	}{
		{name: "source busy", outcome: restoreapp.OutcomeSourceBusy, status: http.StatusConflict,
			body: `{"error":"lease is already being provisioned or restored","code":409,"reason":"source_busy"}`},
		{name: "target busy", outcome: restoreapp.OutcomeTargetBusy, status: http.StatusConflict,
			body: `{"error":"lease is already being provisioned or restored","code":409,"reason":"target_busy"}`},
		{name: "target not pending", outcome: restoreapp.OutcomeTargetNotPending, status: http.StatusConflict,
			body: `{"error":"lease is not pending; only a fresh lease can be restored into","code":409,"reason":"target_not_pending"}`},
		{name: "backend conflict has no inferred lease reason", outcome: restoreapp.OutcomeBackendInvalidState, status: http.StatusConflict,
			body: `{"error":"lease not in a restorable state","code":409}`},
		{name: "tier refusal keeps existing numeric code", outcome: restoreapp.OutcomeTierTooSmall, status: http.StatusUnprocessableEntity,
			body: `{"error":"retained data exceeds the requested smaller tier","code":422}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kp := testutil.NewTestKeyPair("test-tenant")
			leaseUUID := testutil.ValidUUID1
			providerUUID := testutil.ValidUUID2
			chainClient := &mockChainClient{
				getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{
						Uuid: uuid, Tenant: kp.Address, ProviderUuid: providerUUID,
						State: billingtypes.LEASE_STATE_PENDING,
					}, nil
				},
			}
			called := false
			handlers := &Handlers{
				client: chainClient, providerUUID: providerUUID, bech32Prefix: "manifest",
				restoreService: restoreServiceFunc(func(_ context.Context, command restoreapp.Command) restoreapp.Result {
					called = true
					assert.Equal(t, restoreapp.Command{
						TargetLeaseUUID: leaseUUID, SourceLeaseUUID: fromLeaseUUID, Tenant: kp.Address,
					}, command)
					return restoreapp.Result{Outcome: tc.outcome}
				}),
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/leases/"+leaseUUID+"/restore",
				strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
			request.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
			request.SetPathValue("lease_uuid", leaseUUID)
			response := httptest.NewRecorder()

			handlers.RestoreLease(response, request)

			require.True(t, called)
			assert.Equal(t, tc.status, response.Code)
			assert.JSONEq(t, tc.body, response.Body.String())
		})
	}
}
