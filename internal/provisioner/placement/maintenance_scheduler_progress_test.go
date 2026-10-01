package placement

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

type recoveryProductionPayloadWriter struct{ store *payload.Store }

func (p recoveryProductionPayloadWriter) OverwritePayload(lease string, value []byte) error {
	return p.store.Put(lease, value)
}

func applyRecoveryCompletionForTest(t *testing.T, authority *MaintenanceCoordinator, command MaintenanceCommand) {
	t.Helper()
	body, err := json.Marshal(backend.CallbackPayload{LeaseUUID: command.LeaseUUID(), Status: backend.CallbackStatusSuccess, BackendStorageID: command.BackendStorageID().String(), MaintenanceID: command.ID().String()})
	require.NoError(t, err)
	route, err := url.Parse(command.CallbackURL())
	require.NoError(t, err)
	verifier, consumer := hmacauth.NewCallbackProofBoundary()
	callbacks, err := authority.coordinator.execution.AuthenticatedCallbackCoordinator(consumer)
	require.NoError(t, err)
	now := time.Now()
	const secret = "review-maintenance-completion-secret-0123456789"
	proof, err := verifier.VerifyRoutedWithTime(secret, http.MethodPost, route.RequestURI(), body, hmacauth.SignWithTime(secret, http.MethodPost, route.RequestURI(), body, now), command.BackendStorageID().String(), route.Path, time.Minute, time.Minute, now)
	require.NoError(t, err)
	_, err = callbacks.Apply(t.Context(), proof)
	require.NoError(t, err)
}

func TestMaintenanceRecoveryLatchedPayloadStorePreservesOtherTenantProgress(t *testing.T) {
	var delayed, restartHealthy atomic.Bool
	var restarts atomic.Int32
	tenants := map[string]string{}
	reader := maintenanceLeaseReaderFunc(func(ctx context.Context, lease string) (*billingtypes.Lease, error) {
		if delayed.Load() {
			timer := time.NewTimer(30 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		tenant := tenants[lease]
		if tenant == "" {
			tenant = "tenant-test"
		}
		return &billingtypes.Lease{Uuid: lease, Tenant: tenant, ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE}, nil
	})
	client := &executionTestBackend{name: "backend-a", restart: func(context.Context, backend.RestartRequest) error {
		restarts.Add(1)
		if !restartHealthy.Load() {
			return errors.New("initial ambiguous delivery")
		}
		return nil
	}}
	base, store := newMaintenanceCoordinatorForTest(t, reader, client)
	scope := requireAdmissionScope(t, store, store.CurrentAdmissionBaseline(), "backend-a")
	commands := make([]PreparedMaintenanceCommand, 0, 8)
	for index := range 8 {
		prepared := prepareTenantMaintenance(t, store, scope, 900+index, "tenant-a", []byte("confirmed manifest"))
		tenants[prepared.Command().LeaseUUID()] = "tenant-a"
		commands = append(commands, prepared)
	}
	path := filepath.Join(t.TempDir(), "real-payloads.db")
	payloadStore, err := payload.NewStore(payload.StoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chmod(path, 0o600); _ = payloadStore.Close() })
	authority, err := base.coordinator.execution.MaintenanceCoordinator(recoveryProductionPayloadWriter{store: payloadStore})
	require.NoError(t, err)
	app, err := authority.Application(nil, 100*time.Millisecond)
	require.NoError(t, err)
	for _, prepared := range commands {
		c := prepared.Command()
		req, err := NewMaintenanceApplicationRequest(c.ID(), c.LeaseUUID(), c.Tenant(), c.Kind(), c.Payload())
		require.NoError(t, err)
		require.Equal(t, MaintenanceApplicationAccepted, app.Execute(t.Context(), req).Outcome())
		applyRecoveryCompletionForTest(t, authority, c)
	}
	request, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, maintenanceIDA), maintenanceLease, "tenant-test", MaintenanceCommandRestart, nil)
	require.NoError(t, err)
	require.Equal(t, MaintenanceApplicationServiceUnavailable, app.Execute(t.Context(), request).Outcome())
	require.Equal(t, int32(1), restarts.Load())
	restartHealthy.Store(true)
	// Withdraw the actual store's authority; subsequent writes fail fast.
	require.NoError(t, os.Chmod(path, 0o640))
	require.ErrorIs(t, payloadStore.Healthy(), payload.ErrStoreAuthorityUnavailable)
	require.NoError(t, os.Chmod(path, 0o600))
	require.ErrorIs(t, payloadStore.Healthy(), payload.ErrStoreAuthorityUnavailable)
	// Discard callback wake hints already issued before recovery.
	for len(store.maintenanceChanged) != 0 {
		<-store.maintenanceChanged
	}
	delayed.Store(true)
	for range 3 {
		// A pass can use its whole budget on the independently healthy restart
		// and return no persistence error. The invariant is bounded opportunity,
		// not a particular count of failed writes in each timed pass.
		if err := app.RecoverPending(t.Context()); err != nil {
			require.True(t, errors.Is(err, payload.ErrStoreAuthorityUnavailable) || errors.Is(err, context.DeadlineExceeded), err)
		}
	}
	require.Equal(t, int32(2), restarts.Load(), "confirmed backlog must leave another tenant recovery opportunities despite slow chain observations and a latched payload store")
	for _, prepared := range commands {
		rec, found, err := store.LookupMaintenanceCommand(prepared.Command().LeaseUUID(), prepared.Command().ID())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, MaintenanceOutcomePending, rec.Outcome())
		require.Equal(t, maintenancePayloadConfirmed, rec.Command().phase)
	}
	delayed.Store(false)
	require.ErrorContains(t, app.RecoverPending(t.Context()), "payload store authority is unavailable")
	require.Equal(t, int32(2), restarts.Load(), "a settled restart must not repeat when later recovery has more time")
	require.Empty(t, store.maintenanceChanged, "persistent local failure must not create a self-waking retry loop")
}

func TestMaintenanceRecoveryBusyOwnersCannotPinEitherClass(t *testing.T) {
	const count = maxMaintenanceRecoveryCommandsPerBackendPass + 8
	var updateCalls int
	restartCalls := make(map[string]int)
	acceptRetries := false
	client := &executionTestBackend{
		name: "backend-a",
		update: func(context.Context, backend.UpdateRequest) error {
			updateCalls++
			return nil
		},
		restart: func(_ context.Context, request backend.RestartRequest) error {
			restartCalls[request.LeaseUUID]++
			if !acceptRetries {
				return errors.New("initial ambiguous delivery")
			}
			return nil
		},
	}
	base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	scope := requireAdmissionScope(t, store, store.CurrentAdmissionBaseline(), "backend-a")
	var confirmed, ordinary []MaintenanceCommand
	for index := range count {
		confirmed = append(confirmed, prepareTenantMaintenance(t, store, scope, 2000+index, "tenant-test", []byte("confirmed")).Command())
		ordinary = append(ordinary, prepareTenantMaintenance(t, store, scope, 3000+index, "tenant-test", []byte("unused")).Command())
	}
	payloads := &maintenanceProgressPayloads{err: errors.New("payload persistence unavailable")}
	authority, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
	require.NoError(t, err)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	for _, command := range confirmed {
		request, err := NewMaintenanceApplicationRequest(command.ID(), command.LeaseUUID(), command.Tenant(), command.Kind(), command.Payload())
		require.NoError(t, err)
		require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
		applyRecoveryCompletionForTest(t, authority, command)
	}
	for _, command := range ordinary {
		request, err := NewMaintenanceApplicationRequest(command.ID(), command.LeaseUUID(), command.Tenant(), MaintenanceCommandRestart, nil)
		require.NoError(t, err)
		require.Equal(t, MaintenanceApplicationServiceUnavailable, application.Execute(t.Context(), request).Outcome())
	}
	acceptRetries = true
	locked := make(map[*maintenanceHeld]bool)
	lock := func(command MaintenanceCommand) {
		held := application.held[command.LeaseUUID()]
		held.dispatchMu.Lock()
		locked[held] = true
	}
	unlock := func(command MaintenanceCommand) {
		held := application.held[command.LeaseUUID()]
		require.True(t, locked[held])
		delete(locked, held)
		held.dispatchMu.Unlock()
	}
	t.Cleanup(func() {
		for held := range locked {
			held.dispatchMu.Unlock()
		}
	})
	for _, command := range confirmed {
		lock(command)
	}
	for _, command := range ordinary[:maxMaintenanceRecoveryCommandsPerBackendPass] {
		lock(command)
	}
	require.NoError(t, application.RecoverPending(t.Context()))
	require.Zero(t, payloads.writes, "live dispatch ownership excludes confirmed payload writes")
	for _, command := range ordinary {
		require.Equal(t, 1, restartCalls[command.LeaseUUID()], "the first ordinary batch is entirely owned by live requests")
	}
	require.NoError(t, application.RecoverPending(t.Context()))
	for index, command := range ordinary {
		wantCalls := 1
		if index >= maxMaintenanceRecoveryCommandsPerBackendPass {
			wantCalls = 2
		}
		require.Equal(t, wantCalls, restartCalls[command.LeaseUUID()], "busy owners cannot pin the ordinary batch cursor")
	}
	unlock(confirmed[0])
	unlock(ordinary[0])
	require.ErrorContains(t, application.RecoverPending(t.Context()), "payload persistence unavailable")
	require.Equal(t, 1, payloads.writes, "released confirmed ownership must receive another opportunity")
	require.Equal(t, 2, restartCalls[ordinary[0].LeaseUUID()], "the independently rotating ordinary class must also progress")
	require.Equal(t, count, updateCalls, "accepted updates must never become backend delivery again")
}
