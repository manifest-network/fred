package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestRestartPlansEffectiveIngressBeforeMaintenanceAcceptance(t *testing.T) {
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"app": {Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
	}}
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "app.example.com"}}
	compose := &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error { return nil },
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-c1", Service: "app", State: "running"}}, nil
		},
		DownFn: func(context.Context, string, time.Duration) error { return nil },
	}
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		RemoveContainerFn: func(context.Context, string) error { return nil },
	}
	installStackStrictCohortInventory(t, mock, compose)
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: stackMaintenanceLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			SKU: "docker-small", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: items, StackManifest: stack, ContainerIDs: []string{"old-c1"},
			ServiceContainers: map[string][]string{"app": {"old-c1"}},
		}},
	})
	b.compose = compose
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "provider.example", Entrypoint: "websecure"}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	callbacks := make(chan backend.CallbackPayload, 1)
	server := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode callback: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		callbacks <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	operationID := mustDockerOperationID("123e4567-e89b-42d3-a456-426614174000")
	lifecycleURL := server.URL + "?lifecycle_id=" + operationID.String()
	seedStackMaintenanceAuthority(t, b, stackMaintenanceLeaseUUID, stack, items, operationID,
		server.URL+"?operation_id="+operationID.String(), lifecycleURL, server.Client())

	// An operator disables ingress while an older active release still owns
	// a domain. Restart must persist exactly the labels the new cohort emits.
	b.cfg.Ingress.Enabled = false
	require.NoError(t, b.Restart(t.Context(), backend.RestartRequest{
		MaintenanceID: newTestMaintenanceID(t), LeaseUUID: stackMaintenanceLeaseUUID, CallbackURL: lifecycleURL,
	}))
	select {
	case payload := <-callbacks:
		require.Equal(t, backend.CallbackStatusSuccess, payload.Status)
	case <-time.After(5 * time.Second):
		t.Fatal("restart failed to settle its effective ingress target")
	}
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		current := b.provisions[stackMaintenanceLeaseUUID]
		return current.Status == backend.ProvisionStatusReady && current.Items[0].CustomDomain == ""
	}, 5*time.Second, time.Millisecond)
	active, err := b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, active.Items[0].CustomDomain, "the durable target must describe emitted ingress")
	inventory, err := mock.ListManagedContainers(t.Context())
	require.NoError(t, err)
	require.Len(t, inventory, 1)
	require.Empty(t, inventory[0].CustomDomain)
	require.Eventually(t, func() bool {
		pending, err := b.callbackStore.ListPending()
		return err == nil && len(pending) == 0
	}, 5*time.Second, time.Millisecond, "callback acknowledgment must drain before the fixture shuts down")
}
