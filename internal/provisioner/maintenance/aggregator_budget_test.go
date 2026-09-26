package maintenance

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func aggregatorMaintenanceService(t *testing.T, tenants []string, client *fakeBackend) (*Service, []Command) {
	t.Helper()
	leases := make([]string, len(tenants))
	for index := range leases {
		leases[index] = fmt.Sprintf("31000000-0000-4000-8000-%012d", index)
	}
	store, _ := newPlacementAuthority(t, leases...)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	raw, found := maintenanceSeedPlans.Load(store)
	require.True(t, found)
	plan := raw.(maintenanceSeedPlan)
	plan.leaseTenants = make(map[string]string, len(leases))
	chain := testChain(leases...)
	commands := make([]Command, len(leases))
	for index, lease := range leases {
		plan.leaseTenants[lease] = tenants[index]
		chain.leases[lease].Tenant = tenants[index]
		commands[index] = Command{
			ID: requestID(t, testRequestA), LeaseUUID: lease, Tenant: tenants[index],
			Kind: KindUpdate, Payload: []byte("candidate"),
		}
	}
	maintenanceSeedPlans.Store(store, plan)
	coordinator := maintenanceCoordinatorForTest(t, store, chain, fakeRouter{backend: client}, &fakePayloads{})
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	return service, commands
}

func TestAggregatorConcurrentRolloutBorrowsMaintenanceBudget(t *testing.T) {
	// These are the aggregator shapes already configured in manifest-deploy.
	// No new provider or backend configuration is needed to retain concurrency.
	for environment, tenant := range map[string]string{
		"mainnet-morpheus": "manifest1qt9mc4svrj9xryfx5ru0szs3srmn8hu2uadt3l",
		"dev":              "manifest15yj0wgnexgj2l70p5dlngdgll324xv548s5taj",
	} {
		t.Run(environment, func(t *testing.T) {
			const leases = 200
			tenants := make([]string, leases)
			for index := range tenants {
				tenants[index] = tenant
			}
			entered := make(chan struct{}, leases)
			release := make(chan struct{})
			var releaseOnce sync.Once
			var workers sync.WaitGroup
			defer func() { releaseOnce.Do(func() { close(release) }); workers.Wait() }()
			wait := func() error { entered <- struct{}{}; <-release; return nil }
			client := &fakeBackend{
				restart: func(backend.RestartRequest) error { return wait() },
				update:  func(backend.UpdateRequest) error { return wait() },
			}
			service, commands := aggregatorMaintenanceService(t, tenants, client)
			results := make(chan Result, leases)
			for index, command := range commands {
				if index%2 == 0 {
					command.Kind, command.Payload = KindRestart, nil
				} else {
					command.Payload = bytes.Repeat([]byte("x"), 128<<10)
				}
				workers.Go(func() { results <- service.Execute(t.Context(), command) })
			}
			timeout := time.NewTimer(time.Minute)
			defer timeout.Stop()
			for range leases {
				select {
				case <-entered:
				case result := <-results:
					t.Fatalf("ordinary aggregator rollout was refused before dispatch: %v (%v)", result.Outcome(), result.Cause())
				case <-timeout.C:
					t.Fatal("aggregator rollout was starved behind its own pending work")
				}
			}
			releaseOnce.Do(func() { close(release) })
			workers.Wait()
			for range leases {
				result := <-results
				require.Equal(t, OutcomeAccepted, result.Outcome(), result.Cause())
			}
			require.Equal(t, leases/2, client.restartCount())
			require.Equal(t, leases/2, client.updateCount())
		})
	}
}

func TestMaintenanceReservationProjectsTypedRetryWithoutDispatch(t *testing.T) {
	const incumbentCommands = 1024
	tenants := make([]string, incumbentCommands+2)
	for index := range incumbentCommands {
		tenants[index] = "incumbent"
	}
	tenants[incumbentCommands], tenants[incumbentCommands+1] = "newcomer", "later-newcomer"
	client := &fakeBackend{}
	service, commands := aggregatorMaintenanceService(t, tenants, client)
	for _, command := range commands[:incumbentCommands-1] {
		result := service.Execute(t.Context(), command)
		require.Equal(t, OutcomeAccepted, result.Outcome(), result.Cause())
	}
	refused := service.Execute(t.Context(), commands[incumbentCommands-1])
	require.Equal(t, OutcomeCapacityReserved, refused.Outcome(), refused.Cause())
	require.Equal(t, incumbentCommands-1, client.updateCount(), "reservation refusal must precede backend dispatch")
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), commands[incumbentCommands]).Outcome(), "a newcomer can consume the reserved opportunity")
	require.Equal(t, OutcomeServiceUnavailable, service.Execute(t.Context(), commands[incumbentCommands+1]).Outcome(), "finite global capacity still binds")
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), commands[0]).Outcome(), "exact admitted replay keeps its authority when full")
}
