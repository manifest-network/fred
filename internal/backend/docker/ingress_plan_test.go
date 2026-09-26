package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func buildTestComposeProject(t testing.TB, params composeProjectParams) *composetypes.Project {
	t.Helper()
	plan, err := newEffectiveIngressPlan(params.Ingress, params.Stack, params.Items)
	require.NoError(t, err)
	return buildPlannedComposeProject(params, plan)
}

type testIngressLabelParams struct {
	LeaseUUID, ServiceName, NetworkName, CustomDomain string
	Instance, Quantity                                int
	Ingress                                           IngressConfig
}

func applyTestIngressLabels(labels map[string]string, p testIngressLabelParams, ports map[string]manifest.PortConfig) {
	applyIngressLabels(labels, ingressLabelParams{
		LeaseUUID: p.LeaseUUID, ServiceName: p.ServiceName, NetworkName: p.NetworkName,
		Instance: p.Instance, Quantity: p.Quantity,
	}, newIngressRoute(p.Ingress, ports, p.CustomDomain))
}

func TestEffectiveIngressPlanMatchesRealLabelWriter(t *testing.T) {
	for _, test := range []struct {
		name       string
		enabled    bool
		ports      map[string]manifest.PortConfig
		domain     string
		dnsReady   bool
		wantDomain string
		wantDNS    int
	}{
		{name: "disabled", domain: "app.example.com", ports: map[string]manifest.PortConfig{"80/tcp": {}}, dnsReady: true},
		{name: "no routable port", enabled: true, domain: "app.example.com", ports: map[string]manifest.PortConfig{"53/udp": {}}, dnsReady: true},
		{name: "invalid domain", enabled: true, domain: "not a domain", ports: map[string]manifest.PortConfig{"80/tcp": {}}, dnsReady: true},
		{name: "provider domain", enabled: true, domain: "tenant.provider.example", ports: map[string]manifest.PortConfig{"80/tcp": {}}, dnsReady: true},
		{name: "DNS deferred", enabled: true, domain: "app.example.com", ports: map[string]manifest.PortConfig{"8080/tcp": {}}, wantDNS: 1},
		{name: "admitted", enabled: true, domain: "app.example.com", ports: map[string]manifest.PortConfig{"8080/tcp": {}}, dnsReady: true, wantDomain: "app.example.com", wantDNS: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := baseProjectParams()
			params.Ingress = IngressConfig{Enabled: test.enabled, WildcardDomain: "provider.example", Entrypoint: "websecure"}
			params.Stack.Services["web"].Ports = test.ports
			params.Items[0].CustomDomain = test.domain
			desired := slices.Clone(params.Items)
			var dnsCalls int
			b := &Backend{cfg: Config{Ingress: params.Ingress}, customDomainDNSReady: func(context.Context, string) bool {
				dnsCalls++
				return test.dnsReady
			}}
			plan, err := b.admitIngressPlan(t.Context(), params.Stack, desired)
			require.NoError(t, err)
			require.Equal(t, test.wantDNS, dnsCalls)
			require.Equal(t, test.domain, desired[0].CustomDomain, "desired chain metadata must remain intact")
			require.Equal(t, test.wantDomain, plan.effectiveItems()[0].CustomDomain)
			project := buildPlannedComposeProject(params, plan)
			labels := project.Services["web"].Labels
			require.Equal(t, plan.effectiveItems()[0].CustomDomain, labels[LabelCustomDomain])
			if test.wantDomain != "" {
				require.Equal(t, "8080", labels["traefik.http.services."+CustomDomainRouterName(params.LeaseUUID, "web")+"-svc.loadbalancer.server.port"])
			}
			b.customDomainDNSReady = func(context.Context, string) bool {
				t.Fatal("accepted ingress must not repeat DNS admission")
				return false
			}
			restored, err := restoreIngressPlan(params.Ingress, params.Stack, plan.effectiveItems())
			require.NoError(t, err)
			assert.Equal(t, labels, buildPlannedComposeProject(params, restored).Services["web"].Labels)
			detached := plan.effectiveItems()
			detached[0].CustomDomain = "foreign.example"
			assert.Equal(t, test.wantDomain, plan.effectiveItems()[0].CustomDomain)
		})
	}
}

func TestIngressPlanDoesNotSilentlyReinterpretPreviouslyAcceptedDomain(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"].Ports = map[string]manifest.PortConfig{"80/tcp": {}}
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "web", CustomDomain: "old.example.com"}}
	_, err := restoreIngressPlan(IngressConfig{}, params.Stack, items)
	require.ErrorContains(t, err, "durable effective ingress differs")
	assert.Equal(t, "old.example.com", items[0].CustomDomain)
}

func routableCustomDomainStack(serviceNames ...string) *manifest.StackManifest {
	stack := &manifest.StackManifest{Services: make(map[string]*manifest.Manifest, len(serviceNames))}
	for _, serviceName := range serviceNames {
		stack.Services[serviceName] = &manifest.Manifest{Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}}
	}
	return stack
}

func TestIngressPlanUnroutableDomainDoesNotReconcileForever(t *testing.T) {
	for _, ports := range []map[string]manifest.PortConfig{nil, {"53/udp": {}}} {
		stack := restoreStackManifest()
		stack.Services[manifest.DefaultServiceName].Ports = ports
		projection := &provision{ProvisionState: leasesm.ProvisionState{
			LeaseUUID: durableCallbackTestLeaseUUID, Status: backend.ProvisionStatusReady,
			StackManifest: stack,
			Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		}}
		b := newBackendForTest(&mockDockerClient{}, map[string]*provision{durableCallbackTestLeaseUUID: projection})
		b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "provider.example"}
		b.customDomainDNSReady = func(context.Context, string) bool {
			t.Fatal("an unrenderable route must not trigger DNS admission")
			return true
		}
		desired := slices.Clone(projection.Items)
		desired[0].CustomDomain = "app.example.com"
		for range 3 {
			require.NoError(t, b.ReconcileCustomDomain(t.Context(), durableCallbackTestLeaseUUID, desired))
		}
		require.Empty(t, projection.Items[0].CustomDomain)
		require.Equal(t, "app.example.com", desired[0].CustomDomain)
	}
}

// This exercises durable admission, the real Compose label writer, the strict
// inventory classifier, settlement, and callback delivery. The inventory helper
// reads the emitted labels; it does not copy expected metadata from the intent.
func TestEffectiveIngressProvisionAndRestoreSettleFromEmittedLabels(t *testing.T) {
	for _, test := range []struct {
		name       string
		enabled    bool
		ports      map[string]manifest.PortConfig
		domain     string
		dnsReady   bool
		wantDomain string
		wantDNS    int32
	}{
		{name: "disabled", domain: "app.example.com", ports: map[string]manifest.PortConfig{"80/tcp": {}}, dnsReady: true},
		{name: "no routable port", enabled: true, domain: "app.example.com", ports: map[string]manifest.PortConfig{"53/udp": {}}, dnsReady: true},
		{name: "invalid domain", enabled: true, domain: "not a domain", ports: map[string]manifest.PortConfig{"80/tcp": {}}, dnsReady: true},
		{name: "DNS deferred", enabled: true, domain: "app.example.com", ports: map[string]manifest.PortConfig{"80/tcp": {}}, wantDNS: 1},
		{name: "admitted", enabled: true, domain: "app.example.com", ports: map[string]manifest.PortConfig{"80/tcp": {}}, dnsReady: true, wantDomain: "app.example.com", wantDNS: 1},
	} {
		for _, restoring := range []bool{false, true} {
			kind := "provision"
			if restoring {
				kind = "restore"
			}
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				const destination = "22222222-2222-4222-8222-222222222222"
				const source = "11111111-1111-4111-8111-111111111111"
				mock := &mockDockerClient{
					PullImageFn: func(context.Context, string, time.Duration) error { return nil },
					InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
						return &ContainerInfo{ContainerID: id, Status: "running"}, nil
					},
				}
				b := newBackendForProvisionTest(t, mock, nil)
				b.cfg.StartupVerifyDuration = 10 * time.Millisecond
				b.cfg.Ingress = IngressConfig{Enabled: test.enabled, WildcardDomain: "provider.example", Entrypoint: "websecure"}
				var dnsCalls atomic.Int32
				b.customDomainDNSReady = func(context.Context, string) bool {
					// Readiness changes immediately after acceptance. Execution must
					// retain the admitted decision instead of asking DNS again.
					return dnsCalls.Add(1) == 1 && test.dnsReady
				}
				stack := restoreStackManifest()
				stack.Services[manifest.DefaultServiceName].Ports = test.ports
				callback := make(chan backend.CallbackPayload, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					var payload backend.CallbackPayload
					if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
						t.Errorf("decode callback: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					callback <- payload
					w.WriteHeader(http.StatusNoContent)
				}))
				defer server.Close()
				var retained *shared.RetentionStore
				if restoring {
					retained = attachRetentionStore(t, b)
					items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}
					require.NoError(t, putRetentionForTest(t, retained, shared.RetentionEntry{
						OriginalLeaseUUID: source, Tenant: "tenant-a", ProviderUUID: destination,
						Items: items, ResourceProfiles: testResourceProfiles(t, items), StackManifest: stack,
						CallbackURL: "http://localhost/callbacks/provision", Status: shared.RetentionStatusActive,
						RetainedVolumeNames: []string{retainedName(canonicalVolumeName(source, manifest.DefaultServiceName, 0))},
						Generation:          1, CreatedAt: time.Now(),
					}))
					b.volumes = &mockVolumeManager{RenameVolumeFn: func(string, string) error { return nil }}
				}
				rebuildCallbackSender(b, server.Client())
				defer startRestoreCallbackReplay(t, b)()
				if restoring {
					request := restoreRequest(destination, source, server.URL+"/callbacks/provision")
					request.Items[0].CustomDomain = test.domain
					require.NoError(t, b.Restore(t.Context(), request))
					require.Equal(t, test.domain, request.Items[0].CustomDomain, "desired request is not an effective projection")
				} else {
					payload, err := json.Marshal(stack)
					require.NoError(t, err)
					request := newProvisionRequest(destination, "tenant-a", "docker-small", 1, payload)
					request.CallbackURL = testOperationCallbackURL(server.URL + "/callbacks/provision")
					request.Items[0].CustomDomain = test.domain
					require.NoError(t, b.Provision(t.Context(), request))
					require.Equal(t, test.domain, request.Items[0].CustomDomain, "desired request is not an effective projection")
				}
				select {
				case payload := <-callback:
					require.Equal(t, backend.CallbackStatusSuccess, payload.Status)
				case <-time.After(5 * time.Second):
					t.Fatal("effective ingress failed to settle and deliver its callback")
				}
				require.Eventually(t, func() bool {
					b.provisionsMu.RLock()
					defer b.provisionsMu.RUnlock()
					projection := b.provisions[destination]
					return projection != nil && projection.Status == backend.ProvisionStatusReady &&
						projection.Items[0].CustomDomain == test.wantDomain
				}, 5*time.Second, 10*time.Millisecond)
				require.Equal(t, test.wantDNS, dnsCalls.Load(), "DNS admission must occur only before durable acceptance")
				inventory, err := mock.ListManagedContainers(t.Context())
				require.NoError(t, err)
				require.Len(t, inventory, 1)
				require.Equal(t, test.wantDomain, inventory[0].CustomDomain)
				if restoring {
					finalizeRestoreRetentionForTest(t, b, retained, source)
					entry, err := retained.Get(source)
					require.NoError(t, err)
					require.Nil(t, entry, "successful effective restore must complete retention finalization")
				}
				if test.wantDomain != "" {
					// Removing the routable port during maintenance must also
					// persist an absent effective domain before rendering. Strict
					// post-effect comparison must continue to settle the new cohort.
					stack.Services[manifest.DefaultServiceName].Ports = nil
					payload, err := json.Marshal(stack)
					require.NoError(t, err)
					b.provisionsMu.RLock()
					lifecycleURL := b.provisions[destination].LifecycleCallbackURL
					b.provisionsMu.RUnlock()
					require.NoError(t, b.Update(t.Context(), backend.UpdateRequest{
						MaintenanceID: newTestMaintenanceID(t), LeaseUUID: destination,
						CallbackURL: lifecycleURL, Payload: payload,
					}))
					select {
					case payload := <-callback:
						require.Equal(t, backend.CallbackStatusSuccess, payload.Status)
					case <-time.After(5 * time.Second):
						t.Fatal("maintenance removing the ingress port failed to settle")
					}
					require.Eventually(t, func() bool {
						b.provisionsMu.RLock()
						defer b.provisionsMu.RUnlock()
						projection := b.provisions[destination]
						return projection.Status == backend.ProvisionStatusReady && projection.Items[0].CustomDomain == ""
					}, 5*time.Second, 10*time.Millisecond)
					inventory, err := mock.ListManagedContainers(t.Context())
					require.NoError(t, err)
					require.Len(t, inventory, 1)
					require.Empty(t, inventory[0].CustomDomain)
				}
			})
		}
	}
}
