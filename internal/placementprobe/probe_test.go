package placementprobe

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestNewClientsUseEachBackendsResolvedHMACSecret(t *testing.T) {
	t.Parallel()
	const (
		secretA = "backend-a-secret-0123456789abcdef"
		secretB = "backend-b-secret-0123456789abcdef"
	)
	idA := probeStorageID("550e8400-e29b-41d4-a716-446655440000")
	idB := probeStorageID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	newServer := func(storageID backendidentity.ID, secret string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			response.Header().Set(backendidentity.ResponseHeader, storageID.String())
			signature := request.Header.Get(hmacauth.SignatureHeader)
			if err := hmacauth.VerifyRequest(secret, request, nil, signature, 5*time.Minute); err != nil {
				http.Error(response, "unauthorized", http.StatusUnauthorized)
				return
			}
			response.Header().Set("Content-Type", "application/json")
			switch request.URL.Path {
			case "/provisions":
				assert.NoError(t, json.NewEncoder(response).Encode(
					backend.ListProvisionsResponse{Provisions: []backend.ProvisionInfo{}},
				))
			case "/retentions":
				assert.NoError(t, json.NewEncoder(response).Encode(
					backend.ListRetentionsResponse{Retentions: []backend.RetainedLease{}},
				))
			default:
				http.NotFound(response, request)
			}
		}))
	}
	serverA := newServer(idA, secretA)
	serverB := newServer(idB, secretB)
	t.Cleanup(serverA.Close)
	t.Cleanup(serverB.Close)
	cfg := &config.Config{Backends: []config.BackendConfig{
		{Name: "backend-a", URL: serverA.URL, Timeout: time.Second, HMACSecret: secretA},
		{Name: "backend-b", URL: serverB.URL, Timeout: time.Second, HMACSecret: secretB},
	}}

	clients, err := NewClients(cfg)
	require.NoError(t, err)
	inventories, err := Collect(t.Context(), clients)
	require.NoError(t, err)
	assert.Equal(t, idA, inventories["backend-a"].StorageIdentity)
	assert.Equal(t, idB, inventories["backend-b"].StorageIdentity)
}

const (
	probeTargetLease         = "018f47a2-8b1c-7def-8123-456789abcdef"
	probeOtherLeaseA         = "018f47a2-8b1c-7def-8123-456789abcdee"
	probeOtherLeaseB         = "018f47a2-8b1c-7def-8123-456789abcded"
	probePriorGeneration     = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	probeAttemptedGeneration = "550e8400-e29b-41d4-a716-446655440000"
)

type nilInventoryClient struct{}

func (*nilInventoryClient) Name() string { panic("nil inventory client must be rejected") }

func (*nilInventoryClient) ListProvisions(context.Context) ([]backend.ProvisionInfo, error) {
	panic("nil inventory client must be rejected")
}

func (*nilInventoryClient) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	panic("nil inventory client must be rejected")
}

func (*nilInventoryClient) ListProvisionsWithIdentity(context.Context) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	panic("nil inventory client must be rejected")
}

func (*nilInventoryClient) ListRetentionsWithIdentity(context.Context) ([]backend.RetainedLease, backendidentity.ID, error) {
	panic("nil inventory client must be rejected")
}

func TestCollectRejectsNilClient(t *testing.T) {
	var client *nilInventoryClient
	_, err := Collect(context.Background(), []Client{client})
	require.ErrorIs(t, err, ErrIncompleteInventory)
	assert.ErrorContains(t, err, "client is nil")
}

type staticInventoryClient struct {
	name        string
	provisions  []backend.ProvisionInfo
	retentions  []backend.RetainedLease
	provisionID backendidentity.ID
	retentionID backendidentity.ID
}

func (client *staticInventoryClient) Name() string {
	if client.name == "" {
		return "backend-a"
	}
	return client.name
}

func (client *staticInventoryClient) ListProvisions(context.Context) ([]backend.ProvisionInfo, error) {
	return client.provisions, nil
}

func (client *staticInventoryClient) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	return client.retentions, nil
}

func probeStorageID(value string) backendidentity.ID {
	id, err := backendidentity.Parse(value)
	if err != nil {
		panic(err)
	}
	return id
}

func (client *staticInventoryClient) storageIDs() (backendidentity.ID, backendidentity.ID) {
	defaultID := probeStorageID("e740d246-4d23-4c48-88d7-90fa8c42ab87")
	provisionID := client.provisionID
	if !provisionID.Valid() {
		provisionID = defaultID
	}
	retentionID := client.retentionID
	if !retentionID.Valid() {
		retentionID = defaultID
	}
	return provisionID, retentionID
}

func (client *staticInventoryClient) ListProvisionsWithIdentity(context.Context) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	provisionID, _ := client.storageIDs()
	return client.provisions, provisionID, nil
}

func (client *staticInventoryClient) ListRetentionsWithIdentity(context.Context) ([]backend.RetainedLease, backendidentity.ID, error) {
	_, retentionID := client.storageIDs()
	return client.retentions, retentionID, nil
}

func TestCollectRejectsNilInventorySlices(t *testing.T) {
	tests := []struct {
		name        string
		client      *staticInventoryClient
		wantMessage string
	}{
		{
			name: "null provisions",
			client: &staticInventoryClient{
				retentions: []backend.RetainedLease{},
			},
			wantMessage: "null provisions inventory",
		},
		{
			name: "null retentions",
			client: &staticInventoryClient{
				provisions: []backend.ProvisionInfo{},
			},
			wantMessage: "null retentions inventory",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Collect(t.Context(), []Client{test.client})
			require.ErrorIs(t, err, ErrIncompleteInventory)
			assert.ErrorContains(t, err, test.wantMessage)
		})
	}
}

func TestCollectRejectsStorageIdentityDisagreementAcrossEndpoints(t *testing.T) {
	t.Parallel()

	client := &staticInventoryClient{
		provisions:  []backend.ProvisionInfo{},
		retentions:  []backend.RetainedLease{},
		provisionID: probeStorageID("550e8400-e29b-41d4-a716-446655440000"),
		retentionID: probeStorageID("6ba7b811-9dad-41d1-80b4-00c04fd430c8"),
	}

	inventories, err := Collect(t.Context(), []Client{client})
	assert.Nil(t, inventories, "a split-lineage backend must not yield a partial snapshot")
	require.ErrorIs(t, err, ErrIncompleteInventory)
	assert.ErrorContains(t, err, "inventory storage identities disagree")
}

func TestCollectRejectsStorageIdentitySharedByDifferentBackends(t *testing.T) {
	t.Parallel()

	sharedID := probeStorageID("550e8400-e29b-41d4-a716-446655440000")
	clients := []Client{
		&staticInventoryClient{
			name: "backend-a", provisions: []backend.ProvisionInfo{},
			retentions: []backend.RetainedLease{}, provisionID: sharedID, retentionID: sharedID,
		},
		&staticInventoryClient{
			name: "backend-b", provisions: []backend.ProvisionInfo{},
			retentions: []backend.RetainedLease{}, provisionID: sharedID, retentionID: sharedID,
		},
	}

	inventories, err := Collect(t.Context(), clients)
	assert.Nil(t, inventories, "one physical lineage cannot authorize two configured names")
	require.ErrorIs(t, err, ErrIncompleteInventory)
	assert.ErrorContains(t, err, "share storage identity")
}

func TestCollectReturnsOnlyCompleteDistinctIdentitySnapshot(t *testing.T) {
	t.Parallel()

	idA := probeStorageID("550e8400-e29b-41d4-a716-446655440000")
	idB := probeStorageID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	clients := []Client{
		&staticInventoryClient{
			name: "backend-a", provisions: []backend.ProvisionInfo{},
			retentions: []backend.RetainedLease{}, provisionID: idA, retentionID: idA,
		},
		&staticInventoryClient{
			name: "backend-b", provisions: []backend.ProvisionInfo{},
			retentions: []backend.RetainedLease{}, provisionID: idB, retentionID: idB,
		},
	}

	inventories, err := Collect(t.Context(), clients)
	require.NoError(t, err)
	require.Len(t, inventories, 2)
	assert.Equal(t, idA, inventories["backend-a"].StorageIdentity)
	assert.Equal(t, idB, inventories["backend-b"].StorageIdentity)
}

type repairCandidateStub struct {
	lease   string
	matches func(string, placement.LifecycleObservation) bool
}

func (candidate repairCandidateStub) LeaseUUID() string { return candidate.lease }

func (candidate repairCandidateStub) MatchesPreservedProvision(
	backendName string,
	observation placement.LifecycleObservation,
) bool {
	return candidate.matches != nil && candidate.matches(backendName, observation)
}

func absentRepairCandidate() repairCandidateStub {
	return repairCandidateStub{lease: probeTargetLease}
}

func completeInventory(
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) Inventory {
	storageID, err := backendidentity.New()
	if err != nil {
		panic(err)
	}
	if provisions == nil {
		provisions = []backend.ProvisionInfo{}
	}
	if retentions == nil {
		retentions = []backend.RetainedLease{}
	}
	return Inventory{
		StorageIdentity: storageID,
		Provisions:      provisions,
		Retentions:      retentions,
	}
}

func TestRequireRepairEvidenceFailsClosedForAttemptOnlyCandidate(t *testing.T) {
	tests := []struct {
		name        string
		backends    []string
		inventories map[string]Inventory
		want        error
	}{
		{
			name:     "complete absence",
			backends: []string{"backend-a", "backend-b"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{LeaseUUID: probeOtherLeaseA}}, nil,
				),
				"backend-b": completeInventory(
					nil, []backend.RetainedLease{{LeaseUUID: probeOtherLeaseB}},
				),
			},
		},
		{
			name:        "silent backend",
			backends:    []string{"backend-a", "backend-b"},
			inventories: map[string]Inventory{"backend-a": completeInventory(nil, nil)},
			want:        ErrIncompleteInventory,
		},
		{
			name:     "positive provision",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{LeaseUUID: probeTargetLease}}, nil,
				),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "positive retention",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					nil, []backend.RetainedLease{{LeaseUUID: probeTargetLease}},
				),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "malformed lease identity",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{LeaseUUID: "not-a-uuid"}}, nil,
				),
			},
			want: ErrIncompleteInventory,
		},
		{
			name:     "duplicate identity",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{LeaseUUID: probeOtherLeaseA}},
					[]backend.RetainedLease{{LeaseUUID: probeOtherLeaseA}},
				),
			},
			want: ErrIncompleteInventory,
		},
		{
			name:     "extra inventory",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, nil),
				"backend-b": completeInventory(nil, nil),
			},
			want: ErrIncompleteInventory,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := RequireRepairEvidence(
				test.backends, test.inventories, absentRepairCandidate(),
			)
			if test.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestRequireRepairEvidenceAllowsOnlyExactPreservedGeneration(t *testing.T) {
	priorID, err := lifecycle.ParseID(probePriorGeneration)
	require.NoError(t, err)
	candidate := repairCandidateStub{
		lease: probeTargetLease,
		matches: func(backendName string, observation placement.LifecycleObservation) bool {
			return backendName == "backend-a" &&
				observation.Kind == placement.LifecycleObservationTyped &&
				observation.ID == priorID
		},
	}
	typed := func(id string) *backend.LifecycleGenerationObservation {
		return &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   id,
		}
	}

	tests := []struct {
		name        string
		backends    []string
		inventories map[string]Inventory
		want        error
	}{
		{
			name:     "sole prior generation on owner backend",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID:           probeTargetLease,
					LifecycleGeneration: typed(probePriorGeneration),
				}}, nil),
			},
		},
		{
			name:     "attempted generation",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID:           probeTargetLease,
					LifecycleGeneration: typed(probeAttemptedGeneration),
				}}, nil),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "missing generation",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{LeaseUUID: probeTargetLease}}, nil,
				),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "legacy generation does not match typed prior",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID: probeTargetLease,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationLegacy,
					},
				}}, nil),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "unusable generation",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID: probeTargetLease,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationUnusable,
					},
				}}, nil),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "prior generation on another backend",
			backends: []string{"backend-a", "backend-b"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, nil),
				"backend-b": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID:           probeTargetLease,
					LifecycleGeneration: typed(probePriorGeneration),
				}}, nil),
			},
			want: ErrLeasePresent,
		},
		{
			name:     "target retention alongside prior generation",
			backends: []string{"backend-a"},
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{
						LeaseUUID:           probeTargetLease,
						LifecycleGeneration: typed(probePriorGeneration),
					}},
					[]backend.RetainedLease{{LeaseUUID: probeTargetLease}},
				),
			},
			want: ErrIncompleteInventory,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := RequireRepairEvidence(test.backends, test.inventories, candidate)
			if test.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestRequireRepairEvidenceReportsEveryDisallowedLocationDeterministically(t *testing.T) {
	err := RequireRepairEvidence(
		[]string{"backend-b", "backend-a"},
		map[string]Inventory{
			"backend-a": completeInventory(
				nil, []backend.RetainedLease{{LeaseUUID: probeTargetLease}},
			),
			"backend-b": completeInventory(
				[]backend.ProvisionInfo{{LeaseUUID: probeTargetLease}}, nil,
			),
		},
		absentRepairCandidate(),
	)
	require.ErrorIs(t, err, ErrLeasePresent)
	assert.ErrorContains(t, err,
		"backend-a/retentions, backend-b/provisions (not the exact preserved pre-attempt lifecycle generation)")
	assert.False(t, errors.Is(err, ErrIncompleteInventory))
}

type conflictCandidateStub struct {
	lease      string
	selected   string
	candidates []string
}

func (candidate conflictCandidateStub) LeaseUUID() string       { return candidate.lease }
func (candidate conflictCandidateStub) SelectedBackend() string { return candidate.selected }
func (candidate conflictCandidateStub) CandidateBackends() []string {
	return append([]string(nil), candidate.candidates...)
}

func validConflictCandidate() conflictCandidateStub {
	return conflictCandidateStub{
		lease:      probeTargetLease,
		selected:   "backend-a",
		candidates: []string{"backend-a", "backend-b"},
	}
}

func TestRequireConflictRepairEvidenceRequiresExactlyOneSelectedPositive(t *testing.T) {
	typed := &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationTyped,
		ID:   probePriorGeneration,
	}
	tests := []struct {
		name        string
		inventories map[string]Inventory
		want        error
	}{
		{
			name: "selected active provision",
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID: probeTargetLease, LifecycleGeneration: typed,
				}}, nil),
				"backend-b": completeInventory(nil, nil),
			},
		},
		{
			name: "selected retention",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, []backend.RetainedLease{{
					LeaseUUID: probeTargetLease,
				}}),
				"backend-b": completeInventory(nil, nil),
			},
		},
		{
			name: "target absent everywhere",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, nil),
				"backend-b": completeInventory(nil, nil),
			},
			want: ErrConflictOwnerEvidence,
		},
		{
			name: "positive only on another candidate",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, nil),
				"backend-b": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID: probeTargetLease,
				}}, nil),
			},
			want: ErrConflictOwnerEvidence,
		},
		{
			name: "positive on two candidates",
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID: probeTargetLease,
				}}, nil),
				"backend-b": completeInventory(nil, []backend.RetainedLease{{
					LeaseUUID: probeTargetLease,
				}}),
			},
			want: ErrConflictOwnerEvidence,
		},
		{
			name: "duplicate across selected endpoints",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(
					[]backend.ProvisionInfo{{LeaseUUID: probeTargetLease}},
					[]backend.RetainedLease{{LeaseUUID: probeTargetLease}},
				),
				"backend-b": completeInventory(nil, nil),
			},
			want: ErrIncompleteInventory,
		},
		{
			name: "one candidate probe missing",
			inventories: map[string]Inventory{
				"backend-a": completeInventory([]backend.ProvisionInfo{{
					LeaseUUID: probeTargetLease,
				}}, nil),
			},
			want: ErrIncompleteInventory,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence, err := RequireConflictRepairEvidence(
				[]string{"backend-a", "backend-b"}, test.inventories, validConflictCandidate(),
			)
			if test.want == nil {
				require.NoError(t, err)
				assert.NotEqual(t, placement.RepairInventorySnapshot{}, evidence)
				return
			}
			require.ErrorIs(t, err, test.want)
			assert.Equal(t, placement.RepairInventorySnapshot{}, evidence)
		})
	}
}

func TestRequireConflictRepairEvidenceRequiresExactCandidateTopology(t *testing.T) {
	inventories := map[string]Inventory{
		"backend-a": completeInventory([]backend.ProvisionInfo{{
			LeaseUUID: probeTargetLease,
		}}, nil),
		"backend-b": completeInventory(nil, nil),
	}
	tests := []conflictCandidateStub{
		{lease: probeTargetLease, selected: "backend-a", candidates: []string{"backend-a"}},
		{lease: probeTargetLease, selected: "backend-a", candidates: []string{"backend-b", "backend-a"}},
		{lease: probeTargetLease, selected: "backend-a", candidates: []string{"backend-a", "backend-c"}},
		{lease: probeTargetLease, selected: "backend-c", candidates: []string{"backend-a", "backend-b"}},
	}
	for _, candidate := range tests {
		_, err := RequireConflictRepairEvidence(
			[]string{"backend-a", "backend-b"}, inventories, candidate,
		)
		require.ErrorIs(t, err, ErrIncompleteInventory)
	}
}
