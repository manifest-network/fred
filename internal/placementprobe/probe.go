// Package placementprobe collects complete backend inventory snapshots for
// offline placement safety tools. It is an adapter layer: placement authority
// remains in the placement package, while config, TLS, HMAC, pagination, and
// concurrent HTTP collection stay out of that domain model.
package placementprobe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/tlsconfig"
	"github.com/manifest-network/fred/internal/util"
)

var (
	// ErrIncompleteInventory means the configured fleet was not represented by
	// one complete, structurally valid provision-and-retention snapshot per
	// backend. Absence is not evidence when this error is returned.
	ErrIncompleteInventory = errors.New("backend inventory snapshot is incomplete")

	// ErrLeasePresent means at least one configured backend reported target
	// evidence that cannot be proved to be the exact preserved pre-attempt
	// provision. An offline refusal repair must not proceed.
	ErrLeasePresent = errors.New("lease is positively present in backend inventory")

	// ErrConflictOwnerEvidence means complete inventory did not report the
	// operator-selected conflict owner exactly once while reporting the target
	// absent from every other configured backend.
	ErrConflictOwnerEvidence = errors.New("inventory does not prove exactly one selected conflict owner")
)

// Client is the narrow backend inventory port used by offline placement tools.
type Client interface {
	Name() string
	ListProvisionsWithIdentity(context.Context) ([]backend.ProvisionInfo, backendidentity.ID, error)
	ListRetentionsWithIdentity(context.Context) ([]backend.RetainedLease, backendidentity.ID, error)
}

// Inventory is one backend's complete wire inventory. ProvisionInfo is kept
// intact because an owned-attempt repair must distinguish the preserved prior
// lifecycle generation from the attempted generation; lease presence alone is
// insufficient for that proof.
type Inventory struct {
	StorageIdentity backendidentity.ID
	Provisions      []backend.ProvisionInfo
	Retentions      []backend.RetainedLease
}

// RepairCandidate is the opaque placement-owned evidence matcher consumed by
// the inventory adapter. The candidate exposes no prior lifecycle ID: it can
// only answer whether one explicit observation matches the generation that the
// exact current attempt must preserve.
type RepairCandidate interface {
	LeaseUUID() string
	MatchesPreservedProvision(string, placement.LifecycleObservation) bool
}

// ConflictRepairCandidate is the narrow read-only view of placement-owned
// conflict authority needed by the live inventory adapter.
type ConflictRepairCandidate interface {
	LeaseUUID() string
	SelectedBackend() string
	CandidateBackends() []string
}

// NewClients constructs inventory clients with the exact HMAC, timeout, and
// TLS/mTLS settings already validated in the providerd config.
func NewClients(cfg *config.Config) ([]Client, error) {
	return newClients(cfg, nil)
}

// NewIdentityBoundClients constructs offline repair clients pinned to the
// stopped placement database. This prevents a same-name replacement endpoint
// from supplying false evidence that authorizes an irreversible repair.
func NewIdentityBoundClients(
	cfg *config.Config,
	resolver backend.BackendStorageIdentityResolver,
) ([]Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("provider config is required")
	}
	if util.IsNilInterface(resolver) {
		return nil, fmt.Errorf("backend storage identity resolver is required")
	}
	owners := make(map[backendidentity.ID]string, len(cfg.Backends))
	pins := make(fixedIdentityResolver, len(cfg.Backends))
	for _, backendConfig := range cfg.Backends {
		id, bound := resolver.ExpectedBackendStorageIdentity(backendConfig.Name)
		if !bound || !id.Valid() {
			return nil, fmt.Errorf(
				"%w: backend %q has no durable storage identity; run a complete upgraded-fleet reconciliation before repair",
				ErrIncompleteInventory, backendConfig.Name,
			)
		}
		if owner, duplicate := owners[id]; duplicate && owner != backendConfig.Name {
			return nil, fmt.Errorf(
				"%w: backends %q and %q share storage identity %s",
				ErrIncompleteInventory, owner, backendConfig.Name, id,
			)
		}
		owners[id] = backendConfig.Name
		pins[backendConfig.Name] = id
	}
	return newClients(cfg, pins)
}

// fixedIdentityResolver detaches the stopped database's exact active pins at
// client construction. Requests therefore need no store lock, which permits a
// final repair probe to run while placement mutation admission is held.
type fixedIdentityResolver map[string]backendidentity.ID

func (resolver fixedIdentityResolver) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	id, ok := resolver[backendName]
	return id, ok && id.Valid()
}

func newClients(
	cfg *config.Config,
	resolver backend.BackendStorageIdentityResolver,
) ([]Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("provider config is required")
	}
	clients := make([]Client, 0, len(cfg.Backends))
	for _, backendConfig := range cfg.Backends {
		hmacSecret, err := cfg.ResolveBackendHMACSecret(backendConfig.Name)
		if err != nil {
			return nil, fmt.Errorf("backend %q: resolve HMAC secret: %w", backendConfig.Name, err)
		}
		var tlsClientConfig *tls.Config
		if backendConfig.TLSCAFile != "" || backendConfig.TLSClientCertFile != "" ||
			backendConfig.TLSClientKeyFile != "" || backendConfig.TLSSkipVerify {
			var err error
			tlsClientConfig, err = tlsconfig.ClientConfig(
				backendConfig.TLSCAFile,
				backendConfig.TLSSkipVerify,
				backendConfig.TLSClientCertFile,
				backendConfig.TLSClientKeyFile,
			)
			if err != nil {
				return nil, fmt.Errorf("backend %q: build TLS client config: %w", backendConfig.Name, err)
			}
		}
		clientConfig := backend.HTTPClientConfig{
			Name:            backendConfig.Name,
			BaseURL:         backendConfig.URL,
			Timeout:         backendConfig.Timeout,
			Secret:          string(hmacSecret),
			TLSClientConfig: tlsClientConfig,
		}
		if resolver == nil {
			clients = append(clients, backend.NewBootstrapInventoryClient(clientConfig))
			continue
		}
		client, err := backend.NewIdentityBoundHTTPClient(clientConfig, resolver)
		if err != nil {
			return nil, fmt.Errorf("backend %q: create identity-bound client: %w", backendConfig.Name, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

// Collect fetches both complete inventories from every client concurrently.
// It returns no partial map: one endpoint error cancels the remaining work and
// fails the whole snapshot.
func Collect(
	ctx context.Context,
	clients []Client,
) (map[string]Inventory, error) {
	if ctx == nil {
		return nil, fmt.Errorf("inventory context is required")
	}
	type namedClient struct {
		name   string
		client Client
	}
	namedClients := make([]namedClient, 0, len(clients))
	seenNames := make(map[string]struct{}, len(clients))
	for _, client := range clients {
		if util.IsNilInterface(client) {
			return nil, fmt.Errorf("%w: client is nil", ErrIncompleteInventory)
		}
		name := client.Name()
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("%w: client has a blank backend name", ErrIncompleteInventory)
		}
		if _, duplicate := seenNames[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate backend client %q", ErrIncompleteInventory, name)
		}
		seenNames[name] = struct{}{}
		namedClients = append(namedClients, namedClient{name: name, client: client})
	}

	inventories := make(map[string]Inventory, len(clients))
	var inventoriesMu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	for _, named := range namedClients {
		named := named
		group.Go(func() error {
			provisions, provisionID, err := named.client.ListProvisionsWithIdentity(groupCtx)
			if err != nil {
				return fmt.Errorf("%w: backend %q: collect complete provisions inventory: %w",
					ErrIncompleteInventory, named.name, err)
			}
			if provisions == nil {
				return fmt.Errorf("%w: backend %q returned a null provisions inventory",
					ErrIncompleteInventory, named.name)
			}
			retentions, retentionID, err := named.client.ListRetentionsWithIdentity(groupCtx)
			if err != nil {
				return fmt.Errorf("%w: backend %q: collect complete retentions inventory: %w",
					ErrIncompleteInventory, named.name, err)
			}
			if retentions == nil {
				return fmt.Errorf("%w: backend %q returned a null retentions inventory",
					ErrIncompleteInventory, named.name)
			}
			if !provisionID.Valid() || !retentionID.Valid() || provisionID != retentionID {
				return fmt.Errorf(
					"%w: backend %q inventory storage identities disagree (provisions=%s retentions=%s)",
					ErrIncompleteInventory, named.name, provisionID, retentionID,
				)
			}

			inventory := Inventory{
				StorageIdentity: provisionID,
				Provisions:      make([]backend.ProvisionInfo, len(provisions)),
				Retentions:      make([]backend.RetainedLease, len(retentions)),
			}
			copy(inventory.Provisions, provisions)
			copy(inventory.Retentions, retentions)
			inventoriesMu.Lock()
			inventories[named.name] = inventory
			inventoriesMu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	storageOwners := make(map[backendidentity.ID]string, len(inventories))
	for backendName, inventory := range inventories {
		if owner, duplicate := storageOwners[inventory.StorageIdentity]; duplicate && owner != backendName {
			return nil, fmt.Errorf(
				"%w: backends %q and %q share storage identity %s",
				ErrIncompleteInventory, owner, backendName, inventory.StorageIdentity,
			)
		}
		storageOwners[inventory.StorageIdentity] = backendName
	}
	return inventories, nil
}

// RequireRepairEvidence accepts only a complete point-in-time snapshot that
// rules out the attempted generation. An attempt-only target must be absent
// everywhere. A target with a confirmed owner may have exactly one active
// provision on that same backend only when its explicit lifecycle observation
// exactly matches the candidate's preserved pre-attempt generation. Retention,
// another backend, the attempted generation, or missing/unknown/unusable/
// mismatched generation evidence fails closed. Even a successful result does
// not prove that an earlier request can no longer take effect; the mutating
// command separately requires an operator drain attestation for that causal
// fact.
func RequireRepairEvidence(
	configuredBackends []string,
	inventories map[string]Inventory,
	candidate RepairCandidate,
) error {
	if util.IsNilInterface(candidate) {
		return fmt.Errorf("%w: repair candidate is required", ErrIncompleteInventory)
	}
	target := candidate.LeaseUUID()
	if !canonicalLeaseUUID(target) {
		return fmt.Errorf("%w: target lease identity %q is not a canonical UUID",
			ErrIncompleteInventory, target)
	}

	configured := make(map[string]struct{}, len(configuredBackends))
	var problems []string
	storageOwners := make(map[backendidentity.ID]string, len(inventories))
	for _, backendName := range configuredBackends {
		if strings.TrimSpace(backendName) == "" {
			problems = append(problems, "configured backend name is blank")
			continue
		}
		if _, duplicate := configured[backendName]; duplicate {
			problems = append(problems, fmt.Sprintf("configured backend %q occurs more than once", backendName))
		}
		configured[backendName] = struct{}{}
		if _, ok := inventories[backendName]; !ok {
			problems = append(problems, fmt.Sprintf("configured backend %q has no complete inventory", backendName))
		}
	}

	var disallowedPositive []string
	for backendName, inventory := range inventories {
		if _, ok := configured[backendName]; !ok {
			problems = append(problems, fmt.Sprintf("inventory was supplied for unconfigured backend %q", backendName))
		}
		if !inventory.StorageIdentity.Valid() {
			problems = append(problems, fmt.Sprintf(
				"backend %q has no valid storage identity", backendName,
			))
		} else if owner, duplicate := storageOwners[inventory.StorageIdentity]; duplicate && owner != backendName {
			problems = append(problems, fmt.Sprintf(
				"backends %q and %q share storage identity %s",
				owner, backendName, inventory.StorageIdentity,
			))
		} else {
			storageOwners[inventory.StorageIdentity] = backendName
		}
		if inventory.Provisions == nil {
			problems = append(problems, fmt.Sprintf(
				"backend %q has no complete provisions inventory", backendName,
			))
		}
		if inventory.Retentions == nil {
			problems = append(problems, fmt.Sprintf(
				"backend %q has no complete retentions inventory", backendName,
			))
		}
		seen := make(map[string]string)
		for _, provision := range inventory.Provisions {
			leaseUUID := provision.LeaseUUID
			if !canonicalLeaseUUID(leaseUUID) {
				problems = append(problems, fmt.Sprintf(
					"backend %q provisions inventory contains non-canonical lease identity %q",
					backendName, leaseUUID,
				))
				continue
			}
			if previous, duplicate := seen[leaseUUID]; duplicate {
				problems = append(problems, fmt.Sprintf(
					"backend %q reports lease %q more than once (%s and provisions)",
					backendName, leaseUUID, previous,
				))
			} else {
				seen[leaseUUID] = "provisions"
			}
			if leaseUUID == target && !candidate.MatchesPreservedProvision(
				backendName, lifecycleObservation(provision.LifecycleGeneration),
			) {
				disallowedPositive = append(disallowedPositive, fmt.Sprintf(
					"%s/provisions (not the exact preserved pre-attempt lifecycle generation)",
					backendName,
				))
			}
		}
		for _, retention := range inventory.Retentions {
			leaseUUID := retention.LeaseUUID
			if !canonicalLeaseUUID(leaseUUID) {
				problems = append(problems, fmt.Sprintf(
					"backend %q retentions inventory contains non-canonical lease identity %q",
					backendName, leaseUUID,
				))
				continue
			}
			if previous, duplicate := seen[leaseUUID]; duplicate {
				problems = append(problems, fmt.Sprintf(
					"backend %q reports lease %q more than once (%s and retentions)",
					backendName, leaseUUID, previous,
				))
			} else {
				seen[leaseUUID] = "retentions"
			}
			if leaseUUID == target {
				disallowedPositive = append(disallowedPositive, backendName+"/retentions")
			}
		}
	}

	if len(problems) != 0 {
		slices.Sort(problems)
		return fmt.Errorf("%w:\n- %s", ErrIncompleteInventory, strings.Join(problems, "\n- "))
	}
	if len(disallowedPositive) != 0 {
		slices.Sort(disallowedPositive)
		return fmt.Errorf("%w: lease %q reported by %s", ErrLeasePresent, target,
			strings.Join(disallowedPositive, ", "))
	}
	return nil
}

// VerifyRepairEvidence performs the full attempt-repair validation and returns
// placement-owned facts that the stopped Store must revalidate before it can
// mint a mutation capability. The digest covers the complete inventories, not
// only the target lease, so evidence from another snapshot cannot be swapped
// into an already-minted proof.
func VerifyRepairEvidence(
	configuredBackends []string,
	inventories map[string]Inventory,
	candidate RepairCandidate,
) (placement.RepairInventorySnapshot, error) {
	if err := RequireRepairEvidence(configuredBackends, inventories, candidate); err != nil {
		return placement.RepairInventorySnapshot{}, err
	}
	return repairInventorySnapshot(configuredBackends, inventories)
}

// RequireConflictRepairEvidence accepts only a complete structurally valid
// inventory in which the target is positively present exactly once, on the
// selected durable candidate, and absent from every other configured backend.
// Every durable candidate must belong to the exact configured fleet. The
// returned value is an opaque placement-owned fact that preserves the
// provision-versus-retention distinction; retained data never mints runtime
// lifecycle authority.
func RequireConflictRepairEvidence(
	configuredBackends []string,
	inventories map[string]Inventory,
	candidate ConflictRepairCandidate,
) (placement.RepairInventorySnapshot, error) {
	if util.IsNilInterface(candidate) {
		return placement.RepairInventorySnapshot{}, fmt.Errorf(
			"%w: conflict repair candidate is required", ErrIncompleteInventory,
		)
	}
	target := candidate.LeaseUUID()
	selected := candidate.SelectedBackend()
	if !canonicalLeaseUUID(target) {
		return placement.RepairInventorySnapshot{}, fmt.Errorf(
			"%w: target lease identity %q is not a canonical UUID",
			ErrIncompleteInventory, target,
		)
	}
	if strings.TrimSpace(selected) == "" {
		return placement.RepairInventorySnapshot{}, fmt.Errorf(
			"%w: selected backend is blank", ErrIncompleteInventory,
		)
	}

	configured := make(map[string]struct{}, len(configuredBackends))
	var problems []string
	storageOwners := make(map[backendidentity.ID]string, len(inventories))
	for _, backendName := range configuredBackends {
		if strings.TrimSpace(backendName) == "" {
			problems = append(problems, "configured backend name is blank")
			continue
		}
		if _, duplicate := configured[backendName]; duplicate {
			problems = append(problems, fmt.Sprintf(
				"configured backend %q occurs more than once", backendName,
			))
		}
		configured[backendName] = struct{}{}
		if _, ok := inventories[backendName]; !ok {
			problems = append(problems, fmt.Sprintf(
				"configured backend %q has no complete inventory", backendName,
			))
		}
	}
	candidates := candidate.CandidateBackends()
	canonicalCandidates := slices.Clone(candidates)
	slices.Sort(canonicalCandidates)
	canonicalCandidates = slices.Compact(canonicalCandidates)
	if len(candidates) < 2 || !slices.Equal(candidates, canonicalCandidates) {
		problems = append(problems, "durable conflict candidate set is incomplete or non-canonical")
	}
	if !slices.Contains(candidates, selected) {
		problems = append(problems, fmt.Sprintf(
			"selected backend %q is not a durable conflict candidate", selected,
		))
	}
	for _, backendName := range candidates {
		if _, ok := configured[backendName]; !ok {
			problems = append(problems, fmt.Sprintf(
				"durable conflict candidate %q is outside the configured fleet", backendName,
			))
		}
	}

	type positiveObservation struct {
		backendName string
		provision   *backend.ProvisionInfo
		retention   bool
	}
	var positives []positiveObservation
	for backendName, inventory := range inventories {
		if _, ok := configured[backendName]; !ok {
			problems = append(problems, fmt.Sprintf(
				"inventory was supplied for unconfigured backend %q", backendName,
			))
		}
		if !inventory.StorageIdentity.Valid() {
			problems = append(problems, fmt.Sprintf(
				"backend %q has no valid storage identity", backendName,
			))
		} else if owner, duplicate := storageOwners[inventory.StorageIdentity]; duplicate && owner != backendName {
			problems = append(problems, fmt.Sprintf(
				"backends %q and %q share storage identity %s",
				owner, backendName, inventory.StorageIdentity,
			))
		} else {
			storageOwners[inventory.StorageIdentity] = backendName
		}
		if inventory.Provisions == nil {
			problems = append(problems, fmt.Sprintf(
				"backend %q has no complete provisions inventory", backendName,
			))
		}
		if inventory.Retentions == nil {
			problems = append(problems, fmt.Sprintf(
				"backend %q has no complete retentions inventory", backendName,
			))
		}
		seen := make(map[string]string)
		for i := range inventory.Provisions {
			provision := &inventory.Provisions[i]
			leaseUUID := provision.LeaseUUID
			if !canonicalLeaseUUID(leaseUUID) {
				problems = append(problems, fmt.Sprintf(
					"backend %q provisions inventory contains non-canonical lease identity %q",
					backendName, leaseUUID,
				))
				continue
			}
			if previous, duplicate := seen[leaseUUID]; duplicate {
				problems = append(problems, fmt.Sprintf(
					"backend %q reports lease %q more than once (%s and provisions)",
					backendName, leaseUUID, previous,
				))
			} else {
				seen[leaseUUID] = "provisions"
			}
			if leaseUUID == target {
				copy := *provision
				positives = append(positives, positiveObservation{
					backendName: backendName,
					provision:   &copy,
				})
			}
		}
		for _, retention := range inventory.Retentions {
			leaseUUID := retention.LeaseUUID
			if !canonicalLeaseUUID(leaseUUID) {
				problems = append(problems, fmt.Sprintf(
					"backend %q retentions inventory contains non-canonical lease identity %q",
					backendName, leaseUUID,
				))
				continue
			}
			if previous, duplicate := seen[leaseUUID]; duplicate {
				problems = append(problems, fmt.Sprintf(
					"backend %q reports lease %q more than once (%s and retentions)",
					backendName, leaseUUID, previous,
				))
			} else {
				seen[leaseUUID] = "retentions"
			}
			if leaseUUID == target {
				positives = append(positives, positiveObservation{
					backendName: backendName,
					retention:   true,
				})
			}
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		return placement.RepairInventorySnapshot{}, fmt.Errorf(
			"%w:\n- %s", ErrIncompleteInventory, strings.Join(problems, "\n- "),
		)
	}
	if len(positives) != 1 || positives[0].backendName != selected {
		locations := make([]string, 0, len(positives))
		for _, positive := range positives {
			kind := "provisions"
			if positive.retention {
				kind = "retentions"
			}
			locations = append(locations, positive.backendName+"/"+kind)
		}
		slices.Sort(locations)
		return placement.RepairInventorySnapshot{}, fmt.Errorf(
			"%w: lease %q selected backend %q; positive locations=%v",
			ErrConflictOwnerEvidence, target, selected, locations,
		)
	}
	return repairInventorySnapshot(configuredBackends, inventories)
}

func repairInventorySnapshot(
	configuredBackends []string,
	inventories map[string]Inventory,
) (placement.RepairInventorySnapshot, error) {
	converted := make(map[string]placement.RepairBackendInventory, len(inventories))
	for backendName, inventory := range inventories {
		converted[backendName] = placement.RepairBackendInventory{
			StorageIdentity: inventory.StorageIdentity,
			Provisions:      inventory.Provisions,
			Retentions:      inventory.Retentions,
		}
	}
	return placement.NewRepairInventorySnapshot(configuredBackends, converted)
}

func lifecycleObservation(
	observation *backend.LifecycleGenerationObservation,
) placement.LifecycleObservation {
	if observation == nil {
		return placement.LifecycleObservation{Kind: placement.LifecycleObservationUnknown}
	}
	switch observation.Kind {
	case backend.LifecycleGenerationUnknown:
		if observation.ID == "" {
			return placement.LifecycleObservation{Kind: placement.LifecycleObservationUnknown}
		}
	case backend.LifecycleGenerationLegacy:
		if observation.ID == "" {
			return placement.LifecycleObservation{Kind: placement.LifecycleObservationLegacy}
		}
	case backend.LifecycleGenerationTyped:
		id, err := lifecycle.ParseID(observation.ID)
		if err == nil {
			return placement.LifecycleObservation{Kind: placement.LifecycleObservationTyped, ID: id}
		}
	case backend.LifecycleGenerationUnusable:
		if observation.ID == "" {
			return placement.LifecycleObservation{Kind: placement.LifecycleObservationUnusable}
		}
	}
	return placement.LifecycleObservation{Kind: placement.LifecycleObservationUnusable}
}

func canonicalLeaseUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}
