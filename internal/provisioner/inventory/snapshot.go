// Package inventory owns the process-local capability that turns two
// independently collected backend inventory endpoints into negative evidence.
// A Snapshot is useful only to the exact collector/topology that issued it and
// only until that collector starts its next epoch.
package inventory

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

var (
	ErrInvalidTopology = errors.New("invalid inventory collector topology")
	ErrInvalidSession  = errors.New("invalid inventory collection session")
)

type collectorMarker struct{ _ byte }
type sessionMarker struct{ _ byte }

// Collector is bound once to an exact backend-name topology. Begin invalidates
// every Snapshot from an older collection epoch before any external read for
// the new epoch can begin.
type Collector struct {
	mu       sync.RWMutex
	marker   *collectorMarker
	topology []string
	known    map[string]struct{}
	epoch    uint64
	current  *sessionMarker
}

// NewCollector rejects blank or duplicate backend names and retains an
// immutable canonical topology.
func NewCollector(backendNames []string) (*Collector, error) {
	canonical := slices.Clone(backendNames)
	if len(canonical) == 0 {
		return nil, fmt.Errorf("%w: at least one backend is required", ErrInvalidTopology)
	}
	seen := make(map[string]struct{}, len(canonical))
	for _, name := range canonical {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("%w: backend name is blank", ErrInvalidTopology)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate backend %q", ErrInvalidTopology, name)
		}
		seen[name] = struct{}{}
	}
	slices.Sort(canonical)
	return &Collector{
		marker: &collectorMarker{}, topology: canonical, known: seen,
	}, nil
}

// Binding is opaque proof of one exact Collector and topology. Its zero value
// is invalid; it exposes matching but no way to mint a Snapshot.
type Binding struct {
	collector *Collector
	marker    *collectorMarker
}

func (binding Binding) Valid() bool {
	return binding.collector != nil && binding.marker != nil &&
		binding.collector.marker == binding.marker
}

func (binding Binding) MatchesTopology(backendNames []string) bool {
	if !binding.Valid() {
		return false
	}
	canonical := slices.Clone(backendNames)
	slices.Sort(canonical)
	return slices.Equal(canonical, binding.collector.topology)
}

func (collector *Collector) Binding() Binding {
	if collector == nil || collector.marker == nil {
		return Binding{}
	}
	return Binding{collector: collector, marker: collector.marker}
}

// Session is the mutable, single-epoch collection sink. Endpoint observations
// are recorded independently so success from one endpoint can never stand in
// for the other. Seal returns detached immutable maps.
type Session struct {
	mu        sync.Mutex
	collector *Collector
	issuer    *sessionMarker
	epoch     uint64
	sealed    bool
	provision map[string]endpointObservation
	retention map[string]endpointObservation
	untrusted map[string]map[string]struct{}
}

type endpointObservation struct {
	storageID backendidentity.ID
	present   map[string]struct{}
	// provisions is populated only for the provision endpoint. Keeping the
	// identity and lifecycle generation beside membership makes it impossible
	// to splice a generation collected from another backend, lease, or epoch
	// into an otherwise valid Snapshot.
	provisions map[string]ProvisionObservation
}

// ProvisionObservation is an immutable identity-bearing row from the exact
// provision endpoint recorded in a Session. Its fields are deliberately
// private: callers can inspect a row returned by Snapshot.Provision, but only a
// Session can seal one into inventory authority.
type ProvisionObservation struct {
	leaseUUID    string
	backendName  string
	providerUUID string
	tenant       string
	generation   *backend.LifecycleGenerationObservation
}

func provisionObservation(info backend.ProvisionInfo) ProvisionObservation {
	var generation *backend.LifecycleGenerationObservation
	if info.LifecycleGeneration != nil {
		copy := *info.LifecycleGeneration
		generation = &copy
	}
	return ProvisionObservation{
		leaseUUID: info.LeaseUUID, backendName: info.BackendName,
		providerUUID: info.ProviderUUID, tenant: info.Tenant,
		generation: generation,
	}
}

func (observation ProvisionObservation) LeaseUUID() string    { return observation.leaseUUID }
func (observation ProvisionObservation) BackendName() string  { return observation.backendName }
func (observation ProvisionObservation) ProviderUUID() string { return observation.providerUUID }
func (observation ProvisionObservation) Tenant() string       { return observation.tenant }

// LifecycleGeneration returns a detached copy of the generation carried by
// this exact sealed provision row. Nil is the explicit legacy/third-party
// "unknown" wire observation; callers cannot mutate the Snapshot through it.
func (observation ProvisionObservation) LifecycleGeneration() *backend.LifecycleGenerationObservation {
	if observation.generation == nil {
		return nil
	}
	copy := *observation.generation
	return &copy
}

func (collector *Collector) Begin() *Session {
	if collector == nil || collector.marker == nil {
		return nil
	}
	collector.mu.Lock()
	collector.epoch++
	issuer := &sessionMarker{}
	collector.current = issuer
	epoch := collector.epoch
	collector.mu.Unlock()
	return &Session{
		collector: collector, issuer: issuer, epoch: epoch,
		provision: make(map[string]endpointObservation, len(collector.topology)),
		retention: make(map[string]endpointObservation, len(collector.topology)),
		untrusted: make(map[string]map[string]struct{}),
	}
}

func (session *Session) record(
	destination map[string]endpointObservation,
	backendName string,
	storageID backendidentity.ID,
	leaseUUIDs []string,
) error {
	if session == nil || session.collector == nil || session.issuer == nil ||
		!storageID.Valid() {
		return ErrInvalidSession
	}
	if _, configured := session.collector.known[backendName]; !configured {
		return fmt.Errorf("%w: backend %q is outside collector topology", ErrInvalidSession, backendName)
	}
	if _, duplicate := destination[backendName]; duplicate {
		return fmt.Errorf("%w: backend %q endpoint was recorded twice", ErrInvalidSession, backendName)
	}
	present := make(map[string]struct{}, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		if strings.TrimSpace(leaseUUID) == "" {
			return fmt.Errorf("%w: blank lease identity", ErrInvalidSession)
		}
		present[leaseUUID] = struct{}{}
	}
	destination[backendName] = endpointObservation{storageID: storageID, present: present}
	return nil
}

func (session *Session) RecordProvision(
	backendName string,
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.sealed {
		return ErrInvalidSession
	}
	if session.collector == nil || session.issuer == nil || !storageID.Valid() {
		return ErrInvalidSession
	}
	if _, configured := session.collector.known[backendName]; !configured {
		return fmt.Errorf("%w: backend %q is outside collector topology", ErrInvalidSession, backendName)
	}
	if _, duplicate := session.provision[backendName]; duplicate {
		return fmt.Errorf("%w: backend %q endpoint was recorded twice", ErrInvalidSession, backendName)
	}
	present := make(map[string]struct{}, len(provisions))
	observations := make(map[string]ProvisionObservation, len(provisions))
	for _, provision := range provisions {
		if strings.TrimSpace(provision.LeaseUUID) == "" {
			return fmt.Errorf("%w: blank lease identity", ErrInvalidSession)
		}
		if provision.BackendName != backendName {
			return fmt.Errorf(
				"%w: provision %q names backend %q, expected %q",
				ErrInvalidSession, provision.LeaseUUID, provision.BackendName, backendName,
			)
		}
		if _, duplicate := present[provision.LeaseUUID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate provision %q from backend %q",
				ErrInvalidSession, provision.LeaseUUID, backendName,
			)
		}
		present[provision.LeaseUUID] = struct{}{}
		observations[provision.LeaseUUID] = provisionObservation(provision)
	}
	session.provision[backendName] = endpointObservation{
		storageID: storageID, present: present, provisions: observations,
	}
	return nil
}

func (session *Session) RecordRetention(
	backendName string,
	storageID backendidentity.ID,
	leaseUUIDs []string,
) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.sealed {
		return ErrInvalidSession
	}
	return session.record(session.retention, backendName, storageID, leaseUUIDs)
}

// RecordBackend atomically records the two independently fetched endpoint
// responses for one backend. It is the construction boundary used after the
// placement sweep has verified their shared physical identity. Neither half
// becomes negative evidence if validation of the other half fails.
func (session *Session) RecordBackend(
	backendName string,
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
	retentionLeaseUUIDs []string,
) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.sealed || session.collector == nil || session.issuer == nil ||
		!storageID.Valid() {
		return ErrInvalidSession
	}
	if _, configured := session.collector.known[backendName]; !configured {
		return fmt.Errorf("%w: backend %q is outside collector topology", ErrInvalidSession, backendName)
	}
	if _, duplicate := session.provision[backendName]; duplicate {
		return fmt.Errorf("%w: backend %q provision endpoint was recorded twice", ErrInvalidSession, backendName)
	}
	if _, duplicate := session.retention[backendName]; duplicate {
		return fmt.Errorf("%w: backend %q retention endpoint was recorded twice", ErrInvalidSession, backendName)
	}

	provisionPresent := make(map[string]struct{}, len(provisions))
	provisionObservations := make(map[string]ProvisionObservation, len(provisions))
	for _, provision := range provisions {
		if strings.TrimSpace(provision.LeaseUUID) == "" {
			return fmt.Errorf("%w: blank lease identity", ErrInvalidSession)
		}
		if provision.BackendName != backendName {
			return fmt.Errorf(
				"%w: provision %q names backend %q, expected %q",
				ErrInvalidSession, provision.LeaseUUID, provision.BackendName, backendName,
			)
		}
		if _, duplicate := provisionPresent[provision.LeaseUUID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate provision %q from backend %q",
				ErrInvalidSession, provision.LeaseUUID, backendName,
			)
		}
		provisionPresent[provision.LeaseUUID] = struct{}{}
		provisionObservations[provision.LeaseUUID] = provisionObservation(provision)
	}
	retentionPresent := make(map[string]struct{}, len(retentionLeaseUUIDs))
	for _, leaseUUID := range retentionLeaseUUIDs {
		if strings.TrimSpace(leaseUUID) == "" {
			return fmt.Errorf("%w: blank lease identity", ErrInvalidSession)
		}
		if _, duplicate := retentionPresent[leaseUUID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate retention %q from backend %q",
				ErrInvalidSession, leaseUUID, backendName,
			)
		}
		if _, provisioned := provisionPresent[leaseUUID]; provisioned {
			return fmt.Errorf(
				"%w: lease %q is both provisioned and retained on backend %q",
				ErrInvalidSession, leaseUUID, backendName,
			)
		}
		retentionPresent[leaseUUID] = struct{}{}
	}

	session.provision[backendName] = endpointObservation{
		storageID: storageID, present: provisionPresent, provisions: provisionObservations,
	}
	session.retention[backendName] = endpointObservation{
		storageID: storageID, present: retentionPresent,
	}
	return nil
}

// RecordUntrusted records conservative positive membership whose endpoint
// payload was not admitted as routing authority. It can prevent absence and
// support a quarantine, but can never establish a positive owner.
func (session *Session) RecordUntrusted(backendName string, leaseUUIDs []string) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.sealed || session.collector == nil || session.issuer == nil {
		return ErrInvalidSession
	}
	if _, configured := session.collector.known[backendName]; !configured {
		return fmt.Errorf("%w: backend %q is outside collector topology", ErrInvalidSession, backendName)
	}
	present := session.untrusted[backendName]
	if present == nil {
		present = make(map[string]struct{}, len(leaseUUIDs))
		session.untrusted[backendName] = present
	}
	for _, leaseUUID := range leaseUUIDs {
		if strings.TrimSpace(leaseUUID) == "" {
			return fmt.Errorf("%w: blank lease identity", ErrInvalidSession)
		}
		present[leaseUUID] = struct{}{}
	}
	return nil
}

// Snapshot is a sealed observation from one exact collection epoch. Its maps,
// issuer, and epoch are private so callers can transport but not assemble or
// combine negative evidence.
type Snapshot struct {
	collector *Collector
	issuer    *sessionMarker
	epoch     uint64
	provision map[string]endpointObservation
	retention map[string]endpointObservation
	untrusted map[string]map[string]struct{}
}

// Present distinguishes an issued Snapshot from its invalid zero value without
// asserting that the issuing epoch is still current for any particular binding.
func (snapshot Snapshot) Present() bool {
	return snapshot.collector != nil && snapshot.issuer != nil && snapshot.epoch != 0
}

func cloneObservations(input map[string]endpointObservation) map[string]endpointObservation {
	result := make(map[string]endpointObservation, len(input))
	for backendName, observation := range input {
		present := make(map[string]struct{}, len(observation.present))
		for leaseUUID := range observation.present {
			present[leaseUUID] = struct{}{}
		}
		result[backendName] = endpointObservation{
			storageID: observation.storageID, present: present,
			provisions: maps.Clone(observation.provisions),
		}
	}
	return result
}

// Provision returns the immutable row sealed for one exact backend and lease.
// Retention-only membership has no ProvisionObservation and therefore cannot
// manufacture lifecycle or runtime-principal authority.
func (snapshot Snapshot) Provision(
	binding Binding,
	backendName string,
	leaseUUID string,
) (ProvisionObservation, bool) {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return ProvisionObservation{}, false
	}
	endpoint, recorded := snapshot.provision[backendName]
	if !recorded {
		return ProvisionObservation{}, false
	}
	observation, present := endpoint.provisions[leaseUUID]
	return observation, present
}

func cloneMembership(input map[string]map[string]struct{}) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{}, len(input))
	for backendName, leases := range input {
		result[backendName] = maps.Clone(leases)
	}
	return result
}

func (session *Session) Seal() (Snapshot, error) {
	if session == nil {
		return Snapshot{}, ErrInvalidSession
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.sealed || session.collector == nil || session.issuer == nil {
		return Snapshot{}, ErrInvalidSession
	}
	session.collector.mu.RLock()
	current := session.collector.current == session.issuer &&
		session.collector.epoch == session.epoch
	session.collector.mu.RUnlock()
	if !current {
		return Snapshot{}, ErrInvalidSession
	}
	session.sealed = true
	return Snapshot{
		collector: session.collector, issuer: session.issuer, epoch: session.epoch,
		provision: cloneObservations(session.provision),
		retention: cloneObservations(session.retention),
		untrusted: cloneMembership(session.untrusted),
	}, nil
}

// ValidFor rejects zero, foreign, mixed, or stale snapshots. Starting a newer
// collection epoch invalidates an older snapshot even if it was never used.
func (snapshot Snapshot) ValidFor(binding Binding) bool {
	if !binding.Valid() || snapshot.collector == nil || snapshot.issuer == nil ||
		snapshot.collector != binding.collector {
		return false
	}
	snapshot.collector.mu.RLock()
	defer snapshot.collector.mu.RUnlock()
	return snapshot.collector.marker == binding.marker &&
		snapshot.collector.current == snapshot.issuer &&
		snapshot.collector.epoch == snapshot.epoch
}

// OwnerAbsent proves both independent endpoints for one fixed backend reported
// the same expected physical storage identity and neither reported leaseUUID.
// Presence in either endpoint always wins.
func (snapshot Snapshot) OwnerAbsent(
	binding Binding,
	backendName string,
	storageID backendidentity.ID,
	leaseUUID string,
) bool {
	if !snapshot.ValidFor(binding) || !storageID.Valid() || leaseUUID == "" {
		return false
	}
	provision, provisioned := snapshot.provision[backendName]
	retention, retained := snapshot.retention[backendName]
	if !provisioned || !retained ||
		provision.storageID != storageID || retention.storageID != storageID {
		return false
	}
	_, provisionPresent := provision.present[leaseUUID]
	_, retentionPresent := retention.present[leaseUUID]
	_, untrustedPresent := snapshot.untrusted[backendName][leaseUUID]
	return !provisionPresent && !retentionPresent && !untrustedPresent
}

// LeasePresent reports conservative positive membership in either independent
// endpoint of any backend recorded in this snapshot.
func (snapshot Snapshot) LeasePresent(binding Binding, leaseUUID string) bool {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return false
	}
	for _, observations := range []map[string]endpointObservation{
		snapshot.provision, snapshot.retention,
	} {
		for _, observation := range observations {
			if _, present := observation.present[leaseUUID]; present {
				return true
			}
		}
	}
	for _, leases := range snapshot.untrusted {
		if _, present := leases[leaseUUID]; present {
			return true
		}
	}
	return false
}

// TrustedReporter reports whether the backend's independently successful
// endpoints agree on physical identity and at least one reports the lease.
// This is the only snapshot fact that can support a positive owner.
func (snapshot Snapshot) TrustedReporter(
	binding Binding,
	backendName string,
	leaseUUID string,
) bool {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return false
	}
	provision, provisioned := snapshot.provision[backendName]
	retention, retained := snapshot.retention[backendName]
	if !provisioned || !retained || provision.storageID != retention.storageID {
		return false
	}
	_, provisionPresent := provision.present[leaseUUID]
	_, retentionPresent := retention.present[leaseUUID]
	return provisionPresent || retentionPresent
}

// Reporter reports any conservative membership fact, including a rejected or
// single-endpoint observation. It can justify quarantine but never ownership.
func (snapshot Snapshot) Reporter(
	binding Binding,
	backendName string,
	leaseUUID string,
) bool {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return false
	}
	if observation, ok := snapshot.provision[backendName]; ok {
		if _, present := observation.present[leaseUUID]; present {
			return true
		}
	}
	if observation, ok := snapshot.retention[backendName]; ok {
		if _, present := observation.present[leaseUUID]; present {
			return true
		}
	}
	_, present := snapshot.untrusted[backendName][leaseUUID]
	return present
}

// UntrustedReporter reports whether backendName supplied an explicitly
// rejected positive for leaseUUID. Unlike Reporter, this preserves the
// observation class: an existing placement on the same backend cannot turn
// rejected identity/lifecycle evidence into a redundant trusted positive.
func (snapshot Snapshot) UntrustedReporter(
	binding Binding,
	backendName string,
	leaseUUID string,
) bool {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return false
	}
	_, present := snapshot.untrusted[backendName][leaseUUID]
	return present
}

// RetentionReporter reports whether the authoritative retention endpoint for
// backendName included leaseUUID. This preserves the positive's semantic
// class: retention proves that data exists on a backend, but it does not prove
// current runtime ownership.
func (snapshot Snapshot) RetentionReporter(
	binding Binding,
	backendName string,
	leaseUUID string,
) bool {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return false
	}
	observation, recorded := snapshot.retention[backendName]
	if !recorded {
		return false
	}
	_, present := observation.present[leaseUUID]
	return present
}

// Complete reports whether every configured backend contributed both endpoint
// observations with one matching physical storage identity.
func (snapshot Snapshot) Complete(binding Binding) bool {
	if !snapshot.ValidFor(binding) {
		return false
	}
	for _, backendName := range snapshot.collector.topology {
		provision, provisioned := snapshot.provision[backendName]
		retention, retained := snapshot.retention[backendName]
		if !provisioned || !retained || provision.storageID != retention.storageID {
			return false
		}
		if len(snapshot.untrusted[backendName]) != 0 {
			return false
		}
	}
	return true
}

// StorageIdentities derives the paired identity observations in this snapshot.
// A backend with only one endpoint or mismatched endpoints is omitted.
func (snapshot Snapshot) StorageIdentities(binding Binding) map[string]backendidentity.ID {
	if !snapshot.ValidFor(binding) {
		return nil
	}
	identities := make(map[string]backendidentity.ID, len(snapshot.collector.topology))
	for _, backendName := range snapshot.collector.topology {
		provision, provisioned := snapshot.provision[backendName]
		retention, retained := snapshot.retention[backendName]
		if provisioned && retained && provision.storageID == retention.storageID {
			identities[backendName] = provision.storageID
		}
	}
	return identities
}

// EmptyBackends derives drain evidence only from a complete snapshot. Both
// endpoint membership sets must be empty for the backend.
func (snapshot Snapshot) EmptyBackends(binding Binding) []string {
	if !snapshot.Complete(binding) {
		return nil
	}
	empty := make([]string, 0, len(snapshot.collector.topology))
	for _, backendName := range snapshot.collector.topology {
		if len(snapshot.provision[backendName].present) == 0 &&
			len(snapshot.retention[backendName].present) == 0 {
			empty = append(empty, backendName)
		}
	}
	return empty
}

// LeaseReporters returns the canonical backend set that reported leaseUUID in
// either endpoint. It is detached and carries facts, not mutation authority.
func (snapshot Snapshot) LeaseReporters(binding Binding, leaseUUID string) []string {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return nil
	}
	reporters := make(map[string]struct{})
	for backendName, observation := range snapshot.provision {
		if _, present := observation.present[leaseUUID]; present {
			reporters[backendName] = struct{}{}
		}
	}
	for backendName, observation := range snapshot.retention {
		if _, present := observation.present[leaseUUID]; present {
			reporters[backendName] = struct{}{}
		}
	}
	for backendName, leases := range snapshot.untrusted {
		if _, present := leases[leaseUUID]; present {
			reporters[backendName] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(reporters))
}

// RetentionReporters returns the canonical backend set whose authoritative
// retention endpoint reported leaseUUID. It is detached observation data, not
// permission to establish an owner.
func (snapshot Snapshot) RetentionReporters(binding Binding, leaseUUID string) []string {
	if !snapshot.ValidFor(binding) || leaseUUID == "" {
		return nil
	}
	reporters := make(map[string]struct{})
	for backendName, observation := range snapshot.retention {
		if _, present := observation.present[leaseUUID]; present {
			reporters[backendName] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(reporters))
}

// LeaseUUIDs returns every positive lease identity in the sealed aggregate.
func (snapshot Snapshot) LeaseUUIDs(binding Binding) []string {
	if !snapshot.ValidFor(binding) {
		return nil
	}
	leases := make(map[string]struct{})
	for _, observations := range []map[string]endpointObservation{
		snapshot.provision, snapshot.retention,
	} {
		for _, observation := range observations {
			for leaseUUID := range observation.present {
				leases[leaseUUID] = struct{}{}
			}
		}
	}
	for _, untrustedLeases := range snapshot.untrusted {
		for leaseUUID := range untrustedLeases {
			leases[leaseUUID] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(leases))
}
