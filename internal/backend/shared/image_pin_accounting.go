package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	bolt "go.etcd.io/bbolt"
)

const maxTenantImagePins = 10_000

// Capacity exhaustion is an admission refusal, not journal corruption. A
// backfill can leave this lease unresolved while continuing other principals.
type imagePinCapacityRefusal struct{ reason string }

func (refusal *imagePinCapacityRefusal) Error() string { return refusal.reason }

// imagePinOwner makes all pin handles over one callback store share a single
// accounting projection and write lock. A second constructor cannot mint an
// independent share over the same durable rows.
type imagePinOwner struct {
	mu      sync.Mutex
	journal *ImagePinJournal
}

type imagePinPrincipal struct{ tenant, provider string }
type imagePinLeaseUsage struct {
	principal imagePinPrincipal
	count     int
}

type imagePinAccounting struct {
	total      int
	unassigned int
	tenants    map[string]int
	leases     map[string]imagePinLeaseUsage
}

// lockedImagePins is the scoped writer for a journal and its accounting
// projection. Only withLockedPins constructs it, holding the shared owner lock
// through durable commit and OnCommit publication. Lease locks are acquired
// inside this scope, consistently ordering pin ownership before lease ownership.
type lockedImagePins struct{ journal *ImagePinJournal }

func (j *ImagePinJournal) withLockedPins(run func(*lockedImagePins) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return run(&lockedImagePins{journal: j})
}

func (j *ImagePinJournal) loadAccounting() (*imagePinAccounting, error) {
	pins, err := j.List()
	if err != nil {
		return nil, err
	}
	accounting := &imagePinAccounting{total: len(pins), tenants: make(map[string]int), leases: make(map[string]imagePinLeaseUsage)}
	for _, pin := range pins {
		usage := accounting.leases[pin.LeaseUUID]
		usage.count++
		accounting.leases[pin.LeaseUUID] = usage
	}
	for lease, usage := range accounting.leases {
		unlock := j.lockLease(lease)
		principal, err := j.durablePinPrincipal(lease)
		unlock()
		if err != nil {
			return nil, err
		}
		usage.principal = principal
		accounting.leases[lease] = usage
		if principal.tenant == "" {
			accounting.unassigned += usage.count
		} else {
			accounting.tenants[principal.tenant] += usage.count
		}
	}
	return accounting, nil
}

// Called while the lease transition lock pins the callback, release and
// retention authorities. Missing historical ownership stays unknown; neither a
// pin's metadata nor a later request supplies an accounting principal.
func (j *ImagePinJournal) durablePinPrincipal(lease string) (imagePinPrincipal, error) {
	var principal imagePinPrincipal
	add := func(tenant, provider string) error {
		if tenant == "" && provider == "" {
			return nil
		}
		candidate := imagePinPrincipal{tenant: tenant, provider: provider}
		if tenant == "" || provider == "" || (principal.tenant != "" && principal != candidate) {
			return fmt.Errorf("image pin lease %q has contradictory durable principals", lease)
		}
		principal = candidate
		return nil
	}
	if err := j.callbacks.view(func(tx *bolt.Tx) error {
		head, _, err := getLeaseMutationHeadTx(tx, lease)
		if err != nil {
			return err
		}
		switch head := head.(type) {
		case operationLeaseMutationHead:
			return add(head.claim.Tenant(), head.claim.ProviderUUID())
		case maintenanceLeaseMutationHead:
			return add(head.claim.Tenant(), head.claim.ProviderUUID())
		case closeLeaseMutationHead:
			return add(head.claim.Tenant(), head.claim.ProviderUUID())
		case closedLeaseMutationHead:
			return add(head.entry.Tenant, head.entry.ProviderUUID)
		}
		return nil
	}); err != nil {
		return imagePinPrincipal{}, err
	}
	releases, err := j.releases.List(lease)
	if err != nil {
		return imagePinPrincipal{}, err
	}
	for _, release := range releases {
		if identity, valid := release.RuntimeIdentity(); valid {
			if err := add(identity.Tenant(), identity.ProviderUUID()); err != nil {
				return imagePinPrincipal{}, err
			}
		}
	}
	retained, err := j.retentions.Get(lease)
	if err != nil && !errors.Is(err, ErrNoRetention) {
		return imagePinPrincipal{}, err
	}
	if retained != nil {
		if err := add(retained.Tenant, retained.ProviderUUID); err != nil {
			return imagePinPrincipal{}, err
		}
	}
	return principal, nil
}

// imagePinWriteAuthority is minted only from a reverified Started origin or
// the backfiller's locked exact generation. The transaction owns this proof;
// callers cannot select a tenant accounting key independently of the write.
type imagePinWriteAuthority struct {
	transaction *imagePinTransaction
	lease       string
	principal   imagePinPrincipal
}

// imagePinTransaction carries all quota changes for one durable write. Working
// counters include earlier writes in the same transaction. Only commit
// publishes them, so a failed multi-reference backfill cannot spend capacity.
type imagePinTransaction struct {
	journal    *ImagePinJournal
	tx         *bolt.Tx
	total      int
	unassigned int
	tenants    map[string]int
	leases     map[string]imagePinLeaseUsage
}

func (pins *lockedImagePins) updatePins(mutate func(*imagePinTransaction) error) error {
	j := pins.journal
	return j.callbacks.update(func(tx *bolt.Tx) error {
		writer := &imagePinTransaction{
			journal: j, tx: tx, total: j.accounting.total, unassigned: j.accounting.unassigned,
			tenants: make(map[string]int), leases: make(map[string]imagePinLeaseUsage),
		}
		if err := mutate(writer); err != nil {
			return err
		}
		tx.OnCommit(func() {
			j.accounting.total, j.accounting.unassigned = writer.total, writer.unassigned
			for tenant, count := range writer.tenants {
				if count == 0 {
					delete(j.accounting.tenants, tenant)
				} else {
					j.accounting.tenants[tenant] = count
				}
			}
			for lease, usage := range writer.leases {
				if usage.count == 0 {
					delete(j.accounting.leases, lease)
				} else {
					j.accounting.leases[lease] = usage
				}
			}
		})
		return nil
	})
}

func (writer *imagePinTransaction) forOrigin(prepared PreparedImageInspection) (imagePinWriteAuthority, error) {
	if prepared.journal == nil || prepared.journal.store != writer.journal.callbacks {
		return imagePinWriteAuthority{}, errors.New("image pin origin belongs to another journal")
	}
	if err := prepared.verifyOrigin(writer.tx); err != nil {
		return imagePinWriteAuthority{}, err
	}
	origin := prepared.origin
	var principal imagePinPrincipal
	switch {
	case origin.operation.Valid():
		principal = imagePinPrincipal{origin.operation.Intent().Tenant(), origin.operation.Intent().ProviderUUID()}
	case origin.maintenance.Valid():
		principal = imagePinPrincipal{origin.maintenance.Intent().Tenant(), origin.maintenance.Intent().ProviderUUID()}
	case origin.compensation.Valid():
		principal = imagePinPrincipal{origin.compensation.Intent().Tenant(), origin.compensation.Intent().ProviderUUID()}
	}
	return writer.bindPrincipal(prepared.record.LeaseUUID, principal)
}

func (writer *imagePinTransaction) forBackfill(subject ImagePinBackfillSubject) (imagePinWriteAuthority, error) {
	if subject.issuer != writer.journal {
		return imagePinWriteAuthority{}, errors.New("image pin backfill belongs to another journal")
	}
	settled, err := imagePinBackfillSettled(writer.tx, subject.lease)
	if err != nil {
		return imagePinWriteAuthority{}, err
	}
	identity, valid := subject.release.RuntimeIdentity()
	if !settled || !valid {
		return imagePinWriteAuthority{}, errors.New("image pin backfill lacks a settled durable principal")
	}
	return writer.bindPrincipal(subject.lease, imagePinPrincipal{identity.Tenant(), identity.ProviderUUID()})
}

func (writer *imagePinTransaction) tenantCount(tenant string) int {
	if count, changed := writer.tenants[tenant]; changed {
		return count
	}
	return writer.journal.accounting.tenants[tenant]
}

func (writer *imagePinTransaction) leaseUsage(lease string) imagePinLeaseUsage {
	if usage, changed := writer.leases[lease]; changed {
		return usage
	}
	return writer.journal.accounting.leases[lease]
}

func (writer *imagePinTransaction) bindPrincipal(lease string, principal imagePinPrincipal) (imagePinWriteAuthority, error) {
	if !canonicalInspectionUUID(lease) || principal.tenant == "" || principal.provider == "" {
		return imagePinWriteAuthority{}, errors.New("image pin requires a durable lease principal")
	}
	usage := writer.leaseUsage(lease)
	if usage.principal.tenant != "" && usage.principal != principal {
		return imagePinWriteAuthority{}, errors.New("image pin principal differs from its durable lease")
	}
	if usage.principal.tenant == "" {
		writer.unassigned -= usage.count
		writer.tenants[principal.tenant] = writer.tenantCount(principal.tenant) + usage.count
		usage.principal = principal
		writer.leases[lease] = usage
	}
	return imagePinWriteAuthority{transaction: writer, lease: lease, principal: principal}, nil
}

func (writer *imagePinTransaction) put(authority imagePinWriteAuthority, pin ImagePin) error {
	if authority.transaction != writer || authority.lease != pin.LeaseUUID {
		return errors.New("image pin requires its exact transaction authority")
	}
	data, err := json.Marshal(pin)
	if err != nil {
		return err
	}
	if _, err := decodeImagePin(data); err != nil {
		return err
	}
	key := imagePinKey(pin.LeaseUUID, pin.ManifestHash, pin.Reference)
	bucket := writer.tx.Bucket(imagePinsBucketName)
	if bucket != nil {
		if previous := bucket.Get(key); previous != nil {
			old, err := decodeImagePin(previous)
			if err != nil {
				return err
			}
			if old.ImageID != pin.ImageID || old.Platform.OS != pin.Platform.OS || old.Platform.Architecture != pin.Platform.Architecture ||
				old.Platform.Variant != pin.Platform.Variant || old.Platform.OSVersion != pin.Platform.OSVersion ||
				!slices.Equal(old.Platform.OSFeatures, pin.Platform.OSFeatures) {
				return errors.New("image pin cannot change immutable content")
			}
			fillRecoveryDigest := old.PullDigest == "" && pin.ImportBytes > 0 && pin.PullDigest != ""
			if pin.ImportBytes <= old.ImportBytes && pin.VerificationBytes <= old.VerificationBytes && !fillRecoveryDigest {
				return nil
			}
			old.ImportBytes = max(old.ImportBytes, pin.ImportBytes)
			old.VerificationBytes = max(old.VerificationBytes, pin.VerificationBytes)
			if fillRecoveryDigest {
				old.PullDigest = pin.PullDigest
			}
			updated, err := json.Marshal(old)
			if err != nil {
				return err
			}
			return bucket.Put(key, updated)
		}
	}
	if writer.total >= maxImagePins {
		return &imagePinCapacityRefusal{reason: "image pin journal capacity exhausted"}
	}
	// Unknown legacy ownership could belong to this tenant. Charging that
	// debt to every fresh share prevents closing authority and reopening the
	// store from manufacturing another allowance. Exact reuse above remains
	// available; only positive orphan pruning can discharge unassigned rows.
	if writer.tenantCount(authority.principal.tenant)+writer.unassigned >= maxTenantImagePins {
		return &imagePinCapacityRefusal{reason: "image pin tenant capacity exhausted or legacy ownership remains unassigned"}
	}
	if bucket == nil {
		bucket, err = writer.tx.CreateBucketIfNotExists(imagePinsBucketName)
		if err != nil {
			return err
		}
	}
	if err := bucket.Put(key, data); err != nil {
		return err
	}
	usage := writer.leaseUsage(pin.LeaseUUID)
	usage.count++
	writer.leases[pin.LeaseUUID] = usage
	writer.tenants[authority.principal.tenant] = writer.tenantCount(authority.principal.tenant) + 1
	writer.total++
	return nil
}

func (writer *imagePinTransaction) remove(key []byte) error {
	bucket := writer.tx.Bucket(imagePinsBucketName)
	if bucket == nil {
		return errors.New("image pin bucket missing")
	}
	data := bucket.Get(key)
	if data == nil {
		return errors.New("image pin selected for pruning is absent")
	}
	pin, err := decodeImagePin(data)
	if err != nil {
		return err
	}
	usage := writer.leaseUsage(pin.LeaseUUID)
	if usage.count <= 0 || writer.total <= 0 {
		return errors.New("image pin accounting differs from the journal")
	}
	if err := bucket.Delete(key); err != nil {
		return err
	}
	usage.count--
	writer.leases[pin.LeaseUUID] = usage
	writer.total--
	if usage.principal.tenant == "" {
		writer.unassigned--
	} else {
		writer.tenants[usage.principal.tenant] = writer.tenantCount(usage.principal.tenant) - 1
	}
	return nil
}
