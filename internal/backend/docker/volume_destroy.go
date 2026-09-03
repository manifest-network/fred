package docker

// Managed-volume destruction, and the one ownership question every destroy must ask.
//
// Its one invariant: a volume is destroyed only when the lease asking for it is the
// lease that owns the bytes. That has to be a choke point rather than a rule, because
// a volume's NAME does not answer the question. While a retention record is restoring,
// the ORIGINAL lease's data physically wears the NEW lease's canonical name
// (adoptRetainedVolumes renames fred-retained-{orig}-{svc}-{i} → fred-{new}-{svc}-{i}
// and leaves it there until the restore commits or rolls back), so for that whole
// window a name-derived destroy set contains another lease's data. Six sites derived
// such a set independently, three of them guarded, and each unguarded one has been its
// own data-loss ticket: ENG-505, ENG-501, ENG-523, ENG-647, ENG-659.
//
// So ownership is resolved HERE, once, from the control plane — the live provision map
// plus the retention store — and `Destroy` is deliberately absent from the
// volumeManager interface the rest of the package holds, which makes
// `b.volumes.Destroy(...)` a compile error everywhere else. Go has no file-level
// visibility, so that stops the unchecked CALL, not the capability itself: another file
// in this package could re-obtain it with the same type assertion. The forbidigo rule in
// .golangci.yml (pattern `\.Destroy$`, excluded only for this file) is what closes that
// half. (ENG-658)

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// volumeDestroyer is the destroy capability, split off volumeManager so no other call
// site can reach it through b.volumes. It is obtained by assertion in volumeOp rather
// than held as a second Backend field on purpose: ~150 tests assign b.volumes directly,
// and a parallel field would silently keep pointing at the manager they replaced —
// destroying through the wrong one. New asserts it once at startup so a
// misconfiguration fails there rather than mid-reap (see assertVolumeDestroyer).
type volumeDestroyer interface {
	Destroy(ctx context.Context, id string) error
}

// claimKind records WHY a volume is spoken for. It never affects whether a destroy is
// permitted — that is decided entirely by the owner — but it is what makes a refusal
// log legible, and it is the vocabulary a reader needs to check this table against the
// retention state machine.
type claimKind uint8

const (
	// claimLive: a tracked provision mounts this canonical name right now.
	claimLive claimKind = iota + 1
	// claimRetained: an ACTIVE record's canonical name, not yet renamed into the
	// fred-retained- namespace (a crash between PutActiveMerged and the rename), or
	// already renamed back by reconcileRetentions' active arm.
	claimRetained
	// claimAdopted: an in-flight restore has adopted this under the NEW lease's
	// canonical name. This is the case the whole file exists for.
	claimAdopted
	// claimRestoreSrc: a restoring record's ORIGINAL-lease canonical name, which can
	// still be on disk when the soft-delete that created the record was interrupted
	// part-way through its renames.
	claimRestoreSrc
)

func (k claimKind) String() string {
	switch k {
	case claimLive:
		return "live_provision"
	case claimRetained:
		return "retained_record"
	case claimAdopted:
		return "restore_adopted"
	case claimRestoreSrc:
		return "restore_source"
	default:
		return "unknown"
	}
}

// volumeClaim is one volume's owner: the lease whose bytes those are, which is NOT
// always the lease its name is built from.
type volumeClaim struct {
	kind  claimKind
	owner string
}

// volumeClaims is the owner table: every managed volume name the control plane can
// currently account for, mapped to the lease that owns it. A name absent from the table
// is owned by nobody, which is exactly what makes it collectable.
//
// It is derived on demand rather than persisted. The inputs (b.provisions, the
// retention store) are already the authority for this question; a second persisted copy
// would be a third thing to keep in sync, and the one recurring failure of this design
// family is a garbage collector acting on a stale view while the owners are live.
type volumeClaims struct {
	byName map[string]volumeClaim
}

// owner reports the claim on name, if any.
func (c *volumeClaims) owner(name string) (volumeClaim, bool) {
	if c == nil {
		return volumeClaim{}, false
	}
	claim, ok := c.byName[name]
	return claim, ok
}

// mayDestroy reports whether the lease asking (owner) is entitled to destroy name.
// Unclaimed names are destroyable by anyone — that is the orphan collector's whole job.
func (c *volumeClaims) mayDestroy(name, owner string) (volumeClaim, bool) {
	claim, claimed := c.owner(name)
	if !claimed {
		return volumeClaim{}, true
	}
	return claim, claimPermits(claim, owner)
}

// claimPermits reports whether owner may destroy bytes an EXISTING claim covers.
// An owner of "" asserts no ownership at all, so any claim refuses it; that is how
// the collectors ask "is this spoken for?" without a second mechanism.
//
// Split out of mayDestroy so the destroy-time re-check (destroyOne) applies the
// identical rule to a freshly-read claim. Two copies of this predicate is exactly
// the drift this file exists to prevent.
func claimPermits(claim volumeClaim, owner string) bool {
	return owner != "" && claim.owner == owner
}

// snapshotVolumeClaims builds the owner table from the two sources that know: the live
// provision map and the retention store.
//
// A read error returns (nil, err) and never a partial table — a partial one would read
// as "unclaimed", which is the direction that destroys data.
func (b *Backend) snapshotVolumeClaims() (*volumeClaims, error) {
	claims := &volumeClaims{byName: make(map[string]volumeClaim)}

	// Live provisions. Deliberately calls canonicalVolumeName rather than formatting
	// the name inline: cleanupOrphanedVolumes used to keep its own copy of that format
	// string, which is precisely the kind of duplicate that drifts.
	b.provisionsMu.RLock()
	for leaseUUID, prov := range b.provisions {
		for _, item := range prov.Items {
			for i := range item.Quantity {
				claims.byName[canonicalVolumeName(leaseUUID, item.ServiceName, i)] = volumeClaim{
					kind: claimLive, owner: leaseUUID,
				}
			}
		}
	}
	b.provisionsMu.RUnlock()

	// A nil store means no record can exist, so no record-derived claim can either.
	if b.retentionStore == nil {
		return claims, nil
	}
	recs, err := b.retentionStore.List()
	if err != nil {
		return nil, fmt.Errorf("list retention records: %w", err)
	}
	for _, e := range recs {
		switch e.Status {
		case shared.RetentionStatusActive:
			for _, retained := range e.RetainedVolumeNames {
				claims.byName[canonicalFromRetained(retained)] = volumeClaim{
					kind: claimRetained, owner: e.OriginalLeaseUUID,
				}
			}
		case shared.RetentionStatusRestoring:
			for _, retained := range e.RetainedVolumeNames {
				// The adopted volume wears the NEW lease's name but belongs to the
				// ORIGINAL lease — the record is its finalizer and reconcileRestoring
				// will re-quarantine it on rollback. Attributing it to the new lease
				// here would authorize exactly the close that ENG-647 fixed.
				claims.byName[retainedToNewCanonical(retained, e.OriginalLeaseUUID, e.NewLeaseUUID)] = volumeClaim{
					kind: claimAdopted, owner: e.OriginalLeaseUUID,
				}
				claims.byName[canonicalFromRetained(retained)] = volumeClaim{
					kind: claimRestoreSrc, owner: e.OriginalLeaseUUID,
				}
			}
		case shared.RetentionStatusReaping:
			// No claim, deliberately. A reaping record is a scheduled DESTROY, not an
			// assertion of ownership — claiming its names would make the finalizer
			// unable to run its own tombstone. The names it carries are re-checked
			// against this table at destroy time all the same (ENG-659), which is what
			// stops a tombstone written before ENG-647 from naming live data.
		}
	}
	return claims, nil
}

// liveClaim reports the live-provision claim on name, read fresh from b.provisions
// rather than from an op's cached table.
//
// This is the destroy-time half of the serialization the claim table cannot provide
// on its own (ENG-681). The table is a point-in-time answer; the writer that races it
// is Provision publishing its reservation, so the check that has to be atomic with the
// RemoveAll is this one, taken under the volume's stripe.
//
// It walks BACKWARDS from the name to its lease rather than rebuilding the table, so it
// costs one map lookup plus one lease's items — it runs once per name inside the destroy
// loop, and an O(all leases) rebuild there would be quadratic in a fleet-sized reap.
//
// Record-derived claims are deliberately not re-read: they are keyed by record, not by
// volume name, so answering "does any record claim this name?" needs the whole-store read
// the op already paid for once. The record transitions that would matter (a restore
// claiming a name mid-sweep) are CAS'd in bbolt and are a different shape from this race.
func leaseUUIDFromVolumeName(name string) (string, bool) {
	if isRetainedVolume(name) {
		return "", false
	}
	managedName, err := parseManagedVolumeName(name)
	if err != nil {
		return "", false
	}
	return managedVolumeLeaseUUID(managedName), true
}

func (b *Backend) liveClaim(name string) (volumeClaim, bool) {
	leaseUUID, ok := leaseUUIDFromVolumeName(name)
	if !ok {
		return volumeClaim{}, false
	}
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	prov, ok := b.provisions[leaseUUID]
	if !ok {
		return volumeClaim{}, false
	}
	for _, item := range prov.Items {
		for i := range item.Quantity {
			if canonicalVolumeName(leaseUUID, item.ServiceName, i) == name {
				return volumeClaim{kind: claimLive, owner: leaseUUID}, true
			}
		}
	}
	return volumeClaim{}, false
}

// destroyReport is one volumeOp.destroy call's outcome, per name. Callers key real
// decisions on the difference between these buckets — whether to release a pool
// reservation, whether to keep a lease Failed for retry, whether to drop a retention
// record — so they are kept apart rather than collapsed into an error.
type destroyReport struct {
	// Destroyed: the bytes are gone. Includes the idempotent no-op for a name that was
	// already absent, which every Destroy implementation reports as success.
	Destroyed []string
	// Claimed: another lease owns these bytes. Not an error — the deliberate skip.
	// They are still on disk, so their footprint must stay counted.
	Claimed []string
	// ClaimedBy records WHO owns each refused name. Callers need it because the claim
	// kind decides how the refusal resolves, and therefore what an operator should do:
	// a restore-held name clears itself when that restore rolls back, whereas a name a
	// LIVE provision holds clears only when that lease is next closed. Reporting both as
	// one reason sends the runbook's triage after a restore that does not exist.
	ClaimedBy map[string]volumeClaim
	// Unproven: ownership could not be established (the retention store could not be
	// read, or the manager cannot destroy at all). Nothing was attempted.
	Unproven []string
	// Errs: the destroy was permitted and failed. The bytes may be partly on disk.
	Errs []error
}

// refused counts names left alone because ownership said no, or could not say.
func (r destroyReport) refused() int { return len(r.Claimed) + len(r.Unproven) }

// leftOnDisk reports whether ANY byte this call was asked to remove may still be there.
// It is the question every accounting caller actually has: a reservation may be
// released, or a record dropped, only when this is false.
func (r destroyReport) leftOnDisk() bool { return r.refused() > 0 || len(r.Errs) > 0 }

// err folds the outcomes a caller must treat as a failure into one error, and returns
// nil otherwise. Unproven is included: an unreadable claim table means the caller could
// not do the job it was asked to do, and must retry rather than report success.
// Claimed is deliberately excluded — nothing went wrong, another owner has it.
func (r destroyReport) err() error {
	errs := r.Errs
	if len(r.Unproven) > 0 {
		errs = append(errs, fmt.Errorf("ownership unprovable for %d volume(s): %v", len(r.Unproven), r.Unproven))
	}
	return errors.Join(errs...)
}

// volumeOp is one logical operation's guarded access to volume destruction.
//
// owner is the lease asserting ownership; "" means no lease does, which is how the two
// collectors (the startup orphan sweep and the retention finalizer) ask for "destroy
// only what nothing claims". The claim table is resolved lazily and at most once per
// op, so a close pays one retention-store read no matter how many volumes it touches —
// the same cost it paid before this existed.
//
// Scope an op to a single operation and let it go. It is deliberately not cached on the
// Backend: the table is a point-in-time answer, and the failure mode this whole file
// guards against is a collector acting on a stale one.
//
// The table is a snapshot, so it is NOT on its own a verdict that survives to the
// RemoveAll it authorizes. What makes the decision atomic is destroyOne: it takes the
// volume name's stripe — the same one setupVolBinds holds across its create-or-reuse —
// and re-reads the live claim under it. A re-provision that has adopted the directory is
// therefore seen and refused, rather than having the bytes deleted from under it mid-loop
// (ENG-681; before ENG-658's choke point that same interleaving had no check at all).
//
// Two things this deliberately does not promise. The record-derived half of the table is
// still point-in-time — see liveClaim for why re-reading it per name is the wrong trade.
// And a provision that arrives entirely AFTER a name's check still finds its directory
// gone: the destroy was correct on everything the control plane knew, and what it removed
// is data a give-up had already declared abandoned (ENG-676). That is a decision to
// revisit at the give-up, not a race to close here.
type volumeOp struct {
	b        *Backend
	owner    string
	logger   *slog.Logger
	resolved bool
	table    *volumeClaims
	resolveE error
}

// volumeOp starts a guarded operation on behalf of owner ("" for the collectors).
func (b *Backend) volumeOp(owner string, logger *slog.Logger) *volumeOp {
	if logger == nil {
		logger = b.logger
	}
	return &volumeOp{b: b, owner: owner, logger: logger}
}

// claims resolves the owner table, once. Callers that do per-name work before deciding
// anything should call this first so they can bail on a read error without having done
// it.
func (o *volumeOp) claims() (*volumeClaims, error) {
	if !o.resolved {
		o.table, o.resolveE = o.b.snapshotVolumeClaims()
		o.resolved = true
	}
	return o.table, o.resolveE
}

// partition splits candidate names into the ones this op's owner may act on and the
// ones another lease owns.
//
// It exists because destroying is not the only way to lose a volume: the close path's
// retain arm RENAMES what it believes are its own volumes into
// fred-retained-{closingLease}-*, and doing that to an adopted volume leaves the
// original record naming a path that no longer exists — the same data loss by a
// different verb (ENG-647). So the ownership question has to be answerable as a query,
// not only as a guarded action.
func (o *volumeOp) partition(candidates []string) (mine, foreign []string, err error) {
	table, err := o.claims()
	if err != nil {
		return nil, nil, err
	}
	for _, name := range candidates {
		if claim, ok := table.mayDestroy(name, o.owner); ok {
			mine = append(mine, name)
		} else {
			foreign = append(foreign, name)
			o.logger.Warn("volume is claimed by another lease; leaving it alone",
				"volume_id", name, "asking_lease_uuid", o.owner,
				"owner_lease_uuid", claim.owner, "claim", claim.kind.String())
		}
	}
	return mine, foreign, nil
}

// destroy removes every name this op's owner is entitled to remove, and is the only
// caller of volumeDestroyer.Destroy in the process.
//
// It never returns an error: the caller needs the per-name breakdown (see
// destroyReport), because "refused" and "failed" and "gone" lead to three different
// decisions about pool reservations and record lifetimes. Call report.err() for the
// collapsed view.
func (o *volumeOp) destroy(ctx context.Context, site string, names ...string) destroyReport {
	var rep destroyReport
	if len(names) == 0 {
		return rep
	}

	sink, ok := o.b.volumes.(volumeDestroyer)
	if !ok {
		// Unreachable with any manager in this repo; see volumeDestroyer's doc. Refuse
		// rather than panic — the safe reading of "I cannot destroy" is "I did not".
		rep.Unproven = names
		o.countRefusals(site, destroyRefusedNoDestroyer, names)
		o.logger.Error("volume manager cannot destroy volumes; destroying nothing",
			"site", site, "volumes", names)
		return rep
	}

	// Every key the owner table produces is a CANONICAL name (pinned by
	// TestVolumeClaims_KeysAreNeverRetainedNames), so a batch made entirely of
	// fred-retained-* names cannot match a claim and resolving the table would be pure
	// cost. That is the ordinary reap: an evicted or expired record carries only the
	// retained names PutActiveMerged wrote, which keeps the ownership read off the hot
	// path — including evictOldest's up-to-32 records inside a single close.
	var table *volumeClaims
	if !allRetainedNames(names) {
		var err error
		table, err = o.claims()
		if err != nil {
			// Fail-safe, matching every other uncertainty in this backend: we cannot tell
			// our volumes from another lease's, so we must not destroy either.
			rep.Unproven = names
			o.countRefusals(site, destroyRefusedUnreadable, names)
			o.logger.Error("cannot read ownership records; destroying nothing this pass",
				"site", site, "asking_lease_uuid", o.owner, "volumes", names, "error", err)
			return rep
		}
	}

	for _, name := range names {
		if claim, permitted := table.mayDestroy(name, o.owner); !permitted {
			o.refuse(&rep, site, name, claim, "refusing to destroy a volume another lease owns")
			continue
		}
		claim, refused, err := o.destroyOne(ctx, sink, name)
		if refused {
			o.refuse(&rep, site, name, claim,
				"refusing to destroy a volume a live provision claimed after the ownership snapshot")
			continue
		}
		if err != nil {
			rep.Errs = append(rep.Errs, fmt.Errorf("volume %s: %w", name, err))
			o.logger.Error("failed to destroy volume", "site", site, "volume_id", name, "error", err)
			continue
		}
		rep.Destroyed = append(rep.Destroyed, name)
	}
	return rep
}

// destroyOne is the atomic unit: take the name's stripe, re-establish that no live
// provision claims it, and only then delete. Returns the refusing claim (refused=true)
// or the destroy's error.
//
// The re-check is what makes the whole file's invariant hold under concurrency. The table
// destroy() consulted is a snapshot; setupVolBinds' create-or-reuse takes the same stripe,
// so once this holds it the answer cannot change underneath the RemoveAll, and a
// re-provision that adopted the directory is seen rather than deleted out from under
// (ENG-681). A provision that arrives entirely after this check still loses its bytes —
// but that is the give-up's decision about abandoned data (ENG-676), not a torn read.
func (o *volumeOp) destroyOne(ctx context.Context, sink volumeDestroyer, name string) (volumeClaim, bool, error) {
	if isRetainedVolume(name) {
		// Nothing creates a fred-retained- name — setupVolBinds only ever builds
		// canonical ones — so there is no create to serialize against here, and no
		// live claim can key it (TestVolumeClaims_KeysAreNeverRetainedNames). Taking
		// the stripe would only block an unrelated volume's create for the length of
		// this RemoveAll, and this is the bulk path (evictOldest can pass 32 records'
		// worth of retained names through a single close).
		return volumeClaim{}, false, o.b.mutationAdapter().destroyVolume(ctx, sink, name)
	}
	mu := o.b.volumeNameMu(name)
	mu.Lock()
	defer mu.Unlock()
	if claim, claimed := o.b.liveClaim(name); claimed && !claimPermits(claim, o.owner) {
		return claim, true, nil
	}
	return volumeClaim{}, false, o.b.mutationAdapter().destroyVolume(ctx, sink, name)
}

// refuse records one name left alone because ownership said no: the report bucket, the
// counter, and a WARN naming the owner. why separates the snapshot's verdict from the
// destroy-time re-check's — the outcome and the metric are identical (a refusal is a
// refusal), but only the second means the two actually raced, which is the one thing an
// operator reading the log cannot reconstruct afterwards.
func (o *volumeOp) refuse(rep *destroyReport, site, name string, claim volumeClaim, why string) {
	rep.Claimed = append(rep.Claimed, name)
	if rep.ClaimedBy == nil {
		rep.ClaimedBy = make(map[string]volumeClaim, 1)
	}
	rep.ClaimedBy[name] = claim
	volumeDestroyRefusedTotal.WithLabelValues(site, destroyRefusedClaimed).Inc()
	o.logger.Warn(why,
		"site", site, "volume_id", name, "asking_lease_uuid", o.owner,
		"owner_lease_uuid", claim.owner, "claim", claim.kind.String())
}

// allRetainedNames reports whether every name is in the fred-retained- namespace, which
// no claim can key. An empty list is not "all retained" — but destroy returns before
// this on an empty list, so the distinction never arises.
func allRetainedNames(names []string) bool {
	for _, n := range names {
		if !isRetainedVolume(n) {
			return false
		}
	}
	return true
}

// countRefusals bumps the refusal counter once per name for a whole-batch refusal.
func (o *volumeOp) countRefusals(site, reason string, names []string) {
	for range names {
		volumeDestroyRefusedTotal.WithLabelValues(site, reason).Inc()
	}
}

// assertVolumeDestroyer checks at construction that the configured volume manager can
// actually destroy, so a manager that cannot fails the daemon at startup — in every
// environment, on every boot — instead of surfacing as a refusal on the one reap that
// needed to work. The result is deliberately discarded: volumeOp re-asserts at use so
// that a test replacing b.volumes is checked against the manager it actually installed.
func assertVolumeDestroyer(vm volumeManager) error {
	if _, ok := vm.(volumeDestroyer); !ok {
		return fmt.Errorf("volume manager %T cannot destroy volumes", vm)
	}
	return nil
}
