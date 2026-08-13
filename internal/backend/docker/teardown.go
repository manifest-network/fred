// teardown.go holds the single primitive every fred teardown path uses to make a
// lease's containers — and the Docker anonymous volumes attached to them — actually
// go away. Its one invariant: NO teardown path may depend on a recorded
// container-id list to find what it must remove (ENG-647).

package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// teardownLeaseContainers removes every container fred owns for a lease, and with
// them the anonymous volumes Docker auto-creates for image VOLUME directives the
// tmpfs override doesn't cover (ENG-372). Every teardown path goes through here.
//
// It re-DISCOVERS the containers instead of trusting a recorded id list. That is how
// the substrate itself works — compose finds a project's containers by re-querying
// the daemon for its project label, storing no ids — and how fred already recovers
// from a crash (recoverState rebuilds every provision from ListManagedContainers). A
// recorded list is strictly weaker than the call it backstops, and on the
// restore-rollback arms it is EMPTY exactly when a container leaked: Restore reserves
// the provision with no ContainerIDs and only the SUCCESS paths ever fill them in, so
// a failed restore's record names none of the containers its compose Up created
// (ENG-647). recordedIDs is still unioned in: it costs nothing and covers a container
// whose fred labels were stripped out-of-band.
//
// Down's error is the trigger, never the verdict. A Down that SUCCEEDS is trusted —
// it is the substrate's own label sweep, and re-listing on every close would tax the
// hot path to re-derive what compose just did. Only a failed Down pays for discovery.
//
// The per-container fallback is only an equivalent substitute for Down because fred's
// projects declare NO named volumes: RemoveContainer passes RemoveVolumes:true, which
// is `docker rm -v` and reaps ANONYMOUS volumes only, while compose Down's
// Volumes:true also reaps a project's named ones. buildComposeProject never sets
// Project.Volumes and every service mount is a bind or a tmpfs, so the two agree
// today — a fact pinned by TestBuildComposeProject_DeclaresNoNamedVolumes, which must
// be revisited before any named volume is introduced.
//
// Returns the ids that may still exist. A nil error means the lease owns nothing on
// this host anymore, so the caller may proceed with the state transition that fact
// unlocks (re-quarantine, record revert, pool release, dropping the provision). A
// non-nil error means AT LEAST ONE container may still be running. A discovery
// failure is itself an error: we cannot prove the host is clean, and the safe
// direction is to keep the lease tracked and retry (cleanup gates fail open toward
// "keep", counted in a metric).
func (b *Backend) teardownLeaseContainers(ctx context.Context, leaseUUID string, recordedIDs []string,
	stopTimeout time.Duration, operation string, logger *slog.Logger) ([]string, error) {
	projectName := composeProjectName(leaseUUID)
	downErr := b.compose.Down(ctx, projectName, stopTimeout)
	if downErr == nil {
		logger.Info("compose down completed", "project", projectName)
		return nil, nil
	}
	// No lease_uuid field here: most callers hand in a logger that already binds it,
	// and slog would emit the key twice. The project name carries the lease anyway.
	logger.Warn("compose down failed, falling back to individual removal",
		"project", projectName, "error", downErr)

	ids, derr := b.discoverLeaseContainers(ctx, leaseUUID, recordedIDs)
	if derr != nil {
		// Cannot enumerate, so cannot prove the host is clean. Report every id we do
		// know of as still-possibly-present rather than an empty list, which a caller
		// would read as "nothing left".
		teardownFallbackTotal.WithLabelValues(operation, teardownOutcomeFailed).Inc()
		return recordedIDs, fmt.Errorf("compose down failed (%w) and container discovery failed: %w", downErr, derr)
	}

	var (
		errs      []error
		remaining []string
	)
	for _, id := range ids {
		if rmErr := b.docker.RemoveContainer(ctx, id); rmErr != nil {
			logger.Error("failed to remove container", "container_id", leasesm.ShortID(id), "error", rmErr)
			errs = append(errs, fmt.Errorf("container %s: %w", leasesm.ShortID(id), rmErr))
			remaining = append(remaining, id)
			continue
		}
		logger.Info("container removed", "container_id", leasesm.ShortID(id))
	}
	if len(errs) > 0 {
		teardownFallbackTotal.WithLabelValues(operation, teardownOutcomeFailed).Inc()
		return remaining, errors.Join(errs...)
	}
	teardownFallbackTotal.WithLabelValues(operation, teardownOutcomeRecovered).Inc()
	return nil, nil
}

// restoringClaimedVolumes returns the canonical volume names that an IN-FLIGHT
// restore has adopted into leaseUUID's namespace but that still belong to ANOTHER
// lease's retention record.
//
// While a record is restoring, the retained data lives under the NEW lease's
// canonical names (fred-{new}-{svc}-{i}), so a close of the new lease sees volumes
// that look like its own. They are not: the original record still names them, and
// destroying them — or re-retaining them under the closing lease, which leaves the
// original record pointing at fred-retained-{original}-* names that no longer exist —
// permanently kills that lease's restore. reconcileRestoring re-quarantines them once
// its rollback can complete, so the right move here is to leave them alone (ENG-647).
//
// This is deliberately the same set cleanupOrphanedVolumes already protects from the
// orphan reaper (recover.go); this is that protection for the close path. Returns an
// error rather than an empty set when the store cannot be read: callers must skip
// destruction entirely instead of guessing, mirroring cleanupOrphanedVolumes'
// fail-safe. A nil retentionStore means no record can exist, so no claim can either.
func (b *Backend) restoringClaimedVolumes(leaseUUID string) (map[string]bool, error) {
	if b.retentionStore == nil {
		return nil, nil
	}
	recs, err := b.retentionStore.List()
	if err != nil {
		return nil, fmt.Errorf("list retention records: %w", err)
	}
	var claimed map[string]bool
	for _, e := range recs {
		if e.Status != shared.RetentionStatusRestoring || e.NewLeaseUUID != leaseUUID {
			continue
		}
		for _, retained := range e.RetainedVolumeNames {
			if claimed == nil {
				claimed = make(map[string]bool, len(e.RetainedVolumeNames))
			}
			claimed[retainedToNewCanonical(retained, e.OriginalLeaseUUID, e.NewLeaseUUID)] = true
		}
	}
	return claimed, nil
}

// discoverLeaseContainers returns the union of the lease's live fred-labelled
// containers and any ids the caller already recorded, deduplicated and stably
// ordered (recorded first, then daemon order) so a failure list reads the same way
// twice. RemoveContainer is idempotent — a not-found is a nil — so overlap between
// the two sources costs nothing.
//
// ListManagedContainers filters on fred.managed=true plus this backend's name, and
// compose-created containers carry both (compose_project.go), so the LeaseUUID match
// below is the same identity recoverState rebuilds state from. Migration `-prev`
// remnants ARE included: recoverState skips them because it is reconstructing live
// state, whereas every caller here is tearing the lease down in full, so a remnant
// holding an anonymous volume is exactly what we came to remove.
func (b *Backend) discoverLeaseContainers(ctx context.Context, leaseUUID string, recordedIDs []string) ([]string, error) {
	all, err := b.docker.ListManagedContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managed containers: %w", err)
	}
	ids := make([]string, 0, len(recordedIDs)+len(all))
	seen := make(map[string]struct{}, len(recordedIDs)+len(all))
	add := func(id string) {
		if id == "" {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, id := range recordedIDs {
		add(id)
	}
	for _, c := range all {
		if c.LeaseUUID == leaseUUID {
			add(c.ContainerID)
		}
	}
	return ids, nil
}
