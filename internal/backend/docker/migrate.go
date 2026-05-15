// Package docker — recover-time legacy→stack migration.
//
// Background. Before the manifest-on-compose unification, the docker
// backend ran two parallel execution paths: a legacy single-service path
// that drove the Docker Engine API directly and a stack path that drove
// Docker Compose. Tasks 4-7 collapsed everything down to the Compose
// path. The change leaves on-disk artefacts (containers, volume
// directories) from legacy provisions in a name-space that no longer
// matches the new code:
//
//   container name   fred-{lease}-{idx}         →  fred-{lease}-app-{idx}
//   volume name      fred-{lease}-{idx}         →  fred-{lease}-app-{idx}
//   compose project  (none)                     →  fred-{lease}
//
// This file owns the planner that, at every fred startup, scans the
// managed-container list, groups legacy containers by lease, and
// produces a [*legacyMigration] for each lease describing the rename
// + recreate work Task 9 will execute. Per-lease (not per-container)
// because a single Compose Up call with RemoveOrphans:true would
// destroy already-migrated siblings of a multi-instance lease.
//
// Manifest sourcing is fail-loud: the planner requires an active entry
// in the release store. We do not attempt to reconstruct a manifest
// from container inspect because the inspected state can't recover
// tmpfs paths, the User directive's resolved UID, depends_on graphs,
// or stop_grace_period — silent reconstruction would produce a stack
// that quietly differs from the tenant's intent. Operators with a
// lease that has no release-store entry must investigate (corrupted
// store, manually-created container) or deprovision the lease.
//
// The pre-pass invocation lives in recover.go and runs before the main
// recovery loop, so all in-memory provision state observed by Update /
// Restart paths is post-migration consistent. Until Task 9 ships an
// execution implementation, the pre-pass plans and then aborts startup
// with a clear error so unmigrated legacy state never reaches the
// downstream Compose path that no longer understands it.

package docker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// legacyMigration describes the work needed to recreate ALL legacy
// containers of one lease as a single stack-form (1-service,
// N-instance) Compose project. Per-lease (not per-container) because
// b.compose.Up runs with RemoveOrphans:true and would destroy
// already-migrated siblings.
type legacyMigration struct {
	LeaseUUID string
	Tenant    string
	SKU       string
	Stack     *manifest.StackManifest
	Instances []legacyMigrationInstance
}

// legacyMigrationInstance captures the per-container state needed to
// recreate one legacy container as a stack-form (service=app)
// instance under the new naming convention.
type legacyMigrationInstance struct {
	LegacyContainer  ContainerInfo
	Mounts           []ContainerMount // managed-volume binds only; len==0 for stateless containers
	NewContainerName string           // fred-{uuid}-app-{idx}
	PrevName         string           // fred-{uuid}-app-{idx}-prev (used by Task 9's rollback-windowed cleanup)
	VolRenames       []volRename      // one entry per managed volume bind
}

// volRename is the (old, new) volume-name pair plus the container-side
// mount target. Task 9 walks these to drive [volumeManager.RenameVolume]
// and to reconstruct the per-instance bind map for the new Compose
// project. Multiple mounts on the same instance share (Old, New) but
// carry distinct Target values; Task 9 dedupes by (Old, New) before
// calling RenameVolume so the rename only fires once per directory.
type volRename struct {
	Old    string
	New    string
	Target string
}

// isLegacyContainer reports whether c is a legacy single-service
// container that needs migration. Three conditions:
//   - has a fred.lease_uuid label (managed by fred);
//   - has NO fred.service_name label (the post-Task-3 marker for the
//     stack-form path);
//   - the container name doesn't end with "-prev" — Task 9's migration
//     renames the legacy container to {newname}-prev as a rollback
//     window before forced removal, and a startup interrupted mid-
//     window leaves a -prev remnant that must NOT be re-migrated on
//     the next boot.
func isLegacyContainer(c ContainerInfo) bool {
	if c.LeaseUUID == "" || c.ServiceName != "" {
		return false
	}
	return !strings.HasSuffix(c.Name, "-prev")
}

// planLegacyMigrations groups legacy containers by lease and produces
// one migration plan per lease. Returns a nil-safe empty slice when
// there are no legacy containers (the common steady-state path on a
// post-migration fleet).
func (b *Backend) planLegacyMigrations(ctx context.Context, all []ContainerInfo, logger *slog.Logger) ([]*legacyMigration, error) {
	byLease := map[string][]ContainerInfo{}
	for _, c := range all {
		if isLegacyContainer(c) {
			byLease[c.LeaseUUID] = append(byLease[c.LeaseUUID], c)
		}
	}
	if len(byLease) == 0 {
		return nil, nil
	}

	// Sort lease UUIDs so plan ordering is deterministic across restarts
	// (helps when correlating recovery logs with operator-side tooling).
	leaseUUIDs := make([]string, 0, len(byLease))
	for u := range byLease {
		leaseUUIDs = append(leaseUUIDs, u)
	}
	sort.Strings(leaseUUIDs)

	plans := make([]*legacyMigration, 0, len(byLease))
	for _, leaseUUID := range leaseUUIDs {
		plan, err := b.planLegacyMigrationForLease(ctx, leaseUUID, byLease[leaseUUID], logger)
		if err != nil {
			return nil, fmt.Errorf("plan lease %s: %w", leaseUUID, err)
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// planLegacyMigrationForLease builds the per-lease migration plan. The
// manifest is sourced exclusively from the release store; if the
// store has no active entry, the migration fails loudly. See the
// package doc for why in-container reconstruction is rejected.
func (b *Backend) planLegacyMigrationForLease(ctx context.Context, leaseUUID string, group []ContainerInfo, logger *slog.Logger) (*legacyMigration, error) {
	if b.releaseStore == nil {
		return nil, fmt.Errorf("no release store configured; cannot read stored manifest for legacy lease %s", leaseUUID)
	}
	rel, relErr := b.releaseStore.LatestActive(leaseUUID)
	switch {
	case relErr != nil:
		return nil, fmt.Errorf("read release store for lease %s: %w", leaseUUID, relErr)
	case rel == nil || len(rel.Manifest) == 0:
		return nil, fmt.Errorf("release store has no active manifest for legacy lease %s; cannot migrate "+
			"(operator: investigate the missing release-store entry or deprovision the lease)", leaseUUID)
	}

	stack, err := manifest.ParsePayload(rel.Manifest)
	if err != nil {
		return nil, fmt.Errorf("parse stored manifest for lease %s: %w", leaseUUID, err)
	}

	instances := make([]legacyMigrationInstance, 0, len(group))
	for _, c := range group {
		// Mounts: prefer the inline list-containers value populated in
		// Task 8.2. A defensive InspectContainer fallback covers the
		// (vanishingly rare) case where the list payload had no mount
		// array attached.
		mounts := c.Mounts
		if mounts == nil {
			inspected, ierr := b.docker.InspectContainer(ctx, c.ContainerID)
			if ierr != nil {
				return nil, fmt.Errorf("inspect legacy container %s for mounts: %w", c.ContainerID, ierr)
			}
			mounts = inspected.Mounts
		}
		managed := filterManagedMounts(b, mounts)

		newName := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, manifest.DefaultServiceName, c.InstanceIndex)
		oldVol := fmt.Sprintf("fred-%s-%d", leaseUUID, c.InstanceIndex)
		newVol := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, manifest.DefaultServiceName, c.InstanceIndex)

		renames := make([]volRename, 0, len(managed))
		for _, m := range managed {
			renames = append(renames, volRename{Old: oldVol, New: newVol, Target: m.Target})
		}

		instances = append(instances, legacyMigrationInstance{
			LegacyContainer:  c,
			Mounts:           managed,
			NewContainerName: newName,
			PrevName:         newName + "-prev",
			VolRenames:       renames,
		})
	}
	sortInstancesByIndex(instances)

	return &legacyMigration{
		LeaseUUID: leaseUUID,
		Tenant:    group[0].Tenant,
		SKU:       group[0].SKU,
		Stack:     stack,
		Instances: instances,
	}, nil
}

// filterManagedMounts keeps only bind mounts whose host source sits
// under the configured volume_data_path. Tmpfs entries and unrelated
// binds (e.g., /etc/localtime) are filtered out — only fred-managed
// volume directories need renaming. A stateless lease legitimately
// returns zero results here.
func filterManagedMounts(b *Backend, mounts []ContainerMount) []ContainerMount {
	root := b.cfg.VolumeDataPath
	if root == "" {
		return nil
	}
	var out []ContainerMount
	for _, m := range mounts {
		if m.Type != "bind" {
			continue
		}
		if !strings.HasPrefix(m.Source, root) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// sortInstancesByIndex sorts a slice of migration instances by
// InstanceIndex so log output and rename order is deterministic
// across boots.
func sortInstancesByIndex(xs []legacyMigrationInstance) {
	sort.SliceStable(xs, func(i, j int) bool {
		return xs[i].LegacyContainer.InstanceIndex < xs[j].LegacyContainer.InstanceIndex
	})
}
