// Package docker — recover-time legacy→stack migration.
//
// Background. Before the manifest-on-compose unification, the docker
// backend ran two parallel execution paths: a legacy single-service path
// that drove the Docker Engine API directly and a stack path that drove
// Docker Compose. Tasks 4-7 collapsed everything down to the Compose
// path. The change leaves on-disk artifacts (containers, volume
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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
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
func (b *Backend) planLegacyMigrations(ctx context.Context, all []ContainerInfo) ([]*legacyMigration, error) {
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
		plan, err := b.planLegacyMigrationForLease(ctx, leaseUUID, byLease[leaseUUID])
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
func (b *Backend) planLegacyMigrationForLease(ctx context.Context, leaseUUID string, group []ContainerInfo) (*legacyMigration, error) {
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
//
// The prefix check uses `root + filepath.Separator` (or the exact
// root path) so a configured root of `/var/lib/fred` does not match
// sibling paths like `/var/lib/fred-other/...`. Without this, a
// neighboring directory whose name happens to begin with the root
// string would be misclassified as managed and renamed under
// migration.
func filterManagedMounts(b *Backend, mounts []ContainerMount) []ContainerMount {
	root := b.cfg.VolumeDataPath
	if root == "" {
		return nil
	}
	rootSep := strings.TrimRight(root, string(filepath.Separator)) + string(filepath.Separator)
	var out []ContainerMount
	for _, m := range mounts {
		if m.Type != "bind" {
			continue
		}
		if !strings.HasPrefix(m.Source, rootSep) && m.Source != strings.TrimRight(root, string(filepath.Separator)) {
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

// executeLegacyMigration carries one [*legacyMigration] through the
// rename-and-recreate pipeline atomically for the whole lease (all
// instances in a single Compose.Up call). The per-lease atomicity is
// load-bearing: compose.Up runs with RemoveOrphans:true (compose.go),
// so a per-instance loop would tear down already-migrated siblings.
//
// Pipeline order (locked):
//  1. Stop every legacy container in the lease + rename each to
//     `<newName>-prev`. Stop must precede volume rename because zfs
//     rejects rename on a busy dataset, and on xfs/btrfs renaming
//     under a live bind risks dangling-inode confusion.
//  2. Rename each instance's managed volume directories to the new
//     service-aware naming convention. Per-instance, per-volume;
//     stateless leases naturally fall through (no managed mounts).
//  3. Build a Compose project for the whole lease with per-instance
//     VolBinds pointing at the just-renamed host paths.
//  4. Compose.Up. Creates N stack-form containers in one shot.
//  5. Wait for ready (verifyStartup, bounded by
//     b.cfg.MigrationReadyTimeout).
//  6. Schedule background removal of all `-prev` containers after
//     b.cfg.MigrationGracePeriod — preserves rollback inspection
//     potential without blocking startup.
//  7. RecordMigration on the release store so the next boot sees the
//     wrapped manifest as the active release. Idempotent on
//     byte-equal payload.
//
// Failure semantics: any step error returns immediately. The caller
// (recoverState) wraps with operator remediation guidance.
//
// Idempotency / crash resumability is bounded:
//   - **Boundary 1 (before Stop+rename-to-prev):** a crash here leaves
//     legacy containers and legacy-named volumes intact. Next boot
//     re-runs the migration from scratch — fully resumable.
//   - **Boundary 2 (after rename-to-prev, before compose.Up):**
//     containers are stopped & renamed to `<name>-prev`, volumes may
//     be partially renamed to new naming. The next boot's planner
//     will not find a fresh legacy container under the original
//     name (it's `-prev` now) and cannot replan the migration from
//     state alone — operator intervention required (see CHANGELOG
//     troubleshooting section). NOT resumable.
//   - **Boundary 3 (after compose.Up, before RecordMigration):** new
//     stack containers exist alongside `-prev` containers. The release
//     store still has the legacy active entry, so the next boot will
//     re-plan. The volume renames are already idempotent (the rename
//     tolerance below skips already-renamed paths). Compose.Up is
//     idempotent on container name. Resumable, but the operator may
//     see two generations of containers transiently.
//   - **Boundary 4 (after RecordMigration, before grace-window
//     removal):** terminal state. Background removal of `-prev`
//     containers is fire-and-forget; if fred restarts inside the
//     grace window, orphan `-prev` containers linger on disk until
//     manual cleanup. Forward progress is durable; cleanup is
//     operator-territory.
func (b *Backend) executeLegacyMigration(ctx context.Context, m *legacyMigration, logger *slog.Logger) error {
	logger = logger.With("lease_uuid", m.LeaseUUID, "instances", len(m.Instances))
	logger.Info("legacy migration starting")

	svc := m.Stack.Services[manifest.DefaultServiceName]
	if svc == nil {
		return fmt.Errorf("internal: wrapped stack missing default service %q", manifest.DefaultServiceName)
	}
	stopGrace := 10 * time.Second
	if svc.StopGracePeriod != nil {
		stopGrace = svc.StopGracePeriod.Duration()
	}

	// 1. Stop + rename every legacy container.
	for _, inst := range m.Instances {
		if err := b.docker.StopContainer(ctx, inst.LegacyContainer.ContainerID, stopGrace); err != nil {
			// Tolerate already-stopped (the docker SDK returns an error for
			// a stop on a non-running container). Don't fail the migration
			// over it — the rename below will surface a real "container
			// missing" condition if it's serious.
			logger.Warn("stop legacy container returned error (continuing)",
				"container_id", inst.LegacyContainer.ContainerID, "error", err)
		}
		if err := b.docker.RenameContainer(ctx, inst.LegacyContainer.ContainerID, inst.PrevName); err != nil {
			if !isAlreadyNamedErr(err, inst.PrevName) {
				return fmt.Errorf("rename %s to %s: %w", inst.LegacyContainer.ContainerID, inst.PrevName, err)
			}
		}
	}

	// 2. Rename managed volume directories. RenameVolume is idempotent
	// (Task 10), so re-runs after a partial migration succeed quietly.
	for _, inst := range m.Instances {
		for _, r := range inst.VolRenames {
			if err := b.volumes.RenameVolume(r.Old, r.New); err != nil {
				return fmt.Errorf("rename volume %s→%s (instance idx=%d): %w",
					r.Old, r.New, inst.LegacyContainer.InstanceIndex, err)
			}
		}
	}

	// 3. Build the Compose project. SKU profile lookup + items list mirror
	// the live provision flow (provision.go:doProvision); the only
	// migration-specific input is the VolBinds map seeded from the
	// just-renamed directories.
	profile, err := b.cfg.GetSKUProfile(m.SKU)
	if err != nil {
		return fmt.Errorf("load SKU profile %s: %w", m.SKU, err)
	}
	quantity := len(m.Instances)
	items := []backend.LeaseItem{{
		SKU:         m.SKU,
		Quantity:    quantity,
		ServiceName: manifest.DefaultServiceName,
	}}

	volBinds := map[string]map[int]serviceVolBinds{
		manifest.DefaultServiceName: {},
	}
	for _, inst := range m.Instances {
		binds := serviceVolBinds{}
		if len(inst.VolRenames) > 0 {
			binds.StatefulBinds = make(map[string]string, len(inst.VolRenames))
			hostRoot := b.volumes.HostPath(inst.VolRenames[0].New) // single volume name per instance
			for _, r := range inst.VolRenames {
				// Match the stack-path convention: subdir per target under hostRoot.
				// The legacy on-disk layout already follows this convention (see
				// provision.go's setupVolBinds / buildStatefulVolumeBinds), so
				// after the parent rename the data sits at exactly
				// `<hostRoot>/sanitize(target)`.
				sanitized := sanitizeVolumePath(r.Target)
				if sanitized == "" {
					return fmt.Errorf("legacy mount target %q is unsupported under stack-form layout", r.Target)
				}
				binds.StatefulBinds[filepath.Join(hostRoot, sanitized)] = r.Target
			}
		}
		volBinds[manifest.DefaultServiceName][inst.LegacyContainer.InstanceIndex] = binds
	}

	project := buildComposeProject(composeProjectParams{
		LeaseUUID: m.LeaseUUID,
		Tenant:    m.Tenant,
		Stack:     m.Stack,
		Items:     items,
		Profiles:  map[string]SKUProfile{m.SKU: profile},
		VolBinds:  volBinds,
		Cfg:       &b.cfg,
	})

	// 4. Compose Up — brings up all instances at once.
	if err := b.compose.Up(ctx, project, composeUpOpts{}); err != nil {
		return fmt.Errorf("compose up: %w", err)
	}

	// 5. Wait for ready. Resolves the new container IDs by name and reuses
	// the existing verifyStartup helper (provision.go), which is
	// health-check-aware: it polls via waitForHealthy when the manifest
	// declares an active health check and falls back to a fixed-wait +
	// inspect when it doesn't. Matches the readiness contract used by
	// the live provision path.
	newIDs, err := b.resolveContainerIDsByName(ctx, namesOf(m.Instances))
	if err != nil {
		return fmt.Errorf("resolve new container IDs: %w", err)
	}
	readyCtx, cancel := context.WithTimeout(ctx, cmp.Or(b.cfg.MigrationReadyTimeout, defaultMigrationReadyTimeout))
	defer cancel()
	if err := b.verifyStartup(readyCtx, svc, newIDs, logger); err != nil {
		return fmt.Errorf("wait for ready: %w", err)
	}

	// 6. Schedule per-instance `-prev` removal after the operator-
	// inspection grace window. Background — must not block startup.
	for _, inst := range m.Instances {
		inst := inst
		go func() {
			select {
			case <-time.After(cmp.Or(b.cfg.MigrationGracePeriod, defaultMigrationGracePeriod)):
			case <-ctx.Done():
				return
			}
			// Use a fresh context with a short timeout so a wedged
			// docker daemon doesn't keep the goroutine alive forever.
			// Logged as warning rather than failing the migration —
			// the data plane is already on the stack-form container;
			// the -prev leftover is an operator cleanup at worst.
			rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer rmCancel()
			if err := b.docker.RemoveContainer(rmCtx, inst.PrevName); err != nil {
				logger.Warn("remove -prev container after grace failed (manual cleanup may be needed)",
					"name", inst.PrevName, "error", err)
			}
		}()
	}

	// 7. Persist wrapped manifest. RecordMigration is idempotent.
	if b.releaseStore != nil {
		data, mErr := json.Marshal(m.Stack)
		if mErr != nil {
			logger.Warn("marshal wrapped manifest for release store (migration still complete)", "error", mErr)
		} else if persistErr := b.releaseStore.RecordMigration(m.LeaseUUID, data); persistErr != nil {
			logger.Warn("release store RecordMigration failed (migration still complete)", "error", persistErr)
		}
	}

	logger.Info("legacy migration complete")
	return nil
}

// isAlreadyNamedErr reports whether a docker RenameContainer failure
// indicates the source container already carries the target name —
// covering the idempotency case where a previous migration run
// renamed and then crashed before the next step. Heuristic against
// the docker SDK's error string; tolerant. We don't want false
// positives here (would mask a real conflict), so we require both
// the target name AND an "already" / "in use" / "conflict" hint.
func isAlreadyNamedErr(err error, targetName string) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.Contains(msg, targetName) {
		return false
	}
	return strings.Contains(msg, "already") ||
		strings.Contains(msg, "in use") ||
		strings.Contains(msg, "conflict")
}

// namesOf returns the new container names for a slice of migration
// instances. Helper for resolveContainerIDsByName.
func namesOf(insts []legacyMigrationInstance) []string {
	out := make([]string, 0, len(insts))
	for _, i := range insts {
		out = append(out, i.NewContainerName)
	}
	return out
}

// resolveContainerIDsByName scans the managed-container list and
// returns the container IDs whose Name matches any of the given
// names. Used by the migration to translate just-created Compose
// container names back to engine IDs for the readiness wait.
//
// Returns an error if any expected name is unresolved — the caller
// already validated the Up call succeeded, so a missing container
// means a name mismatch or a race we can't safely paper over.
func (b *Backend) resolveContainerIDsByName(ctx context.Context, names []string) ([]string, error) {
	containers, err := b.docker.ListManagedContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managed containers: %w", err)
	}
	want := make(map[string]struct{}, len(names))
	for _, n := range names {
		want[n] = struct{}{}
	}
	got := make(map[string]string, len(names)) // name → containerID
	for _, c := range containers {
		if _, ok := want[c.Name]; ok {
			got[c.Name] = c.ContainerID
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := got[n]
		if !ok {
			return nil, fmt.Errorf("container with name %q not found in managed list (Compose Up may not have created it)", n)
		}
		out = append(out, id)
	}
	return out, nil
}
