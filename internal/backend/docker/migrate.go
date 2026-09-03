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
// Restart paths is post-migration consistent. Each plan is then executed
// atomically per lease by executeLegacyMigration; any per-lease failure
// aborts startup with operator-actionable guidance so unmigrated legacy
// state never reaches the downstream Compose path that no longer
// understands it.

package docker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// resolveMigratedBindSource returns the host bind-source path for a migrated
// stateful volume target under hostRoot, failing closed if the target resolves
// through a symlink that escapes hostRoot. Without this, a tenant that planted a
// symlink inside its legacy volume (e.g. `data -> /`) would, at migration, get an
// arbitrary host path bind-mounted into the recreated container — the same escape
// as buildStatefulVolumeBinds (ENG-539). Legacy containers are stopped before this
// runs, so there is no concurrent writer racing the check.
//
// A not-yet-existing subdir is permitted: os.Root has already proven that every
// EXISTING path component stays within the root, so the source Docker creates at
// mount time also stays within it. Any component that escapes the root, or a leaf
// that is itself a symlink, is rejected; a symlink that stays within the root is
// followed (it cannot redirect the bind source outside the volume, and legacy
// containers are stopped so no concurrent writer can re-point it).
func resolveMigratedBindSource(hostRoot, sanitized string) (string, error) {
	root, err := os.OpenRoot(hostRoot)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Volume root absent on disk: there is no directory tree that could
			// hide a tenant-planted symlink, so the source Docker creates under the
			// operator-owned volume path is safe. In the normal migration flow the
			// root exists after RenameVolume; this preserves the pre-ENG-539
			// behavior for the absent-root edge case.
			return filepath.Join(hostRoot, sanitized), nil
		}
		return "", fmt.Errorf("open volume root %q: %w", hostRoot, err)
	}
	defer func() { _ = root.Close() }()

	info, err := root.Lstat(sanitized)
	switch {
	case err == nil:
		if info.Mode()&fs.ModeSymlink != 0 {
			return "", fmt.Errorf("mount target %q resolves through a symlink", sanitized)
		}
	case errors.Is(err, fs.ErrNotExist):
		// Not created yet — allowed; the existing prefix was traversed by os.Root
		// without escaping, so the eventual Docker-created source stays in-root.
	default:
		// "path escapes from parent" or any other error: fail closed.
		return "", fmt.Errorf("resolve mount target %q: %w", sanitized, err)
	}
	return filepath.Join(hostRoot, sanitized), nil
}

// legacyMigration describes the work needed to recreate ALL legacy
// containers of one lease as a single stack-form (1-service,
// N-instance) Compose project. Per-lease (not per-container) because
// b.compose.Up runs with RemoveOrphans:true and would destroy
// already-migrated siblings.
type legacyMigration struct {
	LeaseUUID    string
	Tenant       string
	ProviderUUID string
	SKU          string
	CustomDomain string
	FailCount    int
	Stack        *manifest.StackManifest
	Instances    []legacyMigrationInstance
}

// committedLegacyRollbackCohort carries exact immutable cleanup evidence from
// migration planning into ordinary recovery. The nonzero authority class is
// present only when the same whole-cohort classifier used by stopped adoption
// proved a v0.13 post-RecordMigration generation. Keeping that provenance typed
// avoids turning the mere presence of a `-prev` name into durable migration
// authority.
type committedLegacyRollbackCohort struct {
	remnants             []ContainerInfo
	legacyAuthorityClass shared.LegacyActiveAuthorityClass
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
//   - both original names and migration-created "-prev" rollback remnants are
//     candidates. A rollback-only stop/rename boundary is resumable because
//     stop/rename and volume rename are idempotent. Mixed rollback/stack crash
//     generations are classified separately; their mere presence never grants
//     replay authority.
func isLegacyContainer(c ContainerInfo) bool {
	return c.LeaseUUID != "" && c.ServiceName == ""
}

// isLegacyRollbackRemnant identifies only the exact name produced by
// executeLegacyMigration. A generic "-prev" suffix is not authority to hide a
// managed container from recovery: labels or names can be corrupted or edited
// independently, and an unrelated container must fail normal validation rather
// than being silently treated as rollback evidence.
func isLegacyRollbackRemnant(c ContainerInfo) bool {
	return isLegacyContainer(c) && c.Name == fmt.Sprintf(
		"fred-%s-%s-%d-prev",
		c.LeaseUUID,
		manifest.DefaultServiceName,
		c.InstanceIndex,
	)
}

// planLegacyMigrations groups legacy containers by lease and produces
// one migration plan per lease. Returns a nil-safe empty slice when
// there are no legacy containers (the common steady-state path on a
// post-migration fleet).
func (b *Backend) planLegacyMigrations(
	ctx context.Context,
	all []ContainerInfo,
) ([]*legacyMigration, map[string]committedLegacyRollbackCohort, error) {
	byLease := map[string][]ContainerInfo{}
	allByLease := map[string][]ContainerInfo{}
	for _, c := range all {
		if c.LeaseUUID != "" {
			allByLease[c.LeaseUUID] = append(allByLease[c.LeaseUUID], c)
		}
		if isLegacyContainer(c) {
			byLease[c.LeaseUUID] = append(byLease[c.LeaseUUID], c)
		}
	}
	if len(byLease) == 0 {
		return nil, nil, nil
	}

	// Sort lease UUIDs so plan ordering is deterministic across restarts
	// (helps when correlating recovery logs with operator-side tooling).
	leaseUUIDs := make([]string, 0, len(byLease))
	for u := range byLease {
		leaseUUIDs = append(leaseUUIDs, u)
	}
	sort.Strings(leaseUUIDs)

	plans := make([]*legacyMigration, 0, len(byLease))
	committedRemnants := make(map[string]committedLegacyRollbackCohort)
	for _, leaseUUID := range leaseUUIDs {
		// Once RecordMigration has durably published an exact desired cohort,
		// any exact `-prev` names are rollback-window remnants, not evidence from
		// which a new desired topology may be inferred. Cleanup may have removed
		// only a subset before a crash; re-planning from that subset could
		// downscale the healthy Compose project. Defer their removal until the
		// ordinary recovery path has verified the durable cohort below.
		if b.releaseStore != nil {
			release, err := b.releaseStore.LatestActive(leaseUUID)
			if err != nil {
				return nil, nil, fmt.Errorf("read release store for lease %s: %w", leaseUUID, err)
			}
			if release != nil && len(release.Items) > 0 {
				for _, container := range byLease[leaseUUID] {
					if !isLegacyRollbackRemnant(container) {
						return nil, nil, fmt.Errorf(
							"lease %s has an exact durable release but legacy container %q is not an expected rollback remnant",
							leaseUUID,
							container.Name,
						)
					}
				}
				committedRemnants[leaseUUID] = committedLegacyRollbackCohort{
					remnants: append([]ContainerInfo(nil), byLease[leaseUUID]...),
				}
				continue
			}
			if release != nil && release.Image == "stack" {
				class, _, inspectErr := inspectV013MigrationCrashCohort(
					release,
					allByLease[leaseUUID],
				)
				if inspectErr != nil {
					return nil, nil, fmt.Errorf(
						"inspect v0.13 migration rollback cohort for lease %s: %w",
						leaseUUID,
						inspectErr,
					)
				}
				if class == v013MigrationCrashAfterRelease {
					committedRemnants[leaseUUID] = committedLegacyRollbackCohort{
						remnants: append([]ContainerInfo(nil), byLease[leaseUUID]...),
						// This is the only legacy store shape whose exact
						// whole-cohort proof can safely mint the missing durable
						// migration marker during authority backfill.
						legacyAuthorityClass: shared.LegacyActiveAuthorityMigration,
					}
					continue
				}
				// v0.13's migration-generated stack generation omitted the
				// backend label, so ordinary recovery may see only a partial
				// `-prev` cleanup cohort. A stack release proves RecordMigration
				// committed, but without the full stack inventory or frozen Items
				// the rollback subset cannot prove desired quantity. Never replay
				// that subset as a downscaled deployment.
				return nil, nil, fmt.Errorf(
					"%w: lease %s has a stack active release without frozen items and rollback remnants, but the complete committed stack cohort is not visible; run the stopped storage-identity adoption preflight and resolve the exact v0.13 migration crash lineage before startup",
					ErrV013InterruptedMigration,
					leaseUUID,
				)
			}
		}

		plan, err := b.planLegacyMigrationForLease(ctx, leaseUUID, byLease[leaseUUID])
		if err != nil {
			return nil, nil, fmt.Errorf("plan lease %s: %w", leaseUUID, err)
		}
		plans = append(plans, plan)
	}
	return plans, committedRemnants, nil
}

// planLegacyMigrationForLease builds the per-lease migration plan. The
// manifest is sourced exclusively from the release store; if the
// store has no active entry, the migration fails loudly. See the
// package doc for why in-container reconstruction is rejected.
func (b *Backend) planLegacyMigrationForLease(ctx context.Context, leaseUUID string, group []ContainerInfo) (*legacyMigration, error) {
	if len(group) == 0 {
		return nil, fmt.Errorf("legacy migration group is empty")
	}
	identity := group[0]
	maxFailCount := identity.FailCount
	instanceIndexes := make(map[int]struct{}, len(group))
	for _, container := range group {
		if container.LeaseUUID != leaseUUID || container.Tenant != identity.Tenant ||
			container.ProviderUUID != identity.ProviderUUID || container.SKU != identity.SKU ||
			container.CustomDomain != identity.CustomDomain {
			return nil, fmt.Errorf(
				"legacy container cohort has divergent lease, tenant, provider, SKU, or custom-domain identity",
			)
		}
		if _, duplicate := instanceIndexes[container.InstanceIndex]; duplicate {
			return nil, fmt.Errorf("legacy container cohort has duplicate instance index %d", container.InstanceIndex)
		}
		instanceIndexes[container.InstanceIndex] = struct{}{}
		maxFailCount = max(maxFailCount, container.FailCount)
	}
	for index := range len(group) {
		if _, exists := instanceIndexes[index]; !exists {
			return nil, fmt.Errorf(
				"legacy container cohort has non-contiguous instance indexes: missing %d in [0,%d)",
				index, len(group),
			)
		}
	}
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

	// Defense-in-depth: a legacy lease's stored manifest must resolve to
	// exactly one service named manifest.DefaultServiceName. ParsePayload
	// auto-wraps flat input into {"app": ...}; a stack-shaped stored
	// payload for a legacy-labeled container is unproducible state
	// (legacy writers only ever emitted flat manifests, and post-Task-2
	// stack writers emit either the same flat shape or a 1-service "app"
	// wrap). Fail loudly at planning time so the failure stays adjacent
	// to the release-store read — see the "manifest sourcing is fail-
	// loud" rule in the package doc above.
	if len(stack.Services) != 1 {
		return nil, fmt.Errorf("release store for legacy lease %s has %d services; "+
			"legacy leases must have exactly 1 (operator: investigate corrupted release-store "+
			"entry or deprovision the lease)", leaseUUID, len(stack.Services))
	}
	if _, ok := stack.Services[manifest.DefaultServiceName]; !ok {
		names := make([]string, 0, len(stack.Services))
		for n := range stack.Services {
			names = append(names, n)
		}
		return nil, fmt.Errorf("release store for legacy lease %s has stack-shaped manifest "+
			"with service %q; expected %q (operator: this indicates corrupted release-store "+
			"state — investigate or deprovision)", leaseUUID, names[0], manifest.DefaultServiceName)
	}

	instances := make([]legacyMigrationInstance, 0, len(group))
	for _, c := range group {
		// Mounts: prefer the inline list-containers value populated in
		// Task 8.2. A defensive InspectContainer fallback covers the
		// (vanishingly rare) case where the list payload had no mount
		// array attached.
		mounts := c.Mounts
		if mounts == nil {
			inspected, ierr := b.inspectContainerForRecovery(ctx, c.ContainerID)
			if ierr != nil {
				return nil, fmt.Errorf("inspect legacy container %s for mounts: %w", c.ContainerID, ierr)
			}
			mounts = inspected.Mounts
		}
		managed := filterManagedMounts(b, mounts)

		newName := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, manifest.DefaultServiceName, c.InstanceIndex)
		oldVol := fmt.Sprintf("fred-%s-%d", leaseUUID, c.InstanceIndex)
		newVol := canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, c.InstanceIndex)

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
		LeaseUUID:    leaseUUID,
		Tenant:       identity.Tenant,
		ProviderUUID: identity.ProviderUUID,
		SKU:          identity.SKU,
		CustomDomain: identity.CustomDomain,
		FailCount:    maxFailCount,
		Stack:        stack,
		Instances:    instances,
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
//  6. RecordLegacyMigration on the release store so the next boot sees the
//     wrapped manifest, exact desired cohort, and tokenless runtime authority
//     in one commit. This durable commit is a prerequisite for declaring the
//     migrated substrate authoritative.
//  7. Schedule tracked background removal of all `-prev` containers after
//     b.cfg.MigrationGracePeriod — preserves rollback inspection potential
//     without blocking startup.
//
// Failure semantics: any step error returns immediately. The caller
// (recoverState) wraps with operator remediation guidance.
//
// Idempotency / crash resumability:
//   - **Boundary 1 (before Stop+rename-to-prev):** a crash here leaves
//     legacy containers and legacy-named volumes intact. Next boot
//     re-runs the migration from scratch — fully resumable.
//   - **Boundary 2 (after rename-to-prev, before compose.Up):**
//     containers are stopped & renamed to `<name>-prev`, and volumes may be
//     partially renamed. The planner deliberately includes `-prev` legacy
//     containers; the next boot skips the completed rename, repeats the
//     idempotent volume moves, and converges Compose Up.
//   - **Boundary 3 (after compose.Up, before RecordLegacyMigration):** new
//     stack containers exist alongside `-prev` containers. The release
//     store still has the legacy active entry, so the next boot will
//     re-plan. The volume renames are already idempotent (the rename
//     tolerance below skips already-renamed paths). Compose.Up is
//     idempotent on container name. Resumable, but the operator may
//     see two generations of containers transiently.
//   - **Boundary 4 (after RecordLegacyMigration, before grace-window removal):**
//     forward progress is durable. A restart rediscovers `-prev`, revalidates
//     the idempotent migration, and schedules tracked cleanup again.
func (b *Backend) executeLegacyMigration(ctx context.Context, m *legacyMigration, logger *slog.Logger) error {
	logger = logger.With("lease_uuid", m.LeaseUUID, "instances", len(m.Instances))
	logger.Info("legacy migration starting")

	// Validate callback authority before stopping or renaming anything. Preserve
	// the exact operation-completion route and independently resolve its
	// observation-only pair. Old containers have no lifecycle label, in which
	// case resolution derives it by replacing only operation_id; current labels
	// must already equal that exact derivation. Validate every sibling so
	// container-list order can never select one route from inconsistent durable
	// state after destructive migration work has begun.
	legacyCallbackURL, lifecycleCallbackURL, err := resolveLegacyMigrationCallbackURLs(m.Instances)
	if err != nil {
		return fmt.Errorf("resolve legacy callback routes: %w", err)
	}
	legacyRuntimeAuthority, err := shared.NewLegacyRuntimeAuthority(
		m.Tenant,
		m.ProviderUUID,
		legacyCallbackURL,
		lifecycleCallbackURL,
	)
	if err != nil {
		return fmt.Errorf("freeze legacy migration runtime authority: %w", err)
	}

	svc := m.Stack.Services[manifest.DefaultServiceName]
	if svc == nil {
		return fmt.Errorf("internal: wrapped stack missing default service %q", manifest.DefaultServiceName)
	}
	stopGrace := 10 * time.Second
	if svc.StopGracePeriod != nil {
		stopGrace = svc.StopGracePeriod.Duration()
	}
	// True v0.13 rows have no persisted resource authority. Freeze the one
	// compatibility value before the first destructive migration step, then use
	// this exact snapshot for Compose, accounting, quota recovery, and the
	// migration Release. A later config resize cannot reprice the migrated lease.
	profile, err := b.cfg.GetSKUProfile(m.SKU)
	if err != nil {
		return fmt.Errorf("load SKU profile %s: %w", m.SKU, err)
	}
	quantity := len(m.Instances)
	items := []backend.LeaseItem{{
		SKU:          m.SKU,
		Quantity:     quantity,
		ServiceName:  manifest.DefaultServiceName,
		CustomDomain: m.CustomDomain,
	}}
	resourceProfiles, err := b.snapshotResourceProfiles(items, map[string]SKUProfile{m.SKU: profile})
	if err != nil {
		return fmt.Errorf("snapshot migrated resource profiles: %w", err)
	}
	if b.releaseStore == nil {
		return fmt.Errorf("record migration: release store is required")
	}
	capacityPlanner := b.releaseHistoryCapacityPlanner()
	if capacityPlanner == nil {
		return fmt.Errorf("record migration: release capacity planner is required")
	}
	migrationManifest, err := json.Marshal(m.Stack)
	if err != nil {
		return fmt.Errorf("marshal wrapped migration manifest: %w", err)
	}
	migrationCreatedAt := time.Now()
	if err := capacityPlanner.CheckRecordLegacyMigrationCapacity(
		m.LeaseUUID,
		migrationManifest,
		items,
		resourceProfiles,
		legacyRuntimeAuthority,
		migrationCreatedAt,
	); err != nil {
		return fmt.Errorf("reserve migration release capacity: %w", err)
	}

	// 1. Stop + prove quiescence + rename every legacy container. The proof is
	// required even for an already-`-prev` retry: a name is not runtime state,
	// and advancing while the exact old container still has the bind open can
	// put two generations on the same tenant data.
	for _, inst := range m.Instances {
		if err := b.stopLegacyContainerForMigration(ctx, inst, stopGrace); err != nil {
			return err
		}
		if inst.LegacyContainer.Name == inst.PrevName {
			continue
		}
		if err := b.mutationAdapter().renameContainer(ctx, inst.LegacyContainer.ContainerID, inst.PrevName); err != nil {
			if !isAlreadyNamedErr(err, inst.PrevName) {
				return fmt.Errorf("rename %s to %s: %w", inst.LegacyContainer.ContainerID, inst.PrevName, err)
			}
		}
	}

	// 2. Rename managed volume directories. RenameVolume is idempotent
	// (Task 10), so re-runs after a partial migration succeed quietly.
	for _, inst := range m.Instances {
		for _, r := range inst.VolRenames {
			if err := b.mutationAdapter().renameVolume(ctx, r.Old, r.New); err != nil {
				return fmt.Errorf("rename volume %s→%s (instance idx=%d): %w",
					r.Old, r.New, inst.LegacyContainer.InstanceIndex, err)
			}
		}
	}

	// 3. Build the Compose project. SKU profile lookup + items list mirror
	// the live provision flow (provision.go:doProvision); the only
	// migration-specific input is the VolBinds map seeded from the
	// just-renamed directories.
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
				src, err := resolveMigratedBindSource(hostRoot, sanitized)
				if err != nil {
					return fmt.Errorf("legacy mount target %q: %w", r.Target, err)
				}
				binds.StatefulBinds[src] = r.Target
			}
		}
		volBinds[manifest.DefaultServiceName][inst.LegacyContainer.InstanceIndex] = binds
	}

	// Ensure the per-tenant Docker network exists when isolation is enabled.
	// Mirrors doProvision (provision.go) — without this, migrated containers
	// come up off-network: Traefik routing breaks and inter-container DNS
	// resolves nothing. The network create is idempotent; a concurrent or
	// previous-startup creation is fine.
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		if _, netErr := b.ensureTenantNetwork(ctx, m.Tenant); netErr != nil {
			return fmt.Errorf("ensure tenant network: %w", netErr)
		}
		networkName = TenantNetworkName(m.Tenant)
	}
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:            m.LeaseUUID,
		Tenant:               m.Tenant,
		ProviderUUID:         m.ProviderUUID,
		CallbackURL:          legacyCallbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		BackendName:          b.cfg.Name,
		FailCount:            m.FailCount,
		Stack:                m.Stack,
		Items:                items,
		Profiles:             map[string]SKUProfile{m.SKU: profile},
		NetworkName:          networkName,
		VolBinds:             volBinds,
		Cfg:                  &b.cfg,
		Ingress:              b.cfg.Ingress,
	})

	// 4. Compose Up — brings up all instances at once.
	if err := b.mutationAdapter().composeUp(ctx, project, composeUpOpts{}); err != nil {
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
	// The health-wait can take up to MigrationReadyTimeout (default 90s), which
	// exceeds the caller's context only when that caller passes an
	// inappropriately short one. Start deliberately drives recovery under the
	// backend lifecycle context for exactly this reason (see recoverState call
	// in backend.go, ENG-592); request-scoped callers (RefreshState) keep their
	// own deadline here.
	readyCtx, cancel := context.WithTimeout(ctx, cmp.Or(b.cfg.MigrationReadyTimeout, defaultMigrationReadyTimeout))
	defer cancel()
	if err := b.verifyStartup(readyCtx, svc, newIDs, logger); err != nil {
		return fmt.Errorf("wait for ready: %w", err)
	}

	// 6. Persist the wrapped manifest, exact migrated cohort, and the tokenless
	// runtime authority proven before Stop in one transaction before removing any
	// rollback evidence. A persistence failure is not success: keep every `-prev`
	// container and fail startup so the next boot/operator can recover from both
	// generations rather than silently accepting an unrecorded cohort.
	if err := b.releaseStore.RecordLegacyMigrationAt(
		m.LeaseUUID,
		migrationManifest,
		items,
		resourceProfiles,
		legacyRuntimeAuthority,
		migrationCreatedAt,
	); err != nil {
		return fmt.Errorf("record migration release: %w", err)
	}

	// 7. Schedule per-instance `-prev` removal after the operator-
	// inspection grace window. Background — must not block startup. This
	// tracked cleanup is gated on b.stopCtx, NOT the caller's ctx:
	// it must survive the caller returning (main cancels the short startup
	// context the instant Start returns — keying on ctx would fire Done() at
	// ~0s and leak every migration's `-prev` containers) while still stopping
	// on daemon shutdown (ENG-592). Unlike the synchronous ready-wait above,
	// there is no caller left to honor a deadline once this goroutine detaches.
	rollbackTargets := make([]legacyRollbackCleanupTarget, 0, len(m.Instances))
	for _, inst := range m.Instances {
		rollbackTargets = append(rollbackTargets, legacyRollbackCleanupTarget{
			ContainerID: inst.LegacyContainer.ContainerID,
			Name:        inst.PrevName,
		})
	}
	b.scheduleLegacyPrevCleanup(rollbackTargets, logger)

	logger.Info("legacy migration complete")
	return nil
}

// stopLegacyContainerForMigration establishes the load-bearing precondition for
// moving a legacy container's bind-mounted data. Docker Stop can return an
// error after the daemon accepted the request, and a retry can discover a
// `-prev` container that is nevertheless running. Neither the return value nor
// the name alone proves quiescence, so inspect the exact immutable container ID
// and admit only Docker's explicit non-running states.
func (b *Backend) stopLegacyContainerForMigration(
	ctx context.Context,
	inst legacyMigrationInstance,
	stopGrace time.Duration,
) error {
	containerID := inst.LegacyContainer.ContainerID
	if containerID == "" {
		return errors.New("legacy migration container has no immutable ID")
	}
	stopErr := b.mutationAdapter().stopContainer(ctx, containerID, stopGrace)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stop legacy container %s: %w", containerID, errors.Join(stopErr, err))
	}

	inspected, inspectErr := b.inspectContainerForRecovery(ctx, containerID)
	if inspectErr != nil {
		return fmt.Errorf(
			"prove legacy container %s stopped after stop result: %w",
			containerID,
			errors.Join(stopErr, inspectErr),
		)
	}
	if inspected == nil || inspected.ContainerID != containerID {
		observedID := ""
		if inspected != nil {
			observedID = inspected.ContainerID
		}
		return fmt.Errorf(
			"prove legacy container %s stopped: inspect returned immutable ID %q",
			containerID,
			observedID,
		)
	}

	if isDockerNonRunningStatus(inspected.Status) {
		// These are Docker's explicit non-running states. A prior Stop error is
		// now classified as an ambiguous response to a completed stop, not as
		// permission to guess: the exact postcondition is what authorizes the
		// volume move.
		return nil
	}
	stateErr := fmt.Errorf(
		"legacy container %s remains in non-quiescent state %q",
		containerID,
		inspected.Status,
	)
	return fmt.Errorf("prove legacy container stopped: %w", errors.Join(stopErr, stateErr))
}

// scheduleLegacyPrevCleanup removes rollback-window containers only after the
// caller has proved that an exact migration release is durable and its live
// Compose cohort is valid. It is also used on restart to resume an interrupted
// partial cleanup without ever re-inferring desired topology from the remaining
// rollback subset.
type legacyRollbackCleanupTarget struct {
	ContainerID string
	Name        string
}

func (b *Backend) scheduleLegacyPrevCleanup(targets []legacyRollbackCleanupTarget, logger *slog.Logger) {
	for _, target := range targets {
		b.wg.Go(func() {
			select {
			case <-time.After(cmp.Or(b.cfg.MigrationGracePeriod, defaultMigrationGracePeriod)):
			case <-b.stopCtx.Done():
				return
			}
			// Derive cleanup from the backend lifecycle and re-attest immediately
			// before the destructive call. A storage/daemon swap during the grace
			// window must leave the -prev container for operator inspection, not
			// remove something from the replacement substrate.
			// docker daemon doesn't keep the goroutine alive forever.
			// Logged as warning rather than failing the migration —
			// the data plane is already on the stack-form container;
			// the -prev leftover is an operator cleanup at worst.
			rmCtx, rmCancel := context.WithTimeout(b.stopCtx, 30*time.Second)
			defer rmCancel()
			if err := b.requireStorageIdentity(rmCtx); err != nil {
				logger.Warn("suppressing -prev removal after backend identity verification failed",
					"name", target.Name, "container_id", target.ContainerID, "error", err)
				return
			}
			// Docker container IDs are immutable. Removing by the delayed name
			// could delete an unrelated replacement that acquired it during the
			// grace window after the original vanished.
			if err := b.mutationAdapter().removeContainer(rmCtx, target.ContainerID); err != nil {
				logger.Warn("remove -prev container after grace failed (manual cleanup may be needed)",
					"name", target.Name, "container_id", target.ContainerID, "error", err)
			}
		})
	}
}

// removeCommittedLegacyRollbackRemnants synchronously consumes the rollback
// containers of a durably committed legacy migration before Deprovision deletes
// the release record that identifies them. The ordinary Compose project does not
// own these pre-Compose containers, so a successful Compose Down cannot remove
// them. Leaving cleanup only to the grace-period goroutine creates a crash window:
// after release-history deletion, the next startup sees `-prev` as an uncommitted
// legacy cohort and cannot safely reconstruct it.
func (b *Backend) removeCommittedLegacyRollbackRemnants(
	ctx context.Context,
	leaseUUID string,
	logger *slog.Logger,
) error {
	if b.releaseStore == nil {
		return nil
	}
	releases, err := b.releaseStore.List(leaseUUID)
	if err != nil {
		return fmt.Errorf("read release history before legacy rollback cleanup: %w", err)
	}
	hasMigrationAuthority := false
	for _, release := range releases {
		if release.LegacyMigration {
			hasMigrationAuthority = true
			break
		}
	}
	if !hasMigrationAuthority {
		return nil
	}

	// Reuse close admission's exact whole-cohort proof instead of reconstructing
	// targets from reusable Docker names. The inventory snapshot binds every
	// rollback remnant to an immutable container ID and validates its stopped
	// state, backend, release topology, image, and callback cohort before the
	// first destructive call. An already-removed remnant is ordinary success;
	// an unrelated container that later acquires its old name is never selected.
	targets, err := b.closeLegacyRollbackTargets(
		ctx,
		leaseUUID,
		releases,
		"",
		"",
		"",
		"",
	)
	if err != nil {
		return fmt.Errorf("resolve immutable legacy rollback cleanup targets: %w", err)
	}

	var cleanupErrs []error
	for _, target := range targets {
		if err := b.mutationAdapter().removeContainer(ctx, target.ContainerID); err != nil {
			logger.Warn("failed to remove committed legacy rollback container",
				"name", target.Name,
				"container_id", target.ContainerID,
				"error", err,
			)
			cleanupErrs = append(cleanupErrs, fmt.Errorf(
				"remove %s (%s): %w",
				target.Name,
				target.ContainerID,
				err,
			))
		}
	}
	return errors.Join(cleanupErrs...)
}

func resolveLegacyMigrationCallbackURLs(instances []legacyMigrationInstance) (string, string, error) {
	if len(instances) == 0 {
		return "", "", fmt.Errorf("legacy migration has no instances")
	}
	containers := make([]ContainerInfo, len(instances))
	for index := range instances {
		containers[index] = instances[index].LegacyContainer
	}
	return resolveLegacyContainerCallbackURLs(containers)
}

// resolveLegacyContainerCallbackURLs is the pure callback-authority proof
// shared by stopped adoption and runtime migration. Keeping one predicate is
// load-bearing: a cohort that seals successfully must not later fail startup
// because its individually valid v0.13 siblings disagree about the callback
// generation migration must persist on the replacement stack.
func resolveLegacyContainerCallbackURLs(containers []ContainerInfo) (string, string, error) {
	if len(containers) == 0 {
		return "", "", errors.New("legacy callback cohort has no containers")
	}
	callbackURL := containers[0].CallbackURL
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(
		callbackURL, containers[0].LifecycleCallbackURL,
	)
	if err != nil {
		return "", "", fmt.Errorf("instance %d: %w",
			containers[0].InstanceIndex, err)
	}
	for _, container := range containers[1:] {
		if container.CallbackURL != callbackURL {
			return "", "", fmt.Errorf(
				"instance %d callback_url differs from instance %d",
				container.InstanceIndex,
				containers[0].InstanceIndex,
			)
		}
		resolved, resolveErr := backend.ResolveLifecycleCallbackURL(
			container.CallbackURL,
			container.LifecycleCallbackURL,
		)
		if resolveErr != nil {
			return "", "", fmt.Errorf("instance %d: %w",
				container.InstanceIndex, resolveErr)
		}
		if resolved != lifecycleCallbackURL {
			return "", "", fmt.Errorf(
				"instance %d lifecycle callback route differs from instance %d",
				container.InstanceIndex,
				containers[0].InstanceIndex,
			)
		}
	}
	return callbackURL, lifecycleCallbackURL, nil
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
	containers, err := b.listManagedContainersForRecovery(ctx)
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
