package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/fsidentity"
)

// protectedVolume retains both the manager's namespace and the physical
// directory. A managed name is never used as the reservation identity.
type protectedVolume struct {
	name    string
	root    *fsidentity.Directory
	created bool
}

func (m *storageMutations) volumeLaunchOrigin() shared.VolumeLaunchOrigin {
	if m.compensationSubject.Valid() {
		return shared.VolumeLaunchForCompensation(m.compensationSubject)
	}
	if m.maintenanceSubject.Valid() {
		return shared.VolumeLaunchForMaintenance(m.maintenanceSubject)
	}
	return shared.VolumeLaunchForOperation(m.operationSubject)
}

// quiescedVolumes is minted only after all relevant writers have been stopped
// and re-inspected. It remains private to the complete launch workflow; callers
// receive neither a raw bind map nor an independent start capability.
type quiescedVolumes struct {
	mutations        *storageMutations
	volumes          map[string]protectedVolume
	reserved         *reservedVolumeSet
	releaseNamespace func()
	active           *atomic.Bool
}

func (q *quiescedVolumes) requireActive() error {
	if q == nil || q.active == nil || !q.active.Load() || q.reserved == nil ||
		q.reserved.lifetime == nil || q.reserved.lifetime.released.Load() {
		return errors.New("volume launch authority is unavailable")
	}
	for _, volume := range q.volumes {
		if err := volume.root.VerifyPath(); err != nil {
			return fmt.Errorf("reserved volume identity: %w", err)
		}
	}
	return nil
}

// launchVolume grants filesystem preparation for one exact reserved directory.
// Its copies share the complete launch's lifetime; no filesystem sink accepts a
// caller-selected host destination or a merely same-lease volume name.
type launchVolume struct{ state *launchVolumeState }

type launchVolumeState struct {
	owner     *quiescedVolumes
	name      managedVolumeName
	directory *fsidentity.Directory
	created   bool
}

func (q *quiescedVolumes) lookup(name string) (launchVolume, error) {
	if err := q.requireActive(); err != nil {
		return launchVolume{}, err
	}
	volume, ok := q.volumes[name]
	if !ok {
		return launchVolume{}, fmt.Errorf("volume %q is outside this launch", name)
	}
	parsed, err := parseManagedVolumeName(name)
	if err != nil {
		return launchVolume{}, err
	}
	return launchVolume{state: &launchVolumeState{owner: q, name: parsed, directory: volume.root, created: volume.created}}, nil
}

func (v launchVolume) requireActive() error {
	if v.state == nil {
		return errors.New("reserved volume authority is unavailable")
	}
	if err := v.state.owner.requireActive(); err != nil {
		return err
	}
	volume, ok := v.state.owner.volumes[v.state.name.value()]
	if !ok || volume.root != v.state.directory {
		return errors.New("volume differs from the reserved launch directory")
	}
	return nil
}

func (v launchVolume) rootPath() (string, error) {
	if err := v.requireActive(); err != nil {
		return "", err
	}
	return v.state.directory.Path(), nil
}

func (v launchVolume) wasCreated() bool {
	return v.state != nil && v.state.created
}

func (q *quiescedVolumes) release() {
	if q == nil || q.active == nil || !q.active.Swap(false) {
		return
	}
	q.reserved.release()
	for _, volume := range q.volumes {
		_ = volume.root.Close()
	}
	q.releaseNamespace()
}

// launchCompose is the sole managed Compose launch workflow. Its order cannot
// be rearranged by a lifecycle caller: materialize roots, reserve their physical
// identities, stop writers, prepare every bind, validate the complete graph,
// freeze the image-bound project, then hold exclusion through Compose Start.
func (b *Backend) launchCompose(ctx context.Context, mutations *storageMutations, params composeProjectParams, resources []shared.SKUResourceSnapshot, opts composeUpOpts) error {
	if mutations == nil || params.LeaseUUID != mutations.leaseUUID || params.VolBinds != nil {
		return errors.New("compose launch inputs differ from the physical subject")
	}
	profiles, err := resourceProfileMap(params.Items, resources)
	if err != nil {
		return err
	}
	params.Profiles = profiles
	volumeSetupStartedAt := time.Now()
	volumes, err := b.prepareLaunchVolumes(ctx, mutations, params, resources)
	if err != nil {
		mutations.observeReplacementPhase(phaseVolumeSetup, volumeSetupStartedAt)
		return err
	}
	defer volumes.release()
	binds, _, err := b.setupVolBinds(volumes, ctx, params.LeaseUUID, params.Items,
		resources, params.ImageSetups, b.logger)
	mutations.observeReplacementPhase(phaseVolumeSetup, volumeSetupStartedAt)
	if err != nil {
		return err
	}
	params.VolBinds = binds
	project := buildComposeProject(params)
	if err := volumes.validateProject(project); err != nil {
		return err
	}
	prepared, err := mutations.ops.compose.PrepareProject(project, composeProjectImages(project, params.ImageSetups))
	if err != nil {
		return err
	}
	composeStartedAt := time.Now()
	err = b.volumeLaunches.compose(ctx, volumes, prepared, opts)
	mutations.observeReplacementPhase(phaseComposeUp, composeStartedAt)
	return err
}

// The complete launch owns the phase boundaries, while the exact physical
// subject supplies the existing bounded replacement-operation label.
func (m *storageMutations) observeReplacementPhase(phase string, started time.Time) {
	operation := ""
	if m.maintenanceSubject.Valid() {
		switch kind := m.maintenanceSubject.Intent().Kind(); kind {
		case shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate:
			operation = string(kind)
		}
	} else if m.operationSubject.Valid() && m.operationSubject.Intent().Kind() == shared.OperationIntentRestore {
		operation = string(shared.OperationIntentRestore)
	}
	if operation != "" {
		replacePhaseDurationSeconds.WithLabelValues(operation, phase).Observe(time.Since(started).Seconds())
	}
}

func (b *Backend) prepareLaunchVolumes(ctx context.Context, mutations *storageMutations, params composeProjectParams, resources []shared.SKUResourceSnapshot) (*quiescedVolumes, error) {
	bySKU, err := resourceSnapshotMap(params.Items, resources)
	if err != nil {
		return nil, err
	}
	created := make(map[string]bool)
	paths := make(map[string]string)
	for _, item := range params.Items {
		setup := params.ImageSetups[item.ServiceName]
		profile, ok := params.Profiles[item.SKU]
		if !ok || setup == nil {
			return nil, errors.New("launch requires complete image and resource admission")
		}
		for index := range item.Quantity {
			stateful := profile.DiskMB > 0 && len(setup.Volumes) != 0
			if !stateful && len(setup.WritablePaths) == 0 {
				continue
			}
			name := canonicalVolumeName(params.LeaseUUID, item.ServiceName, index)
			size := profile.DiskMB
			if size <= 0 {
				size = bySKU[item.SKU].ScratchDiskMB
			}
			path, wasCreated, err := b.createManagedVolume(mutations, ctx, name, size)
			if err != nil {
				if !stateful {
					// Writable-path seeding remains best effort. No path capability
					// is issued for this volume; Compose retains its tmpfs fallback.
					continue
				}
				return nil, fmt.Errorf("prepare launch volume %q: %w", name, err)
			}
			paths[name], created[name] = path, wasCreated
		}
	}
	return b.quiesceLaunchVolumes(ctx, mutations, paths, created, nil)
}

func (b *Backend) quiesceLaunchVolumes(ctx context.Context, mutations *storageMutations, paths map[string]string, created map[string]bool, expected map[string]fsidentity.Identity) (*quiescedVolumes, error) {
	releaseNamespace, err := b.volumeAccess.retainNamespace(ctx)
	if err != nil {
		return nil, err
	}
	q := &quiescedVolumes{mutations: mutations, volumes: make(map[string]protectedVolume, len(paths)), releaseNamespace: releaseNamespace, active: new(atomic.Bool)}
	complete := false
	defer func() {
		if complete {
			return
		}
		q.reserved.release()
		for _, volume := range q.volumes {
			_ = volume.root.Close()
		}
		releaseNamespace()
	}()
	ids := make([]fsidentity.Identity, 0, len(paths))
	for name, path := range paths {
		parsed, err := parseManagedVolumeName(name)
		if err != nil || !mutations.volumeNameInScope(parsed) {
			return nil, fmt.Errorf("launch volume %q is outside its subject", name)
		}
		if err := b.volumes.AttestManagedVolume(ctx, parsed); err != nil {
			return nil, fmt.Errorf("attest launch volume %q: %w", name, err)
		}
		root, err := fsidentity.OpenDirectory(path)
		if err != nil {
			return nil, fmt.Errorf("pin launch volume %q: %w", name, err)
		}
		q.volumes[name] = protectedVolume{name: name, root: root, created: created[name]}
		if expected != nil && !root.Identity().Equal(expected[name]) {
			return nil, fmt.Errorf("source volume %q physical identity changed", name)
		}
		ids = append(ids, root.Identity())
	}
	reserved, err := b.volumeAccess.reserve(ctx, ids)
	if err != nil {
		return nil, err
	}
	q.reserved = reserved
	if err := b.volumeLaunches.check(mutations.volumeLaunchOrigin(), reserved.ids); err != nil {
		return nil, err
	}
	if len(paths) != 0 {
		if err := q.stopWriters(ctx); err != nil {
			return nil, err
		}
	}
	q.active.Store(true)
	if err := q.requireActive(); err != nil {
		q.active.Store(false)
		return nil, err
	}
	complete = true
	return q, nil
}

func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}

func (q *quiescedVolumes) affects(source string) (bool, error) {
	if source == "" || !filepath.IsAbs(source) {
		return false, errors.New("container bind source is not an absolute path")
	}
	for _, volume := range q.volumes {
		if pathContains(volume.root.Path(), source) || pathContains(source, volume.root.Path()) {
			return true, nil
		}
	}
	sourceIdentity, sourceErr := fsidentity.InspectDirectory(source)
	if sourceErr == nil {
		// A bind alias of a parent directory can replace descendants too. Only
		// the source itself is compared with root ancestors: comparing both
		// ancestor chains would classify every path sharing / as a writer.
		for _, volume := range q.volumes {
			for parent := filepath.Dir(volume.root.Path()); ; parent = filepath.Dir(parent) {
				identity, err := fsidentity.InspectDirectory(parent)
				if err != nil {
					return false, err
				}
				if sourceIdentity.Equal(identity) {
					return true, nil
				}
				if parent == filepath.Dir(parent) {
					break
				}
			}
		}
	}
	// Aliases outside the conventional namespace still share a physical
	// ancestor. Retain uncertainty if an existing source cannot be inspected.
	for path := filepath.Clean(source); ; path = filepath.Dir(path) {
		identity, err := fsidentity.InspectDirectory(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			// Docker also permits file bind mounts. Their parent, rather than
			// the file itself, is the relevant directory ancestor.
			info, statErr := os.Lstat(path)
			if statErr == nil && info.Mode().IsRegular() {
				continue
			}
			return false, err
		}
		if err == nil {
			for _, volume := range q.volumes {
				if identity.Equal(volume.root.Identity()) {
					return true, nil
				}
			}
		}
		if path == filepath.Dir(path) {
			return false, nil
		}
	}
}

func (q *quiescedVolumes) stopWriters(ctx context.Context) error {
	all, err := q.mutations.ops.backend.docker.ListVolumeWriters(ctx)
	if err != nil {
		return fmt.Errorf("inventory volume writers: %w", err)
	}
	var writers []string
	for _, candidate := range all {
		for _, mount := range candidate.Mounts {
			if (mount.Type != "bind" && mount.Type != "volume") || mount.ReadOnly {
				continue
			}
			affected, err := q.affects(mount.Source)
			if err != nil {
				return fmt.Errorf("classify volume writer %q: %w", candidate.ContainerID, err)
			}
			if !affected {
				continue
			}
			writers = append(writers, candidate.ContainerID)
			break
		}
	}
	slices.Sort(writers)
	var retirements []priorVolumeWriter
	for _, id := range slices.Compact(writers) {
		writer, err := q.mutations.priorVolumeWriter(ctx, id)
		if err != nil {
			return fmt.Errorf("volume writer interference: %w", err)
		}
		retirements = append(retirements, writer)
	}
	// Classify every writer before retiring any: an unknown or current failed
	// target must be handled by its diagnostic cleanup owner first.
	for _, writer := range retirements {
		if err := writer.retire(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (q *quiescedVolumes) validateProject(project *composetypes.Project) error {
	if err := q.requireActive(); err != nil {
		return err
	}
	type mountSite struct {
		service string
		root    fsidentity.Identity
		path    string
		write   bool
	}
	var sites []mountSite
	for service, spec := range project.Services {
		for _, mount := range spec.Volumes {
			if mount.Type != composetypes.VolumeTypeBind {
				continue
			}
			resolved, err := filepath.EvalSymlinks(mount.Source)
			if err != nil {
				return fmt.Errorf("resolve prepared mount: %w", err)
			}
			matched := false
			for _, volume := range q.volumes {
				if !pathContains(volume.root.Path(), resolved) {
					continue
				}
				rel, err := filepath.Rel(volume.root.Path(), resolved)
				if err != nil {
					return err
				}
				sites = append(sites, mountSite{service: service, root: volume.root.Identity(), path: rel, write: !mount.ReadOnly})
				matched = true
				break
			}
			if !matched {
				return errors.New("prepared bind escaped its reserved volume")
			}
		}
	}
	for _, writer := range sites {
		for _, pending := range sites {
			if writer.service != pending.service && writer.write && writer.root.Equal(pending.root) &&
				writer.path != pending.path && pathContains(writer.path, pending.path) {
				return errors.New("launch mount graph lets one target replace another target's pending bind source")
			}
		}
	}
	return nil
}
