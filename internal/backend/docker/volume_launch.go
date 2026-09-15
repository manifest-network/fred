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

func (q *quiescedVolumes) requireReservation() error {
	if q == nil || q.active == nil || !q.active.Load() || q.reserved == nil ||
		q.reserved.lifetime == nil || q.reserved.lifetime.released.Load() {
		return errors.New("volume launch authority is unavailable")
	}
	return nil
}

// Complete launch boundaries re-attest the whole set. Per-volume filesystem
// capabilities use requireVolume instead, so preparing V directories does not
// reopen all V roots for every individual filesystem operation.
func (q *quiescedVolumes) requireActive() error {
	if err := q.requireReservation(); err != nil {
		return err
	}
	for _, volume := range q.volumes {
		if err := verifyReservedVolumeRoot(volume.root); err != nil {
			return err
		}
	}
	return nil
}

func (q *quiescedVolumes) requireVolume(name managedVolumeName, directory *fsidentity.Directory) error {
	if err := q.requireReservation(); err != nil {
		return err
	}
	volume, ok := q.volumes[name.value()]
	if !ok || volume.root != directory {
		return errors.New("volume differs from the reserved launch directory")
	}
	return verifyReservedVolumeRoot(directory)
}

func verifyReservedVolumeRoot(directory *fsidentity.Directory) error {
	if err := directory.VerifyPath(); err != nil {
		return fmt.Errorf("reserved volume identity: %w", err)
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
	if err := q.requireReservation(); err != nil {
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
	if err := q.requireVolume(parsed, volume.root); err != nil {
		return launchVolume{}, err
	}
	return launchVolume{state: &launchVolumeState{owner: q, name: parsed, directory: volume.root, created: volume.created}}, nil
}

func (v launchVolume) requireActive() error {
	if v.state == nil {
		return errors.New("reserved volume authority is unavailable")
	}
	return v.state.owner.requireVolume(v.state.name, v.state.directory)
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
	q.releaseResources()
}

// releaseResources also handles partially constructed launches. No waiter may
// acquire physical exclusion while this owner still pins a retiring inode.
func (q *quiescedVolumes) releaseResources() {
	for _, volume := range q.volumes {
		_ = volume.root.Close()
	}
	q.reserved.release()
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
	names := make([]managedVolumeName, 0, len(paths))
	for raw := range paths {
		name, err := parseManagedVolumeName(raw)
		if err != nil || !mutations.volumeNameInScope(name) {
			return nil, fmt.Errorf("launch volume %q is outside its subject", raw)
		}
		names = append(names, name)
	}
	releaseNamespace, err := b.volumeAccess.retainNamespace(ctx, names)
	if err != nil {
		return nil, err
	}
	q := &quiescedVolumes{mutations: mutations, volumes: make(map[string]protectedVolume, len(paths)), releaseNamespace: releaseNamespace, active: new(atomic.Bool)}
	complete := false
	defer func() {
		if complete {
			return
		}
		q.releaseResources()
	}()
	owned, err := b.reserveManagedNamespaceRoots(ctx, names)
	if err != nil {
		return nil, err
	}
	q.reserved = owned.reservation
	for name, path := range paths {
		parsed, err := parseManagedVolumeName(name)
		if err != nil || !mutations.volumeNameInScope(parsed) {
			return nil, fmt.Errorf("launch volume %q is outside its subject", name)
		}
		if err := b.volumes.AttestManagedVolume(ctx, parsed); err != nil {
			return nil, fmt.Errorf("attest launch volume %q: %w", name, err)
		}
		root, err := owned.openRoot(parsed, path)
		if err != nil {
			return nil, fmt.Errorf("pin launch volume %q: %w", name, err)
		}
		q.volumes[name] = protectedVolume{name: name, root: root, created: created[name]}
		if expected != nil && !root.Identity().Equal(expected[name]) {
			return nil, fmt.Errorf("source volume %q physical identity changed", name)
		}
	}
	if err := b.volumeLaunches.check(mutations.volumeLaunchOrigin(), q.reserved.ids); err != nil {
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
	// Docker accepts links and non-directory leaves as bind sources. Resolve a
	// link before classifying it: skipping it could hide a writer inside a
	// protected root. Missing leaves are checked through their existing parents.
	resolved, err := filepath.EvalSymlinks(source)
	if err == nil {
		source = resolved
		for _, volume := range q.volumes {
			if pathContains(volume.root.Path(), source) || pathContains(source, volume.root.Path()) {
				return true, nil
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
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
		resolved, err := filepath.EvalSymlinks(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		info, err := os.Lstat(resolved)
		if err != nil {
			return false, err
		}
		if !info.IsDir() {
			// A socket, FIFO, device, or ordinary file cannot contain a managed
			// directory. Do not open the leaf; its parent may still be an alias
			// of a protected root and must be checked.
			continue
		}
		identity, err := fsidentity.InspectDirectory(resolved)
		if err != nil {
			return false, err
		}
		for _, volume := range q.volumes {
			if identity.Equal(volume.root.Identity()) {
				return true, nil
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
	// Index canonical roots and the path components below each physical root.
	// Work scales with the input's total path length, not the square of its bind
	// count. Repeated sources share symlink resolution within this validation.
	roots := make(map[string]fsidentity.Identity, len(q.volumes))
	for _, volume := range q.volumes {
		roots[volume.root.Path()] = volume.root.Identity()
	}
	graphs := make(map[fsidentity.Identity]*launchMountNode)
	resolvedSources := make(map[string]string)
	for service, spec := range project.Services {
		for _, mount := range spec.Volumes {
			if mount.Type != composetypes.VolumeTypeBind {
				continue
			}
			resolved, ok := resolvedSources[mount.Source]
			if !ok {
				var err error
				resolved, err = filepath.EvalSymlinks(mount.Source)
				if err != nil {
					return fmt.Errorf("resolve prepared mount: %w", err)
				}
				resolvedSources[mount.Source] = resolved
			}
			rootPath := resolved
			for {
				if _, ok := roots[rootPath]; ok {
					break
				}
				parent := filepath.Dir(rootPath)
				if parent == rootPath {
					return errors.New("prepared bind escaped its reserved volume")
				}
				rootPath = parent
			}
			identity := roots[rootPath]
			graph := graphs[identity]
			if graph == nil {
				graph = &launchMountNode{}
				graphs[identity] = graph
			}
			rel, err := filepath.Rel(rootPath, resolved)
			if err != nil {
				return err
			}
			graph.add(rel, service, !mount.ReadOnly)
		}
	}
	for _, graph := range graphs {
		if err := graph.validate(launchMountWriters{}); err != nil {
			return err
		}
	}
	return nil
}

// Equal mount sources are allowed. Only a writable strict ancestor belonging
// to another service can exchange a bind source that is still being mounted.
type launchMountNode struct {
	children map[string]*launchMountNode
	services map[string]bool
}

func (n *launchMountNode) add(path, service string, write bool) {
	if path != "." {
		for part := range strings.SplitSeq(path, string(filepath.Separator)) {
			if n.children == nil {
				n.children = make(map[string]*launchMountNode)
			}
			child := n.children[part]
			if child == nil {
				child = &launchMountNode{}
				n.children[part] = child
			}
			n = child
		}
	}
	if n.services == nil {
		n.services = make(map[string]bool)
	}
	n.services[service] = n.services[service] || write
}

// Two distinct ancestors suffice: every descendant service differs from at
// least one of them. Keeping their names avoids copying a growing ancestor set.
type launchMountWriters struct {
	services [2]string
	count    int
}

func (w launchMountWriters) with(service string) launchMountWriters {
	for _, existing := range w.services[:w.count] {
		if existing == service {
			return w
		}
	}
	if w.count < len(w.services) {
		w.services[w.count] = service
		w.count++
	}
	return w
}

func (n *launchMountNode) validate(ancestors launchMountWriters) error {
	for service := range n.services {
		for _, writer := range ancestors.services[:ancestors.count] {
			if writer != service {
				return errors.New("launch mount graph lets one target replace another target's pending bind source")
			}
		}
	}
	for service, write := range n.services {
		if write {
			ancestors = ancestors.with(service)
		}
	}
	for _, child := range n.children {
		if err := child.validate(ancestors); err != nil {
			return err
		}
	}
	return nil
}
