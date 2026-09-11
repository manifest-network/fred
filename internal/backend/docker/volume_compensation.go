package docker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"

	"github.com/compose-spec/compose-go/v2/format"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/mount"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// captureCompensationVolumeRoots records physical identity, not reusable path
// permission. Replay must reserve and re-attest this same directory before any
// source writer can be launched.
func (b *Backend) captureCompensationVolumeRoots(ctx context.Context, snapshots []compensationContainerRecord) ([]compensationVolumeRoot, error) {
	release, err := b.volumeAccess.retainNamespace(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	roots := make(map[string]fsidentity.Identity)
	for _, snapshot := range snapshots {
		if snapshot.Config == nil {
			return nil, errors.New("source volume capture requires runtime identity")
		}
		meta, err := parseLabelMeta(snapshot.Config.Labels)
		if err != nil {
			return nil, err
		}
		name, err := parseManagedVolumeName(canonicalVolumeName(snapshot.Config.Labels[LabelLeaseUUID], snapshot.Config.Labels[LabelServiceName], meta.InstanceIndex))
		if err != nil {
			return nil, err
		}
		for _, observed := range snapshot.Mounts {
			if observed.Type != "bind" {
				continue
			}
			if err := b.volumes.AttestManagedVolume(ctx, name); err != nil {
				return nil, err
			}
			if err := requireManagedVolumeMountSource(b.cfg.VolumeDataPath, observed.Source, name, observed.Target); err != nil {
				return nil, err
			}
			identity, err := fsidentity.InspectDirectory(b.volumes.HostPath(name.value()))
			if err != nil {
				return nil, err
			}
			if previous, ok := roots[name.value()]; ok && !previous.Equal(identity) {
				return nil, errors.New("source volume changed during capture")
			}
			roots[name.value()] = identity
		}
	}
	names := slices.Sorted(maps.Keys(roots))
	result := make([]compensationVolumeRoot, 0, len(names))
	for _, name := range names {
		result = append(result, compensationVolumeRoot{Name: name, Identity: roots[name]})
	}
	return result, nil
}

// prepareCompensationBinds reconstructs only the source's admitted mount
// layout. A failed target may have replaced its _wp scaffolding or changed
// declared-volume directory ownership. Source data remains in the same attested
// volumes; image-derived writable paths are reseeded from the immutable source.
func (q *quiescedVolumes) prepareCompensationBinds(ctx context.Context, plan compensationLaunchPlan) error {
	if err := q.requireActive(); err != nil {
		return err
	}
	resources, err := resourceSnapshotMap(plan.Source.Items, plan.Source.ResourceProfiles)
	if err != nil {
		return err
	}
	for _, snapshot := range plan.Containers {
		meta, err := parseLabelMeta(snapshot.Config.Labels)
		if err != nil {
			return err
		}
		name := canonicalVolumeName(q.mutations.leaseUUID, snapshot.Config.Labels[LabelServiceName], meta.InstanceIndex)
		var stateful, writable []string
		var hostPath string
		var volume launchVolume
		for _, observed := range snapshot.Mounts {
			if observed.Type != "bind" {
				continue
			}
			var err error
			volume, err = q.lookup(name)
			if err != nil {
				return err
			}
			hostPath, err = volume.rootPath()
			if err != nil {
				return err
			}
			sanitized := sanitizeVolumePath(observed.Target)
			if sanitized == "" {
				return errors.New("source bind target cannot construct a managed path")
			}
			switch observed.Source {
			case filepath.Join(hostPath, writablePathSubdir, sanitized):
				writable = append(writable, observed.Target)
			case filepath.Join(hostPath, sanitized):
				stateful = append(stateful, observed.Target)
			default:
				return errors.New("source bind differs from the reserved managed layout")
			}
		}
		if len(stateful) != 0 {
			uid, gid, err := q.mutations.resolveImageUser(ctx, snapshot.Image, snapshot.Config.User)
			if err != nil {
				return fmt.Errorf("resolve frozen source volume user: %w", err)
			}
			if _, err := volume.prepareStatefulVolumeBinds(ctx, stateful, uid, gid); err != nil {
				return fmt.Errorf("prepare source stateful binds: %w", err)
			}
		}
		if len(writable) != 0 {
			resource, ok := resources[snapshot.Config.Labels[LabelSKU]]
			if !ok {
				return errors.New("source writable paths have no pinned resource allowance")
			}
			sizeMB, err := resource.EffectiveDiskMB()
			if err != nil {
				return err
			}
			binds := q.mutations.ops.backend.setupWritablePathBinds(volume, ctx, snapshot.Image, writable,
				sizeMB*bytesPerMiB, inodeHardLimit(sizeMB, q.mutations.ops.backend.cfg.GetMinAvgFileBytes()))
			for _, target := range writable {
				if binds[filepath.Join(hostPath, writablePathSubdir, sanitizeVolumePath(target))] != target {
					return fmt.Errorf("source writable path %q could not be restored", target)
				}
			}
		}
	}
	return nil
}

// Compose's existing parser defines legacy bind syntax. Compare effective requests
// with the captured daemon observation so an unvalidated alternate HostConfig
// representation cannot smuggle another bind past whole-plan validation.
func compensationMountProject(containers []compensationContainer) (*composetypes.Project, error) {
	project := &composetypes.Project{Services: make(composetypes.Services)}
	for _, snapshot := range containers {
		if snapshot.Host == nil {
			return nil, errors.New("source container lacks host configuration")
		}
		requests := make(map[string]ContainerMount)
		appendBind := func(source, target string, readOnly bool) error {
			if _, duplicate := requests[target]; duplicate {
				return errors.New("source bind target is duplicated")
			}
			requests[target] = ContainerMount{Type: "bind", Source: source, Target: target, ReadOnly: readOnly}
			return nil
		}
		for _, raw := range snapshot.Host.Binds {
			parsed, err := format.ParseVolume(raw)
			if err != nil {
				return nil, err
			}
			if parsed.Type == composetypes.VolumeTypeBind {
				if err := appendBind(parsed.Source, parsed.Target, parsed.ReadOnly); err != nil {
					return nil, err
				}
			}
		}
		for _, spec := range snapshot.Host.Mounts {
			if spec.Type == mount.TypeBind {
				if err := appendBind(spec.Source, spec.Target, spec.ReadOnly); err != nil {
					return nil, err
				}
			}
		}
		service := composetypes.ServiceConfig{}
		for _, observed := range snapshot.Mounts {
			if observed.Type != "bind" {
				continue
			}
			if request, ok := requests[observed.Target]; !ok || request != observed {
				return nil, fmt.Errorf("source bind %q differs from captured daemon configuration", observed.Target)
			}
			delete(requests, observed.Target)
			service.Volumes = append(service.Volumes, composetypes.ServiceVolumeConfig{Type: composetypes.VolumeTypeBind, Source: observed.Source, Target: observed.Target, ReadOnly: observed.ReadOnly})
		}
		if len(requests) != 0 {
			return nil, errors.New("source contains an unobserved bind request")
		}
		if _, duplicate := project.Services[snapshot.Name]; duplicate {
			return nil, errors.New("source container name is duplicated")
		}
		project.Services[snapshot.Name] = service
	}
	return project, nil
}
