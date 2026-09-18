package docker

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/compose-spec/compose-go/v2/format"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	maxLaunchMounts         = 16_384
	maxLaunchMountPathBytes = 32 << 20
)

// launchMountBudget counts the unexpanded plan. No helper or per-instance
// allocation is needed to decide whether all requested services fit together.
type launchMountBudget struct{ mounts, pathBytes int }

func (b *launchMountBudget) reserve(targets []string, additionalMounts, additionalPathBytes, quantity int) error {
	count, bytes := len(targets)+additionalMounts, additionalPathBytes
	for _, target := range targets {
		if len(target) > imageexec.MaxImageVolumePathBytes || len(target) > maxLaunchMountPathBytes-bytes {
			return errors.New("launch mount path byte budget exceeded")
		}
		bytes += len(target)
	}
	if quantity <= 0 || quantity > backend.MaxOperationQuantity ||
		(count != 0 && quantity > (maxLaunchMounts-b.mounts)/count) ||
		(bytes != 0 && quantity > (maxLaunchMountPathBytes-b.pathBytes)/bytes) {
		return errors.New("launch mount count or aggregate path byte budget exceeded")
	}
	b.mounts += count * quantity
	b.pathBytes += bytes * quantity
	return nil
}

// Compensation replays the captured HostConfig, so it must budget that exact
// layout instead of estimating it from today's manifest or security defaults.
// The source-plan decoder invokes this before the target can be mutated;
// recovered plans pass through the same constructor before helper execution.
func admitCompensationMountBudget(plan compensationSourcePlan) error {
	if len(plan.Containers) == 0 || len(plan.Containers) > backend.MaxOperationQuantity {
		return errors.New("source launch exceeds its instance budget")
	}
	var budget launchMountBudget
	for _, snapshot := range plan.Containers {
		if snapshot.Host == nil {
			return errors.New("source launch has no frozen mount configuration")
		}
		// Count before allocating or parsing legacy bind strings. Account for
		// all three Docker representations, including non-bind mounts.
		count := len(snapshot.Host.Binds) + len(snapshot.Host.Mounts) + len(snapshot.Host.Tmpfs)
		if count > maxLaunchMounts-budget.mounts {
			return errors.New("source launch mount count budget exceeded")
		}
		targets := make([]string, 0, count)
		for _, bind := range snapshot.Host.Binds {
			parsed, err := format.ParseVolume(bind)
			if err != nil {
				return fmt.Errorf("source launch bind: %w", err)
			}
			targets = append(targets, parsed.Target)
		}
		for _, mount := range snapshot.Host.Mounts {
			targets = append(targets, mount.Target)
		}
		for target := range snapshot.Host.Tmpfs {
			targets = append(targets, target)
		}
		if err := budget.reserve(targets, 0, 0, 1); err != nil {
			return fmt.Errorf("source launch: %w", err)
		}
	}
	return nil
}

// admittedImageSetup cannot be minted until the entire lease's unexpanded
// image/mount layout fits its quantity budget. Only this type enters helper
// discovery; an individually admitted Image is not sufficient for that phase.
type admittedImageSetup struct {
	image imageexec.Image
	user  string
}

type admittedLaunchImages struct{ services map[string]admittedImageSetup }

func (b *Backend) admitLaunchImages(ctx context.Context, mutations *storageMutations, stack *manifest.StackManifest, items []backend.LeaseItem) (admittedLaunchImages, error) {
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return admittedLaunchImages{}, err
	}
	if stack == nil || len(stack.Services) == 0 || len(stack.Services) > len(items) {
		return admittedLaunchImages{}, errors.New("image preparation requires the complete lease service set")
	}
	images := make(map[string]admittedImageSetup, len(stack.Services))
	var budget launchMountBudget
	for _, item := range items {
		spec := stack.Services[item.ServiceName]
		if spec == nil {
			return admittedLaunchImages{}, errors.New("image preparation service is absent from the lease")
		}
		admitted, err := mutations.admitImage(ctx, spec.Image)
		if err != nil {
			return admittedLaunchImages{}, fmt.Errorf("image admission for service %q: %w", item.ServiceName, err)
		}
		targets := admitted.Volumes()
		var tmpfsTargets []string
		if b.cfg.IsReadonlyRootfs() {
			tmpfsTargets = spec.Tmpfs
		}
		for _, tmpfs := range tmpfsTargets {
			clean := path.Clean(tmpfs)
			for _, target := range targets {
				if clean == target || strings.HasPrefix(clean, target+"/") || strings.HasPrefix(target, clean+"/") {
					return admittedLaunchImages{}, fmt.Errorf("service %q has overlapping image/tmpfs mount targets %q and %q", item.ServiceName, target, tmpfs)
				}
			}
			targets = append(targets, clean)
		}
		additional, additionalBytes := 0, 0
		if b.cfg.IsReadonlyRootfs() {
			// Include the two fixed tmpfs mounts and the worst case for bounded
			// writable-path discovery before creating an inspection helper.
			additional = 2 + maxDetectedWritablePaths
			additionalBytes = len("/tmp") + len("/run") + maxDetectedWritablePaths*imageexec.MaxImageVolumePathBytes
		}
		if err := budget.reserve(targets, additional, additionalBytes, item.Quantity); err != nil {
			return admittedLaunchImages{}, fmt.Errorf("service %q: %w", item.ServiceName, err)
		}
		images[item.ServiceName] = admittedImageSetup{image: admitted, user: spec.User}
	}
	return admittedLaunchImages{services: images}, nil
}

func (b *Backend) inspectImagesForSetup(mutations *storageMutations, ctx context.Context, stack *manifest.StackManifest, items []backend.LeaseItem) (map[string]*imageSetup, error) {
	admitted, err := b.admitLaunchImages(ctx, mutations, stack, items)
	if err != nil {
		return nil, err
	}
	setups := make(map[string]*imageSetup, len(admitted.services))
	for service, image := range admitted.services {
		setup, err := b.setupAdmittedImage(mutations, ctx, image)
		if err != nil {
			return nil, fmt.Errorf("image setup for service %q: %w", service, err)
		}
		setups[service] = setup
	}
	return setups, nil
}
