package docker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/containerd/platforms"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/fsidentity"
)

// compensationContainerRecord captures the effective daemon configuration,
// rather than rebuilding a previous deployment from mutable backend settings.
// Mounts retain the observed managed-volume paths for launch-time re-attestation;
// they are never direct mount authority.
type compensationContainerRecord struct {
	Name     string                    `json:"name"`
	ImageID  string                    `json:"image_id"`
	Platform ocispec.Platform          `json:"platform"`
	Config   *container.Config         `json:"config"`
	Host     *container.HostConfig     `json:"host"`
	Networks *network.NetworkingConfig `json:"networks"`
	Mounts   []ContainerMount          `json:"mounts"`
}

type compensationSourcePlan struct {
	Version     uint8                         `json:"version"`
	Containers  []compensationContainerRecord `json:"containers"`
	VolumeRoots []compensationVolumeRoot      `json:"volume_roots"`
}

type compensationVolumeRoot struct {
	Name     string              `json:"name"`
	Identity fsidentity.Identity `json:"identity"`
}

// compensationContainer contains one currently admitted image and a detached
// frozen execution snapshot. Only the whole-plan volume launch sink consumes it.
type compensationContainer struct {
	Name     string
	Image    imageexec.Image
	Config   *container.Config
	Host     *container.HostConfig
	Networks *network.NetworkingConfig
	Mounts   []ContainerMount
}

type compensationLaunchPlan struct {
	Subject     shared.MaintenanceCompensationSubject
	Source      shared.Release
	Containers  []compensationContainer
	VolumeRoots []compensationVolumeRoot
}

func (b *Backend) captureMaintenanceSource(ctx context.Context, subject shared.MaintenancePhysicalSubject, admit func(context.Context, string) (imageexec.Image, error)) (shared.MaintenanceSourceCapture, error) {
	source, ok := subject.SourceRelease()
	if !ok {
		return shared.MaintenanceSourceCapture{}, errors.New("source capture requires exact release authority")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return shared.MaintenanceSourceCapture{}, err
	}
	var cohort []ContainerInfo
	for _, info := range all {
		if info.LeaseUUID == subject.LeaseUUID() {
			cohort = append(cohort, info)
		}
	}
	if len(cohort) == 0 {
		return shared.UnavailableMaintenanceSource(), nil
	}
	expected, err := backend.ValidateOperationQuantities(source.Items)
	if err != nil {
		return shared.MaintenanceSourceCapture{}, err
	}
	if len(cohort) < expected {
		// Repair remains possible after a prior source instance disappears.
		// Only a positively verified exact subset can forgo replay capture;
		// inventory/read errors, foreign generations, and duplicates refuse.
		// This grants no partial source plan or compensation launch authority.
		if err := b.attestIncompleteMaintenanceSource(ctx, subject, source, cohort); err != nil {
			return shared.MaintenanceSourceCapture{}, err
		}
		return shared.UnavailableMaintenanceSource(), nil
	}
	if err := validateRecoveredReleaseCohort(&source, cohort); err != nil {
		return shared.MaintenanceSourceCapture{}, fmt.Errorf("capture complete source cohort: %w", err)
	}
	slices.SortFunc(cohort, compareContainerIdentity)
	plan := compensationSourcePlan{Version: 1}
	for _, observed := range cohort {
		info, err := b.docker.InspectContainer(ctx, observed.ContainerID)
		if err != nil {
			return shared.MaintenanceSourceCapture{}, err
		}
		if info == nil || info.ContainerID != observed.ContainerID || info.execution == nil {
			return shared.MaintenanceSourceCapture{}, errors.New("source container has no immutable execution snapshot")
		}
		// Inspection may race a source replacement, so validate the final complete
		// set too; inventory alone does not bless later unrelated configuration.
		cohort[len(plan.Containers)] = *info
		snapshot := *info.execution
		image, err := admit(ctx, snapshot.ImageID)
		if err != nil {
			return shared.MaintenanceSourceCapture{}, fmt.Errorf("admit captured source image: %w", err)
		}
		if image.ID() != snapshot.ImageID {
			return shared.MaintenanceSourceCapture{}, errors.New("source image is not independently executable")
		}
		snapshot.Platform = image.Platform()
		plan.Containers = append(plan.Containers, snapshot)
	}
	if err := validateRecoveredReleaseCohort(&source, cohort); err != nil {
		return shared.MaintenanceSourceCapture{}, err
	}
	plan.VolumeRoots, err = b.captureCompensationVolumeRoots(ctx, plan.Containers)
	if err != nil {
		return shared.MaintenanceSourceCapture{}, err
	}
	encoded, err := encodeCompensationSourcePlan(plan)
	if err != nil {
		return shared.MaintenanceSourceCapture{}, err
	}
	if err := b.validateMaintenanceSourcePlan(subject, encoded); err != nil {
		return shared.MaintenanceSourceCapture{}, err
	}
	return shared.CapturedMaintenanceSource(encoded)
}

func (b *Backend) attestIncompleteMaintenanceSource(ctx context.Context, subject shared.MaintenancePhysicalSubject, source shared.Release, cohort []ContainerInfo) error {
	validate := func(containers []ContainerInfo) error {
		seenInstances := make(map[recoveredInstanceKey]struct{}, len(containers))
		seenIDs := make(map[string]struct{}, len(containers))
		for _, info := range containers {
			if err := validateMaintenanceGenerationContainer(subject.LeaseUUID(), source.MaintenanceID, subject.Intent().Backend(), source, info); err != nil {
				return fmt.Errorf("incomplete source contains an unowned instance: %w", err)
			}
			key := recoveredInstanceKey{service: info.ServiceName, sku: info.SKU, index: info.InstanceIndex}
			if _, duplicate := seenInstances[key]; duplicate {
				return errors.New("incomplete source contains a duplicate instance")
			}
			if _, duplicate := seenIDs[info.ContainerID]; duplicate {
				return errors.New("incomplete source contains a duplicate container ID")
			}
			seenInstances[key], seenIDs[info.ContainerID] = struct{}{}, struct{}{}
		}
		return nil
	}
	if err := validate(cohort); err != nil {
		return err
	}
	inspected := make([]ContainerInfo, 0, len(cohort))
	for _, observed := range cohort {
		info, err := b.docker.InspectContainer(ctx, observed.ContainerID)
		if err != nil {
			return fmt.Errorf("inspect incomplete source: %w", err)
		}
		if info == nil || info.ContainerID != observed.ContainerID {
			return errors.New("incomplete source identity changed during inspection")
		}
		inspected = append(inspected, *info)
	}
	return validate(inspected)
}

func (b *Backend) validateMaintenanceSourcePlan(subject shared.MaintenancePhysicalSubject, encoded []byte) error {
	_, err := decodeCompensationSourcePlan(subject, encoded)
	return err
}

func decodeCompensationSourcePlan(subject shared.MaintenancePhysicalSubject, encoded []byte) (compensationSourcePlan, error) {
	plan, err := decodeCompensationSourceSnapshot(encoded)
	if err != nil {
		return plan, err
	}
	source, ok := subject.SourceRelease()
	if !ok || plan.Version != 1 || len(plan.Containers) == 0 {
		return plan, errors.New("invalid maintenance source execution plan")
	}
	expected := make(map[string]struct{})
	for _, item := range source.Items {
		for i := range item.Quantity {
			expected[fmt.Sprintf("%s/%d", item.ServiceName, i)] = struct{}{}
		}
	}
	images := make(map[string]string)
	for _, snapshot := range plan.Containers {
		if snapshot.Config == nil || snapshot.Host == nil || snapshot.Networks == nil || snapshot.Name == "" || !strings.HasPrefix(snapshot.ImageID, "sha256:") || snapshot.Platform.OS == "" || snapshot.Platform.Architecture == "" {
			return plan, errors.New("incomplete source container execution snapshot")
		}
		labels := snapshot.Config.Labels
		imageID, err := digest.Parse(snapshot.ImageID)
		if err != nil || imageID.Algorithm() != digest.SHA256 {
			return plan, errors.New("source execution snapshot has invalid immutable image identity")
		}
		meta, err := parseLabelMeta(labels)
		if err != nil {
			return plan, err
		}
		identity := fmt.Sprintf("%s/%d", labels[LabelServiceName], meta.InstanceIndex)
		if _, ok := expected[identity]; !ok {
			return plan, errors.New("source execution snapshot has unexpected or duplicate instance")
		}
		delete(expected, identity)
		authority, ok := runtimeIdentityForRelease(&source)
		if !ok || labels[LabelLeaseUUID] != subject.LeaseUUID() || labels[LabelTenant] != authority.Tenant() || labels[LabelProviderUUID] != authority.ProviderUUID() || labels[LabelBackendName] != subject.Intent().Backend() || labels[LabelCallbackURL] != authority.CallbackURL() || labels[LabelLifecycleCallbackURL] != authority.LifecycleCallbackURL() || labels[LabelMaintenanceID] != source.MaintenanceID.String() {
			return plan, errors.New("source execution snapshot has divergent runtime authority")
		}
		if err := validateMaintenanceGenerationContainer(subject.LeaseUUID(), source.MaintenanceID, subject.Intent().Backend(), source, ContainerInfo{
			ContainerID: snapshot.Name, LeaseUUID: labels[LabelLeaseUUID], BackendName: labels[LabelBackendName],
			MaintenanceID: source.MaintenanceID, Tenant: labels[LabelTenant], ProviderUUID: labels[LabelProviderUUID],
			CallbackURL: labels[LabelCallbackURL], LifecycleCallbackURL: labels[LabelLifecycleCallbackURL],
			ServiceName: labels[LabelServiceName], SKU: labels[LabelSKU], InstanceIndex: meta.InstanceIndex,
			CustomDomain: labels[LabelCustomDomain], Image: labels[LabelImageReference],
		}); err != nil {
			return plan, fmt.Errorf("source execution snapshot differs from immutable release: %w", err)
		}
		key := labels[LabelServiceName]
		imageIdentity := snapshot.ImageID + "/" + platforms.FormatAll(snapshot.Platform)
		if previous, ok := images[key]; ok && previous != imageIdentity {
			return plan, errors.New("source service contains mixed immutable image identities")
		}
		images[key] = imageIdentity
		if snapshot.Config.Image != snapshot.ImageID {
			return plan, errors.New("source image reference is not frozen")
		}
	}
	if len(expected) != 0 {
		return plan, errors.New("source execution snapshot is incomplete")
	}
	return plan, nil
}

// createCompensationContainer is the raw, construction-only source creation
// operation. The whole-plan volume sink is its sole production caller. Image
// execution remains bound to this DockerClient's admitter/creator lineage.
func (d *DockerClient) createCompensationContainer(ctx context.Context, image imageexec.Image, snapshot compensationContainer) (string, error) {
	config := cloneCompensationConfig(*snapshot.Config)
	config.Labels = maps.Clone(config.Labels)
	config.Labels[LabelCreatedAt] = time.Now().Format(time.RFC3339)
	created, err := d.creator.Create(ctx, image, &config, snapshot.Host, snapshot.Networks, snapshot.Name)
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

func (d *DockerClient) readmitCompensationImage(ctx context.Context, snapshot compensationContainerRecord) (imageexec.Image, error) {
	return d.images.ReAdmit(ctx, snapshot.ImageID, snapshot.Platform, snapshot.Config.Labels[LabelImageReference])
}

func snapshotCompensationContainer(resp container.InspectResponse, mounts []ContainerMount) *compensationContainerRecord {
	if resp.Config == nil || resp.HostConfig == nil || resp.NetworkSettings == nil {
		return nil
	}
	id := resp.Image
	if resp.ImageManifestDescriptor != nil {
		id = resp.ImageManifestDescriptor.Digest.String()
	}
	config := *resp.Config
	config.Image = id
	config.Labels = maps.Clone(config.Labels)
	if config.Labels == nil {
		config.Labels = make(map[string]string)
	}
	if config.Labels[LabelImageReference] == "" {
		config.Labels[LabelImageReference] = resp.Config.Image
	}
	config.Labels[LabelImageID] = id
	networks := make(map[string]*network.EndpointSettings, len(resp.NetworkSettings.Networks))
	for name, endpoint := range resp.NetworkSettings.Networks {
		if endpoint == nil {
			return nil
		}
		// Docker-assigned endpoint addresses/IDs are observations, not a request
		// to reserve the old endpoint. Preserve explicit IPAM and aliases only.
		networks[name] = &network.EndpointSettings{IPAMConfig: endpoint.IPAMConfig, Aliases: slices.Clone(endpoint.Aliases), DriverOpts: maps.Clone(endpoint.DriverOpts), GwPriority: endpoint.GwPriority}
	}
	return &compensationContainerRecord{Name: strings.TrimPrefix(resp.Name, "/"), ImageID: id, Config: &config, Host: resp.HostConfig, Networks: &network.NetworkingConfig{EndpointsConfig: networks}, Mounts: slices.Clone(mounts)}
}

func newCompensationStorageMutations(runner substratemutation.Runner, subject shared.MaintenanceCompensationSubject, ops storageMutationOperations) *storageMutations {
	source, ok := subject.SourceRelease()
	if !ok {
		return nil
	}
	authority, ok := runtimeIdentityForRelease(&source)
	if !ok {
		return nil
	}
	failedTarget, ok := subject.TargetRelease()
	if !ok {
		return nil
	}
	// The primary identity is the source being restored. The only additional
	// retirement identity is this exact failed target, which may have different
	// callback routes after a backend address change.
	return &storageMutations{runner: runner, ops: ops, leaseUUID: subject.Intent().LeaseUUID(), tenant: authority.Tenant(), providerUUID: authority.ProviderUUID(), callbackURL: authority.CallbackURL(), lifecycleURL: authority.LifecycleCallbackURL(), allowedLease: map[string]struct{}{subject.Intent().LeaseUUID(): {}}, predecessor: &failedTarget, maintenanceSubject: subject.FailedTarget(), compensationSubject: subject, inspectionOrigin: shared.ImageInspectionForCompensation(subject)}
}

func bindDockerMaintenanceCompensation(b *Backend, ops storageMutationOperations) error {
	return shared.BindMaintenanceCompensationExecutor(b.maintenanceSettlement, b.stopCtx, b.authorizeStorageMutation, b.completeStorageMutation,
		func(ctx context.Context, subject shared.MaintenancePhysicalSubject) (shared.MaintenanceSourceCapture, error) {
			return b.captureMaintenanceSource(ctx, subject, ops.docker.AdmitImage)
		}, b.validateMaintenanceSourcePlan,
		func(runner substratemutation.Runner, subject shared.MaintenanceCompensationSubject) maintenanceSubstrate {
			mutations := newCompensationStorageMutations(runner, subject, ops)
			return func(ctx context.Context) error { return b.doMaintenanceCompensation(ctx, mutations, subject) }
		},
		func(ctx context.Context, run maintenanceSubstrate, _ shared.MaintenanceCompensationSubject) error {
			return run(ctx)
		},
		func(ctx context.Context, subject shared.MaintenanceCompensationSubject) (shared.MaintenancePhysicalEvidence, error) {
			return b.classifyMaintenanceCompensation(ctx, subject)
		},
	)
}

func (b *Backend) doMaintenanceCompensation(ctx context.Context, mutations *storageMutations, subject shared.MaintenanceCompensationSubject) error {
	if mutations == nil || !subject.Valid() {
		return errors.New("compensation execution authority is invalid")
	}
	if !subject.SourceLaunchRequired() {
		// An earlier source request may still be outstanding. Only the bound final
		// classifier may establish readiness; never issue a duplicate create/start.
		return nil
	}
	// A prelaunch failure may leave the original source wholly intact. The
	// closed classifier proves its complete immutable image/runtime cohort;
	// repeat that observation inside the owned bracket before retaining it.
	if _, err := b.classifyMaintenanceCompensation(ctx, subject); err == nil {
		return mutations.runner.Step(ctx, "attest intact frozen maintenance source", func(ctx context.Context) error {
			_, err := b.classifyMaintenanceCompensation(ctx, subject)
			return err
		})
	}
	plan, err := decodeCompensationSourcePlan(subject.FailedTarget(), subject.Plan())
	if err != nil {
		return err
	}
	source, _ := subject.SourceRelease()
	launch := compensationLaunchPlan{Subject: subject, Source: source, VolumeRoots: plan.VolumeRoots}
	for _, snapshot := range plan.Containers {
		var admitted imageexec.Image
		err := mutations.runner.Prepare(ctx, "readmit immutable source image", func(ctx context.Context) error {
			var err error
			admitted, err = mutations.ops.docker.readmitCompensationImage(ctx, snapshot)
			return err
		})
		if err != nil {
			return fmt.Errorf("readmit immutable compensation image: %w", err)
		}
		if admitted.ID() != snapshot.ImageID || !platforms.OnlyStrict(snapshot.Platform).Match(admitted.Platform()) {
			return errors.New("compensation image content or platform changed")
		}
		launch.Containers = append(launch.Containers, compensationContainer{Name: snapshot.Name, Image: admitted, Config: snapshot.Config, Host: snapshot.Host, Networks: snapshot.Networks, Mounts: snapshot.Mounts})
	}
	if err := b.cleanupFailedMaintenanceTargets(ctx, mutations, subject.FailedTarget(), nil); err != nil {
		return err
	}

	if err := mutations.launchCompensation(ctx, launch); err != nil {
		return err
	}

	stack, err := manifest.ParsePayload(source.Manifest)
	if err != nil {
		return err
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return err
	}
	var cohort []ContainerInfo
	for _, info := range all {
		if info.LeaseUUID == subject.Intent().LeaseUUID() {
			cohort = append(cohort, info)
		}
	}
	if err := validateRecoveredReleaseCohort(&source, cohort); err != nil {
		return err
	}
	_, services := physicalProjection(cohort)
	for service, ids := range services {
		if err := b.verifyStartup(ctx, stack.Services[service], ids, physicalLogger(b, subject.Intent().LeaseUUID())); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) retireCompensationSource(ctx context.Context, mutations *storageMutations, subject shared.MaintenanceCompensationSubject) error {
	if mutations == nil || !subject.Valid() || mutations.compensationSubject != subject {
		return errors.New("source retirement requires exact compensation owner")
	}
	source, ok := subject.SourceRelease()
	if !ok {
		return errors.New("source release unavailable")
	}
	remaining, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return err
	}
	var sourceSurvivors []ContainerInfo
	for _, info := range remaining {
		if info.LeaseUUID != subject.Intent().LeaseUUID() {
			continue
		}
		if err := validateMaintenanceGenerationContainer(subject.Intent().LeaseUUID(), source.MaintenanceID, b.Name(), source, info); err != nil {
			return fmt.Errorf("source compensation found foreign remaining generation: %w", err)
		}
		sourceSurvivors = append(sourceSurvivors, info)
	}
	for _, info := range sourceSurvivors {
		if err := mutations.removeContainer(ctx, info.ContainerID); err != nil {
			return fmt.Errorf("retire exact previous source before name reuse: %w", err)
		}
	}
	return nil
}

func (b *Backend) classifyMaintenanceCompensation(ctx context.Context, subject shared.MaintenanceCompensationSubject) (shared.MaintenancePhysicalEvidence, error) {
	source, ok := subject.SourceRelease()
	if !ok {
		return shared.MaintenancePhysicalEvidence{}, errors.New("compensation classifier has no source release")
	}
	plan, err := decodeCompensationSourcePlan(subject.FailedTarget(), subject.Plan())
	if err != nil {
		return shared.MaintenancePhysicalEvidence{}, err
	}
	wanted := make(map[string]compensationContainerRecord, len(plan.Containers))
	for _, snapshot := range plan.Containers {
		wanted[snapshot.Name] = snapshot
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return shared.MaintenancePhysicalEvidence{}, err
	}
	var cohort []ContainerInfo
	for _, info := range all {
		if info.LeaseUUID != subject.Intent().LeaseUUID() {
			continue
		}
		inspected, err := b.docker.InspectContainer(ctx, info.ContainerID)
		if err != nil {
			return shared.MaintenancePhysicalEvidence{}, err
		}
		snapshot, ok := wanted[info.Name]
		if !ok || inspected == nil || inspected.execution == nil || inspected.execution.ImageID != snapshot.ImageID {
			return shared.MaintenancePhysicalEvidence{}, errors.New("source compensation image or instance differs from frozen execution")
		}
		delete(wanted, info.Name)
		cohort = append(cohort, *inspected)
	}
	if len(wanted) != 0 {
		return shared.MaintenancePhysicalEvidence{}, errors.New("source compensation cohort is incomplete")
	}
	if err := validateRecoveredReleaseCohort(&source, cohort); err != nil {
		return shared.MaintenancePhysicalEvidence{}, err
	}
	readiness, err := b.classifyRecoveredMaintenanceReadiness(ctx, source, cohort)
	if err != nil {
		return shared.MaintenancePhysicalEvidence{}, err
	}
	ids, services := physicalProjection(cohort)
	if readiness == maintenanceReadinessUnready {
		return shared.NewMaintenanceCompensationSourceFailed(subject, ids, services)
	}
	return shared.NewMaintenanceSourceReady(subject.FailedTarget(), ids, services)
}
