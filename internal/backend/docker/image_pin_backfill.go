package docker

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// The observer is captured once with the actual recovered projection and SDK.
// Neither a mutable tag nor a caller-supplied container list can mint a pin.
type dockerImagePinObserver struct {
	backend *Backend
	docker  *DockerClient
}

func newImagePinBackfiller(b *Backend, d *DockerClient, pins *shared.ImagePinJournal) (*shared.ImagePinBackfiller, error) {
	if b == nil || d == nil || d.images == nil || d.backendName == "" || b.cfg.Name != d.backendName {
		return nil, errors.New("image pin backfill requires the exact backend Docker client")
	}
	return shared.NewImagePinBackfiller(pins, dockerImagePinObserver{backend: b, docker: d})
}

func (o dockerImagePinObserver) ObserveImagePins(ctx context.Context, subject shared.ImagePinBackfillSubject) ([]shared.ImagePinBackfillObservation, error) {
	release := subject.Release()
	identity, valid := release.RuntimeIdentity()
	if !valid || len(release.Items) == 0 {
		return nil, errors.New("active release lacks complete recovered runtime authority")
	}
	o.backend.provisionsMu.RLock()
	current := o.backend.provisions[subject.LeaseUUID()]
	if current == nil {
		o.backend.provisionsMu.RUnlock()
		return nil, errors.New("active release has no recovered physical projection")
	}
	projection := recoveredFromProvision(current)
	o.backend.provisionsMu.RUnlock()
	if projection.ActiveReleaseVersion != release.Version || projection.ActiveOperationID != identity.OperationID() ||
		projection.LeaseUUID != subject.LeaseUUID() || projection.Tenant != identity.Tenant() || projection.ProviderUUID != identity.ProviderUUID() {
		return nil, errors.New("recovered physical projection differs from the active generation")
	}
	var cohort []ContainerInfo
	byReference := make(map[string]shared.ImagePinBackfillObservation)
	seen := make(map[string]bool)
	for service, ids := range projection.ServiceContainers {
		for _, id := range ids {
			if id == "" || seen[id] {
				return nil, errors.New("recovered physical projection has an invalid container identity")
			}
			seen[id] = true
			actual, err := o.docker.client.ContainerInspect(ctx, id)
			if err != nil {
				return nil, err
			}
			if actual.ContainerJSONBase == nil || actual.ID != id || actual.Config == nil {
				return nil, errors.New("container inspection did not prove the exact recovered identity")
			}
			labels := actual.Config.Labels
			if err := validateStrictManagedContainerLabels(id, o.docker.backendName, labels); err != nil {
				return nil, err
			}
			if labels[LabelLeaseUUID] != subject.LeaseUUID() || labels[LabelServiceName] != service {
				return nil, errors.New("container inspection differs from the recovered lease/service")
			}
			ref, err := containerImageReference(actual.Config.Image, actual.Image, labels)
			if err != nil {
				return nil, err
			}
			meta, err := parseLabelMeta(labels)
			if err != nil {
				return nil, err
			}
			maintenanceID, err := parseContainerMaintenanceID(labels[LabelMaintenanceID])
			if err != nil {
				return nil, err
			}
			cohort = append(cohort, ContainerInfo{
				ContainerID: id, LeaseUUID: labels[LabelLeaseUUID], BackendName: labels[LabelBackendName],
				Tenant: labels[LabelTenant], ProviderUUID: labels[LabelProviderUUID], SKU: labels[LabelSKU],
				ServiceName: service, InstanceIndex: meta.InstanceIndex, Image: ref,
				CallbackURL: labels[LabelCallbackURL], LifecycleCallbackURL: labels[LabelLifecycleCallbackURL],
				MaintenanceID: maintenanceID, CustomDomain: labels[LabelCustomDomain],
			})
			imageID := actual.Image
			if actual.ImageManifestDescriptor != nil {
				imageID = actual.ImageManifestDescriptor.Digest.String()
			}
			if !isImmutableImageID(imageID) {
				return nil, errors.New("container image identity is not immutable")
			}
			// Discover platform only from the same exact local ID, then re-admit
			// that identity while preserving the historical reference as metadata.
			local, err := o.docker.images.Admit(ctx, imageID)
			if err != nil {
				return nil, err
			}
			admitted, err := o.docker.images.ReAdmit(ctx, imageID, local.Platform(), ref)
			if err != nil {
				return nil, err
			}
			inspection, err := o.docker.client.ImageInspect(ctx, admitted.ID())
			if err != nil {
				return nil, err
			}
			if inspection.ID != admitted.ID() {
				return nil, errors.New("recovery digest inspection changed the exact image identity")
			}
			observation := shared.ImagePinBackfillObservation{Reference: ref, ImageID: admitted.ID(), Platform: admitted.Platform(), PullDigest: imageRecoveryDigest(ref, admitted.ID(), inspection)}
			if previous, found := byReference[ref]; found && !reflect.DeepEqual(previous, observation) {
				return nil, fmt.Errorf("replicas using %q have different execution content", ref)
			}
			byReference[ref] = observation
		}
	}
	if err := validateRecoveredReleaseCohort(&release, cohort); err != nil {
		return nil, err
	}
	observations := make([]shared.ImagePinBackfillObservation, 0, len(byReference))
	for _, observation := range byReference {
		observations = append(observations, observation)
	}
	return observations, nil
}
