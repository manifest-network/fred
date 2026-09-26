package imageexec

import composeapi "github.com/docker/compose/v5/pkg/api"

// composeBuildLabels is the replacement authority for the three exact labels
// Compose writes into built images. Image-supplied values are never retained.
// The direct projection neutralizes them; a prepared project owns their values.
type composeBuildLabels struct {
	project string
	service string
	version string
}

func (composeBuildLabels) owns(key string) bool {
	switch key {
	case composeapi.ProjectLabel, composeapi.ServiceLabel, composeapi.VersionLabel:
		return true
	default:
		return false
	}
}

func (labels composeBuildLabels) forProject(project, service string) composeBuildLabels {
	labels.project = project
	labels.service = service
	labels.version = composeapi.ComposeVersion
	return labels
}

func (labels composeBuildLabels) apply(target map[string]string) {
	target[composeapi.ProjectLabel] = labels.project
	target[composeapi.ServiceLabel] = labels.service
	target[composeapi.VersionLabel] = labels.version
}

// DirectCreationLabels returns the detached neutral projection installed by
// DockerCreator. Helper receipt construction and recovery use this same policy.
// Empty values must be written explicitly: omission would inherit image labels.
func DirectCreationLabels() map[string]string {
	labels := make(map[string]string, 3)
	(composeBuildLabels{}).apply(labels)
	return labels
}
