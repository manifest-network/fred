package imageexec

import composeapi "github.com/docker/compose/v5/pkg/api"

// composeBuildLabels is the replacement authority for the exact labels Compose
// writes into images it builds: project, service and version on every build,
// and the builder stamp its classic (non-BuildKit) builder adds. Image-supplied
// values are never retained. The direct projection neutralizes them; a prepared
// project owns the first three and always clears the builder stamp.
type composeBuildLabels struct {
	project string
	service string
	version string
}

// ownedComposeBuildLabel pairs one owned Compose build key with its
// replacement policy: the value apply writes in place of whatever the image or
// caller supplied.
type ownedComposeBuildLabel struct {
	key     string
	replace func(composeBuildLabels) string
}

// ownedComposeBuildLabels is the single authority for which reserved keys
// image admission tolerates and what every creation path writes for them.
// owns and apply both derive from it, so a key cannot be admitted without
// being overwritten, or overwritten without being admitted.
var ownedComposeBuildLabels = [...]ownedComposeBuildLabel{
	{key: composeapi.ProjectLabel, replace: func(l composeBuildLabels) string { return l.project }},
	{key: composeapi.ServiceLabel, replace: func(l composeBuildLabels) string { return l.service }},
	{key: composeapi.VersionLabel, replace: func(l composeBuildLabels) string { return l.version }},
	// Nothing reads the builder stamp. It is always written empty: Docker
	// copies an image label into any container key the config leaves out.
	{key: composeapi.ImageBuilderLabel, replace: func(composeBuildLabels) string { return "" }},
}

// owns is an exact, case-sensitive match. Every other reserved key, including
// case or Unicode variants of these, stays refused by image admission.
func (composeBuildLabels) owns(key string) bool {
	for _, owned := range ownedComposeBuildLabels {
		if owned.key == key {
			return true
		}
	}
	return false
}

func (labels composeBuildLabels) forProject(project, service string) composeBuildLabels {
	labels.project = project
	labels.service = service
	labels.version = composeapi.ComposeVersion
	return labels
}

// apply writes every owned key explicitly, empty values included.
func (labels composeBuildLabels) apply(target map[string]string) {
	for _, owned := range ownedComposeBuildLabels {
		target[owned.key] = owned.replace(labels)
	}
}

// DirectCreationLabels returns the detached neutral projection installed by
// DockerCreator. Helper receipt construction and recovery use this same policy.
// Empty values must be written explicitly: omission would inherit image labels.
func DirectCreationLabels() map[string]string {
	labels := make(map[string]string, len(ownedComposeBuildLabels))
	(composeBuildLabels{}).apply(labels)
	return labels
}
