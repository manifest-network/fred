package docker

import (
	"context"
	"errors"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/fsidentity"
)

// volumeLaunchCoordinator retains only the closed journal workflow. Backend
// never receives a callback-store writer or a way to clear launch debt by ID.
type volumeLaunchCoordinator struct {
	check          func(shared.VolumeLaunchOrigin, []fsidentity.Identity) error
	checkNamespace func(string) error
	compose        func(context.Context, *quiescedVolumes, imageexec.PreparedProject, composeUpOpts) error
	sourceFirst    func(context.Context, *quiescedVolumes, compensationContainer) (string, volumeLaunchCompletion, substratemutation.CompletedStep, error)
}

type volumeLaunchCompletion struct {
	finish func(substratemutation.CompletedStep) error
}

func (c volumeLaunchCompletion) complete(step substratemutation.CompletedStep) error {
	if c.finish == nil {
		return errors.New("volume launch completion is unavailable")
	}
	return c.finish(step)
}
