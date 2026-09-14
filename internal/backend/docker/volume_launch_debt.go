package docker

import (
	"context"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/fsidentity"
)

// volumeLaunchCoordinator retains only the closed journal workflow. Backend
// never receives a callback-store writer or a way to clear launch debt by ID.
type volumeLaunchCoordinator struct {
	check          func(shared.VolumeLaunchOrigin, []fsidentity.Identity) error
	checkNamespace func(string) error
	pendingCount   func() (int, error)
	compose        func(context.Context, *quiescedVolumes, imageexec.PreparedProject, composeUpOpts) error
	source         func(context.Context, *quiescedVolumes, compensationStartup) error
}
