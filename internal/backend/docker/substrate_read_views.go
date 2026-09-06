package docker

import (
	"context"

	networktypes "github.com/docker/docker/api/types/network"
)

// dockerReadView is a closure projection, not an interface narrowing cast. Its
// dynamic type has no Docker mutation methods, so b.docker cannot be asserted
// back to dockerMutationSink.
type dockerReadView struct {
	ping                 func(context.Context) error
	daemonInfo           func(context.Context) (DaemonSecurityInfo, error)
	close                func() error
	inspectImage         func(context.Context, string) (*ImageInfo, error)
	inspectContainer     func(context.Context, string) (*ContainerInfo, error)
	containerLogs        func(context.Context, string, int) (string, error)
	listContainers       func(context.Context) ([]ContainerInfo, error)
	listContainersStrict func(context.Context) ([]ContainerInfo, error)
	listNetworks         func(context.Context) ([]networktypes.Inspect, error)
	containerEvents      func(context.Context) (<-chan ContainerEvent, <-chan error)
}

func projectDockerRead(client dockerReadClient) dockerReadClient {
	return dockerReadView{
		ping: client.Ping, daemonInfo: client.DaemonInfo, close: client.Close,
		inspectImage: client.InspectImage, inspectContainer: client.InspectContainer,
		containerLogs: client.ContainerLogs, listContainers: client.ListManagedContainers,
		listContainersStrict: client.ListManagedContainersStrict,
		listNetworks:         client.ListManagedNetworks, containerEvents: client.ContainerEvents,
	}
}

func (v dockerReadView) Ping(ctx context.Context) error { return v.ping(ctx) }
func (v dockerReadView) DaemonInfo(ctx context.Context) (DaemonSecurityInfo, error) {
	return v.daemonInfo(ctx)
}
func (v dockerReadView) Close() error { return v.close() }
func (v dockerReadView) InspectImage(ctx context.Context, image string) (*ImageInfo, error) {
	return v.inspectImage(ctx, image)
}
func (v dockerReadView) InspectContainer(ctx context.Context, id string) (*ContainerInfo, error) {
	return v.inspectContainer(ctx, id)
}
func (v dockerReadView) ContainerLogs(ctx context.Context, id string, tail int) (string, error) {
	return v.containerLogs(ctx, id, tail)
}
func (v dockerReadView) ListManagedContainers(ctx context.Context) ([]ContainerInfo, error) {
	return v.listContainers(ctx)
}
func (v dockerReadView) ListManagedContainersStrict(ctx context.Context) ([]ContainerInfo, error) {
	return v.listContainersStrict(ctx)
}
func (v dockerReadView) ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error) {
	return v.listNetworks(ctx)
}
func (v dockerReadView) ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
	return v.containerEvents(ctx)
}

type composeReadView struct {
	ps func(context.Context, string) ([]composeContainerSummary, error)
}

func projectComposeRead(compose composeReader) composeReader {
	return composeReadView{ps: compose.PS}
}

func (v composeReadView) PS(ctx context.Context, project string) ([]composeContainerSummary, error) {
	return v.ps(ctx, project)
}

type volumeReadView struct {
	list                 func() ([]string, error)
	listForProof         func(context.Context) ([]string, error)
	attest               func(context.Context, managedVolumeName) error
	requireNoInterrupted func(context.Context) error
	validate             func() error
	hostPath             func(string) string
	usage                func(context.Context, string) (int64, error)
	kind                 func() string
}

type pinnedVolumeReadView struct {
	volumeReadView
	pin    func() error
	verify func() error
}

func projectVolumeRead(volumes volumeReader) volumeReader {
	view := volumeReadView{
		list: volumes.List, listForProof: volumes.ListForProof,
		attest:               volumes.AttestManagedVolume,
		requireNoInterrupted: volumes.RequireNoInterruptedVolumeMutations,
		validate:             volumes.Validate, hostPath: volumes.HostPath,
		usage: volumes.Usage, kind: volumes.Kind,
	}
	if pinner, ok := volumes.(identityRootPinner); ok {
		return pinnedVolumeReadView{volumeReadView: view, pin: pinner.PinIdentityRoot, verify: pinner.VerifyIdentityRoot}
	}
	return view
}

func (v volumeReadView) List() ([]string, error) { return v.list() }
func (v volumeReadView) ListForProof(ctx context.Context) ([]string, error) {
	return v.listForProof(ctx)
}
func (v volumeReadView) AttestManagedVolume(ctx context.Context, name managedVolumeName) error {
	return v.attest(ctx, name)
}
func (v volumeReadView) RequireNoInterruptedVolumeMutations(ctx context.Context) error {
	return v.requireNoInterrupted(ctx)
}
func (v volumeReadView) Validate() error                                     { return v.validate() }
func (v volumeReadView) HostPath(name string) string                         { return v.hostPath(name) }
func (v volumeReadView) Usage(ctx context.Context, id string) (int64, error) { return v.usage(ctx, id) }
func (v volumeReadView) Kind() string                                        { return v.kind() }
func (v pinnedVolumeReadView) PinIdentityRoot() error                        { return v.pin() }
func (v pinnedVolumeReadView) VerifyIdentityRoot() error                     { return v.verify() }
