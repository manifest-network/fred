package docker

import (
	"context"
	"io"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/image"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/client"
)

// dockerSDKView retains only the SDK operations used outside the typed image
// creation boundary. Capturing method values instead of retaining a client
// interface prevents callers from recovering ContainerCreate by type assertion.
type dockerSDKView struct {
	close             func() error
	ping              func(context.Context) (types.Ping, error)
	info              func(context.Context) (system.Info, error)
	imagePull         func(context.Context, string, image.PullOptions) (io.ReadCloser, error)
	copyFromContainer func(context.Context, string, string) (io.ReadCloser, container.PathStat, error)
	containerInspect  func(context.Context, string) (container.InspectResponse, error)
	containerList     func(context.Context, container.ListOptions) ([]container.Summary, error)
	containerLogs     func(context.Context, string, container.LogsOptions) (io.ReadCloser, error)
	containerRemove   func(context.Context, string, container.RemoveOptions) error
	containerRename   func(context.Context, string, string) error
	containerStart    func(context.Context, string, container.StartOptions) error
	containerStop     func(context.Context, string, container.StopOptions) error
	networkCreate     func(context.Context, string, networktypes.CreateOptions) (networktypes.CreateResponse, error)
	networkInspect    func(context.Context, string, networktypes.InspectOptions) (networktypes.Inspect, error)
	networkList       func(context.Context, networktypes.ListOptions) ([]networktypes.Summary, error)
	networkRemove     func(context.Context, string) error
	events            func(context.Context, events.ListOptions) (<-chan events.Message, <-chan error)
}

func newDockerSDKView(cli *client.Client) dockerSDKView {
	return dockerSDKView{
		close:             cli.Close,
		ping:              cli.Ping,
		info:              cli.Info,
		imagePull:         cli.ImagePull,
		copyFromContainer: cli.CopyFromContainer,
		containerInspect:  cli.ContainerInspect,
		containerList:     cli.ContainerList,
		containerLogs:     cli.ContainerLogs,
		containerRemove:   cli.ContainerRemove,
		containerRename:   cli.ContainerRename,
		containerStart:    cli.ContainerStart,
		containerStop:     cli.ContainerStop,
		networkCreate:     cli.NetworkCreate,
		networkInspect:    cli.NetworkInspect,
		networkList:       cli.NetworkList,
		networkRemove:     cli.NetworkRemove,
		events:            cli.Events,
	}
}

func (v dockerSDKView) Close() error { return v.close() }

func (v dockerSDKView) Ping(ctx context.Context) (types.Ping, error) { return v.ping(ctx) }

func (v dockerSDKView) Info(ctx context.Context) (system.Info, error) { return v.info(ctx) }

func (v dockerSDKView) ImagePull(ctx context.Context, ref string, opts image.PullOptions) (io.ReadCloser, error) {
	return v.imagePull(ctx, ref, opts)
}

func (v dockerSDKView) CopyFromContainer(ctx context.Context, id, path string) (io.ReadCloser, container.PathStat, error) {
	return v.copyFromContainer(ctx, id, path)
}

func (v dockerSDKView) ContainerInspect(ctx context.Context, id string) (container.InspectResponse, error) {
	return v.containerInspect(ctx, id)
}

func (v dockerSDKView) ContainerList(ctx context.Context, opts container.ListOptions) ([]container.Summary, error) {
	return v.containerList(ctx, opts)
}

func (v dockerSDKView) ContainerLogs(ctx context.Context, id string, opts container.LogsOptions) (io.ReadCloser, error) {
	return v.containerLogs(ctx, id, opts)
}

func (v dockerSDKView) ContainerRemove(ctx context.Context, id string, opts container.RemoveOptions) error {
	return v.containerRemove(ctx, id, opts)
}

func (v dockerSDKView) ContainerRename(ctx context.Context, id, name string) error {
	return v.containerRename(ctx, id, name)
}

func (v dockerSDKView) ContainerStart(ctx context.Context, id string, opts container.StartOptions) error {
	return v.containerStart(ctx, id, opts)
}

func (v dockerSDKView) ContainerStop(ctx context.Context, id string, opts container.StopOptions) error {
	return v.containerStop(ctx, id, opts)
}

func (v dockerSDKView) NetworkCreate(ctx context.Context, name string, opts networktypes.CreateOptions) (networktypes.CreateResponse, error) {
	return v.networkCreate(ctx, name, opts)
}

func (v dockerSDKView) NetworkInspect(ctx context.Context, id string, opts networktypes.InspectOptions) (networktypes.Inspect, error) {
	return v.networkInspect(ctx, id, opts)
}

func (v dockerSDKView) NetworkList(ctx context.Context, opts networktypes.ListOptions) ([]networktypes.Summary, error) {
	return v.networkList(ctx, opts)
}

func (v dockerSDKView) NetworkRemove(ctx context.Context, id string) error {
	return v.networkRemove(ctx, id)
}

func (v dockerSDKView) Events(ctx context.Context, opts events.ListOptions) (<-chan events.Message, <-chan error) {
	return v.events(ctx, opts)
}

// containerFileReader is the sole SDK operation needed to read an image file
// from an inspection helper that has already crossed the creation boundary.
type containerFileReader interface {
	CopyFromContainer(context.Context, string, string) (io.ReadCloser, container.PathStat, error)
}
