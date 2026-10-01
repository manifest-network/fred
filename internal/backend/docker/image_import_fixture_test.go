package docker

import (
	"context"
	"testing"
	"time"
)

// pullImageForTest prepares explicit integration fixtures. Production clients
// have no standalone import method: workloads enter the durable capacity owner.
func pullImageForTest(t *testing.T, docker *DockerClient, ctx context.Context, reference string, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stage := t.TempDir()
	cfg := DefaultConfig()
	loader, err := docker.newImageLoader(stage, cfg.ImageMaxSizeMB*imageMiB)
	if err != nil {
		return err
	}
	info, err := docker.client.Info(ctx)
	if err != nil {
		return err
	}
	if err := requireBoundedImageStore(info); err != nil {
		return err
	}
	fs := localFilesystemCapacity{}
	paths := []string{stage, info.DockerRootDir}
	if err := requireImageImportSpace(fs, paths, cfg.ImageMaxSizeMB*imageMiB, cfg.ImageDiskMinFreeMB*imageMiB); err != nil {
		return err
	}
	prepared, err := loader.Prepare(ctx, reference, daemonImagePlatform(info))
	if err != nil {
		return err
	}
	defer func() { _ = prepared.Close() }()
	if err := requireImageImportSpace(fs, paths, prepared.ImportBytes(), cfg.ImageDiskMinFreeMB*imageMiB); err != nil {
		return err
	}
	_, err = loader.Import(ctx, prepared)
	return err
}
