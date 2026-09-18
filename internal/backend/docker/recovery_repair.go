package docker

import (
	"context"
	"errors"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// openDockerRepairProbe verifies existing daemon/volume storage without opening
// the runtime's stores, creating actors, or starting recovery workers.
func openDockerRepairProbe(ctx context.Context, cfg Config, logger *slog.Logger) (*Backend, func() error, error) {
	if ctx == nil || logger == nil {
		return nil, nil, errors.New("docker repair requires context and logger")
	}
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}
	client, err := NewDockerClient(ctx, cfg.DockerHost, cfg.Name)
	if err != nil {
		return nil, nil, err
	}
	volumes, err := newVolumeManager(cfg.VolumeDataPath, cfg.VolumeFilesystem, cfg.GetMinAvgFileBytes(), logger)
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	probe := &Backend{cfg: cfg, docker: projectDockerRead(client), volumes: projectVolumeRead(volumes)}
	if err := probe.loadStorageIdentity(ctx); err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return probe, client.Close, nil
}

// InspectUnsettledDockerEffectsForConfig reports the exact stopped-journal
// snapshot and operator fencing statement needed for exceptional recovery.
func InspectUnsettledDockerEffectsForConfig(ctx context.Context, cfg Config, logger *slog.Logger) (inspection shared.DockerRecoveryInspection, err error) {
	probe, closeProbe, err := openDockerRepairProbe(ctx, cfg, logger)
	if err != nil {
		return inspection, err
	}
	defer func() { err = errors.Join(err, closeProbe()) }()
	inspection, err = shared.InspectDockerRecovery(ctx, cfg.CallbackDBPath, probe.storageAuthority)
	return inspection, errors.Join(err, probe.verifyStorageSubstrate(ctx))
}

// RepairUnsettledDockerEffectsForConfig is an offline operator workflow. It
// does not stop Docker or infer a fence from inventory. Its exact acknowledgement
// attests that old clients and daemon/runtime requests have already been fenced.
func RepairUnsettledDockerEffectsForConfig(ctx context.Context, cfg Config, logger *slog.Logger, acknowledgement, backupPath string) (result shared.DockerRecoveryRepairResult, err error) {
	probe, closeProbe, err := openDockerRepairProbe(ctx, cfg, logger)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, closeProbe()) }()
	return shared.RepairDockerRecovery(ctx, cfg.CallbackDBPath, probe.storageAuthority, acknowledgement, backupPath,
		func(ctx context.Context) error { return probe.verifyStorageSubstrate(ctx) })
}
