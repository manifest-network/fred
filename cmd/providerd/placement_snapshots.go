package main

import (
	"errors"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/placementsnapshot"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// newPlacementSnapshots binds the configured snapshot directory and builds the
// snapshot loop. It returns a nil service when snapshots are disabled. The
// returned close releases the directory; call it only after the loop has been
// joined.
func newPlacementSnapshots(
	cfg *config.Config,
	placements *placement.Store,
	payloads *payload.Store,
) (*placementsnapshot.Service, func() error, error) {
	noClose := func() error { return nil }
	if cfg.PlacementSnapshotDir == "" {
		return nil, noClose, nil
	}
	settings, err := placementsnapshot.NewSettings(cfg.PlacementSnapshotInterval, cfg.PlacementSnapshotRetain)
	if err != nil {
		return nil, noClose, err
	}
	directory, err := placementsnapshot.OpenDirectory(
		cfg.PlacementSnapshotDir,
		cfg.ProviderUUID,
		placementsnapshot.LiveDatabases{
			Placements: cfg.PlacementStoreDBPath,
			Payloads:   cfg.PayloadStoreDBPath,
		},
	)
	if err != nil {
		return nil, noClose, err
	}
	service, err := placementsnapshot.NewService(placements, payloads, directory, settings)
	if err != nil {
		return nil, noClose, errors.Join(err, directory.Close())
	}
	return service, directory.Close, nil
}
