package main

import (
	"context"
	"errors"
	"sync"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/placementsnapshot"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// joinWithin waits for group until ctx ends, and reports whether it joined.
func joinWithin(ctx context.Context, group *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// newPlacementSnapshots binds the configured snapshot directory and builds the
// snapshot loop. It returns a nil service when snapshots are disabled. The
// returned close releases the directory; a loop still running at that point
// fails its next directory operation.
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
