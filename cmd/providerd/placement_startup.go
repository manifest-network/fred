package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/tlsconfig"
	"github.com/manifest-network/fred/internal/util"
)

const backendTopologyProbeTimeout = 2 * time.Minute

// callbackHMACSecrets binds configured per-backend keys to the immutable
// storage identities established by the prepared placement authority. Callback
// ingress then selects by physical lineage rather than trusting a body-supplied
// backend name.
func callbackHMACSecrets(
	cfg *config.Config,
	resolver backend.BackendStorageIdentityResolver,
) (map[backendidentity.ID]string, error) {
	if cfg == nil {
		return nil, errors.New("provider config is required")
	}
	if util.IsNilInterface(resolver) {
		return nil, errors.New("backend storage identity resolver is required")
	}
	secrets := make(map[backendidentity.ID]string, len(cfg.Backends))
	for _, configuredBackend := range cfg.Backends {
		secret, err := cfg.ResolveBackendHMACSecret(configuredBackend.Name)
		if err != nil {
			return nil, fmt.Errorf("backend %q: resolve callback HMAC secret: %w", configuredBackend.Name, err)
		}
		storageID, bound := resolver.ExpectedBackendStorageIdentity(configuredBackend.Name)
		if !bound || !storageID.Valid() {
			return nil, fmt.Errorf(
				"backend %q has no prepared storage identity for callback authentication",
				configuredBackend.Name,
			)
		}
		if _, duplicate := secrets[storageID]; duplicate {
			return nil, fmt.Errorf(
				"backend %q reuses callback storage identity %s",
				configuredBackend.Name, storageID,
			)
		}
		secrets[storageID] = string(secret)
	}
	return secrets, nil
}

// preparePlacementBackends is the first stateful startup phase. It opens and
// verifies the already-prepared placement authority, validates every backend
// client configuration, and establishes or re-attests the exact storage
// topology before signer derivation, chain writes, subscribers, schedulers, or
// tenant ingress can start.
func preparePlacementBackends(
	ctx context.Context,
	cfg *config.Config,
) (_ *placement.Store, _ []backend.BackendEntry, resultErr error) {
	if ctx == nil {
		return nil, nil, errors.New("placement startup context is required")
	}
	if cfg == nil {
		return nil, nil, errors.New("provider config is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	store, err := placement.OpenStore(cfg.PlacementStoreDBPath, cfg.ProviderUUID)
	if err != nil {
		return nil, nil, fmt.Errorf("open prepared placement authority: %w", err)
	}
	returnStore := false
	defer func() {
		if returnStore {
			return
		}
		if closeErr := store.Close(); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close placement authority after startup failure: %w", closeErr)
		}
	}()

	backendNames := make([]string, 0, len(cfg.Backends))
	entries := make([]backend.BackendEntry, 0, len(cfg.Backends))
	for _, backendConfig := range cfg.Backends {
		backendNames = append(backendNames, backendConfig.Name)
		client, clientErr := newProductionBackendClient(backendConfig, cfg, store)
		if clientErr != nil {
			return nil, nil, clientErr
		}
		entries = append(entries, backend.BackendEntry{
			Backend: client,
			Match: backend.MatchCriteria{
				SKUs: backendConfig.SKUs,
			},
			IsDefault: backendConfig.IsDefault,
		})
	}

	requiresFleetProbe, err := store.BackendTopologyRequiresIdentityProbe(backendNames)
	if err != nil {
		return nil, nil, fmt.Errorf("validate configured backend topology: %w", err)
	}
	if requiresFleetProbe {
		// A membership change needs one complete proposed-fleet observation.
		// This is intentionally stricter than ordinary restart: adding or removing
		// authority is an offline-like transition and cannot proceed around a down
		// node or an incomplete storage identity set.
		probeClients, probeErr := placementprobe.NewClients(cfg)
		if probeErr != nil {
			return nil, nil, fmt.Errorf("construct backend topology identity probe: %w", probeErr)
		}
		probeCtx, cancel := context.WithTimeout(ctx, backendTopologyProbeTimeout)
		inventories, collectErr := placementprobe.Collect(probeCtx, probeClients)
		cancel()
		if collectErr != nil {
			return nil, nil, fmt.Errorf("collect proposed backend topology identities: %w", collectErr)
		}
		observations := make(map[string]placement.CompleteBackendObservation, len(inventories))
		for backendName, inventory := range inventories {
			observation, observationErr := placement.NewCompleteBackendObservation(
				inventory.StorageIdentity,
				inventory.Provisions,
				inventory.Retentions,
			)
			if observationErr != nil {
				return nil, nil, fmt.Errorf(
					"validate complete topology observation for backend %q: %w",
					backendName,
					observationErr,
				)
			}
			observations[backendName] = observation
		}
		if err := store.ConfigureBackendTopologyWithCompleteObservations(
			backendNames,
			observations,
		); err != nil {
			return nil, nil, fmt.Errorf("commit backend topology and storage identities: %w", err)
		}
	} else {
		if err := store.VerifyBackendTopology(backendNames); err != nil {
			return nil, nil, fmt.Errorf("verify durable backend topology: %w", err)
		}
		// An unchanged topology must tolerate an ordinary node outage. Probe all
		// endpoints concurrently, fail only on a positive storage-identity
		// contradiction, and leave transport/unhealthy endpoints degraded. Every
		// later side effect repeats the same identity check at the HTTP boundary.
		err = attestPinnedBackendIdentities(ctx, entries)
		if err != nil {
			return nil, nil, fmt.Errorf("attest pinned backend storage identities: %w", err)
		}
	}

	for index, backendConfig := range cfg.Backends {
		slog.Info("configured identity-bound backend",
			"name", backendConfig.Name,
			"url", backendConfig.URL,
			"skus", backendConfig.SKUs,
			"default", backendConfig.IsDefault,
			"topology_probe", requiresFleetProbe,
			"client", entries[index].Backend.Name(),
		)
	}
	slog.Info("placement authority verified before signer initialization",
		"db_path", cfg.PlacementStoreDBPath,
		"backends", len(entries),
	)
	returnStore = true
	return store, entries, nil
}

func newProductionBackendClient(
	backendConfig config.BackendConfig,
	cfg *config.Config,
	resolver backend.BackendStorageIdentityResolver,
) (*backend.HTTPClient, error) {
	hmacSecret, err := cfg.ResolveBackendHMACSecret(backendConfig.Name)
	if err != nil {
		return nil, fmt.Errorf("backend %q: resolve HMAC secret: %w", backendConfig.Name, err)
	}
	var tlsClientConfig *tls.Config
	if backendConfig.TLSCAFile != "" || backendConfig.TLSClientCertFile != "" ||
		backendConfig.TLSClientKeyFile != "" || backendConfig.TLSSkipVerify {
		var err error
		tlsClientConfig, err = tlsconfig.ClientConfig(
			backendConfig.TLSCAFile,
			backendConfig.TLSSkipVerify,
			backendConfig.TLSClientCertFile,
			backendConfig.TLSClientKeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf("backend %q: build TLS client config: %w", backendConfig.Name, err)
		}
	}
	client, err := backend.NewIdentityBoundHTTPClient(backend.HTTPClientConfig{
		Name:                    backendConfig.Name,
		BaseURL:                 backendConfig.URL,
		Timeout:                 backendConfig.Timeout,
		Secret:                  string(hmacSecret),
		TLSClientConfig:         tlsClientConfig,
		RequestDuration:         metrics.BackendRequestDuration,
		RequestsTotal:           metrics.BackendRequestsTotal,
		CircuitBreakerState:     metrics.BackendCircuitBreakerState,
		MalformedErrorBodyTotal: metrics.BackendMalformedErrorBodyTotal,
	}, resolver)
	if err != nil {
		return nil, fmt.Errorf("backend %q: create identity-bound client: %w", backendConfig.Name, err)
	}
	return client, nil
}

func attestPinnedBackendIdentities(
	ctx context.Context,
	entries []backend.BackendEntry,
) error {
	return attestPinnedBackendIdentitiesWithin(ctx, entries, backendTopologyProbeTimeout)
}

func attestPinnedBackendIdentitiesWithin(
	ctx context.Context,
	entries []backend.BackendEntry,
	timeout time.Duration,
) error {
	if ctx == nil {
		return errors.New("backend identity attestation context is required")
	}
	if timeout <= 0 {
		return errors.New("backend identity attestation timeout must be positive")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	group, groupCtx := errgroup.WithContext(probeCtx)
	for _, entry := range entries {
		entry := entry
		group.Go(func() error {
			if util.IsNilInterface(entry.Backend) {
				return errors.New("backend identity attestation received a nil backend")
			}
			err := entry.Backend.Health(groupCtx)
			if err == nil {
				return nil
			}
			if parentErr := ctx.Err(); parentErr != nil {
				return parentErr
			}
			if permanentBackendIdentityError(err) {
				return fmt.Errorf("backend %q contradicted its durable storage identity: %w",
					entry.Backend.Name(), err)
			}
			slog.Warn("backend unavailable during startup identity attestation; continuing degraded",
				"backend", entry.Backend.Name(),
				"error", err,
			)
			return nil
		})
	}
	waitErr := group.Wait()
	if parentErr := ctx.Err(); parentErr != nil {
		return parentErr
	}
	return waitErr
}

func permanentBackendIdentityError(err error) bool {
	if backend.IsBackendStorageIdentityMissingServerError(err) {
		return false
	}
	return errors.Is(err, backend.ErrBackendStorageIdentityUnbound) ||
		errors.Is(err, backend.ErrBackendStorageIdentityMissing) ||
		errors.Is(err, backend.ErrBackendStorageIdentityMismatch) ||
		errors.Is(err, backend.ErrBackendUpgradeRequired) ||
		errors.Is(err, backendidentity.ErrIdentityDrift)
}
