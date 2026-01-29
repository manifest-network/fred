package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"

	"github.com/manifest-network/fred/internal/api"
	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/scheduler"
	"github.com/manifest-network/fred/internal/watcher"
)


// safeGo runs a function in a goroutine with panic recovery.
// If the function panics, the panic is converted to an error and sent to errChan.
// The component name is included in the error for debugging.
func safeGo(wg *sync.WaitGroup, errChan chan<- error, component string, fn func() error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("%s panic: %v", component, r)
			}
		}()
		if err := fn(); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("%s error: %w", component, err)
		}
	}()
}

var (
	configFile string
	rootCmd    = &cobra.Command{
		Use:   "providerd",
		Short: "Manifest Provider Daemon",
		Long:  `A daemon that watches for lease events, auto-acknowledges them, serves tenant authentication API, and periodically withdraws funds.`,
		RunE:  run,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "path to config file")

	// Configure SDK with manifest bech32 prefixes
	config := sdk.GetConfig()
	config.SetBech32PrefixForAccount("manifest", "manifestpub")
	config.SetBech32PrefixForValidator("manifestvaloper", "manifestvaloperpub")
	config.SetBech32PrefixForConsensusNode("manifestvalcons", "manifestvalconspub")
	config.Seal()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Set up structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	slog.Info("starting providerd",
		"provider_uuid", cfg.ProviderUUID,
		"chain_id", cfg.ChainID,
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize signer
	signer, err := chain.NewSigner(chain.SignerConfig{
		KeyringBackend: cfg.KeyringBackend,
		KeyringDir:     cfg.KeyringDir,
		KeyName:        cfg.KeyName,
		ChainID:        cfg.ChainID,
		GasLimit:       cfg.GasLimit,
		GasPrice:       cfg.GasPrice,
		FeeDenom:       cfg.FeeDenom,
	})
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}
	slog.Info("signer initialized", "address", signer.Address())

	// Initialize chain client
	chainClient, err := chain.NewClient(chain.ClientConfig{
		Endpoint:       cfg.GRPCEndpoint,
		TLSEnabled:     cfg.GRPCTLSEnabled,
		TLSCAFile:      cfg.GRPCTLSCAFile,
		TLSSkipVerify:  cfg.GRPCTLSSkipVerify,
		TxPollInterval: cfg.TxPollInterval,
		TxTimeout:      cfg.TxTimeout,
		QueryPageLimit: cfg.QueryPageLimit,
	}, signer)
	if err != nil {
		return fmt.Errorf("failed to create chain client: %w", err)
	}
	defer chainClient.Close()
	slog.Info("chain client connected", "endpoint", cfg.GRPCEndpoint, "tls", cfg.GRPCTLSEnabled)

	// Initialize event subscriber
	eventSub, err := chain.NewEventSubscriber(chain.EventSubscriberConfig{
		URL:              cfg.WebSocketURL,
		ProviderUUID:     cfg.ProviderUUID,
		PingInterval:     cfg.WebSocketPingInterval,
		ReconnectInitial: cfg.WebSocketReconnectInitial,
		ReconnectMax:     cfg.WebSocketReconnectMax,
	})
	if err != nil {
		return fmt.Errorf("failed to create event subscriber: %w", err)
	}
	slog.Info("event subscriber initialized", "url", cfg.WebSocketURL)

	// Initialize backends
	slog.Info("initializing provisioner with backends", "count", len(cfg.Backends))

	var backendEntries []backend.BackendEntry
	for _, bcfg := range cfg.Backends {
		client := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    bcfg.Name,
			BaseURL: bcfg.URL,
			Timeout: bcfg.Timeout,
		})

		backendEntries = append(backendEntries, backend.BackendEntry{
			Backend: client,
			Match: backend.MatchCriteria{
				SKUPrefix: bcfg.SKUPrefix,
			},
			IsDefault: bcfg.IsDefault,
		})

		slog.Info("configured backend",
			"name", bcfg.Name,
			"url", bcfg.URL,
			"sku_prefix", bcfg.SKUPrefix,
			"default", bcfg.IsDefault,
		)
	}

	// Create backend router
	backendRouter, err := backend.NewRouter(backend.RouterConfig{
		Backends: backendEntries,
	})
	if err != nil {
		return fmt.Errorf("failed to create backend router: %w", err)
	}

	// Create payload store if database path is configured
	var payloadStore *provisioner.PayloadStore
	if cfg.PayloadStoreDBPath != "" {
		payloadStore, err = provisioner.NewPayloadStore(provisioner.PayloadStoreConfig{
			DBPath:          cfg.PayloadStoreDBPath,
			TTL:             cfg.PayloadStoreTTL,
			CleanupInterval: cfg.PayloadStoreCleanupFreq,
		})
		if err != nil {
			return fmt.Errorf("failed to create payload store: %w", err)
		}
	} else {
		slog.Warn("payload store disabled (no payload_store_db_path configured)")
	}

	// Create provision manager
	provisionMgr, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:    cfg.ProviderUUID,
		CallbackBaseURL: cfg.CallbackBaseURL,
		PayloadStore:    payloadStore,
	}, backendRouter, chainClient)
	if err != nil {
		return fmt.Errorf("failed to create provision manager: %w", err)
	}

	// Create event bridge to forward chain events to Watermill
	eventBridge := provisioner.NewEventBridge(eventSub, provisionMgr)

	slog.Info("provisioner initialized",
		"backends", len(cfg.Backends),
		"callback_url", cfg.CallbackBaseURL,
	)

	// Initialize watcher for cross-provider events only
	leaseWatcher := watcher.New(chainClient, eventSub, cfg.ProviderUUID)

	// Initialize API server
	apiServer, err := api.NewServer(api.ServerConfig{
		Addr:                 cfg.APIListenAddr,
		ProviderUUID:         cfg.ProviderUUID,
		Bech32Prefix:         cfg.Bech32Prefix,
		TLSCertFile:          cfg.TLSCertFile,
		TLSKeyFile:           cfg.TLSKeyFile,
		RateLimitRPS:         cfg.RateLimitRPS,
		RateLimitBurst:       cfg.RateLimitBurst,
		TenantRateLimitRPS:   cfg.TenantRateLimitRPS,
		TenantRateLimitBurst: cfg.TenantRateLimitBurst,
		TrustedProxies:       cfg.TrustedProxies,
		ReadTimeout:          cfg.HTTPReadTimeout,
		WriteTimeout:         cfg.HTTPWriteTimeout,
		IdleTimeout:          cfg.HTTPIdleTimeout,
		ShutdownTimeout:      cfg.ShutdownTimeout,
		MaxRequestBodySize:   cfg.MaxRequestBodySize,
		CallbackSecret:       cfg.CallbackSecret,
		TokenTrackerDBPath:   cfg.TokenTrackerDBPath,
	}, chainClient, backendRouter, provisionMgr, provisionMgr, provisionMgr)
	if err != nil {
		return fmt.Errorf("failed to create API server: %w", err)
	}

	// Initialize withdrawal scheduler
	withdrawScheduler := scheduler.NewWithdrawScheduler(chainClient, scheduler.WithdrawSchedulerConfig{
		ProviderUUID:              cfg.ProviderUUID,
		Interval:                  cfg.WithdrawInterval,
		MaxWithdrawIterations:     cfg.MaxWithdrawIterations,
		CreditCheckErrorThreshold: cfg.CreditCheckErrorThreshold,
		CreditCheckRetryInterval:  cfg.CreditCheckRetryInterval,
	})

	// Create reconciler for level-triggered state reconciliation
	reconciler, err := provisioner.NewReconciler(provisioner.ReconcilerConfig{
		ProviderUUID:    cfg.ProviderUUID,
		CallbackBaseURL: cfg.CallbackBaseURL,
		Interval:        cfg.ReconciliationInterval,
	}, chainClient, backendRouter, provisionMgr)
	if err != nil {
		return fmt.Errorf("failed to create reconciler: %w", err)
	}

	// Wire up cross-provider credit depletion detection
	// When another provider's withdrawal depletes a tenant's credit, trigger our withdrawal
	// to auto-close our leases for that tenant
	leaseWatcher.SetWithdrawTrigger(withdrawScheduler.TriggerWithdraw)

	// Perform startup operations sequentially to avoid same-block transaction conflicts
	// WithdrawOnce waits for block inclusion before returning, ensuring the next tx is in a different block
	slog.Info("performing initial withdrawal")
	withdrawScheduler.WithdrawOnce(ctx)

	// Run startup reconciliation to recover from any crash
	// This compares chain state vs backend state and fixes inconsistencies
	slog.Info("performing startup reconciliation")
	if err := reconciler.RunOnce(ctx); err != nil {
		slog.Error("startup reconciliation failed", "error", err)
		// Continue anyway - periodic reconciliation will retry
	}

	// Start background components with WaitGroup for graceful shutdown.
	// Each component is wrapped with panic recovery via safeGo() to prevent
	// silent crashes and convert panics to errors.
	var wg sync.WaitGroup
	errChan := make(chan error, 7)

	// Start event subscriber (single reader, multiple consumers via Subscribe())
	safeGo(&wg, errChan, "event subscriber", func() error {
		return eventSub.Start(ctx)
	})

	// Start provisioner
	safeGo(&wg, errChan, "provision manager", func() error {
		return provisionMgr.Start(ctx)
	})

	// Start event bridge (subscribes to eventSub, forwards to Watermill)
	safeGo(&wg, errChan, "event bridge", func() error {
		return eventBridge.Start(ctx)
	})

	// Start watcher for cross-provider events (subscribes to eventSub)
	safeGo(&wg, errChan, "watcher", func() error {
		return leaseWatcher.Start(ctx)
	})

	// Start API server
	safeGo(&wg, errChan, "api server", func() error {
		return apiServer.Start(ctx)
	})

	// Start withdrawal scheduler
	safeGo(&wg, errChan, "scheduler", func() error {
		return withdrawScheduler.Start(ctx)
	})

	// Start periodic reconciliation
	safeGo(&wg, errChan, "reconciler", func() error {
		return reconciler.Start(ctx)
	})

	slog.Info("providerd started successfully",
		"api_addr", cfg.APIListenAddr,
		"withdraw_interval", cfg.WithdrawInterval,
		"reconciliation_interval", cfg.ReconciliationInterval,
		"rate_limit_rps", cfg.RateLimitRPS,
		"rate_limit_burst", cfg.RateLimitBurst,
		"tenant_rate_limit_rps", cfg.TenantRateLimitRPS,
		"tenant_rate_limit_burst", cfg.TenantRateLimitBurst,
		"backends", len(cfg.Backends),
	)

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		slog.Info("received shutdown signal", "signal", sig)
	case err := <-errChan:
		slog.Error("component error", "error", err)
	}

	// Graceful shutdown
	slog.Info("shutting down...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// Wait for in-flight provisions to drain BEFORE shutting down the API server.
	// Backends send completion callbacks via HTTP, so the API server must remain
	// running to receive them during the drain period.
	drainTimeout := cfg.ShutdownTimeout / 2
	remaining := provisionMgr.WaitForDrain(shutdownCtx, drainTimeout)
	if remaining > 0 {
		slog.Warn("proceeding with shutdown despite pending provisions",
			"remaining", remaining,
			"note", "these will be recovered by reconciliation on restart",
		)
	}

	// Shutdown API server (stops accepting new requests, drains active connections)
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shutdown API server gracefully", "error", err)
	}

	// Signal all components to stop via context cancellation.
	// This triggers ctx.Done() in all component loops.
	cancel()

	// Stop withdrawal scheduler and wait for any in-flight withdrawal to complete.
	// This ensures we don't interrupt a withdrawal transaction mid-flight.
	withdrawScheduler.Stop()

	// Close event subscriber to unblock any components waiting on events.
	// Components check ctx.Done() first, so they'll exit cleanly.
	eventSub.Close()

	// Wait for all goroutines to finish with timeout.
	// This includes provisionMgr.Start() which runs the Watermill router.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timedOut := false
	select {
	case <-done:
		slog.Info("all components stopped gracefully")
	case <-shutdownCtx.Done():
		timedOut = true
		slog.Warn("shutdown timed out, some components may not have stopped cleanly")
	}

	// Always close provision manager to clean up Watermill router and payload store.
	// This is safe even if components are still running - Watermill handles concurrent Close().
	// We must close this regardless of timeout to prevent resource leaks.
	if err := provisionMgr.Close(); err != nil {
		slog.Error("failed to close provision manager", "error", err)
	}

	// If we timed out, give components a brief additional grace period.
	// This helps prevent goroutine leaks in edge cases.
	if timedOut {
		gracePeriod := 2 * time.Second
		slog.Info("waiting additional grace period for lingering components", "duration", gracePeriod)
		select {
		case <-done:
			slog.Info("all components stopped after grace period")
		case <-time.After(gracePeriod):
			slog.Warn("some components did not stop within grace period")
		}
	}

	slog.Info("providerd stopped")
	return nil
}
