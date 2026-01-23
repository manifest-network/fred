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

const (
	// shutdownTimeout is the maximum time to wait for graceful shutdown
	shutdownTimeout = 30 * time.Second
)

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

	// Create provision manager
	provisionMgr, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:    cfg.ProviderUUID,
		CallbackBaseURL: cfg.CallbackBaseURL,
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
	apiServer := api.NewServer(api.ServerConfig{
		Addr:               cfg.APIListenAddr,
		ProviderUUID:       cfg.ProviderUUID,
		Bech32Prefix:       cfg.Bech32Prefix,
		TLSCertFile:        cfg.TLSCertFile,
		TLSKeyFile:         cfg.TLSKeyFile,
		RateLimitRPS:       cfg.RateLimitRPS,
		RateLimitBurst:     cfg.RateLimitBurst,
		ReadTimeout:        cfg.HTTPReadTimeout,
		WriteTimeout:       cfg.HTTPWriteTimeout,
		IdleTimeout:        cfg.HTTPIdleTimeout,
		MaxRequestBodySize: cfg.MaxRequestBodySize,
	}, chainClient, backendRouter, provisionMgr)

	// Initialize withdrawal scheduler
	withdrawScheduler := scheduler.NewWithdrawScheduler(chainClient, scheduler.WithdrawSchedulerConfig{
		ProviderUUID:              cfg.ProviderUUID,
		Interval:                  cfg.WithdrawInterval,
		MaxWithdrawIterations:     cfg.MaxWithdrawIterations,
		CreditCheckErrorThreshold: cfg.CreditCheckErrorThreshold,
		CreditCheckRetryInterval:  cfg.CreditCheckRetryInterval,
	})

	// Wire up cross-provider credit depletion detection
	// When another provider's withdrawal depletes a tenant's credit, trigger our withdrawal
	// to auto-close our leases for that tenant
	leaseWatcher.SetWithdrawTrigger(withdrawScheduler.TriggerWithdraw)

	// Perform startup operations sequentially to avoid same-block transaction conflicts
	// WithdrawOnce waits for block inclusion before returning, ensuring the next tx is in a different block
	slog.Info("performing initial withdrawal")
	withdrawScheduler.WithdrawOnce(ctx)

	// Start background components with WaitGroup for graceful shutdown
	var wg sync.WaitGroup
	errChan := make(chan error, 6)

	// Start event subscriber (single reader, multiple consumers via Subscribe())
	wg.Go(func() {
		if err := eventSub.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("event subscriber error: %w", err)
		}
	})

	// Start provisioner
	wg.Go(func() {
		if err := provisionMgr.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("provision manager error: %w", err)
		}
	})

	// Start event bridge (subscribes to eventSub, forwards to Watermill)
	wg.Go(func() {
		if err := eventBridge.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("event bridge error: %w", err)
		}
	})

	// Start watcher for cross-provider events (subscribes to eventSub)
	wg.Go(func() {
		if err := leaseWatcher.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("watcher error: %w", err)
		}
	})

	wg.Go(func() {
		if err := apiServer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("api server error: %w", err)
		}
	})

	wg.Go(func() {
		if err := withdrawScheduler.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- fmt.Errorf("scheduler error: %w", err)
		}
	})

	slog.Info("providerd started successfully",
		"api_addr", cfg.APIListenAddr,
		"withdraw_interval", cfg.WithdrawInterval,
		"rate_limit_rps", cfg.RateLimitRPS,
		"rate_limit_burst", cfg.RateLimitBurst,
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

	// Signal all components to stop
	cancel()

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Shutdown API server (uses its own timeout)
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shutdown API server gracefully", "error", err)
	}

	// Close provision manager
	if err := provisionMgr.Close(); err != nil {
		slog.Error("failed to close provision manager", "error", err)
	}

	// Close event subscriber
	eventSub.Close()

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("all components stopped gracefully")
	case <-shutdownCtx.Done():
		slog.Warn("shutdown timed out, some components may not have stopped cleanly")
	}

	slog.Info("providerd stopped")
	return nil
}
