package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"

	"github.com/manifest-network/fred/internal/api"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/scheduler"
	"github.com/manifest-network/fred/internal/watcher"
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
		"auto_acknowledge", cfg.AutoAcknowledge,
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize signer
	signer, err := chain.NewSigner(cfg.KeyringBackend, cfg.KeyringDir, cfg.KeyName, cfg.ChainID)
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}
	slog.Info("signer initialized", "address", signer.Address())

	// Initialize chain client
	chainClient, err := chain.NewClient(chain.ClientConfig{
		Endpoint:      cfg.GRPCEndpoint,
		TLSEnabled:    cfg.GRPCTLSEnabled,
		TLSCAFile:     cfg.GRPCTLSCAFile,
		TLSSkipVerify: cfg.GRPCTLSSkipVerify,
	}, signer)
	if err != nil {
		return fmt.Errorf("failed to create chain client: %w", err)
	}
	defer chainClient.Close()
	slog.Info("chain client connected", "endpoint", cfg.GRPCEndpoint, "tls", cfg.GRPCTLSEnabled)

	// Initialize event subscriber
	eventSub, err := chain.NewEventSubscriber(cfg.WebSocketURL, cfg.ProviderUUID)
	if err != nil {
		return fmt.Errorf("failed to create event subscriber: %w", err)
	}
	slog.Info("event subscriber initialized", "url", cfg.WebSocketURL)

	// Initialize watcher
	leaseWatcher := watcher.New(chainClient, eventSub, cfg.ProviderUUID, cfg.AutoAcknowledge)

	// Initialize API server
	apiServer := api.NewServer(api.ServerConfig{
		Addr:           cfg.APIListenAddr,
		ProviderUUID:   cfg.ProviderUUID,
		Bech32Prefix:   cfg.Bech32Prefix,
		TLSCertFile:    cfg.TLSCertFile,
		TLSKeyFile:     cfg.TLSKeyFile,
		RateLimitRPS:   cfg.RateLimitRPS,
		RateLimitBurst: cfg.RateLimitBurst,
	}, chainClient)

	// Initialize withdrawal scheduler
	withdrawScheduler := scheduler.NewWithdrawScheduler(chainClient, cfg.ProviderUUID, cfg.WithdrawInterval)

	// Perform startup operations sequentially to avoid same-block transaction conflicts
	// First: withdraw any accumulated funds
	slog.Info("performing initial withdrawal")
	withdrawScheduler.WithdrawOnce(ctx)

	// Second: scan and acknowledge pending leases (must wait for withdrawal tx to be in a block)
	pendingCount, err := leaseWatcher.ScanAndAcknowledge(ctx)
	if err != nil {
		slog.Error("failed to scan/acknowledge pending leases", "error", err)
		// Continue startup - this is not fatal
	} else if pendingCount > 0 {
		slog.Info("startup lease scan complete", "pending_count", pendingCount)
	}

	// Start background components
	errChan := make(chan error, 4)

	go func() {
		if err := leaseWatcher.Start(ctx); err != nil {
			errChan <- fmt.Errorf("watcher error: %w", err)
		}
	}()

	go func() {
		if err := apiServer.Start(ctx); err != nil {
			errChan <- fmt.Errorf("api server error: %w", err)
		}
	}()

	go func() {
		if err := withdrawScheduler.Start(ctx); err != nil {
			errChan <- fmt.Errorf("scheduler error: %w", err)
		}
	}()

	slog.Info("providerd started successfully",
		"api_addr", cfg.APIListenAddr,
		"withdraw_interval", cfg.WithdrawInterval,
		"rate_limit_rps", cfg.RateLimitRPS,
		"rate_limit_burst", cfg.RateLimitBurst,
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
	cancel()

	// Give components time to clean up
	apiServer.Shutdown(ctx)
	eventSub.Close()

	slog.Info("providerd stopped")
	return nil
}
