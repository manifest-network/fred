// k3s-backend is an HTTP server that implements the Fred backend protocol
// for provisioning K3s workloads. ENG-133 ships the scaffold: the binary
// boots, serves the BACKEND_GUIDE HTTP contract, signs and verifies
// callbacks, and probes the configured cluster's reachability via a single
// client-go Discovery().ServerVersion() call. The provisioner is a stub
// that posts status=failed, error="not implemented" callbacks; real K8s
// provisioning logic lands in ENG-134+.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/manifest-network/fred/internal/backend/k3s"
	"github.com/manifest-network/fred/internal/config"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "config.k3s.yaml", "Path to configuration file")
	flag.Parse()

	// Bootstrap logger for startup messages (before config is loaded).
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load configuration
	cfg, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Apply environment variable overrides
	applyEnvOverrides(&cfg)

	// Re-configure logger with the configured log level.
	logLevel, err := config.ParseLogLevel(cmp.Or(cfg.LogLevel, "info"))
	if err != nil {
		logger.Error("invalid log_level in config", "error", err)
		os.Exit(1)
	}
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)
	logger.Info("starting k3s-backend",
		"version", version,
		"log_level", cfg.LogLevel,
		"kubeconfig_path", cfg.KubeconfigPath,
	)

	// Log SKU mappings for visibility
	for uuid, profile := range cfg.SKUMapping {
		logger.Info("SKU mapping", "uuid", uuid, "profile", profile)
	}

	// Create backend
	b, err := k3s.New(cfg, logger)
	if err != nil {
		logger.Error("failed to create backend", "error", err)
		os.Exit(1)
	}

	// Start backend
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := b.Start(ctx); err != nil {
		cancel()
		logger.Error("failed to start backend", "error", err)
		os.Exit(1)
	}
	cancel()

	// Create server
	server := NewServer(b, string(cfg.CallbackSecret), logger)

	// Setup HTTP server. ReadHeaderTimeout closes the slow-loris attack
	// surface (a client that trickles request headers to hold a goroutine
	// open indefinitely). The 10s window is generous for legitimate Fred
	// traffic on a LAN/VPN while still bounding malicious slow-headers.
	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Start HTTP server
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for shutdown signal or server error
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	// startupErr captures a ListenAndServe failure (port in use, bind refused,
	// etc.) so the process can exit non-zero after the graceful-shutdown path
	// runs. Without this, supervisors / k8s liveness probes / CI would see the
	// "binary that never bound" as a successful run.
	var startupErr error
	select {
	case <-sigCh:
		logger.Info("shutting down...")
	case err := <-serverErr:
		logger.Error("HTTP server error, shutting down", "error", err)
		startupErr = err
	}

	// Graceful shutdown — drain HTTP first so in-flight Provisions
	// complete (and their wg-tracked goroutines exit) before stores
	// close. This ordering is what validates the wg.Add hardening
	// rationale in provision_stub.go's Provision().
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP shutdown error", "error", err)
	}

	if err := b.Stop(); err != nil {
		logger.Error("backend shutdown error", "error", err)
	}

	logger.Info("shutdown complete")

	// Propagate ListenAndServe failure as a non-zero exit. The graceful-
	// shutdown path above still runs (so the bbolt stores close cleanly) but
	// we MUST NOT report success when the binary never accepted a single
	// request — k8s liveness, systemd, and CI all key off exit code.
	if startupErr != nil {
		os.Exit(1)
	}
}

func loadConfig(path string) (k3s.Config, error) {
	cfg := k3s.DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return cfg, fmt.Errorf("config file not found: %s", path)
		}
		return cfg, fmt.Errorf("failed to read config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to parse config: %w", err)
	}

	return cfg, nil
}

func applyEnvOverrides(cfg *k3s.Config) {
	if addr := os.Getenv("K3S_BACKEND_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if secret := os.Getenv("K3S_BACKEND_CALLBACK_SECRET"); secret != "" {
		cfg.CallbackSecret = config.Secret(secret)
	}
	if host := os.Getenv("K3S_BACKEND_HOST_ADDRESS"); host != "" {
		cfg.HostAddress = host
	}
	// KUBECONFIG is the standard K8s convention for pointing at a
	// kubeconfig file (kubectl reads it; client-go's default loader
	// honors it). When cfg.KubeconfigPath is empty in YAML and
	// KUBECONFIG is set in the env, propagate it so the operator gets
	// a single discoverable knob. When cfg.KubeconfigPath is explicitly
	// set in YAML, that wins — env should not override an explicit
	// config value.
	//
	// Single-path KUBECONFIG (no os.PathListSeparator) flows into
	// cfg.KubeconfigPath — the resolver's first-tier branch loads a
	// single explicit file.
	//
	// Multi-path KUBECONFIG (e.g. KUBECONFIG=/path/a:/path/b on Linux)
	// is split via filepath.SplitList and placed in
	// cfg.KubeconfigPathList. resolveRESTConfig hands the list to
	// client-go's clientcmd.ClientConfigLoadingRules (Precedence:)
	// for the canonical merge semantics — context/cluster/user entries
	// in earlier paths shadow those in later paths. Without this
	// branch, an explicit multi-path KUBECONFIG would be silently
	// ignored whenever the binary runs in-cluster (the in-cluster
	// branch would win) and would only flow through to the default
	// loader on hosts where in-cluster lookup fails.
	if cfg.KubeconfigPath == "" {
		if kc := os.Getenv("KUBECONFIG"); kc != "" {
			if strings.ContainsRune(kc, os.PathListSeparator) {
				cfg.KubeconfigPathList = filepath.SplitList(kc)
			} else {
				cfg.KubeconfigPath = kc
			}
		}
	}
}
