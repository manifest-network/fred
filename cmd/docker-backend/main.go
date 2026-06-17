// docker-backend is an HTTP server that implements the Fred backend protocol
// for provisioning Docker containers.
package main

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/backend/shared/httpserver"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/tlsconfig"
)

var version = "dev"

func main() {
	configPath, showVersion, err := parseFlags(os.Args[1:], os.Stdout)
	if errors.Is(err, flag.ErrHelp) {
		// -h/-help: usage already written to stderr; this is a clean exit.
		os.Exit(0)
	}
	if err != nil {
		// flag.ContinueOnError already wrote the error and usage to stderr.
		os.Exit(2)
	}
	if showVersion {
		os.Exit(0)
	}

	// Bootstrap logger for startup messages (before config is loaded).
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load configuration
	cfg, err := loadConfig(configPath)
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
	logger.Info("starting docker-backend", "version", version, "log_level", cfg.LogLevel)

	// Log SKU mappings for visibility
	for uuid, profile := range cfg.SKUMapping {
		logger.Info("SKU mapping", "uuid", uuid, "profile", profile)
	}

	// Create backend
	b, err := docker.New(cfg, logger)
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
	server := httpserver.NewServer(b, string(cfg.CallbackSecret), logger)

	// Build the listener TLS config up front so a bad cert fails fast before we
	// announce readiness. Config.Validate (run in docker.New) already enforces
	// field pairing; ServerConfig loads and parses the actual files.
	var tlsServerConfig *tls.Config
	if cfg.TLSCertFile != "" {
		tlsServerConfig, err = tlsconfig.ServerConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile, cfg.TLSClientAllowedNames)
		if err != nil {
			logger.Error("failed to build TLS config", "error", err)
			os.Exit(1)
		}
	}

	// Setup HTTP server
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		TLSConfig:    tlsServerConfig, // nil => plaintext HTTP
	}

	// Start HTTP server
	serverErr := make(chan error, 1)
	go func() {
		var serveErr error
		if tlsServerConfig != nil {
			logger.Info("starting HTTPS server", "addr", cfg.ListenAddr,
				"mtls", cfg.TLSClientCAFile != "", "pinned_names", len(cfg.TLSClientAllowedNames))
			// The cert/key live in tlsServerConfig.Certificates (loaded by
			// tlsconfig.ServerConfig), so the file arguments are empty.
			serveErr = httpServer.ListenAndServeTLS("", "")
		} else {
			logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
			serveErr = httpServer.ListenAndServe()
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErr <- serveErr
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

	// Graceful shutdown
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

// parseFlags parses docker-backend's command-line arguments using a private
// FlagSet (never the global flag.CommandLine). When -version is set it writes
// the build-injected version to out and returns showVersion=true so main can
// exit 0 before doing any startup work — config load, Docker connection, etc.
// This mirrors providerd's `--version` (wired automatically by cobra) so an
// operator can query either binary's version without a valid config present.
//
// Usage and parse errors go to the FlagSet's default output (stderr); -h/-help
// surfaces as flag.ErrHelp so the caller can exit 0.
func parseFlags(args []string, out io.Writer) (configPath string, showVersion bool, err error) {
	fs := flag.NewFlagSet("docker-backend", flag.ContinueOnError)
	cfgPath := fs.String("config", "docker-backend.yaml", "Path to configuration file")
	showVer := fs.Bool("version", false, "print version information and exit")
	if err := fs.Parse(args); err != nil {
		return "", false, err
	}
	if *showVer {
		fmt.Fprintf(out, "docker-backend version %s\n", version)
		return "", true, nil
	}
	return *cfgPath, false, nil
}

func loadConfig(path string) (docker.Config, error) {
	cfg := docker.DefaultConfig()

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

func applyEnvOverrides(cfg *docker.Config) {
	if addr := os.Getenv("DOCKER_BACKEND_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if secret := os.Getenv("DOCKER_BACKEND_CALLBACK_SECRET"); secret != "" {
		cfg.CallbackSecret = config.Secret(secret)
	}
	if host := os.Getenv("DOCKER_BACKEND_HOST_ADDRESS"); host != "" {
		cfg.HostAddress = host
	}
	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost != "" {
		cfg.DockerHost = dockerHost
	}
}

