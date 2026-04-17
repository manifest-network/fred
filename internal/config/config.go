package config

import (
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

// ParseLogLevel converts a string log level to slog.Level.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level: %q (valid: debug, info, warn, error)", s)
	}
}

// Default values for configuration.
const (
	DefaultMaxRequestBodySize int64 = 1 << 20 // 1MB
)

// Config holds all configuration for the provider daemon.
type Config struct {
	ChainID              string        `mapstructure:"chain_id"`
	GRPCEndpoint         string        `mapstructure:"grpc_endpoint"`
	WebSocketURL         string        `mapstructure:"websocket_url"`
	ProviderUUID         string        `mapstructure:"provider_uuid"`
	ProviderAddress      string        `mapstructure:"provider_address"`
	KeyringBackend       string        `mapstructure:"keyring_backend"`
	KeyringDir           string        `mapstructure:"keyring_dir"`
	KeyName              string        `mapstructure:"key_name"`
	APIListenAddr        string        `mapstructure:"api_listen_addr"`
	WithdrawInterval     time.Duration `mapstructure:"withdraw_interval"`
	TLSCertFile          string        `mapstructure:"tls_cert_file"`
	TLSKeyFile           string        `mapstructure:"tls_key_file"`
	Bech32Prefix         string        `mapstructure:"bech32_prefix"`
	RateLimitRPS         float64       `mapstructure:"rate_limit_rps"`
	RateLimitBurst       int           `mapstructure:"rate_limit_burst"`
	TenantRateLimitRPS   float64       `mapstructure:"tenant_rate_limit_rps"`   // Per-tenant rate limit (requests per second)
	TenantRateLimitBurst int           `mapstructure:"tenant_rate_limit_burst"` // Per-tenant burst limit
	TrustedProxies       []string      `mapstructure:"trusted_proxies"`         // CIDR blocks of trusted proxies for X-Forwarded-For
	CORSOrigins          []string      `mapstructure:"cors_origins"`            // Allowed CORS origins for browser clients. Defaults to ["*"] (all origins).
	GRPCTLSEnabled       bool          `mapstructure:"grpc_tls_enabled"`
	GRPCTLSCAFile        string        `mapstructure:"grpc_tls_ca_file"`
	GRPCTLSSkipVerify    bool          `mapstructure:"grpc_tls_skip_verify"`
	GasLimit             uint64        `mapstructure:"gas_limit"`
	MaxGasLimit          uint64        `mapstructure:"max_gas_limit"` // 0 = no cap; if set, caps the gas limit during out-of-gas retries
	GasPrice             int64         `mapstructure:"gas_price"`
	// GasAdjustment is a multiplier applied to gas_limit when signing a tx,
	// providing headroom above the static estimate and reducing OOG retries.
	// Matches the Cosmos SDK CLI flag of the same name. Default: 1.2.
	// Must satisfy 1.0 <= GasAdjustment <= 3.0 (see Validate).
	GasAdjustment float64 `mapstructure:"gas_adjustment"`
	FeeDenom      string  `mapstructure:"fee_denom"`

	// Timeout configuration
	HTTPReadTimeout       time.Duration `mapstructure:"http_read_timeout"`
	HTTPWriteTimeout      time.Duration `mapstructure:"http_write_timeout"`
	HTTPIdleTimeout       time.Duration `mapstructure:"http_idle_timeout"`
	WebSocketPingInterval time.Duration `mapstructure:"websocket_ping_interval"`
	TxPollInterval        time.Duration `mapstructure:"tx_poll_interval"`
	TxTimeout             time.Duration `mapstructure:"tx_timeout"`

	// Query and pagination limits
	QueryPageLimit        int `mapstructure:"query_page_limit"`
	MaxWithdrawIterations int `mapstructure:"max_withdraw_iterations"`

	// WebSocket reconnection backoff
	WebSocketReconnectInitial time.Duration `mapstructure:"websocket_reconnect_initial"`
	WebSocketReconnectMax     time.Duration `mapstructure:"websocket_reconnect_max"`

	// API limits
	MaxRequestBodySize int64 `mapstructure:"max_request_body_size"`

	// Credit check thresholds
	CreditCheckErrorThreshold int           `mapstructure:"credit_check_error_threshold"`
	CreditCheckRetryInterval  time.Duration `mapstructure:"credit_check_retry_interval"`

	// Backend configuration
	Backends        []BackendConfig `mapstructure:"backends"`
	CallbackBaseURL string          `mapstructure:"callback_base_url"`
	CallbackSecret  Secret          `mapstructure:"callback_secret"` // HMAC secret for callback authentication

	// Parallel signing (authz sub-signers)
	SubSignerCount             int           `mapstructure:"sub_signer_count"`               // 0 = single signer (default)
	SubSignerMinBalance        string        `mapstructure:"sub_signer_min_balance"`         // Top-up when below this (default: "10000000umfx")
	SubSignerTopUpAmount       string        `mapstructure:"sub_signer_top_up_amount"`       // Amount per top-up (default: "50000000umfx")
	SubSignerFundCheckInterval time.Duration `mapstructure:"sub_signer_fund_check_interval"` // Balance check interval (default: 1h)

	// Reconciliation configuration
	ReconciliationInterval time.Duration `mapstructure:"reconciliation_interval"`

	// Production mode enforces security requirements at startup
	ProductionMode bool `mapstructure:"production_mode"`

	// Token replay protection
	TokenTrackerDBPath string `mapstructure:"token_tracker_db_path"`

	// Payload store configuration
	PayloadStoreDBPath string `mapstructure:"payload_store_db_path"`

	// Placement store configuration (enables round-robin backend routing)
	PlacementStoreDBPath string `mapstructure:"placement_store_db_path"`

	// Shutdown configuration
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`

	// Logging
	LogLevel string `mapstructure:"log_level"`
}

// BackendConfig configures a single provisioning backend.
type BackendConfig struct {
	Name      string        `mapstructure:"name"`
	URL       string        `mapstructure:"url"`
	Timeout   time.Duration `mapstructure:"timeout"`
	SKUs      []string      `mapstructure:"skus"`
	IsDefault bool          `mapstructure:"default"`
}

// TLSEnabled returns true if TLS is configured.
func (c *Config) TLSEnabled() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

// Load reads configuration from a YAML file and/or environment variables.
// Environment variables use the PROVIDER_ prefix (e.g., PROVIDER_CHAIN_ID).
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("production_mode", false)
	v.SetDefault("chain_id", "manifest-1")
	v.SetDefault("grpc_endpoint", "localhost:9090")
	v.SetDefault("websocket_url", "ws://localhost:26657/websocket")
	v.SetDefault("keyring_backend", "file")
	v.SetDefault("api_listen_addr", ":8080")
	v.SetDefault("withdraw_interval", "1h")
	v.SetDefault("bech32_prefix", "manifest")
	v.SetDefault("rate_limit_rps", 10.0)        // 10 requests per second (global)
	v.SetDefault("rate_limit_burst", 20)        // burst of 20 requests (global)
	v.SetDefault("tenant_rate_limit_rps", 5.0)  // 5 requests per second per tenant
	v.SetDefault("tenant_rate_limit_burst", 10) // burst of 10 requests per tenant
	v.SetDefault("cors_origins", []string{"*"}) // Allow all origins by default
	v.SetDefault("grpc_tls_enabled", false)
	v.SetDefault("grpc_tls_ca_file", "")
	v.SetDefault("grpc_tls_skip_verify", false)
	v.SetDefault("gas_limit", 1500000)
	v.SetDefault("max_gas_limit", 0)    // 0 = no cap; if set, caps the gas limit during out-of-gas retries
	v.SetDefault("gas_price", 25)       // micro-units of fee_denom per gas unit; fee = gas_limit * gas_price / 1_000_000
	v.SetDefault("gas_adjustment", 1.2) // multiplier applied to gas_limit at sign time (matches Cosmos CLI default)
	v.SetDefault("fee_denom", "umfx")

	// Timeout defaults
	v.SetDefault("http_read_timeout", "15s")
	v.SetDefault("http_write_timeout", "15s")
	v.SetDefault("http_idle_timeout", "60s")
	v.SetDefault("websocket_ping_interval", "30s")
	v.SetDefault("tx_poll_interval", "500ms")
	v.SetDefault("tx_timeout", "30s")

	// Query and pagination defaults
	v.SetDefault("query_page_limit", 100)
	v.SetDefault("max_withdraw_iterations", 100)

	// WebSocket reconnection defaults
	v.SetDefault("websocket_reconnect_initial", "1s")
	v.SetDefault("websocket_reconnect_max", "60s")

	// API limits defaults
	v.SetDefault("max_request_body_size", DefaultMaxRequestBodySize)

	// Credit check defaults
	v.SetDefault("credit_check_error_threshold", 3)
	v.SetDefault("credit_check_retry_interval", "30s")

	// Parallel signing defaults
	v.SetDefault("sub_signer_count", 0)
	v.SetDefault("sub_signer_min_balance", "10000000umfx")
	v.SetDefault("sub_signer_top_up_amount", "50000000umfx")
	v.SetDefault("sub_signer_fund_check_interval", "1h")

	// Reconciliation defaults
	v.SetDefault("reconciliation_interval", "5m")

	// Shutdown defaults
	v.SetDefault("shutdown_timeout", "30s")

	// Logging defaults
	v.SetDefault("log_level", "info")

	// Environment variable support
	v.SetEnvPrefix("PROVIDER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Config file
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Detect removed config keys that viper silently ignores.
	// Old configs with sku_prefix would leave backends[i].skus empty,
	// causing silent misrouting to the default backend.
	if backends := v.Get("backends"); backends != nil {
		if backendSlice, ok := backends.([]any); ok {
			for i, entry := range backendSlice {
				if m, ok := entry.(map[string]any); ok {
					if _, has := m["sku_prefix"]; has {
						return nil, fmt.Errorf("backends[%d]: sku_prefix is no longer supported (on-chain SKUs are always UUIDs); replace sku_prefix with skus: [list of exact SKU UUIDs]", i)
					}
				}
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// Validate checks that required configuration fields are set and valid.
func (c *Config) Validate() error {
	// Required fields
	if c.ProviderUUID == "" {
		return fmt.Errorf("provider_uuid is required")
	}
	if !IsValidUUID(c.ProviderUUID) {
		return fmt.Errorf("provider_uuid is not a valid UUID format")
	}
	if c.ProviderAddress == "" {
		return fmt.Errorf("provider_address is required")
	}
	if c.KeyName == "" {
		return fmt.Errorf("key_name is required")
	}
	if c.KeyringDir == "" {
		return fmt.Errorf("keyring_dir is required")
	}
	if c.Bech32Prefix == "" {
		return fmt.Errorf("bech32_prefix is required")
	}

	// Numeric validations
	if c.WithdrawInterval <= 0 {
		return fmt.Errorf("withdraw_interval must be positive")
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("rate_limit_rps must be positive")
	}
	if c.RateLimitBurst <= 0 {
		return fmt.Errorf("rate_limit_burst must be positive")
	}
	if c.TenantRateLimitRPS < 0 {
		return fmt.Errorf("tenant_rate_limit_rps cannot be negative")
	}
	if c.TenantRateLimitBurst < 0 {
		return fmt.Errorf("tenant_rate_limit_burst cannot be negative")
	}
	if c.GasLimit == 0 {
		return fmt.Errorf("gas_limit must be positive")
	}
	if c.GasLimit > math.MaxInt64 {
		return fmt.Errorf("gas_limit %d exceeds maximum (%d)", c.GasLimit, int64(math.MaxInt64))
	}
	if c.MaxGasLimit != 0 && c.MaxGasLimit < c.GasLimit {
		return fmt.Errorf("max_gas_limit (%d) must be >= gas_limit (%d)", c.MaxGasLimit, c.GasLimit)
	}
	if c.MaxGasLimit > math.MaxInt64 {
		return fmt.Errorf("max_gas_limit %d exceeds maximum (%d)", c.MaxGasLimit, int64(math.MaxInt64))
	}
	if c.GasPrice < 0 {
		return fmt.Errorf("gas_price cannot be negative")
	}
	// NaN comparisons always return false, so check it explicitly before
	// the range check — otherwise `gas_adjustment = nan` via env var would
	// silently pass validation and skip the adjustment branch in the signer.
	if math.IsNaN(c.GasAdjustment) {
		return fmt.Errorf("gas_adjustment must not be NaN")
	}
	if c.GasAdjustment < 1.0 || c.GasAdjustment > 3.0 {
		return fmt.Errorf("gas_adjustment must be between 1.0 and 3.0, got %f", c.GasAdjustment)
	}
	if c.FeeDenom == "" {
		return fmt.Errorf("fee_denom is required")
	}

	// Timeout validations
	if c.HTTPReadTimeout <= 0 {
		return fmt.Errorf("http_read_timeout must be positive")
	}
	if c.HTTPWriteTimeout <= 0 {
		return fmt.Errorf("http_write_timeout must be positive")
	}
	if c.HTTPIdleTimeout <= 0 {
		return fmt.Errorf("http_idle_timeout must be positive")
	}
	if c.WebSocketPingInterval <= 0 {
		return fmt.Errorf("websocket_ping_interval must be positive")
	}
	if c.TxPollInterval <= 0 {
		return fmt.Errorf("tx_poll_interval must be positive")
	}
	if c.TxTimeout <= 0 {
		return fmt.Errorf("tx_timeout must be positive")
	}

	// Query and pagination validations
	if c.QueryPageLimit <= 0 {
		return fmt.Errorf("query_page_limit must be positive")
	}
	if c.MaxWithdrawIterations <= 0 {
		return fmt.Errorf("max_withdraw_iterations must be positive")
	}

	// WebSocket reconnection validations
	if c.WebSocketReconnectInitial <= 0 {
		return fmt.Errorf("websocket_reconnect_initial must be positive")
	}
	if c.WebSocketReconnectMax <= 0 {
		return fmt.Errorf("websocket_reconnect_max must be positive")
	}

	// API limits validations
	if c.MaxRequestBodySize <= 0 {
		return fmt.Errorf("max_request_body_size must be positive")
	}

	// Credit check validations
	if c.CreditCheckErrorThreshold <= 0 {
		return fmt.Errorf("credit_check_error_threshold must be positive")
	}
	if c.CreditCheckRetryInterval <= 0 {
		return fmt.Errorf("credit_check_retry_interval must be positive")
	}

	// Parallel signing validations
	if c.SubSignerCount < 0 {
		return fmt.Errorf("sub_signer_count must be non-negative")
	}
	if c.SubSignerCount > 0 && c.SubSignerFundCheckInterval <= 0 {
		return fmt.Errorf("sub_signer_fund_check_interval must be positive when sub_signer_count > 0")
	}

	// Reconciliation validations
	if c.ReconciliationInterval <= 0 {
		return fmt.Errorf("reconciliation_interval must be positive")
	}

	// Shutdown validations
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout must be positive")
	}

	// URL/endpoint validations
	if c.WebSocketURL != "" {
		wsURL, err := url.Parse(c.WebSocketURL)
		if err != nil {
			return fmt.Errorf("websocket_url is not a valid URL: %w", err)
		}
		if wsURL.Scheme != "ws" && wsURL.Scheme != "wss" {
			return fmt.Errorf("websocket_url must use ws:// or wss:// scheme")
		}
	}

	if c.GRPCEndpoint != "" {
		if _, _, err := net.SplitHostPort(c.GRPCEndpoint); err != nil {
			return fmt.Errorf("grpc_endpoint must be in host:port format: %w", err)
		}
	}

	// TLS file validation - if one is set, both must be set
	if (c.TLSCertFile != "") != (c.TLSKeyFile != "") {
		return fmt.Errorf("both tls_cert_file and tls_key_file must be set together")
	}

	// Security warning for gRPC TLS skip verify (non-production mode)
	if c.GRPCTLSEnabled && c.GRPCTLSSkipVerify {
		slog.Warn("SECURITY WARNING: grpc_tls_skip_verify is enabled - TLS certificate verification is disabled, vulnerable to MITM attacks")
	}

	// Backend validation - at least one backend is required
	if len(c.Backends) == 0 {
		return fmt.Errorf("at least one backend must be configured")
	}

	hasDefault := false
	seenNames := make(map[string]bool)

	for i, b := range c.Backends {
		if b.Name == "" {
			return fmt.Errorf("backends[%d].name is required", i)
		}
		if seenNames[b.Name] {
			return fmt.Errorf("duplicate backend name: %s", b.Name)
		}
		seenNames[b.Name] = true

		if b.URL == "" {
			return fmt.Errorf("backends[%d].url is required", i)
		}
		if err := validateHTTPURL(b.URL); err != nil {
			return fmt.Errorf("backends[%d].url: %w", i, err)
		}

		if b.IsDefault {
			if hasDefault {
				return fmt.Errorf("multiple default backends specified")
			}
			hasDefault = true
		}
	}

	// callback_base_url is required for backend callbacks
	if c.CallbackBaseURL == "" {
		return fmt.Errorf("callback_base_url is required")
	}
	if err := validateHTTPURL(c.CallbackBaseURL); err != nil {
		return fmt.Errorf("callback_base_url: %w", err)
	}
	// Normalize: strip trailing slashes to avoid double slashes when joining paths
	c.CallbackBaseURL = strings.TrimRight(c.CallbackBaseURL, "/")

	// callback_secret is required for callback authentication (HMAC)
	if c.CallbackSecret == "" {
		return fmt.Errorf("callback_secret is required for callback authentication")
	}
	if len(c.CallbackSecret) < 32 {
		return fmt.Errorf("callback_secret must be at least 32 characters")
	}

	// Production mode security enforcement (runs after all basic validation)
	if c.ProductionMode {
		// Only block skip_verify when TLS is actually enabled; if TLS is
		// disabled the flag is meaningless and not a security concern.
		if c.GRPCTLSEnabled && c.GRPCTLSSkipVerify {
			return fmt.Errorf("production_mode: grpc_tls_skip_verify cannot be enabled with grpc_tls_enabled")
		}
		if c.TokenTrackerDBPath == "" {
			return fmt.Errorf("production_mode: token_tracker_db_path is required for replay protection")
		}

		// SSRF protection: block loopback, link-local, and unspecified addresses
		if err := validateExternalURL(c.CallbackBaseURL); err != nil {
			return fmt.Errorf("production_mode: callback_base_url: %w", err)
		}
		for i, b := range c.Backends {
			if err := validateExternalURL(b.URL); err != nil {
				return fmt.Errorf("production_mode: backends[%d].url: %w", i, err)
			}
		}
	}

	return nil
}

// IsValidUUID checks if a string is a valid UUID format (RFC 4122).
func IsValidUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

// validateHTTPURL checks that a URL is an absolute http/https URL with non-empty host.
func validateHTTPURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL must use http:// or https:// scheme, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return fmt.Errorf("URL must have a host")
	}
	return nil
}

// validateExternalURL rejects URLs that point to loopback, link-local, or
// unspecified addresses. Private IPs (RFC 1918) are allowed since backends
// commonly run on private networks. Multicast and broadcast addresses are
// not checked because TCP (required by HTTP) does not support them. Only IP
// literals and the hostname "localhost" are checked; other hostnames are
// allowed through (no DNS resolution).
func validateExternalURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	hostname := parsed.Hostname()                // strips port and IPv6 brackets
	hostname = strings.TrimSuffix(hostname, ".") // normalize FQDN notation

	// Block "localhost" hostname (case-insensitive)
	if strings.EqualFold(hostname, "localhost") {
		return fmt.Errorf("URL must not use localhost")
	}

	// Parse as IP literal; non-IP hostnames pass through
	ip := net.ParseIP(hostname)
	if ip == nil {
		return nil
	}

	if ip.IsLoopback() {
		return fmt.Errorf("URL must not use a loopback address")
	}
	if ip.IsLinkLocalUnicast() {
		return fmt.Errorf("URL must not use a link-local address")
	}
	if ip.IsUnspecified() {
		return fmt.Errorf("URL must not use an unspecified address")
	}

	return nil
}
