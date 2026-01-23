package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

// Default values for configuration.
const (
	DefaultMaxRequestBodySize int64 = 1 << 20 // 1MB
)

// Config holds all configuration for the provider daemon.
type Config struct {
	ChainID           string        `mapstructure:"chain_id"`
	GRPCEndpoint      string        `mapstructure:"grpc_endpoint"`
	WebSocketURL      string        `mapstructure:"websocket_url"`
	ProviderUUID      string        `mapstructure:"provider_uuid"`
	ProviderAddress   string        `mapstructure:"provider_address"`
	KeyringBackend    string        `mapstructure:"keyring_backend"`
	KeyringDir        string        `mapstructure:"keyring_dir"`
	KeyName           string        `mapstructure:"key_name"`
	APIListenAddr    string        `mapstructure:"api_listen_addr"`
	WithdrawInterval time.Duration `mapstructure:"withdraw_interval"`
	TLSCertFile      string        `mapstructure:"tls_cert_file"`
	TLSKeyFile        string        `mapstructure:"tls_key_file"`
	Bech32Prefix      string        `mapstructure:"bech32_prefix"`
	RateLimitRPS      float64       `mapstructure:"rate_limit_rps"`
	RateLimitBurst    int           `mapstructure:"rate_limit_burst"`
	GRPCTLSEnabled    bool          `mapstructure:"grpc_tls_enabled"`
	GRPCTLSCAFile     string        `mapstructure:"grpc_tls_ca_file"`
	GRPCTLSSkipVerify bool          `mapstructure:"grpc_tls_skip_verify"`
	GasLimit          uint64        `mapstructure:"gas_limit"`
	GasPrice          int64         `mapstructure:"gas_price"`
	FeeDenom          string        `mapstructure:"fee_denom"`

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
	CallbackSecret  string          `mapstructure:"callback_secret"` // HMAC secret for callback authentication

	// Reconciliation configuration
	ReconciliationInterval time.Duration `mapstructure:"reconciliation_interval"`
}

// BackendConfig configures a single provisioning backend.
type BackendConfig struct {
	Name      string        `mapstructure:"name"`
	URL       string        `mapstructure:"url"`
	Timeout   time.Duration `mapstructure:"timeout"`
	SKUPrefix string        `mapstructure:"sku_prefix"`
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
	v.SetDefault("chain_id", "manifest-1")
	v.SetDefault("grpc_endpoint", "localhost:9090")
	v.SetDefault("websocket_url", "ws://localhost:26657/websocket")
	v.SetDefault("keyring_backend", "file")
	v.SetDefault("api_listen_addr", ":8080")
	v.SetDefault("withdraw_interval", "1h")
	v.SetDefault("bech32_prefix", "manifest")
	v.SetDefault("rate_limit_rps", 10.0)   // 10 requests per second
	v.SetDefault("rate_limit_burst", 20)   // burst of 20 requests
	v.SetDefault("grpc_tls_enabled", false)
	v.SetDefault("grpc_tls_ca_file", "")
	v.SetDefault("grpc_tls_skip_verify", false)
	v.SetDefault("gas_limit", 500000)
	v.SetDefault("gas_price", 25)        // price per gas unit in smallest denom
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

	// Reconciliation defaults
	v.SetDefault("reconciliation_interval", "5m")

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
	if c.GasLimit == 0 {
		return fmt.Errorf("gas_limit must be positive")
	}
	if c.GasPrice < 0 {
		return fmt.Errorf("gas_price cannot be negative")
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

	// Reconciliation validations
	if c.ReconciliationInterval <= 0 {
		return fmt.Errorf("reconciliation_interval must be positive")
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
