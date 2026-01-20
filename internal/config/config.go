package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// uuidRegex validates UUID format (RFC 4122)
var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Config holds all configuration for the provider daemon.
type Config struct {
	ChainID          string        `mapstructure:"chain_id"`
	GRPCEndpoint     string        `mapstructure:"grpc_endpoint"`
	WebSocketURL     string        `mapstructure:"websocket_url"`
	ProviderUUID     string        `mapstructure:"provider_uuid"`
	ProviderAddress  string        `mapstructure:"provider_address"`
	KeyringBackend   string        `mapstructure:"keyring_backend"`
	KeyringDir       string        `mapstructure:"keyring_dir"`
	KeyName          string        `mapstructure:"key_name"`
	APIListenAddr    string        `mapstructure:"api_listen_addr"`
	WithdrawInterval time.Duration `mapstructure:"withdraw_interval"`
	AutoAcknowledge  bool          `mapstructure:"auto_acknowledge"`
	TLSCertFile      string        `mapstructure:"tls_cert_file"`
	TLSKeyFile       string        `mapstructure:"tls_key_file"`
	Bech32Prefix     string        `mapstructure:"bech32_prefix"`
	RateLimitRPS     float64       `mapstructure:"rate_limit_rps"`
	RateLimitBurst   int           `mapstructure:"rate_limit_burst"`
	GRPCTLSEnabled   bool          `mapstructure:"grpc_tls_enabled"`
	GRPCTLSCAFile    string        `mapstructure:"grpc_tls_ca_file"`
	GRPCTLSSkipVerify bool         `mapstructure:"grpc_tls_skip_verify"`
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
	v.SetDefault("auto_acknowledge", true)
	v.SetDefault("bech32_prefix", "manifest")
	v.SetDefault("rate_limit_rps", 10.0)   // 10 requests per second
	v.SetDefault("rate_limit_burst", 20)   // burst of 20 requests
	v.SetDefault("grpc_tls_enabled", false)
	v.SetDefault("grpc_tls_ca_file", "")
	v.SetDefault("grpc_tls_skip_verify", false)

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

// Validate checks that required configuration fields are set.
func (c *Config) Validate() error {
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
	return nil
}

// IsValidUUID checks if a string is a valid UUID format (RFC 4122).
func IsValidUUID(uuid string) bool {
	return uuidRegex.MatchString(uuid)
}
