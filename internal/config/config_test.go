package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIsValidUUID_Valid(t *testing.T) {
	validUUIDs := []string{
		"01234567-89ab-cdef-0123-456789abcdef",
		"ABCDEF01-2345-6789-ABCD-EF0123456789",
		"12345678-1234-1234-1234-123456789abc",
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
	}

	for _, uuid := range validUUIDs {
		if !IsValidUUID(uuid) {
			t.Errorf("IsValidUUID(%q) = false, want true", uuid)
		}
	}
}

func TestIsValidUUID_Invalid(t *testing.T) {
	invalidUUIDs := []string{
		"",
		"not-a-uuid",
		"01234567-89ab-cdef-0123",
		"01234567-89ab-cdef-0123-456789abcdefg",
		"01234567-89ab-cdef-0123-456789abcdeg",
		"01234567-89ab-cdef-0123-456789abcde",
		"g1234567-89ab-cdef-0123-456789abcdef",
		"01234567-89ab-cdef-0123-456789abcdef ",
		" 01234567-89ab-cdef-0123-456789abcdef",
	}

	for _, uuid := range invalidUUIDs {
		if IsValidUUID(uuid) {
			t.Errorf("IsValidUUID(%q) = true, want false", uuid)
		}
	}
}

func TestIsValidUUID_WithoutDashes(t *testing.T) {
	// google/uuid accepts UUIDs without dashes (valid per RFC 4122)
	if !IsValidUUID("0123456789abcdef0123456789abcdef") {
		t.Error("IsValidUUID should accept UUIDs without dashes")
	}
}

func TestConfig_Validate_MissingRequired(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "missing provider_uuid",
			cfg:     Config{},
			wantErr: "provider_uuid is required",
		},
		{
			name: "invalid provider_uuid",
			cfg: Config{
				ProviderUUID: "not-a-uuid",
			},
			wantErr: "provider_uuid is not a valid UUID format",
		},
		{
			name: "missing provider_address",
			cfg: Config{
				ProviderUUID: "01234567-89ab-cdef-0123-456789abcdef",
			},
			wantErr: "provider_address is required",
		},
		{
			name: "missing key_name",
			cfg: Config{
				ProviderUUID:    "01234567-89ab-cdef-0123-456789abcdef",
				ProviderAddress: "manifest1abc",
			},
			wantErr: "key_name is required",
		},
		{
			name: "missing keyring_dir",
			cfg: Config{
				ProviderUUID:    "01234567-89ab-cdef-0123-456789abcdef",
				ProviderAddress: "manifest1abc",
				KeyName:         "provider",
			},
			wantErr: "keyring_dir is required",
		},
		{
			name: "missing bech32_prefix",
			cfg: Config{
				ProviderUUID:    "01234567-89ab-cdef-0123-456789abcdef",
				ProviderAddress: "manifest1abc",
				KeyName:         "provider",
				KeyringDir:      "/home/provider/.manifest",
			},
			wantErr: "bech32_prefix is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Errorf("Validate() = nil, want error containing %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := Config{
		ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
		ProviderAddress:           "manifest1abc",
		KeyName:                   "provider",
		KeyringDir:                "/home/provider/.manifest",
		Bech32Prefix:              "manifest",
		WithdrawInterval:          time.Hour,
		RateLimitRPS:              10,
		RateLimitBurst:            20,
		GRPCEndpoint:              "localhost:9090",
		WebSocketURL:              "ws://localhost:26657/websocket",
		GasLimit:                  500000,
		GasPrice:                  25,
		FeeDenom:                  "umfx",
		HTTPReadTimeout:           15 * time.Second,
		HTTPWriteTimeout:          15 * time.Second,
		HTTPIdleTimeout:           60 * time.Second,
		WebSocketPingInterval:     30 * time.Second,
		TxPollInterval:            500 * time.Millisecond,
		TxTimeout:                 30 * time.Second,
		QueryPageLimit:            100,
		MaxWithdrawIterations:     100,
		WebSocketReconnectInitial: time.Second,
		WebSocketReconnectMax:     60 * time.Second,
		MaxRequestBodySize:        1 << 20,
		CreditCheckErrorThreshold: 3,
		CreditCheckRetryInterval:  30 * time.Second,
		ReconciliationInterval:    5 * time.Minute,
		Backends:                  []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}},
		CallbackBaseURL:           "http://localhost:8080",
		CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestConfig_Validate_NoBackends(t *testing.T) {
	cfg := Config{
		ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
		ProviderAddress:           "manifest1abc",
		KeyName:                   "provider",
		KeyringDir:                "/home/provider/.manifest",
		Bech32Prefix:              "manifest",
		WithdrawInterval:          time.Hour,
		RateLimitRPS:              10,
		RateLimitBurst:            20,
		GasLimit:                  500000,
		GasPrice:                  25,
		FeeDenom:                  "umfx",
		HTTPReadTimeout:           15 * time.Second,
		HTTPWriteTimeout:          15 * time.Second,
		HTTPIdleTimeout:           60 * time.Second,
		WebSocketPingInterval:     30 * time.Second,
		TxPollInterval:            500 * time.Millisecond,
		TxTimeout:                 30 * time.Second,
		QueryPageLimit:            100,
		MaxWithdrawIterations:     100,
		WebSocketReconnectInitial: time.Second,
		WebSocketReconnectMax:     60 * time.Second,
		MaxRequestBodySize:        1 << 20,
		CreditCheckErrorThreshold: 3,
		CreditCheckRetryInterval:  30 * time.Second,
		ReconciliationInterval:    5 * time.Minute,
		// No backends configured
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Validate() = nil, want error about missing backends")
		return
	}
	if !strings.Contains(err.Error(), "at least one backend must be configured") {
		t.Errorf("Validate() error = %q, want error containing 'at least one backend must be configured'", err.Error())
	}
}

func TestConfig_Validate_CallbackSecret(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			Backends:                  []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}},
			CallbackBaseURL:           "http://localhost:8080",
		}
	}

	tests := []struct {
		name           string
		callbackSecret string
		wantErr        string
	}{
		{
			name:           "missing callback_secret",
			callbackSecret: "",
			wantErr:        "callback_secret is required",
		},
		{
			name:           "callback_secret too short",
			callbackSecret: "short",
			wantErr:        "callback_secret must be at least 32 characters",
		},
		{
			name:           "callback_secret exactly 32 chars",
			callbackSecret: "12345678901234567890123456789012",
			wantErr:        "", // should pass
		},
		{
			name:           "valid callback_secret",
			callbackSecret: "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
			wantErr:        "", // should pass
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.CallbackSecret = tt.callbackSecret
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Errorf("Validate() = nil, want error containing %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_NumericFields(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			Backends:                  []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}},
			CallbackBaseURL:           "http://localhost:8080",
			CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
		}
	}

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "zero withdraw_interval",
			modify: func(c *Config) {
				c.WithdrawInterval = 0
			},
			wantErr: "withdraw_interval must be positive",
		},
		{
			name: "negative withdraw_interval",
			modify: func(c *Config) {
				c.WithdrawInterval = -time.Hour
			},
			wantErr: "withdraw_interval must be positive",
		},
		{
			name: "zero rate_limit_rps",
			modify: func(c *Config) {
				c.RateLimitRPS = 0
			},
			wantErr: "rate_limit_rps must be positive",
		},
		{
			name: "negative rate_limit_rps",
			modify: func(c *Config) {
				c.RateLimitRPS = -1
			},
			wantErr: "rate_limit_rps must be positive",
		},
		{
			name: "zero rate_limit_burst",
			modify: func(c *Config) {
				c.RateLimitBurst = 0
			},
			wantErr: "rate_limit_burst must be positive",
		},
		{
			name: "negative rate_limit_burst",
			modify: func(c *Config) {
				c.RateLimitBurst = -1
			},
			wantErr: "rate_limit_burst must be positive",
		},
		{
			name: "zero gas_limit",
			modify: func(c *Config) {
				c.GasLimit = 0
			},
			wantErr: "gas_limit must be positive",
		},
		{
			name: "negative gas_price",
			modify: func(c *Config) {
				c.GasPrice = -1
			},
			wantErr: "gas_price cannot be negative",
		},
		{
			name: "empty fee_denom",
			modify: func(c *Config) {
				c.FeeDenom = ""
			},
			wantErr: "fee_denom is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Errorf("Validate() = nil, want error %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_URLFields(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			Backends:                  []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}},
			CallbackBaseURL:           "http://localhost:8080",
			CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
		}
	}

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "invalid websocket scheme http",
			modify: func(c *Config) {
				c.WebSocketURL = "http://localhost:26657/websocket"
			},
			wantErr: "websocket_url must use ws:// or wss:// scheme",
		},
		{
			name: "invalid websocket scheme https",
			modify: func(c *Config) {
				c.WebSocketURL = "https://localhost:26657/websocket"
			},
			wantErr: "websocket_url must use ws:// or wss:// scheme",
		},
		{
			name: "valid wss scheme",
			modify: func(c *Config) {
				c.WebSocketURL = "wss://localhost:26657/websocket"
			},
			wantErr: "", // should pass
		},
		{
			name: "grpc endpoint missing port",
			modify: func(c *Config) {
				c.GRPCEndpoint = "localhost"
			},
			wantErr: "grpc_endpoint must be in host:port format",
		},
		{
			name: "valid grpc endpoint",
			modify: func(c *Config) {
				c.GRPCEndpoint = "localhost:9090"
			},
			wantErr: "", // should pass
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Errorf("Validate() = nil, want error %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_TLSPair(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			Backends:                  []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}},
			CallbackBaseURL:           "http://localhost:8080",
			CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
		}
	}

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "only cert file",
			modify: func(c *Config) {
				c.TLSCertFile = "/path/to/cert.pem"
			},
			wantErr: "both tls_cert_file and tls_key_file must be set together",
		},
		{
			name: "only key file",
			modify: func(c *Config) {
				c.TLSKeyFile = "/path/to/key.pem"
			},
			wantErr: "both tls_cert_file and tls_key_file must be set together",
		},
		{
			name: "both files set",
			modify: func(c *Config) {
				c.TLSCertFile = "/path/to/cert.pem"
				c.TLSKeyFile = "/path/to/key.pem"
			},
			wantErr: "", // should pass
		},
		{
			name:    "neither file set",
			modify:  func(c *Config) {},
			wantErr: "", // should pass
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Errorf("Validate() = nil, want error %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_TLSEnabled(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		expected bool
	}{
		{
			name:     "no TLS files",
			cfg:      Config{},
			expected: false,
		},
		{
			name:     "only cert file",
			cfg:      Config{TLSCertFile: "/path/to/cert.pem"},
			expected: false,
		},
		{
			name:     "only key file",
			cfg:      Config{TLSKeyFile: "/path/to/key.pem"},
			expected: false,
		},
		{
			name: "both files",
			cfg: Config{
				TLSCertFile: "/path/to/cert.pem",
				TLSKeyFile:  "/path/to/key.pem",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.TLSEnabled(); got != tt.expected {
				t.Errorf("TLSEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Create a minimal config file with required values to test defaults
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
provider_uuid: "01234567-89ab-cdef-0123-456789abcdef"
provider_address: "manifest1abc"
key_name: "provider"
keyring_dir: "/home/provider/.manifest"
callback_base_url: "http://localhost:8080"
callback_secret: "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq"
backends:
  - name: "mock"
    url: "http://localhost:9000"
    default: true
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Check defaults
	if cfg.ChainID != "manifest-1" {
		t.Errorf("ChainID = %q, want %q", cfg.ChainID, "manifest-1")
	}
	if cfg.GRPCEndpoint != "localhost:9090" {
		t.Errorf("GRPCEndpoint = %q, want %q", cfg.GRPCEndpoint, "localhost:9090")
	}
	if cfg.WebSocketURL != "ws://localhost:26657/websocket" {
		t.Errorf("WebSocketURL = %q, want %q", cfg.WebSocketURL, "ws://localhost:26657/websocket")
	}
	if cfg.KeyringBackend != "file" {
		t.Errorf("KeyringBackend = %q, want %q", cfg.KeyringBackend, "file")
	}
	if cfg.APIListenAddr != ":8080" {
		t.Errorf("APIListenAddr = %q, want %q", cfg.APIListenAddr, ":8080")
	}
	if cfg.Bech32Prefix != "manifest" {
		t.Errorf("Bech32Prefix = %q, want %q", cfg.Bech32Prefix, "manifest")
	}
	if cfg.RateLimitRPS != 10.0 {
		t.Errorf("RateLimitRPS = %v, want %v", cfg.RateLimitRPS, 10.0)
	}
	if cfg.RateLimitBurst != 20 {
		t.Errorf("RateLimitBurst = %v, want %v", cfg.RateLimitBurst, 20)
	}
}

func TestLoad_ConfigOverrides(t *testing.T) {
	// Test that config file values override defaults
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
provider_uuid: "01234567-89ab-cdef-0123-456789abcdef"
provider_address: "manifest1abc"
key_name: "provider"
keyring_dir: "/home/provider/.manifest"
chain_id: "test-chain-1"
rate_limit_rps: 50
callback_base_url: "http://localhost:8080"
callback_secret: "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq"
backends:
  - name: "mock"
    url: "http://localhost:9000"
    default: true
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ChainID != "test-chain-1" {
		t.Errorf("ChainID = %q, want %q", cfg.ChainID, "test-chain-1")
	}
	if cfg.RateLimitRPS != 50.0 {
		t.Errorf("RateLimitRPS = %v, want %v", cfg.RateLimitRPS, 50.0)
	}
}

func TestConfig_Validate_BackendURLs(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
		}
	}

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "valid http backend URL",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}}
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "",
		},
		{
			name: "valid https backend URL",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "https://backend.example.com:9000", IsDefault: true}}
				c.CallbackBaseURL = "https://fred.example.com:8080"
			},
			wantErr: "",
		},
		{
			name: "relative backend URL",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "/api/provision", IsDefault: true}}
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "backends[0].url: URL must use http:// or https:// scheme",
		},
		{
			name: "backend URL without scheme",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "localhost:9000", IsDefault: true}}
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "backends[0].url: URL must use http:// or https:// scheme",
		},
		{
			name: "backend URL with ftp scheme",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "ftp://localhost:9000", IsDefault: true}}
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "backends[0].url: URL must use http:// or https:// scheme",
		},
		{
			name: "backend URL without host",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "http:///path", IsDefault: true}}
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "backends[0].url: URL must have a host",
		},
		{
			name: "relative callback URL",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}}
				c.CallbackBaseURL = "/callbacks"
			},
			wantErr: "callback_base_url: URL must use http:// or https:// scheme",
		},
		{
			name: "callback URL without scheme",
			modify: func(c *Config) {
				c.Backends = []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}}
				c.CallbackBaseURL = "localhost:8080"
			},
			wantErr: "callback_base_url: URL must use http:// or https:// scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Errorf("Validate() = nil, want error containing %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_CallbackURLNormalization(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
		}
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no trailing slash",
			input:    "http://localhost:8080",
			expected: "http://localhost:8080",
		},
		{
			name:     "single trailing slash",
			input:    "http://localhost:8080/",
			expected: "http://localhost:8080",
		},
		{
			name:     "multiple trailing slashes",
			input:    "http://localhost:8080///",
			expected: "http://localhost:8080",
		},
		{
			name:     "with path and trailing slash",
			input:    "http://localhost:8080/api/",
			expected: "http://localhost:8080/api",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Backends = []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}}
			cfg.CallbackBaseURL = tt.input

			err := cfg.Validate()
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}

			if cfg.CallbackBaseURL != tt.expected {
				t.Errorf("CallbackBaseURL = %q, want %q", cfg.CallbackBaseURL, tt.expected)
			}
		})
	}
}

func TestConfig_Validate_ProductionMode(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
			ProviderAddress:           "manifest1abc",
			KeyName:                   "provider",
			KeyringDir:                "/home/provider/.manifest",
			Bech32Prefix:              "manifest",
			WithdrawInterval:          time.Hour,
			RateLimitRPS:              10,
			RateLimitBurst:            20,
			GasLimit:                  500000,
			GasPrice:                  25,
			FeeDenom:                  "umfx",
			HTTPReadTimeout:           15 * time.Second,
			HTTPWriteTimeout:          15 * time.Second,
			HTTPIdleTimeout:           60 * time.Second,
			WebSocketPingInterval:     30 * time.Second,
			TxPollInterval:            500 * time.Millisecond,
			TxTimeout:                 30 * time.Second,
			QueryPageLimit:            100,
			MaxWithdrawIterations:     100,
			WebSocketReconnectInitial: time.Second,
			WebSocketReconnectMax:     60 * time.Second,
			MaxRequestBodySize:        1 << 20,
			CreditCheckErrorThreshold: 3,
			CreditCheckRetryInterval:  30 * time.Second,
			ReconciliationInterval:    5 * time.Minute,
			Backends:                  []BackendConfig{{Name: "mock", URL: "http://10.0.0.1:9000", IsDefault: true}},
			CallbackBaseURL:           "http://10.0.0.1:8080",
			CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
		}
	}

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "production mode blocks tls skip verify",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.GRPCTLSEnabled = true
				c.GRPCTLSSkipVerify = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
			},
			wantErr: "production_mode: grpc_tls_skip_verify cannot be enabled with grpc_tls_enabled",
		},
		{
			name: "production mode requires token tracker",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = ""
			},
			wantErr: "production_mode: token_tracker_db_path is required for replay protection",
		},
		{
			name: "production mode allows valid secure config",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.GRPCTLSEnabled = true
				c.GRPCTLSSkipVerify = false
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
			},
			wantErr: "",
		},
		{
			name: "production mode allows skip verify when tls is disabled",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.GRPCTLSEnabled = false
				c.GRPCTLSSkipVerify = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
			},
			wantErr: "", // skip_verify is meaningless when TLS is disabled
		},
		{
			name: "non-production mode allows tls skip verify",
			modify: func(c *Config) {
				c.ProductionMode = false
				c.GRPCTLSEnabled = true
				c.GRPCTLSSkipVerify = true
			},
			wantErr: "",
		},
		{
			name: "non-production mode allows missing token tracker",
			modify: func(c *Config) {
				c.ProductionMode = false
				c.TokenTrackerDBPath = ""
			},
			wantErr: "",
		},
		{
			name: "production mode blocks loopback callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://127.0.0.1:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use a loopback address",
		},
		{
			name: "production mode blocks localhost callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use localhost",
		},
		{
			name: "production mode blocks link-local callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://169.254.169.254"
			},
			wantErr: "production_mode: callback_base_url: URL must not use a link-local address",
		},
		{
			name: "production mode blocks loopback backend URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.Backends = []BackendConfig{{Name: "mock", URL: "http://127.0.0.1:9000", IsDefault: true}}
			},
			wantErr: "production_mode: backends[0].url: URL must not use a loopback address",
		},
		{
			name: "production mode blocks unspecified callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://0.0.0.0:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use an unspecified address",
		},
		{
			name: "production mode blocks IPv6 loopback callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://[::1]:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use a loopback address",
		},
		{
			name: "production mode blocks IPv6 link-local callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://[fe80::1]:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use a link-local address",
		},
		{
			name: "production mode blocks IPv6 unspecified callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://[::]:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use an unspecified address",
		},
		{
			name: "production mode blocks uppercase Localhost callback URL",
			modify: func(c *Config) {
				c.ProductionMode = true
				c.TokenTrackerDBPath = "/var/lib/fred/tokens.db"
				c.CallbackBaseURL = "http://Localhost:8080"
			},
			wantErr: "production_mode: callback_base_url: URL must not use localhost",
		},
		{
			name: "non-production mode allows loopback URLs",
			modify: func(c *Config) {
				c.ProductionMode = false
				c.Backends = []BackendConfig{{Name: "mock", URL: "http://127.0.0.1:9000", IsDefault: true}}
				c.CallbackBaseURL = "http://localhost:8080"
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Errorf("Validate() = nil, want error containing %q", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoad_ConfigFile(t *testing.T) {
	// Create a temp config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
chain_id: "file-chain-1"
grpc_endpoint: "file-endpoint:9090"
provider_uuid: "abcdef01-2345-6789-abcd-ef0123456789"
provider_address: "manifest1xyz"
key_name: "filekey"
keyring_dir: "/file/keyring"
bech32_prefix: "manifest"
rate_limit_rps: 100
callback_base_url: "http://localhost:8080"
callback_secret: "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq"
backends:
  - name: "mock"
    url: "http://localhost:9000"
    default: true
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ChainID != "file-chain-1" {
		t.Errorf("ChainID = %q, want %q", cfg.ChainID, "file-chain-1")
	}
	if cfg.GRPCEndpoint != "file-endpoint:9090" {
		t.Errorf("GRPCEndpoint = %q, want %q", cfg.GRPCEndpoint, "file-endpoint:9090")
	}
	if cfg.ProviderUUID != "abcdef01-2345-6789-abcd-ef0123456789" {
		t.Errorf("ProviderUUID = %q, want %q", cfg.ProviderUUID, "abcdef01-2345-6789-abcd-ef0123456789")
	}
	if cfg.RateLimitRPS != 100.0 {
		t.Errorf("RateLimitRPS = %v, want %v", cfg.RateLimitRPS, 100.0)
	}
}
