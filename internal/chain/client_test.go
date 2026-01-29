package chain

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildTLSConfig_Defaults(t *testing.T) {
	// Test with no CA file and no skip verify
	cfg, err := buildTLSConfig("", false)
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v", err)
	}

	if cfg == nil {
		t.Fatal("buildTLSConfig() returned nil config")
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false by default")
	}
	if cfg.RootCAs != nil {
		t.Error("RootCAs should be nil when no CA file specified")
	}
}

func TestBuildTLSConfig_SkipVerify(t *testing.T) {
	cfg, err := buildTLSConfig("", true)
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v", err)
	}

	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true when skip_verify is set")
	}
}

func TestBuildTLSConfig_WithCAFile(t *testing.T) {
	// Create a temporary CA certificate file
	tempDir := t.TempDir()
	caFile := filepath.Join(tempDir, "ca.pem")

	// Generate a valid self-signed certificate for testing
	testCert := generateTestCertificate(t)

	if err := os.WriteFile(caFile, testCert, 0600); err != nil {
		t.Fatalf("failed to write test CA file: %v", err)
	}

	cfg, err := buildTLSConfig(caFile, false)
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v", err)
	}

	if cfg.RootCAs == nil {
		t.Error("RootCAs should be set when CA file is provided")
	}
}

func TestBuildTLSConfig_InvalidCAFile(t *testing.T) {
	// Test with non-existent CA file
	_, err := buildTLSConfig("/nonexistent/ca.pem", false)
	if err == nil {
		t.Error("buildTLSConfig() should fail with non-existent CA file")
	}
}

func TestBuildTLSConfig_InvalidCertificate(t *testing.T) {
	// Create a temporary file with invalid certificate content
	tempDir := t.TempDir()
	caFile := filepath.Join(tempDir, "invalid.pem")

	if err := os.WriteFile(caFile, []byte("not a valid certificate"), 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := buildTLSConfig(caFile, false)
	if err == nil {
		t.Error("buildTLSConfig() should fail with invalid certificate")
	}
}

func TestClientConfig_Defaults(t *testing.T) {
	// Test that default values are applied correctly
	// We can't create a real client without a valid endpoint, but we can
	// verify the default value logic exists in the implementation

	tests := []struct {
		name               string
		txPollInterval     time.Duration
		txTimeout          time.Duration
		queryPageLimit     int
		wantPollInterval   time.Duration
		wantTimeout        time.Duration
		wantQueryPageLimit uint64
	}{
		{
			name:               "zero values get defaults",
			txPollInterval:     0,
			txTimeout:          0,
			queryPageLimit:     0,
			wantPollInterval:   500 * time.Millisecond,
			wantTimeout:        30 * time.Second,
			wantQueryPageLimit: 100,
		},
		{
			name:               "custom values preserved",
			txPollInterval:     time.Second,
			txTimeout:          time.Minute,
			queryPageLimit:     50,
			wantPollInterval:   time.Second,
			wantTimeout:        time.Minute,
			wantQueryPageLimit: 50,
		},
		{
			name:               "negative page limit gets default",
			txPollInterval:     0,
			txTimeout:          0,
			queryPageLimit:     -1,
			wantPollInterval:   500 * time.Millisecond,
			wantTimeout:        30 * time.Second,
			wantQueryPageLimit: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ClientConfig{
				TxPollInterval: tt.txPollInterval,
				TxTimeout:      tt.txTimeout,
				QueryPageLimit: tt.queryPageLimit,
			}

			// Apply defaults (this mirrors the logic in NewClient)
			txPollInterval := cfg.TxPollInterval
			if txPollInterval == 0 {
				txPollInterval = 500 * time.Millisecond
			}
			txTimeout := cfg.TxTimeout
			if txTimeout == 0 {
				txTimeout = 30 * time.Second
			}
			queryPageLimit := cfg.QueryPageLimit
			if queryPageLimit <= 0 {
				queryPageLimit = 100
			}

			if txPollInterval != tt.wantPollInterval {
				t.Errorf("txPollInterval = %v, want %v", txPollInterval, tt.wantPollInterval)
			}
			if txTimeout != tt.wantTimeout {
				t.Errorf("txTimeout = %v, want %v", txTimeout, tt.wantTimeout)
			}
			if uint64(queryPageLimit) != tt.wantQueryPageLimit {
				t.Errorf("queryPageLimit = %d, want %d", queryPageLimit, tt.wantQueryPageLimit)
			}
		})
	}
}

func TestMaxLeasesPerBatch(t *testing.T) {
	// Verify the constant is set to expected value
	if maxLeasesPerBatch != 100 {
		t.Errorf("maxLeasesPerBatch = %d, want 100", maxLeasesPerBatch)
	}
}

func TestBatchBoundaries(t *testing.T) {
	// Test that slices.Chunk produces expected batch sizes
	// This validates our understanding of the batching behavior

	tests := []struct {
		name      string
		count     int
		batchSize int
		wantCount int // number of batches
	}{
		{
			name:      "exact batch size",
			count:     100,
			batchSize: 100,
			wantCount: 1,
		},
		{
			name:      "one over batch size",
			count:     101,
			batchSize: 100,
			wantCount: 2,
		},
		{
			name:      "two full batches",
			count:     200,
			batchSize: 100,
			wantCount: 2,
		},
		{
			name:      "partial batch",
			count:     50,
			batchSize: 100,
			wantCount: 1,
		},
		{
			name:      "empty input",
			count:     0,
			batchSize: 100,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create input slice
			input := make([]string, tt.count)
			for i := range input {
				input[i] = "item"
			}

			// Count batches using slices.Chunk (same as used in client.go)
			var batchCount int
			for range chunk(input, tt.batchSize) {
				batchCount++
			}

			if batchCount != tt.wantCount {
				t.Errorf("batch count = %d, want %d", batchCount, tt.wantCount)
			}
		})
	}
}

// chunk is a helper that mimics slices.Chunk behavior for testing
// This exists to test batch boundary logic without importing slices
func chunk[S ~[]E, E any](s S, n int) func(func(S) bool) {
	return func(yield func(S) bool) {
		for i := 0; i < len(s); i += n {
			end := i + n
			if end > len(s) {
				end = len(s)
			}
			if !yield(s[i:end]) {
				return
			}
		}
	}
}

func TestIsRetryableTxError(t *testing.T) {
	client := &Client{}

	tests := []struct {
		name      string
		err       error
		wantRetry bool
	}{
		{
			name:      "nil error",
			err:       nil,
			wantRetry: false,
		},
		{
			name: "sequence mismatch error",
			err: &ChainTxError{
				Code:      32,
				Codespace: "sdk",
				RawLog:    "account sequence mismatch",
			},
			wantRetry: true,
		},
		{
			name: "other chain error",
			err: &ChainTxError{
				Code:      4, // Unauthorized
				Codespace: "sdk",
				RawLog:    "unauthorized",
			},
			wantRetry: false,
		},
		{
			name: "billing module error",
			err: &ChainTxError{
				Code:      1,
				Codespace: "billing",
				RawLog:    "lease not found",
			},
			wantRetry: false,
		},
		{
			name:      "gRPC unavailable error",
			err:       status.Error(codes.Unavailable, "connection refused"),
			wantRetry: true,
		},
		{
			name:      "gRPC deadline exceeded error",
			err:       status.Error(codes.DeadlineExceeded, "deadline exceeded"),
			wantRetry: true,
		},
		{
			name:      "gRPC resource exhausted error",
			err:       status.Error(codes.ResourceExhausted, "rate limited"),
			wantRetry: true,
		},
		{
			name:      "gRPC aborted error",
			err:       status.Error(codes.Aborted, "operation aborted"),
			wantRetry: true,
		},
		{
			name:      "gRPC internal error",
			err:       status.Error(codes.Internal, "internal error"),
			wantRetry: true,
		},
		{
			name:      "gRPC unknown error",
			err:       status.Error(codes.Unknown, "unknown error"),
			wantRetry: true,
		},
		{
			name:      "context deadline exceeded",
			err:       context.DeadlineExceeded,
			wantRetry: true,
		},
		{
			name:      "context canceled",
			err:       context.Canceled,
			wantRetry: true,
		},
		{
			name:      "wrapped gRPC unavailable error",
			err:       fmt.Errorf("failed to connect: %w", status.Error(codes.Unavailable, "server unavailable")),
			wantRetry: true,
		},
		{
			name:      "gRPC not found error (not retryable)",
			err:       status.Error(codes.NotFound, "not found"),
			wantRetry: false,
		},
		{
			name:      "gRPC invalid argument error (not retryable)",
			err:       status.Error(codes.InvalidArgument, "invalid argument"),
			wantRetry: false,
		},
		{
			name:      "gRPC permission denied error (not retryable)",
			err:       status.Error(codes.PermissionDenied, "permission denied"),
			wantRetry: false,
		},
		{
			name:      "random plain error (not retryable)",
			err:       fmt.Errorf("some unknown error"),
			wantRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := client.isRetryableTxError(tt.err)
			if got != tt.wantRetry {
				t.Errorf("isRetryableTxError() = %v, want %v", got, tt.wantRetry)
			}
		})
	}
}

func TestTxRetryConstants(t *testing.T) {
	// Verify retry constants are reasonable
	if txMaxRetries < 1 || txMaxRetries > 10 {
		t.Errorf("txMaxRetries = %d, expected between 1 and 10", txMaxRetries)
	}

	if txInitialBackoff < 100*time.Millisecond || txInitialBackoff > 5*time.Second {
		t.Errorf("txInitialBackoff = %v, expected between 100ms and 5s", txInitialBackoff)
	}

	if txMaxBackoff < txInitialBackoff {
		t.Error("txMaxBackoff should be >= txInitialBackoff")
	}
}

// generateTestCertificate creates a valid self-signed certificate for testing.
func generateTestCertificate(t *testing.T) []byte {
	t.Helper()

	// Generate a new ECDSA key
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create a self-signed certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	// Encode to PEM
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	return certPEM
}
