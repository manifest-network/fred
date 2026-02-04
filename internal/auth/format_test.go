package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatSignData(t *testing.T) {
	tests := []struct {
		name      string
		tenant    string
		leaseUUID string
		timestamp int64
		expected  string
	}{
		{
			name:      "standard inputs",
			tenant:    "manifest1abc",
			leaseUUID: "lease-uuid-1",
			timestamp: 1700000000,
			expected:  "manifest1abc:lease-uuid-1:1700000000",
		},
		{
			name:      "empty tenant",
			tenant:    "",
			leaseUUID: "lease-uuid-1",
			timestamp: 1700000000,
			expected:  ":lease-uuid-1:1700000000",
		},
		{
			name:      "zero timestamp",
			tenant:    "tenant-1",
			leaseUUID: "lease-uuid-1",
			timestamp: 0,
			expected:  "tenant-1:lease-uuid-1:0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatSignData(tt.tenant, tt.leaseUUID, tt.timestamp)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestFormatPayloadSignData(t *testing.T) {
	tests := []struct {
		name        string
		leaseUUID   string
		metaHashHex string
		timestamp   int64
		expected    string
	}{
		{
			name:        "standard inputs",
			leaseUUID:   "lease-uuid-1",
			metaHashHex: "abcdef1234567890",
			timestamp:   1700000000,
			expected:    "manifest lease data lease-uuid-1 abcdef1234567890 1700000000",
		},
		{
			name:        "empty hash",
			leaseUUID:   "lease-uuid-1",
			metaHashHex: "",
			timestamp:   1700000000,
			expected:    "manifest lease data lease-uuid-1  1700000000",
		},
		{
			name:        "zero timestamp",
			leaseUUID:   "lease-uuid-1",
			metaHashHex: "abcdef",
			timestamp:   0,
			expected:    "manifest lease data lease-uuid-1 abcdef 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatPayloadSignData(tt.leaseUUID, tt.metaHashHex, tt.timestamp)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}
