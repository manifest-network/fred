package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeToBytes_RoundTrip(t *testing.T) {
	original := time.Now()
	bytes := TimeToBytes(original)
	restored := BytesToTime(bytes)

	assert.Equal(t, original.UnixNano(), restored.UnixNano())
}

func TestBytesToTime_InvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"too short", []byte{1, 2, 3}},
		{"too long", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BytesToTime(tt.input)
			assert.True(t, result.IsZero())
		})
	}
}

func TestTimeToBytes_Length(t *testing.T) {
	bytes := TimeToBytes(time.Now())
	assert.Len(t, bytes, 8)
}
