package util

import (
	"testing"
	"time"
)

func TestTimeToBytes_RoundTrip(t *testing.T) {
	original := time.Now()
	bytes := TimeToBytes(original)
	restored := BytesToTime(bytes)

	if original.UnixNano() != restored.UnixNano() {
		t.Errorf("round-trip failed: original=%v, restored=%v", original.UnixNano(), restored.UnixNano())
	}
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
			if !result.IsZero() {
				t.Errorf("BytesToTime(%v) = %v, want zero time", tt.input, result)
			}
		})
	}
}

func TestTimeToBytes_Length(t *testing.T) {
	bytes := TimeToBytes(time.Now())
	if len(bytes) != 8 {
		t.Errorf("TimeToBytes() returned %d bytes, want 8", len(bytes))
	}
}
