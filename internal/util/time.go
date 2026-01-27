// Package util provides shared utility functions.
package util

import (
	"encoding/binary"
	"time"
)

// TimeToBytes converts a time.Time to bytes for storage.
// Uses big-endian encoding of UnixNano for consistent ordering.
func TimeToBytes(t time.Time) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t.UnixNano()))
	return b
}

// BytesToTime converts bytes back to time.Time.
// Returns zero time if the input is not exactly 8 bytes.
func BytesToTime(b []byte) time.Time {
	if len(b) != 8 {
		return time.Time{}
	}
	nano := int64(binary.BigEndian.Uint64(b))
	return time.Unix(0, nano)
}
