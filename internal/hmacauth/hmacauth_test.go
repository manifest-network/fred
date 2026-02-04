package hmacauth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSignAndVerify(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`{"lease_uuid":"abc-123"}`)

	t.Run("valid signature", func(t *testing.T) {
		sig := Sign(secret, body)
		assert.NoError(t, Verify(secret, body, sig, 5*time.Minute))
	})

	t.Run("expired timestamp", func(t *testing.T) {
		sig := SignWithTime(secret, body, time.Now().Add(-10*time.Minute))
		assert.Error(t, Verify(secret, body, sig, 5*time.Minute))
	})

	t.Run("wrong secret", func(t *testing.T) {
		sig := Sign("wrong-secret-wrong-secret-wrong!", body)
		assert.Error(t, Verify(secret, body, sig, 5*time.Minute))
	})

	t.Run("tampered body", func(t *testing.T) {
		sig := Sign(secret, body)
		assert.Error(t, Verify(secret, []byte(`{"lease_uuid":"TAMPERED"}`), sig, 5*time.Minute))
	})

	t.Run("malformed signature missing sha256", func(t *testing.T) {
		sig := "t=1234567890"
		assert.Error(t, Verify(secret, body, sig, 5*time.Minute))
	})

	t.Run("malformed signature missing timestamp", func(t *testing.T) {
		assert.Error(t, Verify(secret, body, "sha256=abc123", 5*time.Minute))
	})
}

func TestParseSignature(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		ts, hex, ok := ParseSignature("t=1234567890,sha256=abcdef")
		assert.True(t, ok)
		assert.Equal(t, int64(1234567890), ts)
		assert.Equal(t, "abcdef", hex)
	})

	t.Run("with whitespace", func(t *testing.T) {
		ts, hex, ok := ParseSignature("t=1234567890, sha256=abcdef")
		assert.True(t, ok)
		assert.Equal(t, int64(1234567890), ts)
		assert.Equal(t, "abcdef", hex)
	})

	t.Run("no comma", func(t *testing.T) {
		_, _, ok := ParseSignature("t=1234567890sha256=abcdef")
		assert.False(t, ok)
	})

	t.Run("empty string", func(t *testing.T) {
		_, _, ok := ParseSignature("")
		assert.False(t, ok)
	})
}

func TestSignWithTime(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`hello`)
	ts := time.Unix(1700000000, 0)

	sig := SignWithTime(secret, body, ts)

	// Verify round-trips correctly
	parsedTs, _, ok := ParseSignature(sig)
	assert.True(t, ok)
	assert.Equal(t, int64(1700000000), parsedTs)
}
