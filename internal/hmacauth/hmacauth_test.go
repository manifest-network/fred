package hmacauth

import (
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`{"lease_uuid":"abc-123"}`)

	t.Run("valid signature", func(t *testing.T) {
		sig := Sign(secret, body)
		if err := Verify(secret, body, sig, 5*time.Minute); err != nil {
			t.Errorf("expected valid signature, got error: %v", err)
		}
	})

	t.Run("expired timestamp", func(t *testing.T) {
		sig := SignWithTime(secret, body, time.Now().Add(-10*time.Minute))
		if err := Verify(secret, body, sig, 5*time.Minute); err == nil {
			t.Error("expected error for expired timestamp")
		}
	})

	t.Run("wrong secret", func(t *testing.T) {
		sig := Sign("wrong-secret-wrong-secret-wrong!", body)
		if err := Verify(secret, body, sig, 5*time.Minute); err == nil {
			t.Error("expected error for wrong secret")
		}
	})

	t.Run("tampered body", func(t *testing.T) {
		sig := Sign(secret, body)
		if err := Verify(secret, []byte(`{"lease_uuid":"TAMPERED"}`), sig, 5*time.Minute); err == nil {
			t.Error("expected error for tampered body")
		}
	})

	t.Run("malformed signature missing sha256", func(t *testing.T) {
		sig := "t=1234567890"
		if err := Verify(secret, body, sig, 5*time.Minute); err == nil {
			t.Error("expected error for malformed signature")
		}
	})

	t.Run("malformed signature missing timestamp", func(t *testing.T) {
		if err := Verify(secret, body, "sha256=abc123", 5*time.Minute); err == nil {
			t.Error("expected error for missing timestamp")
		}
	})
}

func TestParseSignature(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		ts, hex, ok := ParseSignature("t=1234567890,sha256=abcdef")
		if !ok || ts != 1234567890 || hex != "abcdef" {
			t.Errorf("unexpected result: ts=%d, hex=%s, ok=%v", ts, hex, ok)
		}
	})

	t.Run("with whitespace", func(t *testing.T) {
		ts, hex, ok := ParseSignature("t=1234567890, sha256=abcdef")
		if !ok || ts != 1234567890 || hex != "abcdef" {
			t.Errorf("unexpected result: ts=%d, hex=%s, ok=%v", ts, hex, ok)
		}
	})

	t.Run("no comma", func(t *testing.T) {
		_, _, ok := ParseSignature("t=1234567890sha256=abcdef")
		if ok {
			t.Error("expected parse failure for missing comma")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		_, _, ok := ParseSignature("")
		if ok {
			t.Error("expected parse failure for empty string")
		}
	})
}

func TestSignWithTime(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`hello`)
	ts := time.Unix(1700000000, 0)

	sig := SignWithTime(secret, body, ts)

	// Verify round-trips correctly
	parsedTs, _, ok := ParseSignature(sig)
	if !ok {
		t.Fatal("failed to parse generated signature")
	}
	if parsedTs != 1700000000 {
		t.Errorf("expected timestamp 1700000000, got %d", parsedTs)
	}
}
