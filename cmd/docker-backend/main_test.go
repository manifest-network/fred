package main

import (
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestVerifySignature(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`{"lease_uuid":"abc-123"}`)

	t.Run("valid signature", func(t *testing.T) {
		sig := hmacauth.Sign(secret, body)
		if err := hmacauth.Verify(secret, body, sig, 5*time.Minute); err != nil {
			t.Errorf("expected valid signature, got error: %v", err)
		}
	})

	t.Run("expired timestamp", func(t *testing.T) {
		sig := hmacauth.SignWithTime(secret, body, time.Now().Add(-10*time.Minute))
		if err := hmacauth.Verify(secret, body, sig, 5*time.Minute); err == nil {
			t.Error("expected error for expired timestamp")
		}
	})

	t.Run("wrong secret", func(t *testing.T) {
		sig := hmacauth.Sign("wrong-secret-wrong-secret-wrong!", body)
		if err := hmacauth.Verify(secret, body, sig, 5*time.Minute); err == nil {
			t.Error("expected error for wrong secret")
		}
	})

	t.Run("tampered body", func(t *testing.T) {
		sig := hmacauth.Sign(secret, body)
		if err := hmacauth.Verify(secret, []byte(`{"lease_uuid":"TAMPERED"}`), sig, 5*time.Minute); err == nil {
			t.Error("expected error for tampered body")
		}
	})

	t.Run("malformed signature missing sha256", func(t *testing.T) {
		sig := "t=1234567890"
		if err := hmacauth.Verify(secret, body, sig, 5*time.Minute); err == nil {
			t.Error("expected error for malformed signature")
		}
	})

	t.Run("malformed signature missing timestamp", func(t *testing.T) {
		if err := hmacauth.Verify(secret, body, "sha256=abc123", 5*time.Minute); err == nil {
			t.Error("expected error for missing timestamp")
		}
	})
}
