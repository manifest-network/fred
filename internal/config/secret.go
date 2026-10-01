package config

import "log/slog"

// Secret is a string type that redacts its value in fmt, JSON, and slog output.
// Use string(s) to access the raw value when needed (e.g., for HMAC computation).
type Secret string

func (Secret) String() string               { return "[REDACTED]" }
func (Secret) GoString() string             { return "[REDACTED]" }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }
func (Secret) LogValue() slog.Value         { return slog.StringValue("[REDACTED]") }
func (Secret) MarshalText() ([]byte, error) { return []byte("[REDACTED]"), nil }

// RotationSecret is a verify-only HMAC key held during a key rotation. It is a
// distinct type from Secret, so it cannot be passed where a signing key is
// expected, and it redacts like Secret. hmacauth.NewVerifyKeys is its only
// consumer; rotation_key_invariant_test.go keeps every read of it there.
type RotationSecret string

func (RotationSecret) String() string               { return "[REDACTED]" }
func (RotationSecret) GoString() string             { return "[REDACTED]" }
func (RotationSecret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }
func (RotationSecret) LogValue() slog.Value         { return slog.StringValue("[REDACTED]") }
func (RotationSecret) MarshalText() ([]byte, error) { return []byte("[REDACTED]"), nil }
