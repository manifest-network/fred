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
