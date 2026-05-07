// Package auth provides the canonical signed-message formats used by Fred's
// tenant authentication.
//
// Two formats exist:
//
//   - Tenant access tokens (most endpoints): "{tenant}:{lease_uuid}:{unix_timestamp}"
//   - Payload upload tokens (POST /data only):
//     "manifest lease data {lease_uuid} {meta_hash_hex} {unix_timestamp}"
//
// The payload variant binds the token to a specific deployment payload via
// meta_hash, preventing a stolen access token from being used to upload a
// different payload to the same lease.
//
// Both formats are then signed using ADR-036 (see internal/adr036) and
// wrapped in a base64-encoded JSON envelope; see internal/api/auth.go and
// internal/api/payload_auth.go for the envelope shapes.
package auth

import "fmt"

// FormatSignData formats the sign data for an auth token.
// This is the canonical format that must be signed by the tenant.
func FormatSignData(tenant, leaseUUID string, timestamp int64) []byte {
	data := fmt.Sprintf("%s:%s:%d", tenant, leaseUUID, timestamp)
	return []byte(data)
}

// FormatPayloadSignData formats the sign data for a payload upload auth token.
// This includes the meta_hash to bind the signature to a specific payload.
// Format: "manifest lease data {lease_uuid} {meta_hash_hex} {unix_timestamp}"
func FormatPayloadSignData(leaseUUID, metaHashHex string, timestamp int64) []byte {
	data := fmt.Sprintf("manifest lease data %s %s %d", leaseUUID, metaHashHex, timestamp)
	return []byte(data)
}
