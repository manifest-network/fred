// Package auth provides authentication-related utilities.
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
