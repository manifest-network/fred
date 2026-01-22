// Package auth provides authentication-related utilities.
package auth

import "fmt"

// FormatSignData formats the sign data for an auth token.
// This is the canonical format that must be signed by the tenant.
func FormatSignData(tenant, leaseUUID string, timestamp int64) []byte {
	data := fmt.Sprintf("%s:%s:%d", tenant, leaseUUID, timestamp)
	return []byte(data)
}
