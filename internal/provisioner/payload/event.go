package payload

// Event represents a payload upload event for Watermill.
type Event struct {
	LeaseUUID   string `json:"lease_uuid"`
	Tenant      string `json:"tenant"`
	MetaHashHex string `json:"meta_hash_hex"` // Hex-encoded SHA-256
}
