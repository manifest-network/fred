package shared

import (
	"github.com/manifest-network/fred/internal/strictjson"
)

const authoritativeRowSchemaVersion uint8 = 1

// These package-local names keep store code focused on its authority model
// while every current store and placement decoder shares one exact JSON
// implementation in internal/strictjson.
func decodeStrictAuthoritativeObject(value []byte, maxBytes int, destination any) error {
	return strictjson.DecodeObject(value, maxBytes, destination)
}
