package shared

import (
	"encoding/json"

	"github.com/manifest-network/fred/internal/strictjson"
)

const authoritativeRowSchemaVersion uint8 = 1

// These package-local names keep store code focused on its authority model
// while every current store and placement decoder shares one exact JSON
// implementation in internal/strictjson.
func decodeStrictAuthoritativeObject(value []byte, maxBytes int, destination any) error {
	return strictjson.DecodeObject(value, maxBytes, destination)
}

func decodeStrictAuthoritativeArray(value []byte, maxBytes int, destination any) error {
	return strictjson.DecodeArray(value, maxBytes, destination)
}

func decodeExactRawJSONObject(
	value []byte,
	allowed map[string]struct{},
) (map[string]json.RawMessage, error) {
	return strictjson.DecodeRawObject(value, maxAuthoritativeRecordBytes, allowed)
}

func validateUniqueJSONObject(value []byte, maxBytes int) error {
	return strictjson.ValidateUniqueObject(value, maxBytes)
}

func validateUniqueJSONArray(value []byte, maxBytes int) error {
	return strictjson.ValidateUniqueArray(value, maxBytes)
}
