package backend

import "github.com/google/uuid"

// IsCanonicalLeaseUUID reports whether value is the lowercase, hyphenated,
// non-nil UUID form used as the durable identity of a lease. Backend HTTP
// boundaries use the exact same predicate as the v2 callback outbox so an
// accepted mutating request cannot create an effect whose callback is later
// rejected by persistence.
func IsCanonicalLeaseUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
