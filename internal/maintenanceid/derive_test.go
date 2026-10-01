package maintenanceid

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeriveShapesACanonicalUUIDv4(t *testing.T) {
	for _, seed := range []string{"a", "b", "\\xff"} {
		id := Derive(sha256.Sum256([]byte(seed)))
		require.True(t, id.Valid(), seed)
		parsed, err := Parse(id.String())
		require.NoError(t, err, seed)
		assert.Equal(t, id, parsed, seed)
		assert.Equal(t, id, Derive(sha256.Sum256([]byte(seed))), "derivation is deterministic")
	}
	assert.NotEqual(t, Derive(sha256.Sum256([]byte("a"))), Derive(sha256.Sum256([]byte("b"))))
	// Every version and variant bit is forced, whatever the digest holds.
	var ones [sha256.Size]byte
	for index := range ones {
		ones[index] = 0xff
	}
	assert.True(t, Derive(ones).Valid())
	assert.True(t, Derive([sha256.Size]byte{}).Valid())
}
