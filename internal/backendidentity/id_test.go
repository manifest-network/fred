package backendidentity

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const canonicalTestID = "550e8400-e29b-41d4-a716-446655440000"

func TestParse(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	assert.True(t, id.Valid())
	assert.Equal(t, canonicalTestID, id.String())

	encoded, err := id.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, canonicalTestID, string(encoded))
}

func TestParseRejectsInvalidRepresentations(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "malformed", value: "not-a-uuid"},
		{name: "nil UUID", value: "00000000-0000-0000-0000-000000000000"},
		{name: "uppercase", value: "550E8400-E29B-41D4-A716-446655440000"},
		{name: "braces", value: "{550e8400-e29b-41d4-a716-446655440000}"},
		{name: "URN", value: "urn:uuid:550e8400-e29b-41d4-a716-446655440000"},
		{name: "no hyphens", value: "550e8400e29b41d4a716446655440000"},
		{name: "UUIDv1", value: "550e8400-e29b-11d4-a716-446655440000"},
		{name: "UUIDv7", value: "018f5e0b-7b16-7cc7-98c4-dc0c0c07398f"},
		{name: "non-RFC variant", value: "550e8400-e29b-41d4-c716-446655440000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			id, err := Parse(test.value)
			assert.ErrorIs(t, err, ErrInvalidID)
			assert.False(t, id.Valid())
			assert.Equal(t, ID{}, id)
		})
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	seen := make(map[ID]struct{})
	for range 64 {
		id, err := New()
		require.NoError(t, err)
		require.True(t, id.Valid())
		assert.NotEqual(t, "invalid", id.String())
		_, duplicate := seen[id]
		assert.False(t, duplicate)
		seen[id] = struct{}{}

		parsed, err := Parse(id.String())
		require.NoError(t, err)
		assert.Equal(t, id, parsed)
	}
}

func TestZeroIDIsInvalid(t *testing.T) {
	t.Parallel()

	var id ID
	assert.False(t, id.Valid())
	assert.Equal(t, "invalid", id.String())
	encoded, err := id.MarshalText()
	assert.ErrorIs(t, err, ErrInvalidID)
	assert.Nil(t, encoded)
}

func TestIDJSONUsesCanonicalText(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)

	encoded, err := json.Marshal(id)
	require.NoError(t, err)
	assert.JSONEq(t, `"550e8400-e29b-41d4-a716-446655440000"`, string(encoded))

	_, err = json.Marshal(ID{})
	assert.True(t, errors.Is(err, ErrInvalidID))
}

func TestWireNames(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "backend_storage_id", QueryParameter)
	assert.Equal(t, "X-Fred-Backend-Storage-ID", ResponseHeader)
	assert.Equal(t, "/_fred/storage/", BoundPathPrefix)
}

func TestBoundPath(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)

	bound, err := BoundPath(id, "/provision")
	require.NoError(t, err)
	assert.Equal(t,
		"/_fred/storage/550e8400-e29b-41d4-a716-446655440000/provision",
		bound,
	)
}

func TestBoundPathRejectsUnsafeInput(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)

	for _, legacyPath := range []string{
		"",
		"/",
		"provision",
		"//provision",
		"/safe//provision",
		"/../provision",
		"/safe/../../provision",
		"/.../provision",
		"/./provision",
		"/safe/./provision",
		"/%2e%2e/provision",
		"/%2E%2E/provision",
		"/%2fprovision",
		"/pro%76ision",
		"/provision?mode=unsafe",
		"/provision#fragment",
		"/provision%3fmode=unsafe",
		"/provision%23fragment",
		"/provision\\nested",
		"/provision\n",
		"/provision/",
		"/café",
		"/zero\u200bwidth",
		"/_fred/storage/already-bound/provision",
		string([]byte{'/', 'x', 0xff}),
		"/bad%escape",
	} {
		t.Run(legacyPath, func(t *testing.T) {
			t.Parallel()

			bound, err := BoundPath(id, legacyPath)
			assert.ErrorIs(t, err, ErrInvalidLegacyPath)
			assert.Empty(t, bound)
		})
	}

	bound, err := BoundPath(ID{}, "/provision")
	assert.ErrorIs(t, err, ErrInvalidID)
	assert.Empty(t, bound)
}
