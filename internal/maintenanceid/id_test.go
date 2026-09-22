package maintenanceid

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validID = "550e8400-e29b-41d4-a716-446655440000"

func TestIDConstructionAndRoundTrip(t *testing.T) {
	id, err := Parse(validID)
	require.NoError(t, err)
	require.True(t, id.Valid())
	assert.Equal(t, validID, id.String())

	text, err := id.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, validID, string(text))

	encoded, err := json.Marshal(id)
	require.NoError(t, err)
	assert.JSONEq(t, `"`+validID+`"`, string(encoded))

	var decoded ID
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, id, decoded)

	random, err := New()
	require.NoError(t, err)
	assert.True(t, random.Valid())
}

func TestParseRejectsNonCanonicalOrNonV4Values(t *testing.T) {
	for _, value := range []string{
		"", "550E8400-E29B-41D4-A716-446655440000",
		"550e8400e29b41d4a716446655440000",
		"00000000-0000-0000-0000-000000000000",
		"550e8400-e29b-11d4-a716-446655440000",
	} {
		t.Run(value, func(t *testing.T) {
			_, err := Parse(value)
			assert.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestInvalidDecodeDoesNotMutateReceiver(t *testing.T) {
	want, err := Parse(validID)
	require.NoError(t, err)
	id := want

	assert.ErrorIs(t, id.UnmarshalText([]byte("invalid")), ErrInvalid)
	assert.Equal(t, want, id)
	assert.ErrorIs(t, json.Unmarshal([]byte(`null`), &id), ErrInvalid)
	assert.Equal(t, want, id)
	_, err = (ID{}).MarshalJSON()
	assert.ErrorIs(t, err, ErrInvalid)
}
