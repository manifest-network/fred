package callbackid

import (
	"errors"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUUIDv4CanonicalWireContract(t *testing.T) {
	invalid := errors.New("invalid test ID")
	const canonical = "550e8400-e29b-41d4-a716-446655440000"

	id, err := Parse(canonical, invalid)
	require.NoError(t, err)
	assert.True(t, id.Valid())
	assert.Equal(t, canonical, id.String())
	text, err := id.MarshalText(invalid)
	require.NoError(t, err)
	assert.Equal(t, canonical, string(text))

	for _, value := range []string{
		"",
		"550E8400-E29B-41D4-A716-446655440000",
		uuid.Nil.String(),
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
	} {
		_, err := Parse(value, invalid)
		require.ErrorIs(t, err, invalid, value)
	}

	zero := UUIDv4{}
	assert.False(t, zero.Valid())
	assert.Equal(t, "invalid", zero.String())
	_, err = zero.MarshalText(invalid)
	require.ErrorIs(t, err, invalid)
	assert.False(t, FromUUID(uuid.Nil).Valid())
}

func TestQueryHelpersPreserveTypedExactlyOnceSemantics(t *testing.T) {
	invalid := errors.New("invalid test ID")
	ambiguous := errors.New("ambiguous test ID")
	nilQuery := errors.New("nil test query")
	const parameter = "typed_id"
	const canonical = "550e8400-e29b-41d4-a716-446655440000"

	parse := func(value string) (UUIDv4, error) { return Parse(value, invalid) }
	id, present, err := ParseQuery(url.Values{}, parameter, ambiguous, parse)
	require.NoError(t, err)
	assert.False(t, present)
	assert.False(t, id.Valid())

	id, present, err = ParseQuery(
		url.Values{parameter: {canonical}}, parameter, ambiguous, parse,
	)
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, canonical, id.String())

	_, present, err = ParseQuery(
		url.Values{parameter: {canonical, canonical}}, parameter, ambiguous, parse,
	)
	assert.True(t, present)
	require.ErrorIs(t, err, ambiguous)

	values := url.Values{"trace": {"keep"}}
	require.NoError(t, SetQuery(values, parameter, nilQuery, func() ([]byte, error) {
		return []byte(canonical), nil
	}))
	assert.Equal(t, canonical, values.Get(parameter))
	assert.Equal(t, "keep", values.Get("trace"))
	require.ErrorIs(t, SetQuery(nil, parameter, nilQuery, func() ([]byte, error) {
		return []byte(canonical), nil
	}), nilQuery)
}
