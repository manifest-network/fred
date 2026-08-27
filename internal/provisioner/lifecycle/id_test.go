package lifecycle

import (
	"encoding"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const canonicalTestID = "123e4567-e89b-42d3-a456-426614174000"

func mustTestID(t testing.TB, text string) ID {
	t.Helper()
	id, err := ParseID(text)
	require.NoError(t, err)
	return id
}

func TestIDZeroValueIsInvalid(t *testing.T) {
	var id ID

	assert.False(t, id.Valid())
	assert.Equal(t, "invalid", id.String())
	text, err := id.MarshalText()
	assert.ErrorIs(t, err, ErrInvalidID)
	assert.Nil(t, text)
	assert.Equal(t, ID{}, newID(uuid.Nil))
}

func TestParseIDAcceptsOnlyCanonicalUUIDv4(t *testing.T) {
	id, err := ParseID(canonicalTestID)
	require.NoError(t, err)
	require.True(t, id.Valid())
	assert.Equal(t, canonicalTestID, id.String())

	text, err := id.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, canonicalTestID, string(text))
}

func TestParseIDRejectsNonCanonicalOrNonV4Values(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{name: "empty", wire: ""},
		{name: "nil UUID", wire: uuid.Nil.String()},
		{name: "uppercase", wire: strings.ToUpper(canonicalTestID)},
		{name: "compact", wire: strings.ReplaceAll(canonicalTestID, "-", "")},
		{name: "braced", wire: "{" + canonicalTestID + "}"},
		{name: "URN", wire: "urn:uuid:" + canonicalTestID},
		{name: "version one", wire: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{name: "version five", wire: "74738ff5-5367-5958-9aee-98fffdcd1876"},
		{name: "non-RFC variant", wire: "123e4567-e89b-42d3-2456-426614174000"},
		{name: "malformed", wire: "not-a-uuid"},
		{name: "leading whitespace", wire: " " + canonicalTestID},
		{name: "trailing whitespace", wire: canonicalTestID + " "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id, err := ParseID(test.wire)
			assert.ErrorIs(t, err, ErrInvalidID)
			assert.False(t, id.Valid())
		})
	}
}

func TestFromOperationIDPreservesCanonicalIdentity(t *testing.T) {
	operationID, err := operation.ParseID(canonicalTestID)
	require.NoError(t, err)

	id, err := FromOperationID(operationID)
	require.NoError(t, err)
	assert.True(t, id.Valid())
	assert.Equal(t, operationID.String(), id.String())
}

func TestFromOperationIDRejectsInvalidSource(t *testing.T) {
	id, err := FromOperationID(operation.OperationID{})
	assert.ErrorIs(t, err, ErrInvalidID)
	assert.False(t, id.Valid())
}

func TestIDImplementsTextMarshaler(t *testing.T) {
	var marshaler encoding.TextMarshaler = mustTestID(t, canonicalTestID)
	text, err := marshaler.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, canonicalTestID, string(text))
}

func TestParseQuery(t *testing.T) {
	t.Run("absent supports legacy callback", func(t *testing.T) {
		id, present, err := ParseQuery(url.Values{"unrelated": {"value"}})
		require.NoError(t, err)
		assert.False(t, present)
		assert.False(t, id.Valid())
	})

	t.Run("present and valid", func(t *testing.T) {
		id, present, err := ParseQuery(url.Values{QueryParameter: {canonicalTestID}})
		require.NoError(t, err)
		require.True(t, present)
		assert.Equal(t, canonicalTestID, id.String())
	})

	invalid := []struct {
		name   string
		values []string
		want   error
	}{
		{name: "empty value", values: []string{""}, want: ErrInvalidID},
		{name: "malformed", values: []string{"abc"}, want: ErrInvalidID},
		{name: "non-v4", values: []string{"6ba7b810-9dad-11d1-80b4-00c04fd430c8"}, want: ErrInvalidID},
		{name: "no values", values: nil, want: ErrAmbiguousQuery},
		{name: "duplicate different values", values: []string{canonicalTestID, "d9428888-122b-41e1-b85c-61c67afba0c6"}, want: ErrAmbiguousQuery},
		{name: "duplicate identical values", values: []string{canonicalTestID, canonicalTestID}, want: ErrAmbiguousQuery},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			id, present, err := ParseQuery(url.Values{QueryParameter: test.values})
			assert.ErrorIs(t, err, test.want)
			assert.True(t, present)
			assert.False(t, id.Valid())
		})
	}
}

func TestSetQuery(t *testing.T) {
	t.Run("writes canonical value and preserves unrelated values", func(t *testing.T) {
		values := url.Values{
			QueryParameter: {"stale", "duplicate"},
			"unrelated":    {"value"},
		}
		err := SetQuery(values, mustTestID(t, canonicalTestID))
		require.NoError(t, err)
		assert.Equal(t, []string{canonicalTestID}, values[QueryParameter])
		assert.Equal(t, []string{"value"}, values["unrelated"])
	})

	t.Run("invalid ID does not mutate destination", func(t *testing.T) {
		values := url.Values{QueryParameter: {"existing"}}
		before := values.Encode()
		err := SetQuery(values, ID{})
		assert.ErrorIs(t, err, ErrInvalidID)
		assert.Equal(t, before, values.Encode())
	})

	t.Run("nil destination returns error", func(t *testing.T) {
		err := SetQuery(nil, mustTestID(t, canonicalTestID))
		assert.ErrorIs(t, err, ErrNilQuery)
	})
}

func TestIDComparable(t *testing.T) {
	one := mustTestID(t, canonicalTestID)
	alsoOne, err := ParseID(canonicalTestID)
	require.NoError(t, err)
	two := mustTestID(t, "d9428888-122b-41e1-b85c-61c67afba0c6")

	set := map[ID]struct{}{one: {}}
	_, hasOne := set[alsoOne]
	_, hasTwo := set[two]
	assert.True(t, hasOne)
	assert.False(t, hasTwo)
}

func FuzzIDTextRoundTrip(f *testing.F) {
	for _, seed := range []string{
		canonicalTestID,
		"d9428888-122b-41e1-b85c-61c67afba0c6",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"not-a-uuid",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, text string) {
		id, err := ParseID(text)
		if err != nil {
			return
		}
		encoded, err := id.MarshalText()
		require.NoError(t, err)
		assert.Equal(t, text, string(encoded))
	})
}

func TestIDErrorsAreDistinct(t *testing.T) {
	assert.False(t, errors.Is(ErrInvalidID, ErrAmbiguousQuery))
	assert.False(t, errors.Is(ErrInvalidID, ErrNilQuery))
}
